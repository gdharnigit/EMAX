import requests
import pandas as pd
import time
import os
from datetime import datetime

def get_alpha_vantage_data(symbol, api_key):
    """Get stock data from Alpha Vantage"""
    url = f'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'apikey': api_key,
        'outputsize': 'full'
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if 'Error Message' in data:
        raise Exception(f"Error: {data['Error Message']}")
    if 'Note' in data:
        raise Exception(f"Rate limit exceeded: {data['Note']}")
    
    # Convert to DataFrame
    time_series = data['Time Series (Daily)']
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    
    # Rename columns to match yfinance format
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = df.astype(float)
    
    # Get last 1 year of data
    one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
    df = df[df.index >= one_year_ago]
    
    return df

def calculate_ema(df, period, column='Close'):
    """Calculate Exponential Moving Average (EMA) for a given period."""
    return df[column].ewm(span=period, adjust=False).mean()

def find_crossover(df, ema_a_col, ema_b_col):
    """Find the crossover points where EMA A crosses EMA B."""
    df['EMA_diff'] = df[ema_a_col] - df[ema_b_col]
    
    # Calculate the shifted difference and handle NaN values
    shifted_diff = df['EMA_diff'].shift(1)
    current_diff = df['EMA_diff']
    
    # Detect crossovers (where sign changes between consecutive points)
    df['Crossover'] = ((shifted_diff * current_diff) < 0).fillna(False)
    
    # Label crossover as bullish if EMA_A > EMA_B after crossover, else bearish
    df['Crossover_Type'] = df.apply(
        lambda row: 'Bullish' if row[ema_a_col] > row[ema_b_col] else 'Bearish', 
        axis=1
    )
    return df

def get_last_crossover(df, x_days):
    """Check if a crossover occurred in the last x_days."""
    today = df.index[-1]
    recent_date_threshold = today - pd.Timedelta(days=x_days)
    
    recent_crossover = df[(df['Crossover'] == True) & (df.index >= recent_date_threshold)]
    
    if not recent_crossover.empty:
        return recent_crossover.iloc[-1]
    return None

def calculate_distance_from_crossover(crossover_level, current_price):
    """Calculate percentage distance from crossover level to current price."""
    return ((current_price - crossover_level) / crossover_level) * 100

def filter_stocks_alpha_vantage(stock_symbols, ema_a, ema_b, x_days, api_key):
    """Filter stocks using Alpha Vantage data"""
    results = []
    
    for i, symbol in enumerate(stock_symbols):
        try:
            print(f"\nProcessing {symbol} ({i+1}/{len(stock_symbols)})...")
            
            # Get data from Alpha Vantage
            stock = get_alpha_vantage_data(symbol, api_key)
            
            # Rate limiting for Alpha Vantage (5 requests per minute)
            if i < len(stock_symbols) - 1:
                print("Waiting 15 seconds to respect rate limits...")
                time.sleep(15)
            
            # Skip if not enough data
            if len(stock) < max(ema_a, ema_b) + 1:
                print(f"Not enough data for {symbol}")
                continue
            
            stock['EMA_A'] = calculate_ema(stock, ema_a)
            stock['EMA_B'] = calculate_ema(stock, ema_b)
            
            # Drop rows with NaN values in EMAs
            stock = stock.dropna(subset=['EMA_A', 'EMA_B'])
            
            if len(stock) < 2:
                print(f"Not enough valid data after EMA calculation for {symbol}")
                continue
            
            stock = find_crossover(stock, 'EMA_A', 'EMA_B')
            recent_crossover = get_last_crossover(stock, x_days)
            
            if recent_crossover is not None:
                crossover_level = (stock.loc[recent_crossover.name, 'EMA_A'] + 
                                 stock.loc[recent_crossover.name, 'EMA_B']) / 2
                current_price = stock['Close'].iloc[-1]
                distance = calculate_distance_from_crossover(crossover_level, current_price)
                
                results.append({
                    'Symbol': symbol,
                    'Crossover Date': recent_crossover.name.strftime('%Y-%m-%d'),
                    'Crossover Type': recent_crossover['Crossover_Type'],
                    'Current Price': current_price,
                    'Percentage Distance (%)': distance
                })
                print(f"✓ Found crossover for {symbol}")
            else:
                print(f"No recent crossover found for {symbol}")
                
        except Exception as e:
            print(f"Error for symbol {symbol}: {e}")
    
    return pd.DataFrame(results)

if __name__ == "__main__":
    # Get API key from environment variable (for GitHub Actions)
    API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not API_KEY:
        print("Error: ALPHA_VANTAGE_API_KEY environment variable not set")
        print("Please set up your Alpha Vantage API key")
        exit(1)
    
    # Stock symbols to analyze
    symbols = ['AAPL', 'MSFT', 'TSLA', 'GOOG', 'AMZN']
    
    print("Starting daily stock analysis...")
    print(f"Analyzing symbols: {symbols}")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run the analysis
    filtered_stocks = filter_stocks_alpha_vantage(
        symbols, 
        ema_a=21, 
        ema_b=55, 
        x_days=70, 
        api_key=API_KEY
    )
    
    print("\n" + "="*50)
    print("ANALYSIS RESULTS")
    print("="*50)
    
    if not filtered_stocks.empty:
        print(filtered_stocks.to_string(index=False))
        
        # Save results to CSV file
        filename = f"results.csv"
        filtered_stocks.to_csv(filename, index=False)
        print(f"\nResults saved to {filename}")
        
        # Also save with date for history
        dated_filename = f"stock_analysis_{datetime.now().strftime('%Y-%m-%d')}.csv"
        filtered_stocks.to_csv(dated_filename, index=False)
        print(f"Results also saved to {dated_filename}")
    else:
        print("No stocks found with EMA crossovers in the specified timeframe.")
        
        # Create empty results file so GitHub Actions doesn't fail
        empty_df = pd.DataFrame(columns=['Symbol', 'Crossover Type', 'Current Price', 'Percentage Distance (%)'])
        empty_df.to_csv("results.csv", index=False)
    
    print("\nAnalysis complete!")
