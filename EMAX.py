import os
import requests
import pandas as pd
import time


def get_twelvedata_data(symbol, api_key):
    """Fetch daily OHLC data for a symbol from TwelveData API."""
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "apikey": api_key,
        "format": "JSON",
        "outputsize": 5000,  # get max data points possible
    }

    response = requests.get(url, params=params)
    data = response.json()

    if "status" in data and data["status"] == "error":
        raise Exception(f"TwelveData API error for {symbol}: {data.get('message', '')}")

    if "values" not in data:
        raise Exception(f"No data found for symbol {symbol}")

    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index("datetime", inplace=True)
    df = df.sort_index()

    # Rename columns to standard OHLCV with proper types
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    })
    df = df.astype({
        "Open": float,
        "High": float,
        "Low": float,
        "Close": float,
        "Volume": float
    })

    # Filter last 1 year
    one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
    df = df[df.index >= one_year_ago]

    return df


def calculate_ema(df, period, column="Close"):
    """Calculate Exponential Moving Average (EMA) for a given period."""
    return df[column].ewm(span=period, adjust=False).mean()


def find_crossover(df, ema_a_col, ema_b_col):
    """Find crossover points where EMA A crosses EMA B."""
    df["EMA_diff"] = df[ema_a_col] - df[ema_b_col]

    shifted_diff = df["EMA_diff"].shift(1)
    current_diff = df["EMA_diff"]

    df["Crossover"] = ((shifted_diff * current_diff) < 0).fillna(False)

    df["Crossover_Type"] = df.apply(
        lambda row: "Bullish" if row[ema_a_col] > row[ema_b_col] else "Bearish",
        axis=1,
    )
    return df


def get_last_crossover(df, x_days):
    """Get the last crossover event within x_days."""
    today = df.index[-1]
    threshold = today - pd.Timedelta(days=x_days)

    recent = df[(df["Crossover"] == True) & (df.index >= threshold)]

    if not recent.empty:
        return recent.iloc[-1]
    return None


def calculate_distance_from_crossover(crossover_level, current_price):
    """Calculate % distance from crossover price."""
    return ((current_price - crossover_level) / crossover_level) * 100


def filter_stocks_twelvedata(stock_symbols, ema_a, ema_b, x_days, api_key):
    """Filter stocks using TwelveData API."""
    results = []

    for i, symbol in enumerate(stock_symbols):
        try:
            print(f"\nProcessing {symbol} ({i + 1}/{len(stock_symbols)})...")

            stock = get_twelvedata_data(symbol, api_key)

            # To avoid hitting rate limits (8 req/sec free tier), add small delay if needed
            if i < len(stock_symbols) - 1:
                time.sleep(0.2)

            if len(stock) < max(ema_a, ema_b) + 1:
                print(f"Not enough data for {symbol}")
                continue

            stock["EMA_A"] = calculate_ema(stock, ema_a)
            stock["EMA_B"] = calculate_ema(stock, ema_b)

            stock = stock.dropna(subset=["EMA_A", "EMA_B"])

            if len(stock) < 2:
                print(f"Not enough valid data after EMA calculation for {symbol}")
                continue

            stock = find_crossover(stock, "EMA_A", "EMA_B")
            recent_crossover = get_last_crossover(stock, x_days)

            if recent_crossover is not None:
                crossover_level = (recent_crossover["EMA_A"] + recent_crossover["EMA_B"]) / 2
                current_price = stock["Close"].iloc[-1]
                distance = calculate_distance_from_crossover(crossover_level, current_price)

                results.append({
                    "Symbol": symbol,
                    "Crossover Date": recent_crossover.name.strftime("%Y-%m-%d"),
                    "Crossover Type": recent_crossover["Crossover_Type"],
                    "Current Price": current_price,
                    "Percentage Distance (%)": distance,
                })
                print(f"✓ Found crossover for {symbol}")
            else:
                print(f"No recent crossover found for {symbol}")

        except Exception as e:
            print(f"Error for {symbol}: {e}")

    df_results = pd.DataFrame(results)
    return df_results


if __name__ == "__main__":
    # Read API key from environment variable
    API_KEY_TWELVE = os.getenv("TWELVEDATA_API_KEY")
    if not API_KEY_TWELVE:
        raise ValueError(
            "Please set the TWELVEDATA_API_KEY environment variable (e.g. export TWELVEDATA_API_KEY=your_key)"
        )

    # Example stock list (replace with your own symbols)
    symbols = ["AAPL", "MSFT", "TSLA", "GOOG", "AMZN"]

    filtered = filter_stocks_twelvedata(
        stock_symbols=symbols,
        ema_a=21,
        ema_b=55,
        x_days=30,
        api_key=API_KEY_TWELVE,
    )

    print("\nFiltered stocks with recent EMA crossovers:")
    print(filtered)

if not filtered.empty:
    filtered.to_csv("stock_analysis_results.csv", index=False)
    print("\nResults saved to stock_analysis_results.csv")
else:
    print("\nNo results to save.")
