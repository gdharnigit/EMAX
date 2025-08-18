import os
import requests
import pandas as pd
import time
from datetime import datetime

# === CONFIGURATION ===
API_KEY_TWELVE = os.environ["TWELVEDATA_API_KEY"]

#SYMBOLS = ["AAPL", "MSFT","VSCO"]  # Example list
SYMBOLSMID = ['SMCI', 'DECK', 'RS', 'CSL', 'GDDY', 'MANH', 'GGG', 'SAIA', 'VST', 'LII', 'OC', 'WSO', 'WSM', 'RPM', 'XPO', 'WPC', 'DT', 'NBIX', 'LECO', 'BURL', 'PSTG', 'BLD', 'ELS', 'ACM', 'GLPI', 'RNR', 'EME', 'FND', 'PFGC', 'REXR', 'RGA', 'WMS', 'TOL', 'NVT', 'USFD', 'CASY', 'OVV', 'RGEN', 'PEN', 'FIVE', 'X', 'IBKR', 'FBIN', 'TTC', 'UTHR', 'CUBE', 'CG', 'ITT', 'DKS', 'ALLY', 'RRX', 'TREX', 'SCI', 'CLF', 'EWBC', 'KNX', 'ELF', 'NLY', 'LSCC', 'TTEK', 'LAMR', 'DINO', 'TXRH', 'THC', 'KNSL', 'CLH', 'CCK', 'TPX', 'CHE', 'BJ', 'WTRG', 'WEX', 'CELH', 'SWAV', 'ERIE', 'ATR', 'UNM', 'CW', 'WING', 'CHK', 'EGP', 'JLL', 'EXP', 'FIX', 'MTN', 'DBX', 'WCC', 'CHDN', 'WWD', 'PRI', 'CIEN', 'ALV', 'COHR', 'GNTX', 'SSD', 'ONTO', 'DCI', 'LAD', 'CACI', 'OLED', 'AFG', 'WBS', 'MIDD', 'MUSA', 'LEA', 'SKX', 'BRBR', 'SF', 'JAZZ', 'FHN', 'LNW', 'ARMK', 'PVH', 'BWXT', 'GPK', 'RBC', 'AYI', 'PCTY', 'MEDP', 'OSK', 'ACHC', 'RGLD', 'EHC', 'OHI', 'AGCO', 'NNN', 'UFPI', 'KBR', 'VOYA', 'CNM', 'MORN', 'ORI', 'MKSI', 'NYT', 'SWN', 'INGR', 'WFRD', 'FR', 'FCN', 'SAIC', 'BRX', 'RRC', 'LSTR', 'AXTA', 'FLR', 'STAG', 'NOV', 'JEF', 'GXO', 'HQY', 'EQH', 'MAT', 'SEIC', 'BERY', 'FNF', 'BRKR', 'HRB', 'DAR', 'FNF', 'WH', 'CFR', 'OGE', 'HLI', 'CHRD', 'MSA', 'CROX', 'CMC', 'EXEL', 'OLN', 'QLYS', 'THO', 'PNFP', 'CGNX', 'CR', 'ARW', 'MTDR', 'AR', 'EVR', 'FAF', 'SSB', 'MASI', 'GMED', 'RMBS', 'CBSH', 'LFUS', 'HXL', 'PLNT', 'HR', 'SIGI', 'NOVT', 'WTS', 'G', 'BC', 'ADC', 'PB', 'SNX', 'H', 'TMHC', 'PBF', 'OPCH', 'STWD', 'WTFC', 'CHX', 'ESNT', 'VNT', 'PR', 'MUR', 'SON', 'CPRI', 'RLI', 'VVV', 'FLS', 'POST', 'AZPN', 'CIVI', 'M', 'MTG', 'TKR', 'AMG', 'NXST', 'R', 'SNV', 'TKO', 'SFM', 'FYBR', 'EXLS', 'ESAB', 'CRUS', 'DTM', 'KEX', 'GTLS', 'ST', 'AA', 'VMI', 'UGI', 'KD', 'PII', 'DLB', 'MMS', 'TDC', 'KRG', 'AIRC', 'ALTM', 'THG', 'ASH', 'UBSI', 'OLLI', 'HALO', 'FNB', 'ONB', 'KBH', 'WLK', 'MSM', 'BYD', 'ASGN', 'FLO', 'RYN', 'NFG', 'ZI', 'IDA', 'SRCL', 'GATX', 'MTSI', 'CC', 'POWI', 'OZK', 'COTY', 'LPX', 'ETRN', 'EEFT', 'HOG', 'CBT', 'SLM', 'HOMB', 'SLAB', 'TEX', 'FCFS', 'SYNA', 'PRGO', 'GPS', 'IRDM', 'VNO', 'MTZ', 'COKE', 'CVLT', 'GBCI', 'VAL', 'NEU', 'AM', 'MDU', 'AVT', 'NJR', 'CADE', 'POR', 'FFIN', 'EQH', 'JHG', 'EXPO', 'CNXC', 'RH', 'HAE', 'YETI', 'COLB', 'LOPE', 'GT', 'GME', 'KNF', 'AZTA', 'NVST', 'LANC', 'AN', 'LNTH', 'ARWR', 'WU', 'KRC', 'ENS', 'BCO', 'HWC', 'SLGN', 'MAN', 'WOLF', 'PCH', 'VLY', 'SWX', 'BDC', 'UMBF', 'KMPR', 'BKH', 'CUZ', 'BLKB', 'ORA', 'NSP', 'PENN', 'BHF', 'AVNT', 'PGNY', 'RUN', 'IRT', 'OGS', 'DOCS', 'VC', 'ALE', 'QDEL', 'SAM', 'EPR', 'CNX', 'PK', 'NEOG', 'WEN', 'PNM', 'ADNT', 'AMKR', 'LEG', 'SBRA', 'SR', 'CAR', 'AMED', 'ZD', 'CHH', 'LITE', 'NYCB', 'FHI', 'ENOV', 'CNO', 'HGV', 'IPGP', 'CRI', 'ASB', 'NARI', 'TNL', 'PAG', 'NWE', 'CXT', 'NSA', 'TCBI', 'IART', 'DOC', 'IBOC', 'VAC', 'TGNA', 'VSH', 'HELE', 'LIVN', 'FOXF', 'COLM', 'WERN', 'CDP', 'GHC', 'GO', 'SMG', 'MP', 'CABO', 'ALGM', 'RCM', 'CALX', 'MPW', 'JWN', 'GEF', 'SHC', 'UAA', 'UA', 'HTZ', 'PPC', 'CADE', 'WU', 'SGAFT', 'USD', 'JPMSW', 'HSBBK', 'GSISW']
SYMBOLSLGE = ["AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","BRKB","TSLA","UNH","LLY","JPM","XOM","JNJ","V","PG","AVGO","MA","HD","CVX","MRK","ABBV","PEP","COST","ADBE","KO","CSCO","WMT","TMO","MCD","PFE","CRM","BAC","ACN","CMCSA","LIN","NFLX","ABT","ORCL","DHR","AMD","WFC","DIS","TXN","PM","VZ","INTU","COP","CAT","AMGN","NEE","INTC","UNP","LOW","IBM","BMY","SPGI","RTX","HON","BA","UPS","GE","QCOM","AMAT","NKE","PLD","MS","GS","NOW","ISRG","ELV","SCHW","BLK","BKNG","MDT","AXP","LMT","SYK","T","TJX","DE","ADP","AMT","PGR","MMC","CVS","SYF","C","CI","GILD","MO","ADI","CB","REGN","BDX","MU","PNC","ZTS","BSX","USB","VRTX","HCA","CL","SO","APD","EQIX","ITW","FCX","NSC","SHW","CME","EOG","DUK","TGT","CSX","WM","EMR","MPC","SLB","FIS","FISV","PXD","GD","FDX","AON","MAR","AEP","PSX","KDP","MCK","COF","TRV","ORLY","MNST","KHC","ALL","AFL","HUM","ETN","NOC","D","AIG","ROP","KMI","PSA","OXY","MSI","CMG","JCI","WELL","SRE","PEG","SPG","MCO","PCAR","HLT","APH","ROST","VLO","IDXX","CTAS","DLR","WMB","YUM","PAYX","PRU","AZO","HPQ","MSCI","A","CTVA","AMP","OTIS","XEL","WEC","KR","PH","OKE","TT","ACGL","ODFL","EXC","RSG","MTB","VRSK","TDG","PPG","EA","ECL","FTNT","LEN","STZ","MLM","FAST","CAH","GWW","RCL","AWK","CDNS","DHI","KEYS","F","HES","WBD","KMB","VICI","DFS","LENB","FITB","WAT","ALB","GLW","ED","BKR","CHTR","NUE","PCG","EFX","HBAN","HIG","ON","VMC","IR","CPRT","SWKS","ZBH","EBAY","EXR","BR","CMS","SYY","DVN","ANSS","TTWO","HOLX","CNC","DTE","CCL","MTD","ILMN","CLX","ETR","MAA","AVB","GRMN","NDAQ","BAX","CARR","XYL","FANG","ULTA","HPE","INVH","K","BALL","LYB","PKG","RF","VTR","CINF","MKC","BIO","PPL","STE","TER","TRGP","NVR","LDOS","CFG","CEG","ALGN","DGX","AKAM","WRB","EXPD","IEX","FE","DRI","MOH","TTWO","LKQ","JBHT","BBY","IP","RJF","CAG","GEN","CF","APA","NRG","AES","LUV","HAS","PFG","BXP","AEE","AOS","BEN","JKHY","HRL","MRO","ZBRA","PNR","HWM","ALLE","LW","NDSN","MAS","FMC","AIZ","TPR","NI","SEE","HSY","CHD","GPC","WHR","TXT","EMN","JNPR","FFIV","RHI","PNW","RE","KIM","REG","UDR","ESS","INCY","DOV","LNT","IPG","OMC","TAP","RMD","VTRS","MGM","DOC","WY","HST","PEAK","PARA","FOX","FOXA","NWS","NWSA"]
SYMBOLS = SYMBOLSMID + SYMBOLSLGE

EMA_A = 21
EMA_B = 55
REFERENCE_EMA = 55  # Third EMA filter

X_DAYS = 40  # Crossover must have happened within last X days
MAX_DISTANCE_PCT = 10  # Price must be within this % from crossover
MIN_VOLUME_MILLIONS = 1.0  # Min average volume in millions

# === FUNCTIONS ===

def get_twelvedata_data(symbol, api_key):
    """Fetch last 1 year daily OHLCV data for a symbol."""
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1day",
        "apikey": api_key,
        "outputsize": 5000,
    }
    r = requests.get(url, params=params)
    data = r.json()
    if "status" in data and data["status"] == "error":
        raise Exception(f"{symbol}: {data.get('message', '')}")
    if "values" not in data:
        raise Exception(f"{symbol}: No data returned")

    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index("datetime", inplace=True)
    df = df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume"
    }).astype({
        "Open": float,
        "High": float,
        "Low": float,
        "Close": float,
        "Volume": float
    })
    df = df.sort_index()
    one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
    return df[df.index >= one_year_ago]

def calculate_ema(df, period, column="Close"):
    return df[column].ewm(span=period, adjust=False).mean()

def find_crossover(df, ema_a_col, ema_b_col):
    df["EMA_diff"] = df[ema_a_col] - df[ema_b_col]
    shifted_diff = df["EMA_diff"].shift(1)
    df["Crossover"] = ((shifted_diff * df["EMA_diff"]) < 0).fillna(False)
    df["Crossover_Type"] = df.apply(
        lambda row: "Bullish" if row[ema_a_col] > row[ema_b_col] else "Bearish",
        axis=1,
    )
    return df

def get_last_crossover(df, x_days):
    today = df.index[-1]
    threshold = today - pd.Timedelta(days=x_days)
    recent = df[(df["Crossover"] == True) & (df.index >= threshold)]
    if not recent.empty:
        return recent.iloc[-1]
    return None

def calculate_distance_from_crossover(crossover_level, current_price):
    return ((current_price - crossover_level) / crossover_level) * 100

def add_summary_and_delta_rows(df, date_str):
    bullish_list = df[df['Crossover Type'] == 'Bullish']['Symbol'].tolist()
    bearish_list = df[df['Crossover Type'] == 'Bearish']['Symbol'].tolist()

    bullish_str = ",".join(bullish_list)
    bearish_str = ",".join(bearish_list)

    # Load yesterday's data if exists
    yesterday_file = f"stock_analysis_results_{(datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')}.csv"
    if os.path.exists(yesterday_file):
        prev_df = pd.read_csv(yesterday_file)
        prev_bullish = []
        prev_bearish = []

        def safe_split_tickers(value):
            if isinstance(value, str):
                return value.split(",") if value else []
            return []

        if 'Symbol' in prev_df.columns:
            prev_bullish_row = prev_df[prev_df['Symbol'] == 'Bullish_Tickers']
            prev_bearish_row = prev_df[prev_df['Symbol'] == 'Bearish_Tickers']

            if not prev_bullish_row.empty:
                prev_bullish = safe_split_tickers(prev_bullish_row.iloc[0]['Percentage Distance (%)'])
            if not prev_bearish_row.empty:
                prev_bearish = safe_split_tickers(prev_bearish_row.iloc[0]['Percentage Distance (%)'])
    else:
        prev_bullish, prev_bearish = [], []

    # Delta lists
    delta_bullish = [t for t in bullish_list if t not in prev_bullish]
    delta_bearish = [t for t in bearish_list if t not in prev_bearish]

    # Create rows
    summary_rows = [
        {
            'Symbol': 'Bullish_Tickers',
            'Crossover Date': '',
            'Crossover Type': '',
            'Current Price': '',
            'Percentage Distance (%)': bullish_str
        },
        {
            'Symbol': 'Bearish_Tickers',
            'Crossover Date': '',
            'Crossover Type': '',
            'Current Price': '',
            'Percentage Distance (%)': bearish_str
        },
        {
            'Symbol': 'Delta_Bullish',
            'Crossover Date': '',
            'Crossover Type': '',
            'Current Price': '',
            'Percentage Distance (%)': ",".join(delta_bullish)
        },
        {
            'Symbol': 'Delta_Bearish',
            'Crossover Date': '',
            'Crossover Type': '',
            'Current Price': '',
            'Percentage Distance (%)': ",".join(delta_bearish)
        }
    ]

    return pd.concat([df, pd.DataFrame(summary_rows)], ignore_index=True)

def filter_stocks_twelvedata():
    results = []
    for i, symbol in enumerate(SYMBOLS):
        try:
            print(f"Processing {symbol} ({i+1}/{len(SYMBOLS)})")
            stock = get_twelvedata_data(symbol, API_KEY_TWELVE)
            if i < len(SYMBOLS) - 1:
                time.sleep(9)  # rate limit

            if len(stock) < max(EMA_A, EMA_B, REFERENCE_EMA) + 21:
                continue

            stock["EMA_A"] = calculate_ema(stock, EMA_A)
            stock["EMA_B"] = calculate_ema(stock, EMA_B)
            stock["EMA_Ref"] = calculate_ema(stock, REFERENCE_EMA)
            stock['Avg_Volume_20'] = stock['Volume'].rolling(window=20).mean()
            stock = stock.dropna()

            stock = find_crossover(stock, "EMA_A", "EMA_B")
            recent = get_last_crossover(stock, X_DAYS)
            if recent is None:
                continue

            crossover_level = (recent["EMA_A"] + recent["EMA_B"]) / 2
            current_price = stock["Close"].iloc[-1]
            distance = calculate_distance_from_crossover(crossover_level, current_price)

            # Filters
            if abs(distance) > MAX_DISTANCE_PCT:
                continue
            if recent["Crossover_Type"] == "Bullish" and current_price <= recent["EMA_Ref"]:
                continue
            if recent["Crossover_Type"] == "Bearish" and current_price >= recent["EMA_Ref"]:
                continue
            avg_vol_millions = recent['Avg_Volume_20'] / 1_000_000
            if avg_vol_millions < MIN_VOLUME_MILLIONS:
                continue

            vol_on_cross = recent["Volume"]
            vol_pct = (vol_on_cross / recent['Avg_Volume_20']) * 100 if recent['Avg_Volume_20'] else None

            results.append({
                "Symbol": symbol,
                "Crossover Date": recent.name.strftime("%Y-%m-%d"),
                "Crossover Type": recent["Crossover_Type"],
                "Current Price": current_price,
                "Percentage Distance (%)": round(distance, 2),
                "Avg_Volume_20": round(avg_vol_millions, 2),
                "Volume_On_Crossover": round(vol_on_cross / 1_000_000, 2),
                "Volume_Pct_of_Avg": round(vol_pct, 2) if vol_pct else None,
            })
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    return pd.DataFrame(results)

# === MAIN ===
if __name__ == "__main__":
    today_str = datetime.now().strftime("%Y-%m-%d")
    df = filter_stocks_twelvedata()
    if not df.empty:
        df_with_summaries = add_summary_and_delta_rows(df, today_str)
        filename = f"stock_analysis_results_{today_str}.csv"
        df_with_summaries.to_csv(filename, index=False)
        print(f"Results saved to {filename}")
    else:
        print("No results found.")
