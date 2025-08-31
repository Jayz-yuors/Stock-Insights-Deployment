import pandas as pd
from db_config import create_connection

def fetch_prices(ticker_symbol, start_date='2015-01-01', end_date=None):
    db = create_connection()
    collection = db['stock_prices']
    query = {'ticker_symbol': ticker_symbol}
    if start_date and end_date:
        query['trade_date'] = {'$gte': start_date, '$lte': end_date}
    elif start_date:
        query['trade_date'] = {'$gte': start_date}
    elif end_date:
        query['trade_date'] = {'$lte': end_date}
    cursor = collection.find(query).sort('trade_date', 1)
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df if not df.empty else None

def fetch_current_price(ticker_symbol):
    db = create_connection()
    collection = db['stock_prices']
    cursor = collection.find({'ticker_symbol': ticker_symbol}).sort('trade_date', -1).limit(1)
    docs = list(cursor)
    return docs[0] if docs else None

def fetch_company_info(ticker_symbol):
    db = create_connection()
    collection = db['companies']
    doc = collection.find_one({'ticker_symbol': ticker_symbol})
    return doc if doc else None

# --- ANALYTICS ---
def compute_sma(df, window=20):
    df['SMA'] = df['close_price'].rolling(window=window).mean()
    return df

def compute_ema(df, window=20):
    df['EMA'] = df['close_price'].ewm(span=window, adjust=False).mean()
    return df

def detect_abrupt_changes(df, threshold=0.05):
    df['pct_change'] = df['close_price'].pct_change()
    abrupt = df[abs(df['pct_change']) > threshold].copy()
    return abrupt[['trade_date', 'close_price', 'pct_change']]

def volatility_and_risk(df, window=20):
    df['volatility'] = df['close_price'].rolling(window=window).std()
    df['risk'] = df['volatility'] / df['close_price']
    return df[['trade_date', 'close_price', 'volatility', 'risk']]

def correlation_analysis(ticker_symbols):
    dfs = []
    company_names = []
    for ticker in ticker_symbols:
        df = fetch_prices(ticker)
        if df is None or df.empty:
            continue
        name = fetch_company_info(ticker)['company_name']
        company_names.append(name)
        df = df.rename(columns={'close_price': name})
        dfs.append(df.set_index('trade_date')[name])
    if not dfs:
        return pd.DataFrame()
    merged = pd.concat(dfs, axis=1, join='inner')
    corr = merged.corr()
    corr.index = company_names
    corr.columns = company_names
    return corr

def compare_companies(ticker_symbols, start_date=None, end_date=None):
    dfs = []
    for ticker in ticker_symbols:
        df = fetch_prices(ticker, start_date, end_date)
        if df is None or df.empty:
            continue
        name = fetch_company_info(ticker)['company_name']
        df = df.rename(columns={'close_price': name})
        dfs.append(df.set_index('trade_date')[name])
    if not dfs:
        return pd.DataFrame()
    merged = pd.concat(dfs, axis=1, join='inner')
    return merged
