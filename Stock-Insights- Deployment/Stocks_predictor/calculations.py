import pandas as pd
from db_config import create_connection

def flatten_close_price(df):
    if df is None or df.empty:
        return df
    close_price_field = None
    for col in df.columns:
        if col.lower().startswith('close'):
            close_price_field = col
            break
    if close_price_field:
        df['close_price'] = df[close_price_field].apply(lambda x: x.get('value') if isinstance(x, dict) else None)
        df = df[df['close_price'].notnull()]
    else:
        raise KeyError("No close price field found for flattening")
    return df

def fetch_prices(ticker_symbol, start_date=None, end_date=None):
    db = create_connection()
    if db is None:
        raise ConnectionError("Could not connect to database.")
    collection = db['stock_prices']
    query = {'ticker_symbol': ticker_symbol}
    if start_date and hasattr(start_date, 'isoformat'):
        start_date = start_date.isoformat()
    if end_date and hasattr(end_date, 'isoformat'):
        end_date = end_date.isoformat()
    if start_date and end_date:
        query['trade_date'] = {'$gte': start_date, '$lte': end_date}
    elif start_date:
        query['trade_date'] = {'$gte': start_date}
    elif end_date:
        query['trade_date'] = {'$lte': end_date}
    cursor = collection.find(query).sort('trade_date', 1)
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
        df = df[df['trade_date'].notnull()]
        df = df.sort_values('trade_date').reset_index(drop=True)
        df = flatten_close_price(df)
    return df if not df.empty else None

def fetch_current_price(ticker_symbol):
    db = create_connection()
    if db is None:
        raise ConnectionError("Could not connect to database.")
    cursor = db['stock_prices'].find({'ticker_symbol': ticker_symbol}).sort('trade_date', -1).limit(1)
    docs = list(cursor)
    return docs[0] if docs else None

def fetch_company_info(ticker_symbol):
    db = create_connection()
    if db is None:
        raise ConnectionError("Could not connect to database.")
    return db['companies'].find_one({'ticker_symbol': ticker_symbol})

def get_close_price_column(df):
    if 'close_price' in df.columns:
        return 'close_price'
    candidates = ['close_price', 'Close', 'close', 'Adj Close', 'adj_close']
    for col in df.columns:
        if 'close' in col.lower():
            return col
    raise KeyError("No close price column found.")

def compute_sma(df, window=20):
    df = df.copy()
    col = get_close_price_column(df)
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(method='ffill')
    df['SMA'] = df[col].rolling(window=window, min_periods=1).mean()
    return df

def compute_ema(df, window=20):
    df = df.copy()
    col = get_close_price_column(df)
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(method='ffill')
    df['EMA'] = df[col].ewm(span=window, adjust=False).mean()
    return df

def detect_abrupt_changes(df, threshold=0.05):
    df = df.copy()
    col = get_close_price_column(df)
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(method='ffill')
    df['pct_change'] = df[col].pct_change()
    abrupt = df[abs(df['pct_change']) > threshold].copy()
    return abrupt[['trade_date', col, 'pct_change']]

def volatility_and_risk(df, window=20):
    df = df.copy()
    col = get_close_price_column(df)
    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(method='ffill')
    df['volatility'] = df[col].rolling(window=window, min_periods=1).std()
    df['risk'] = df['volatility'] / df[col]
    return df[['trade_date', col, 'volatility', 'risk']]

def correlation_analysis(ticker_symbols):
    dfs = []
    company_names = []
    for ticker in ticker_symbols:
        df = fetch_prices(ticker)
        if df is None or df.empty:
            continue
        df = flatten_close_price(df)
        info = fetch_company_info(ticker)
        name = info.get('company_name', ticker) if info else ticker
        df = df.rename(columns={'close_price': name})
        dfs.append(df.set_index('trade_date')[name])
        company_names.append(name)
    if not dfs:
        return pd.DataFrame()
    merged = pd.concat(dfs, axis=1, join='inner')
    return merged.corr().rename(index=dict(zip(merged.columns, company_names)),
                               columns=dict(zip(merged.columns, company_names)))

def compare_companies(ticker_symbols, start_date=None, end_date=None):
    dfs = []
    for ticker in ticker_symbols:
        df = fetch_prices(ticker, start_date, end_date)
        if df is None or df.empty:
            continue
        info = fetch_company_info(ticker)
        name = info.get('company_name', ticker) if info else ticker
        col = get_close_price_column(df)
        df = df.rename(columns={col: name})
        dfs.append(df.set_index('trade_date')[name])
    return pd.concat(dfs, axis=1, join='inner') if dfs else pd.DataFrame()

def best_time_to_invest(df):
    col = get_close_price_column(df)
    if 'SMA' not in df.columns:
        df = compute_sma(df)
    return df.loc[df[col] > df['SMA'], 'trade_date']
