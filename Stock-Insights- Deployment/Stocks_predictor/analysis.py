import pandas as pd
from db_config import create_connection
import matplotlib.pyplot as plt

def fetch_prices(ticker_symbol, start_date=None, end_date=None):
    db = create_connection()
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

def get_close_price_column(df):
    candidates = ['close_price', 'Close', 'close']
    for col in candidates:
        if col in df.columns:
            return col
    raise KeyError(f"None of the expected close price columns {candidates} found in DataFrame columns: {list(df.columns)}")

# --- ANALYTICS ---

def compute_sma(df, window=20):
    close_col = None
    for col in ['close_price', 'Close', 'close']:
        if col in df.columns:
            close_col = col
            break
    if close_col is None:
        raise KeyError("No close price column found")
    df['SMA'] = df[close_col].rolling(window=window).mean()
    return df

def compute_ema(df, window=20):
    close_col = get_close_price_column(df)
    df['EMA'] = df[close_col].ewm(span=window, adjust=False).mean()
    return df

def detect_abrupt_changes(df, threshold=0.05):
    close_col = get_close_price_column(df)
    df['pct_change'] = df[close_col].pct_change()
    abrupt = df[abs(df['pct_change']) > threshold].copy()
    return abrupt[['trade_date', close_col, 'pct_change']]

def volatility_and_risk(df, window=20):
    close_col = get_close_price_column(df)
    df['volatility'] = df[close_col].rolling(window=window).std()
    df['risk'] = df['volatility'] / df[close_col]
    return df[['trade_date', close_col, 'volatility', 'risk']]

def correlation_analysis(ticker_symbols):
    dfs = []
    company_names = []
    for ticker in ticker_symbols:
        df = fetch_prices(ticker)
        if df is None or df.empty:
            continue
        company_info = fetch_company_info(ticker)
        name = company_info['company_name'] if company_info and 'company_name' in company_info else ticker
        company_names.append(name)
        close_col = get_close_price_column(df)
        df = df.rename(columns={close_col: name})
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
        company_info = fetch_company_info(ticker)
        name = company_info['company_name'] if company_info and 'company_name' in company_info else ticker
        close_col = get_close_price_column(df)
        df = df.rename(columns={close_col: name})
        dfs.append(df.set_index('trade_date')[name])
    if not dfs:
        return pd.DataFrame()
    merged = pd.concat(dfs, axis=1, join='inner')
    return merged

# --- VISUALS ---

def plot_prices(df, company_name):
    close_col = get_close_price_column(df)
    plt.figure(figsize=(12, 5))
    plt.plot(df['trade_date'], df[close_col], label='Close Price')
    if 'SMA' in df:
        plt.plot(df['trade_date'], df['SMA'], label='SMA')
    if 'EMA' in df:
        plt.plot(df['trade_date'], df['EMA'], label='EMA')
    plt.title(f"{company_name} Stock Price")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.tight_layout()
    plt.show()

def plot_correlation(corr_matrix):
    plt.figure(figsize=(8, 6))
    plt.imshow(corr_matrix, cmap='coolwarm', vmin=-1, vmax=1)
    plt.colorbar(label='Correlation')
    plt.xticks(range(len(corr_matrix.columns)), corr_matrix.columns, rotation=45)
    plt.yticks(range(len(corr_matrix.index)), corr_matrix.index)
    plt.title("Stock Price Correlation Matrix")
    plt.tight_layout()
    plt.show()

# --- DATA EXPORT ---

def export_data(df, filename):
    df.to_csv(filename, index=False)
    print(f"Data exported to {filename}")

# Pseudo ML: Best Time to Invest (existing logic)

def best_time_to_invest(df):
    close_col = get_close_price_column(df)
    if 'SMA' not in df:
        df = compute_sma(df)
    # Return dates where close price is above SMA
    return df[df[close_col] > df['SMA']]['trade_date']
