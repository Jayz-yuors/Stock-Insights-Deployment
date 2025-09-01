import pandas as pd
import matplotlib.pyplot as plt
from db_config import create_connection

def fetch_prices(ticker_symbol, start_date=None, end_date=None):
    """
    Fetch historical prices for a ticker from the database.
    """
    db = create_connection()
    if db is None :
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
    return df if not df.empty else None

def fetch_current_price(ticker_symbol):
    """
    Fetch the most recent price for a ticker from the database.
    """
    db = create_connection()
    if db is None :
        raise ConnectionError("Could not connect to database.")
    collection = db['stock_prices']
    cursor = collection.find({'ticker_symbol': ticker_symbol}).sort('trade_date', -1).limit(1)
    docs = list(cursor)
    return docs if docs else None

def fetch_company_info(ticker_symbol):
    """
    Fetch company information for a ticker from the database.
    """
    db = create_connection()
    if db is None :
        raise ConnectionError("Could not connect to database.")
    collection = db['companies']
    doc = collection.find_one({'ticker_symbol': ticker_symbol})
    return doc if doc else None

def get_close_price_column(df):
    """
    Get the column name corresponding to close price.
    """
    candidates = ['close_price', 'Close', 'close', 'Adj Close', 'adj_close']
    for col in df.columns:
        if 'close' in col.lower():
            return col
    raise KeyError(f"No close price column found. Columns present: {list(df.columns)}")

def compute_sma(df, window=20):
    """
    Compute simple moving average (SMA) on close price.
    """
    df = df.copy()
    close_col = get_close_price_column(df)
    df[close_col] = pd.to_numeric(df[close_col], errors='coerce').fillna(method='ffill')
    df['SMA'] = df[close_col].rolling(window=window, min_periods=1).mean()
    return df

def compute_ema(df, window=20):
    """
    Compute exponential moving average (EMA) on close price.
    """
    df = df.copy()
    close_col = get_close_price_column(df)
    df[close_col] = pd.to_numeric(df[close_col], errors='coerce').fillna(method='ffill')
    df['EMA'] = df[close_col].ewm(span=window, adjust=False).mean()
    return df

def detect_abrupt_changes(df, threshold=0.05):
    """
    Detect abrupt price changes exceeding given threshold (as proportion).
    """
    df = df.copy()
    close_col = get_close_price_column(df)
    df[close_col] = pd.to_numeric(df[close_col], errors='coerce').fillna(method='ffill')
    df['pct_change'] = df[close_col].pct_change()
    abrupt = df[abs(df['pct_change']) > threshold].copy()
    return abrupt[['trade_date', close_col, 'pct_change']]

def volatility_and_risk(df, window=20):
    """
    Calculate rolling volatility (std) and relative risk.
    """
    df = df.copy()
    close_col = get_close_price_column(df)
    df[close_col] = pd.to_numeric(df[close_col], errors='coerce').fillna(method='ffill')
    df['volatility'] = df[close_col].rolling(window=window, min_periods=1).std()
    df['risk'] = df['volatility'] / df[close_col]
    return df[['trade_date', close_col, 'volatility', 'risk']]

def correlation_analysis(ticker_symbols):
    """
    Compute correlation matrix of closing prices for multiple tickers.
    """
    dfs = []
    company_names = []
    for ticker in ticker_symbols:
        df = fetch_prices(ticker)
        if df is None or df.empty:
            continue
        company_info = fetch_company_info(ticker)
        name = company_info.get('company_name', ticker) if company_info else ticker
        close_col = get_close_price_column(df)
        df = df.rename(columns={close_col: name})
        dfs.append(df.set_index('trade_date')[name])
        company_names.append(name)
    if not dfs:
        return pd.DataFrame()
    merged = pd.concat(dfs, axis=1, join='inner')
    corr = merged.corr()
    corr.index = company_names
    corr.columns = company_names
    return corr

def compare_companies(ticker_symbols, start_date=None, end_date=None):
    """
    Return merged dataframe of closing prices for given companies.
    """
    dfs = []
    for ticker in ticker_symbols:
        df = fetch_prices(ticker, start_date, end_date)
        if df is None or df.empty:
            continue
        company_info = fetch_company_info(ticker)
        name = company_info.get('company_name', ticker) if company_info else ticker
        close_col = get_close_price_column(df)
        df = df.rename(columns={close_col: name})
        dfs.append(df.set_index('trade_date')[name])
    if not dfs:
        return pd.DataFrame()
    merged = pd.concat(dfs, axis=1, join='inner')
    return merged

def plot_prices(df, company_name):
    """
    Plot price and (optional) SMA/EMA for a company.
    """
    close_col = get_close_price_column(df)
    plt.figure(figsize=(12, 5))
    plt.plot(df['trade_date'], df[close_col], label='Close Price')
    if 'SMA' in df.columns:
        plt.plot(df['trade_date'], df['SMA'], label='SMA')
    if 'EMA' in df.columns:
        plt.plot(df['trade_date'], df['EMA'], label='EMA')
    plt.title(f"{company_name} Stock Price", fontsize=14)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Price", fontsize=12)
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

def plot_correlation(corr_matrix):
    """
    Plot a heatmap of correlation matrix.
    """
    if corr_matrix.empty:
        print("Correlation matrix is empty.")
        return
    plt.figure(figsize=(8, 6))
    plt.imshow(corr_matrix, cmap='coolwarm', vmin=-1, vmax=1)
    plt.colorbar(label='Correlation')
    plt.xticks(range(len(corr_matrix.columns)), corr_matrix.columns, rotation=45)
    plt.yticks(range(len(corr_matrix.index)), corr_matrix.index)
    plt.title("Stock Price Correlation Matrix", fontsize=14)
    plt.tight_layout()
    plt.show()

def export_data(df, filename):
    """
    Export dataframe to CSV file.
    """
    try:
        df.to_csv(filename, index=False)
        print(f"Data exported to {filename}")
    except Exception as e:
        print(f"Failed to export data: {e}")

def best_time_to_invest(df):
    """
    Return dates when price is above SMA.
    """
    close_col = get_close_price_column(df)
    if 'SMA' not in df.columns:
        df = compute_sma(df)
    return df.loc[df[close_col] > df['SMA'], 'trade_date']
