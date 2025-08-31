from db_config import create_connection
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


ALPHA_VANTAGE_API_KEY = 'H13FNA09HVUDWWKU'


def fetch_alpha_vantage(ticker):
    try:
        ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
        data, _ = ts.get_daily(symbol=ticker, outputsize='full')
        return data
    except Exception as e:
        print(f"Alpha Vantage error for {ticker}: {e}")
        return None


def fetch_yfinance(ticker):
    try:
        data = yf.download(ticker, period="max", interval="1d", auto_adjust=True)
        if data.empty:
            print(f"YFinance data empty for {ticker}")
            return None
        return data
    except Exception as e:
        print(f"yfinance error for {ticker}: {e}")
        return None


def get_company_list():
    db = create_connection()
    collection = db['companies']
    return [doc['ticker_symbol'] for doc in collection.find({})]


def insert_prices(df, ticker_symbol):
    df.index = pd.to_datetime(df.index)
    df = df[df.index >= '2015-01-01']  # Ensures filter from 2015

    if df.empty:
        print(f"No price data from 2015 onwards for {ticker_symbol}")
        return

    if isinstance(df.columns, pd.MultiIndex):  # Flatten MultiIndex columns (yfinance format)
        df.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in df.columns]

    db = create_connection()
    collection = db['stock_prices']

    df_reset = df.reset_index()
    records = df_reset.to_dict('records')

    for i, record in enumerate(records):
        record['ticker_symbol'] = ticker_symbol

        if 'Date' in record:
            record['trade_date'] = record['Date'].strftime('%Y-%m-%d')
        elif 'index' in record:
            record['trade_date'] = record['index'].strftime('%Y-%m-%d')
        else:
            possible_date_col = df_reset.columns[0]
            if isinstance(record.get(possible_date_col), pd.Timestamp):
                record['trade_date'] = record[possible_date_col].strftime('%Y-%m-%d')
            else:
                raise KeyError("No recognizable date column found.")

        if i % 100 == 0:
            print(f"Processing record {i} for {ticker_symbol}")

        collection.update_one(
            {'ticker_symbol': ticker_symbol, 'trade_date': record['trade_date']},
            {'$set': record},
            upsert=True
        )

    print(f"Inserted/updated stock prices for {ticker_symbol}")


# New helper function to get latest stored trade_date for ticker
def get_latest_trade_date(ticker):
    db = create_connection()
    coll = db['stock_prices']
    latest_doc = coll.find({'ticker_symbol': ticker}).sort('trade_date', -1).limit(1)
    for doc in latest_doc:
        return doc['trade_date']  # returns str 'YYYY-MM-DD'
    return None


def run_fetching():
    print(f"Fetching started: {datetime.now()}")
    tickers = get_company_list()
    print(f"Total tickers to fetch: {len(tickers)}")

    for i, ticker in enumerate(tickers, start=1):
        print(f"\n{i}/{len(tickers)}: Fetching incremental data for {ticker}...")

        latest_date = get_latest_trade_date(ticker)
        if latest_date:
            start_date = datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)
        else:
            start_date = datetime.strptime('2015-01-01', '%Y-%m-%d')

        end_date = datetime.today()

        if start_date > end_date:
            print(f"No new data to fetch for {ticker}.")
            continue

        # Fetch full data (API limitation), then filter locally
        data = fetch_alpha_vantage(ticker)
        if data is None or data.empty:
            print(f"Alpha Vantage failed for {ticker}, trying yfinance...")
            data = fetch_yfinance(ticker)

        if data is None or data.empty:
            print(f"No data found for {ticker}, skipping.")
            continue

        data.index = pd.to_datetime(data.index)
        filtered_data = data[(data.index >= start_date) & (data.index <= end_date)]

        if filtered_data.empty:
            print(f"No new data for {ticker} between {start_date.date()} and {end_date.date()}")
            continue

        insert_prices(filtered_data, ticker)

        # Optional: Sleep to respect API limits if needed
        # time.sleep(12)

    print(f"\nFetching completed: {datetime.now()}")


if __name__ == "__main__":
    run_fetching()
