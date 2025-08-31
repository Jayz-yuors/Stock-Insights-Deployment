from db_config import create_connection
from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
import pandas as pd

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
    db = create_connection()
    collection = db['stock_prices']
    df = df[df.index >= '2015-01-01']
    if df.empty:
        print(f"No price data to insert for {ticker_symbol}")
        return
    records = df.reset_index().to_dict('records')
    for record in records:
        record['ticker_symbol'] = ticker_symbol
        if 'trade_date' not in record:
            record['trade_date'] = str(record['Date'])
        collection.update_one(
            {'ticker_symbol': ticker_symbol, 'trade_date': record['trade_date']},
            {'$set': record},
            upsert=True
        )
    print(f"Inserted/updated records for {ticker_symbol}")

def run_fetching():
    tickers = get_company_list()
    for ticker in tickers:
        print(f"Fetching and inserting for {ticker}")
        data = fetch_alpha_vantage(ticker)
        if data is not None and not data.empty:
            insert_prices(data, ticker)
        else:
            print("Alpha Vantage failed, trying yfinance.")
            data = fetch_yfinance(ticker)
            if data is not None and not data.empty:
                insert_prices(data, ticker)
            else:
                print(f"No data found for {ticker}.")

if __name__ == "__main__":
    test_ticker = "RELIANCE.NS"
    print(f"Testing fetch for {test_ticker}")
    data = fetch_alpha_vantage(test_ticker)
    if data is not None and not data.empty:
        insert_prices(data, test_ticker)
    else:
        print("Alpha Vantage failed, trying yfinance.")
        data = fetch_yfinance(test_ticker)
        if data is not None and not data.empty:
            insert_prices(data, test_ticker)
        else:
            print(f"No data found for {test_ticker}.")
