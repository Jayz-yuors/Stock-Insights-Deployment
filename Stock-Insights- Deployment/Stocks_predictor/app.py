import streamlit as st
import pandas as pd
from analysis import (
    fetch_prices, fetch_current_price, fetch_company_info,
    compute_sma, compute_ema, detect_abrupt_changes,
    volatility_and_risk, correlation_analysis,
    compare_companies, plot_correlation,
    best_time_to_invest, plot_prices, get_close_price_column
)
from data_fetcher import get_company_list, run_fetching
from datetime import datetime

st.set_page_config(page_title="Stocks Predictor Dashboard", layout="wide")

@st.cache_data(ttl=24*60*60)
def update_stock_data():
    run_fetching()

# Uncomment below line to manually update stock data on app start if needed
# update_stock_data()

# Theme selection
theme = st.sidebar.selectbox("Select Theme", options=["Light", "Dark"])
if theme == "Dark":
    st.markdown("""
    <style>
    .stApp {
        background-color: #181a1b;
        color: #eee;
    }
    </style>""", unsafe_allow_html=True)
else:
    st.markdown("""
    <style>
    .stApp {
        background-color: #f8fbff;
        color: #000;
    }
    </style>""", unsafe_allow_html=True)

st.title("📈 Stocks Insights")
st.markdown("Explore and analyze Nifty50 stocks with analytics & interactive charts.")

company_tickers = get_company_list()

st.sidebar.header("Choose One or More Companies")
selected_companies = st.sidebar.multiselect("Select Company Tickers", company_tickers, default=company_tickers[:1])

st.sidebar.header("Date Range (Optional)")
start_date = st.sidebar.date_input("Start date", value=None)
end_date = st.sidebar.date_input("End date", value=None, max_value=datetime.today())

dynamic_end_date = datetime.today().date()

if start_date and end_date and start_date >= end_date:
    st.sidebar.error("Start date must be less than end date.")

def extract_numeric_value(val):
    # Recursively extract numeric value from nested dicts (e.g. {'NS': 1357.199951171875})
    while isinstance(val, dict):
        if not val:
            return None
        val = next(iter(val.values()))
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except ValueError:
            return val
    return None

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Price & Trends",
    "🔍 Abrupt Changes",
    "🤖 ML & Volatility",
    "📚 Compare & Correlate",
    "⚙️ Export & Info"
])

with tab1:
    st.subheader("View Full Historical Price Data & Chart")
    for ticker in selected_companies:
        info = fetch_company_info(ticker)
        company_name_display = info.get('company_name') if info else ticker
        st.markdown(f"### {company_name_display} ({ticker})")
        df = fetch_prices(
            ticker,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else dynamic_end_date
        )

        if df is not None and not df.empty:
            st.write("Data columns for debugging:", df.columns.tolist())
            close_col = get_close_price_column(df)
            # Ensure numeric and filled NaNs for rolling calculations
            df[close_col] = pd.to_numeric(df[close_col], errors='coerce').fillna(method='ffill')

            df = compute_sma(df)
            df = compute_ema(df)

            current = fetch_current_price(ticker)
            try:
                raw_price = current.get(close_col) if current else None
                current_price = extract_numeric_value(raw_price)
                if current_price is None:
                    current_price = "N/A"
            except Exception:
                current_price = "N/A"

            st.metric(label="Current Price", value=current_price)

            df.set_index('trade_date', inplace=True)
            chart_data = df[[close_col, 'SMA', 'EMA']].dropna()
            if not chart_data.empty:
                st.line_chart(chart_data)
            else:
                st.warning("Not enough data to render chart.")
            
            with st.expander("Show Full Historical Data Table (Expandable)"):
                st.dataframe(df.reset_index(), use_container_width=True)
            st.markdown("---")
        else:
            st.warning("No price data for selected company.")

with tab2:
    st.subheader("Abrupt Price Changes Detection")
    threshold = st.slider("Set threshold for abrupt change (%)", 1, 20, value=5) / 100.0
    for ticker in selected_companies:
        info = fetch_company_info(ticker)
        if not info:
            st.warning(f"No info found for ticker {ticker}")
            continue
        company_name_display = info.get('company_name', ticker)
        st.markdown(f"### {company_name_display} ({ticker})")
        df = fetch_prices(
            ticker,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else dynamic_end_date
        )
        if df is not None and not df.empty:
            abrupt = detect_abrupt_changes(df, threshold=threshold)
            st.dataframe(abrupt)
        else:
            st.warning("No data to analyze.")
    st.markdown("---")

with tab3:
    st.subheader("ML & Volatility / Risk Analysis")
    window = st.slider("SMA/Volatility Window (days)", 5, 50, value=20)
    for ticker in selected_companies:
        info = fetch_company_info(ticker)
        if not info:
            st.warning(f"No info found for ticker {ticker}")
            continue
        company_name_display = info.get('company_name', ticker)
        st.markdown(f"### {company_name_display} ({ticker})")
        df = fetch_prices(
            ticker,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else dynamic_end_date
        )
        if df is not None and not df.empty:
            close_col = get_close_price_column(df)
            df[close_col] = pd.to_numeric(df[close_col], errors='coerce').fillna(method='ffill')
            df = compute_sma(df, window=window)
            vr_df = volatility_and_risk(df, window=window)
            vr_df.set_index('trade_date', inplace=True)
            chart_data = vr_df[['volatility', 'risk']].dropna()
            if not chart_data.empty:
                st.line_chart(chart_data)
            else:
                st.warning("Not enough data to render volatility/risk chart.")
        else:
            st.warning("No price data available for volatility analysis.")
    st.markdown("---")

with tab4:
    st.subheader("Compare Multiple Companies & Correlation Analysis")
    if len(selected_companies) > 1:
        merged = compare_companies(
            selected_companies,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else dynamic_end_date
        )
        if not merged.empty:
            # Convert all columns to numeric, coercing errors to NaN
            merged = merged.apply(pd.to_numeric, errors='coerce')
            # Drop any rows containing NaN values after conversion
            merged = merged.dropna()
    
            st.line_chart(merged)
            corr = correlation_analysis(selected_companies)
            st.write("Correlation Matrix of Selected Companies:")
            st.dataframe(corr)
            plot_correlation(corr)
        else:
            st.warning("No overlapping data found for comparison.")
    else:
        st.info("Select two or more companies to compare/correlate.")

with tab5:
    st.subheader("Export Data & Company Info")
    for ticker in selected_companies:
        info = fetch_company_info(ticker)
        if not info:
            st.warning(f"No info found for ticker {ticker}")
            continue
        st.markdown(f"### {info.get('company_name', ticker)} ({ticker}) Info")
        st.json(info)
        df = fetch_prices(
            ticker,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else dynamic_end_date
        )
        if df is not None and not df.empty:
            filename = f"{ticker}_price_data.csv"
            st.download_button("Export Data as CSV", data=df.to_csv(index=False), file_name=filename)
        st.markdown("---")

# Developer credit watermark
st.markdown("""
<style>
.watermark-box {
    position: fixed;
    right: 20px;
    bottom: 20px;
    background: linear-gradient(90deg, #a7e9edcc 10%, #eeaeca33 100%);
    border-radius: 24px;
    padding: 10px 25px 10px 17px;
    color: #222;
    font-size: 18px;
    font-weight: 500;
    box-shadow: 2px 4px 24px rgba(103,90,150,0.05);
    z-index: 1100;
    font-family: 'Montserrat', 'Roboto', sans-serif;
    display: flex;
    align-items: center;
    transition: background 0.5s;
    animation: slide-in 1.25s cubic-bezier(.25,.42,.44,1.15);
}
.watermark-box:hover {
    background: linear-gradient(85deg, #eeaeca99 40%, #a7e9edcc 80%);
    color: #3066be;
}
.watermark-emoji {
    font-size: 26px;
    margin-right: 12px;
    animation: bounce 1.5s infinite;
}
@keyframes bounce {
    0%, 100% { transform: translateY(0px);}
    50%      { transform: translateY(-5px);}
}
@keyframes slide-in {
    0% { opacity:0; left:-160px;}
    100% { opacity:1; left:20px;}
}
</style>
<div class="watermark-box">
<span class="watermark-emoji">🚀</span>
Developed By &nbsp;&nbsp : &nbsp;&nbsp <b><a href="https://www.linkedin.com/in/jay-keluskar-b17601358" target="_blank" style="color: inherit; text-decoration: none;">Jay Keluskar</a></b>
</div>
""", unsafe_allow_html=True)

st.sidebar.info("Made with ❤️ using Streamlit, AlphaVantage, and yfinance APIs.")


