import matplotlib.pyplot as plt

def plot_prices(df, company_name):
    try:
        close_col = 'close_price'
    except KeyError:
        print("Close price column not found; cannot plot.")
        return
    plt.figure(figsize=(12,5))
    plt.plot(df['trade_date'], df[close_col], label='Close Price')
    if 'SMA' in df.columns:
        plt.plot(df['trade_date'], df['SMA'], label='SMA')
    if 'EMA' in df.columns:
        plt.plot(df['trade_date'], df['EMA'], label='EMA')
    plt.title(f"{company_name} Stock Price")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

def plot_correlation(corr_matrix):
    if corr_matrix.empty:
        print("Correlation matrix is empty.")
        return
    plt.figure(figsize=(8,6))
    plt.imshow(corr_matrix, cmap='coolwarm', vmin=-1, vmax=1)
    plt.colorbar(label='Correlation')
    plt.xticks(range(len(corr_matrix.columns)), corr_matrix.columns, rotation=45)
    plt.yticks(range(len(corr_matrix.index)), corr_matrix.index)
    plt.title("Stock Price Correlation Matrix")
    plt.tight_layout()
    plt.show()
