
##pulling daily data from yahoo finance##
import yfinance as yf
import pandas as pd
from datetime import datetime

## pull daily prices from Yahoo Finance ##
def fetch_yahoo_prices(symbols, start_date, end_date):
    df = yf.download(
        tickers=symbols,
        start=start_date,
        end=end_date,
        auto_adjust=False,   # IMPORTANT: keep raw prices
        group_by="ticker"
    )

    rows = []

    for symbol in symbols:
        symbol_df = df[symbol].reset_index()

        for _, r in symbol_df.iterrows():
            rows.append({
                "symbol": symbol,
                "trading_date": r["Date"].date(),
                "open": float(r["Open"]) if pd.notnull(r["Open"]) else None,
                "high": float(r["High"]) if pd.notnull(r["High"]) else None,
                "low": float(r["Low"]) if pd.notnull(r["Low"]) else None,
                "close": float(r["Close"]) if pd.notnull(r["Close"]) else None,
                "adj_close": float(r["Adj Close"]) if pd.notnull(r["Adj Close"]) else None,
                "volume": int(r["Volume"]) if pd.notnull(r["Volume"]) else None,
                "currency": "USD",
                "exchange": None,
                "source": "yahoo_finance",
                "ingestion_timestamp": datetime.utcnow()
            })

    return pd.DataFrame(rows)

if __name__ == "__main__":
    symbols = ["AAPL", "MSFT", "GOOGL"]

    df = fetch_yahoo_prices(
        symbols=symbols,
        start_date="2018-01-01",
        end_date="2024-01-01"
    )

    print(df.head())
    print(f"Rows loaded: {len(df)}")
## to run the above code, keep below line commented ot##
# python3 yahoo_finance_ds.py
