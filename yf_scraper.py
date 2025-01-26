import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

def scrape_yahoo_finance_intraday():
    """
    Scrape 2 days' worth of intraday data for stocks listed in stock_data.py
    and update the respective CSV files in the 'stock_data' folder.
    """
    from stock_data_test import stock_names  # Import tickers from stock_data.py

    # Define the start and end dates for 2 days of data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=2)

    # Ensure the folder exists
    folder_path = "stock_data"
    os.makedirs(folder_path, exist_ok=True)

    for ticker in stock_names:
        try:
            # Download data for the ticker
            print(f"Fetching data for {ticker}...")
            intraday_data = yf.download(
                ticker,
                interval='1m',
                #start=start_date.strftime('%Y-%m-%d'),
                #end=end_date.strftime('%Y-%m-%d')
                start = '2025-01-22',
                end = '2025-01-23'
            )

            if intraday_data.empty:
                continue  # Skip if no data is available

            # File path for the CSV file
            file_path = os.path.join(folder_path, f"{ticker.lower()}_intraday.csv")

            if os.path.exists(file_path):
                # Read existing data
                existing_data = pd.read_csv(file_path, index_col=0, parse_dates=True)

                intraday_data.columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

                # Append new data to the existing data
                combined_data = pd.concat([existing_data, intraday_data])
                
                # Drop duplicate rows based on the index (datetime)
                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
            else:
                # If file doesn't exist, just use the new data
                combined_data = intraday_data

            # Save the updated data back to the CSV file
            combined_data.to_csv(file_path)
            print(f"Updated {file_path}.")

        except Exception:
            pass  # Silently continue to the next ticker if there's an error

if __name__ == "__main__":
    scrape_yahoo_finance_intraday()
