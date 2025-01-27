import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

def scrape_yahoo_finance_intraday():
    """
    Scrape 2 days' worth of intraday data for stocks listed in stock_data.py
    and update the respective CSV files in the 'stock_data' folder.
    """
    from stock_data_test import stock_names

    # Define the start and end dates for 2 days of data
    end_date = datetime.now()
    start_date = end_date - timedelta(days=2)

    # Ensure the folder exists
    folder_path = os.path.join(os.getcwd(), "stock_data")
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
                start = '2025-01-24',
                end = '2025-01-25'
            )

            if intraday_data.empty:
                print(f"No data retrieved for {ticker}.")
                continue  # Skip if no data is available

            intraday_data.columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

            # File path for the CSV file
            file_path = os.path.join(folder_path, f"{ticker.lower()}_intraday.csv")

            if os.path.exists(file_path):
                # Read existing data
                existing_data = pd.read_csv(file_path, index_col=0, parse_dates=True)

                # Append new data to the existing data
                combined_data = pd.concat([existing_data, intraday_data])
                
                # Drop duplicate rows based on the index (datetime)
                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
            else:
                # If file doesn't exist, just use the new data
                combined_data = intraday_data

            # Save the updated data back to the CSV file
            combined_data.to_csv(file_path, index=True)
            print(f"Updated {file_path}.")

        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

if __name__ == "__main__":
    scrape_yahoo_finance_intraday()
