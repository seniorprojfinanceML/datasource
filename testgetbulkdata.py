import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

# Specify the path to your db.env file
env_path = './db.env'  # Adjust the path if your file is located elsewhere

# Load the db.env file
load_dotenv(dotenv_path=env_path)

# Replace with your actual Alpha Vantage API key
api_key = os.getenv('ALPHAVANTAGE_API_KEY')

# Define the list of stock symbols
stocks = ['AMZN', 'CSCO']

# Define the start and end dates
start_date = '2021-01-01'
end_date = '2023-10-22'

# Initialize a list to hold the stock data
stock_data = []

# Loop over each stock symbol
for stock in stocks:
    # Construct the API URL
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&outputsize=full&apikey={api_key}'
    
    # Make the API request
    response = requests.get(url)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Extract the time series data
        time_series = data.get('Time Series (Daily)', {})
        
        # Loop over each date in the time series
        for date, attributes in time_series.items():
            # Check if the date is within the desired date range
            if start_date <= date <= end_date:
                # Extract the required attributes
                stock_data.append({
                    'stockname': stock,
                    'datetime': datetime.strptime(date, '%Y-%m-%d'),
                    'open': float(attributes['1. open']),
                    'close': float(attributes['4. close']),
                    'high': float(attributes['2. high']),
                    'low': float(attributes['3. low']),
                    'volume': int(attributes['5. volume'])
                    # 'market cap': Market cap is not provided by this endpoint.
                })
    else:
        print(f"Failed to retrieve data for {stock}. Status code: {response.status_code}")

# print(stock_data)
# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(stock_data)
df.to_csv(f'{start_date}_{end_date}_google_amazon.csv', index=False)

# Display the DataFrame
# print(df)
