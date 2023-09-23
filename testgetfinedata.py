import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2 import sql

# Specify the path to your db.env file
env_path = './db.env'  # Adjust the path if your file is located elsewhere

# Load the db.env file
load_dotenv(dotenv_path=env_path)

# Replace with your actual Alpha Vantage API key
api_key = os.getenv('ALPHAVANTAGE_API_KEY')

# Define the list of stock symbols
stocks = ['AMZN', 'CSCO', 'SLB', 'PANW']

# Define the start and end dates
start_date = '2011-01-01'
end_date = '2023-09-21'

# Initialize a list to hold the stock data
stock_data = []

db_params = {
    'dbname': os.getenv('DATABASE_DBNAME'),
    'user': os.getenv('DATABASE_USERNAME'),
    'password': os.getenv('DATABASE_PASSWORD'),
    'host': os.getenv('DATABASE_HOST'),
    'port': os.getenv('DATABASE_PORT')
}

table_name = "stock_data_daily"

# Loop over each stock symbol
for stock in stocks:
    # Construct the API URL
    # url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock}&outputsize=full&apikey={api_key}'
    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock}&outputsize=full&apikey={api_key}"
    
    # Make the API request
    response = requests.get(url)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        print(data)
        
        # Extract the time series data
        quote = data.get('Global Quote', {})
        
        if quote:
            stock_data.append({
                'stockname': stock,
                'datetime': datetime.strptime(quote['07. latest trading day'], '%Y-%m-%d'),
                'open': float(quote['02. open']),
                'close': float(quote['05. price']),
                'high': float(quote['03. high']),
                'low': float(quote['04. low']),
                'volume': int(quote['06. volume'])
            })
        else:
            print(f"Failed to retrieve data for {stock}. Status code: {response.status_code}")

# print(stock_data)
# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(stock_data)
# df.to_csv(f'{start_date}_{end_date}_google_amazon.csv', index=False)

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    # Create table if not exists
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        stockname VARCHAR(255),
        datetime TIMESTAMP,
        open_price FLOAT,
        close_price FLOAT,
        high_price FLOAT,
        low_price FLOAT,
        volume INTEGER
    )
    """
    cur.execute(create_table_query)

    # Insert data into the table
    insert_data_query = sql.SQL(f"""
    INSERT INTO {table_name} (
        stockname, datetime, open_price, close_price, high_price, low_price, volume
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)

    for index, row in df.iterrows():
        cur.execute(insert_data_query, (
            row['stockname'],
            row['datetime'],
            row['open'],
            row['close'],
            row['high'],
            row['low'],
            row['volume']
        ))
    
    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

except Exception as e:
    print(f"Failed to connect to the database or insert data. Error: {e}")
