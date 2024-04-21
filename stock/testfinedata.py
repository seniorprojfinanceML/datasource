import os
import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from psycopg2 import sql
import db_code.common as cm

# Load the db.env file
load_dotenv(dotenv_path='db.env')

# Database connection parameters
db_params = {
    'dbname': os.getenv('DATABASE_DBNAME'),
    'user': os.getenv('DATABASE_USERNAME'),
    'password': os.getenv('DATABASE_PASSWORD'),
    'host': os.getenv('DATABASE_HOST'),
    'port': os.getenv('DATABASE_PORT')
}

# Alpha Vantage API Key and Stocks
api_key = os.getenv('ALPHAVANTAGE_API_KEY')
stocks = ['AMZN', 'CSCO', 'SLB', 'PANW']
table_name = "stock_data_daily"

# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    # Create table if not exists
    create_table_query = cm.get_create_table_query(table_name)

    cur.execute(create_table_query)
    
    # Retrieve stock data from Alpha Vantage using GLOBAL_QUOTE endpoint and insert into the database
    insert_data_query = sql.SQL(f"""
    INSERT INTO {table_name} (
        stockname, datetime, open_price, close_price, high_price, low_price, volume
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """)
    
    for stock in stocks:
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={stock}&outputsize=full&apikey={api_key}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            quote = data.get('Global Quote', {})
            print(data)
            if quote:
                cur.execute(insert_data_query, (
                    stock,
                    datetime.strptime(quote['07. latest trading day'], '%Y-%m-%d'),
                    float(quote['02. open']),
                    float(quote['05. price']),
                    float(quote['03. high']),
                    float(quote['04. low']),
                    int(quote['06. volume'])
                ))
        else:
            print(f"Failed to retrieve data for {stock}. Status code: {response.status_code}")
    
    # Commit the transaction and close the connection
    conn.commit()
    cur.close()
    conn.close()

except Exception as e:
    print(f"Failed to connect to the database or insert data. Error: {e}")
