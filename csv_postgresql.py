import csv
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Specify the path to your db.env file
env_path = './db.env'  # Adjust the path if your file is located elsewhere

# Load the db.env file
load_dotenv(dotenv_path=env_path)

# Get database credentials from environment variables
db_params = {
    'dbname': os.getenv('DATABASE_DBNAME'),
    'user': os.getenv('DATABASE_USERNAME'),
    'password': os.getenv('DATABASE_PASSWORD'),
    'host': os.getenv('DATABASE_HOST'),
    'port': os.getenv('DATABASE_PORT')
}

# CSV file path
file_path = '2011-01-01_2023-09-21_google_amazon.csv'

# Assume that the CSV columns are in the order: stockname, datetime, open, close, high, low, volume
csv_columns = ['stockname', 'datetime', 'open', 'close', 'high', 'low', 'volume']
db_columns = ['stockname', 'datetime', 'open_price', 'close_price', 'high_price', 'low_price', 'volume']

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    table_name = "stock_data"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        stockname VARCHAR(255),
        datetime TIMESTAMP,
        open_price FLOAT DEFAULT NULL,
        close_price FLOAT DEFAULT NULL,
        high_price FLOAT DEFAULT NULL,
        low_price FLOAT DEFAULT NULL,
        volume INTEGER DEFAULT NULL
    )
    """
    cur.execute(create_table_query)

    with open(file_path, 'r') as file:
        # Skip the header row in the CSV file
        next(file)
        
        # Use copy_from for efficient bulk insert
        cur.copy_from(file, table_name, sep=',', columns=db_columns)
        
    # Commit the transaction
    conn.commit()
    
    print(f"Data from {file_path} has been inserted into {table_name}.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the database connection
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

