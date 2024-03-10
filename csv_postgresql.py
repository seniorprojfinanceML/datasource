import csv
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
import db_code.common as cm

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
file_path = 'resource\\btcusdt_all_optimize_reorder_25022024_235900_2.csv'

# Assume that the CSV columns are in the order: stockname, datetime, open, close, high, low, volume
# csv_columns = ['stockname', 'datetime', 'open', 'close', 'high', 'low', 'volume']
db_columns = ['time', 'currency', 'close_minmax_scale', 'close', 'ma25_99h', 'ma7_25h', 'ma7_25d', 'ma25_99h_scale', 'ma7_25h_scale', 'ma7_25d_scale']

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    table_name = "crypto_ind_one"

    create_table_query = cm.get_create_table_query(table_name)
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

