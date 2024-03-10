import os
from dotenv import load_dotenv
import psycopg2

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

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)
    print("Connected to the database.")
except Exception as e:
    print(f"Failed to connect to the database. Error: {e}")