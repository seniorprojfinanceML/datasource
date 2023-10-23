from dotenv import dotenv_values
import os

current_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "db.env") 
config = dict(dotenv_values(current_path))
DATABASE_USERNAME = config["DATABASE_USERNAME"]
DATABASE_PASSWORD = config["DATABASE_PASSWORD"]
DATABASE_HOST = config["DATABASE_URL"]
DATABASE_NAME = config["DATABASE_NAME"]
DATABASE_URL = f"postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_HOST}:5432/{DATABASE_NAME}"

db_params = {
    'dbname': os.getenv('DATABASE_DBNAME'),
    'user': os.getenv('DATABASE_USERNAME'),
    'password': os.getenv('DATABASE_PASSWORD'),
    'host': os.getenv('DATABASE_HOST'),
    'port': os.getenv('DATABASE_PORT')
}