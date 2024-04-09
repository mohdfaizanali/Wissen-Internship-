import json
import pandas as pd
from sqlalchemy import create_engine
import os

user = os.environ['postgres_username']
pwd = os.environ['postgres_password']
host = os.environ['postgres_host']
port = os.environ['postgres_port']
database = os.environ['postgres_database']

# Read JSON file
def read_json_file(json_path):
    with open(json_path, 'r') as f:
        data = json.load(f)
    return data

# Connect to PostgreSQL database using SQLAlchemy
def connect_to_db(user, pwd, host, port, database):
    engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{database}')
    return engine

# Read data from Excel file using pandas
def read_excel_file(excel_file_path):
    df = pd.read_excel(excel_file_path)
    return df

if __name__ == "__main__":
    # Path to your JSON file
    json_path = os.environ['json_file_path']

    # Read JSON data
    json_data = read_json_file(json_path)

    # Extract information from JSON
    table_name = json_data[0]['table_name']
    schema_name = json_data[0]['schema']
    excel_file_path = json_data[0]['file_path']

    # Connect to PostgreSQL database
    engine = connect_to_db(user, pwd, host, port, database)

    # Read data from Excel file
    df = read_excel_file(excel_file_path)

    # Create table in PostgreSQL database
    df.to_sql(table_name, engine, schema=schema_name, index=False, if_exists='replace')

    print("Data loaded into PostgreSQL successfully.")
