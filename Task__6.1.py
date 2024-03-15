import pandas as pd
from sqlalchemy import create_engine, types
import os

# Oracle database connection details
or_user = os.environ['oracle_username']
or_pwd = os.environ['oracle_password']
or_host = os.environ['oracle_host']
or_port = os.environ['oracle_port']
or_service = os.environ['oracle_service']

# PostgreSQL database connection details
pg_user = os.environ['postgres_username']
pg_pwd = os.environ['postgres_password']
pg_host = os.environ['postgres_host']
pg_port = os.environ['postgres_port']
pg_database = os.environ['postgres_database']

# Connect to Oracle database using SQLAlchemy
oracle_engine = create_engine(f'oracle+cx_oracle://{or_user}:{or_pwd}@{or_host}:{or_port}/{or_service}')

# Connect to Postgres database using SQLAlchemy
postgres_engine = create_engine(f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_database}')

table_name = 'emp_table1'

# Fetch data from Oracle
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, oracle_engine)

print(df)

# Insert data into PostgreSQL
df.to_sql(name='emp_table1', con=postgres_engine, if_exists="replace", index=False)
print("Data from Oracle table 'table_name' loaded into PostgreSQL.")