import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime
import os

# Oracle database connection details
or_user = os.environ['oracle_username']
or_pwd = os.environ['oracle_password']
or_host = os.environ['oracle_host']
or_port = os.environ['oracle_port']
or_service = os.environ['oracle_service']

# Postgres database connection details
pg_user = os.environ['postgres_username']
pg_pwd = os.environ['postgres_password']
pg_host = os.environ['postgres_host']
pg_port = os.environ['postgres_port']
pg_database = os.environ['postgres_database']

# Connect to Oracle database using SQLAlchemy
oracle_engine = create_engine(f'oracle+cx_oracle://{or_user}:{or_pwd}@{or_host}:{or_port}/{or_service}')

# Connect to Postgres database using SQLAlchemy
postgres_engine = create_engine(f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_database}')

# Define table name
table_name = 'emp_table1'

# Read table schema and data from Oracle into a DataFrame
query = f"SELECT * FROM {table_name}"
df = pd.read_sql(query, con=oracle_engine)

# Infer schema from DataFrame
metadata = MetaData()

columns = []
for col, dtype in zip(df.columns, df.dtypes):
    if str(dtype) == 'object':
        columns.append(Column(col, String))
    elif str(dtype) == 'int64':
        columns.append(Column(col, Integer))
    elif str(dtype) == 'datetime64[ns]':
        columns.append(Column(col, DateTime))

# Create table in PostgreSQL
postgres_table = Table(table_name, metadata, *columns)
metadata.create_all(bind=postgres_engine)

# Write data from DataFrame to PostgreSQL
df.to_sql(name=table_name, con=postgres_engine, if_exists='replace', index=False)

print("Data exported successfully from Oracle to PostgreSQL.")
