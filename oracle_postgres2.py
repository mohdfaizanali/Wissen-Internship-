import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime

# Oracle database connection details
oracle_username = 'system'
oracle_password = '5394'
oracle_host = 'localhost'
oracle_port = '1521'
oracle_service = 'xe'

# PostgreSQL database connection details
postgres_username = 'postgres'
postgres_password = 'Postgres'
postgres_host = 'localhost'
postgres_port = '5432'
postgres_database = 'demo'

# Connect to Oracle database using SQLAlchemy
oracle_engine = create_engine(f'oracle+cx_oracle://{oracle_username}:{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service}')

# Connect to PostgreSQL database using SQLAlchemy
postgres_engine = create_engine(f'postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

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
