import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime
import os

# Oracle database connection details
or_user = os.environ['oracle_username']
or_pwd = os.environ['oracle_password']
or_host = os.environ['oracle_host']
or_port = os.environ['oracle_port']
or_service = os.environ['oracle_service']

# Connect to Oracle database using SQLAlchemy
oracle_engine = create_engine(f'oracle+cx_oracle://{or_user}:{or_pwd}@{or_host}:{or_port}/{or_service}')

# PostgreSQL database connection details
pg_user = os.environ['postgres_username']
pg_pwd = os.environ['postgres_password']
pg_host = os.environ['postgres_host']
pg_port = os.environ['postgres_port']
pg_database = os.environ['postgres_database']

# Connect to PostgreSQL database using SQLAlchemy
postgres_engine = create_engine(f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_database}')

query = "SELECT * FROM emp_table1"
df_pg = pd.read_sql(query, postgres_engine)
df_pg.info()

df_oracle = pd.read_sql(query, oracle_engine)
df_oracle.info()

# Assuming dfpg is your dataframe from PostgreSQL and dfora is your dataframe from Oracle

# Merge the dataframes
merged_df = pd.merge(df_pg, df_oracle, how='outer', indicator=True)

# Select rows only present in Postgres dataframe
diff_pg = merged_df[merged_df['_merge'] == 'left_only'].drop('_merge', axis=1)

# Select rows only present in Oracle dataframe
diff_ora = merged_df[merged_df['_merge'] == 'right_only'].drop('_merge', axis=1)

# Display the differences
print("Rows present only in PostgreSQL dataframe:")
print(diff_pg)

print("\nRows present only in Oracle dataframe:")
print(diff_ora)

# Load data from diff_ora into PostgreSQL table emp_table1
if not diff_ora.empty:
    diff_ora.to_sql('emp_table1', postgres_engine, if_exists='append', index=False)
    print("Data from diff_ora loaded into emp_table1 in PostgreSQL database.")
else:
    print("No differences found between the dataframes. Nothing to load.")

