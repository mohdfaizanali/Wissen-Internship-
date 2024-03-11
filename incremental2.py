import pandas as pd
from sqlalchemy import create_engine

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

# Create database connections
oracle_engine = create_engine(f'oracle+cx_oracle://{oracle_username}:{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service}')
postgres_engine = create_engine(f'postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

# Get the maximum Last_updated timestamp from Oracle emp_table1
max_last_updated_query_oracle = 'SELECT MAX("Last_updated") FROM emp_table1'
max_last_updated_oracle = pd.read_sql(max_last_updated_query_oracle, oracle_engine)
max_last_updated_oracle_value = max_last_updated_oracle.iloc[0, 0]  # Extracting the timestamp value
print(max_last_updated_oracle_value)
# Construct the query to select differing data based on Last_updated from Oracle
# query_diff_ora = f"SELECT * FROM emp_table1 WHERE Last_updated > '{max_last_updated_oracle_value}'"
query_diff_ora = f"SELECT * FROM emp_table1 WHERE \"Last_updated\" > ({max_last_updated_query_oracle})"

# Read differing data from Oracle
diff_ora = pd.read_sql(query_diff_ora, oracle_engine)

# Load differing data into PostgreSQL table emp_table1
if not diff_ora.empty:
    diff_ora.to_sql('emp_table1', postgres_engine, if_exists='append', index=False)
    print("Differing data loaded into emp_table1 in PostgreSQL database.")
else:
    print("No differing data found in Oracle. Nothing to load.")
