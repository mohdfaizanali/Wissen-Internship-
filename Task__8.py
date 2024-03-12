import pandas as pd
from sqlalchemy import create_engine, text

# Create database connections
oracle_engine = create_engine('oracle://system:5394@localhost:1521/xe')
postgres_engine = create_engine('postgresql://postgres:Postgres@localhost:5432/demo')

# Get the maximum Last_updated timestamp from Postgres emp_table1
max_last_updated_query_pg = 'SELECT MAX("Last_updated") FROM emp_table1'
max_last_updated_pg = pd.read_sql(max_last_updated_query_pg, postgres_engine)
max_last_updated_pg_value = max_last_updated_pg.iloc[0, 0]  # Extracting the timestamp value
print("Max Postgres database last date value =", max_last_updated_pg_value)

query_diff_ora = f"""
    SELECT *
    FROM emp_table1
    WHERE "Last_updated" > TO_TIMESTAMP('{max_last_updated_pg_value}', 'YYYY-MM-DD HH24:MI:SS.FF')
"""
diff_ora = pd.read_sql(query_diff_ora, oracle_engine)
print(diff_ora)

# Load differing data into PostgreSQL table emp_table1
if not diff_ora.empty:
    diff_ora.to_sql('emp_table1', postgres_engine, if_exists='append', index=False)
    print("Differing data loaded into emp_table1 in PostgreSQL database.")
else:
    print("No differing data found in Oracle. Nothing to load.")