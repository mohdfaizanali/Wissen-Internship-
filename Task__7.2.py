import pandas as pd
from sqlalchemy import create_engine, text
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

# Create database connections
oracle_engine = create_engine(f'oracle+cx_oracle://{or_user}:{or_pwd}@{or_host}:{or_port}/{or_service}')
postgres_engine = create_engine(f'postgresql://{pg_user}:{pg_pwd}@{pg_host}:{pg_port}/{pg_database}')

# Get the maximum Last_updated timestamp from Postgres emp_table1
max_last_updated_query_pg = 'SELECT MAX("Last_updated") FROM emp_table1'
max_last_updated_pg = pd.read_sql(max_last_updated_query_pg, postgres_engine)
max_last_updated_pg_value = max_last_updated_pg.iloc[0, 0]  # Extracting the timestamp value
print("Max Postgres database last date value =", max_last_updated_pg_value)

# Query differing data from Oracle emp_table1
query_diff_ora = f"""
    SELECT *
    FROM emp_table1
    WHERE "Last_updated" > TO_TIMESTAMP('{max_last_updated_pg_value}', 'YYYY-MM-DD HH24:MI:SS.FF')
"""
diff_ora = pd.read_sql(query_diff_ora, oracle_engine)
print(diff_ora)

# Check for existing records in PostgreSQL with primary key values matching those of the differing data
if not diff_ora.empty:
    primary_key_values = tuple(diff_ora['Emp_id'])
    existing_records_query = text('SELECT * FROM emp_table1 WHERE "Emp_id" IN :ids')
    existing_records_query = existing_records_query.bindparams(ids=primary_key_values)
    existing_records = pd.read_sql(existing_records_query, postgres_engine)
    print(primary_key_values)

    # Delete existing records from Postgres table if found
    if not existing_records.empty:
        with postgres_engine.connect() as connection:
            for index, row in existing_records.iterrows():
                # delete_query = text(f'DELETE FROM emp_table1 WHERE "Emp_id" = {row["Emp_id"]}')
                delete_query = text(f'DELETE FROM emp_table1 WHERE "Emp_id" IN ({row["Emp_id"]})')
                # delete_query = delete_query.bindparams(emp_id=row['Emp_id'])
                connection.execute(delete_query)
                print(delete_query)
            print("Existing records deleted from emp_table1 in PostgreSQL.")

    # Load the differing data into PostgreSQL table emp_table1
    diff_ora.to_sql('emp_table1', postgres_engine, if_exists='append', index=False)
    print("Differing data loaded into emp_table1 in PostgreSQL database.")
else:
    print("No differing data found in Oracle. Nothing to load.")
