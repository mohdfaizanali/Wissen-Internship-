#from Oracle to Postgres
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

# Define the upsert query
upsert_query = f"""
    INSERT INTO emp_table1 ("Emp_id", "Last_updated")
    VALUES (:Emp_id, CURRENT_TIMESTAMP)
    ON CONFLICT ("Emp_id") DO UPDATE
    SET "Last_updated" = EXCLUDED."Last_updated";
"""

# Load differing data into PostgreSQL table emp_table1 using upsert
if not diff_ora.empty:
    try:
        with postgres_engine.connect() as connection:
            for index, row in diff_ora.iterrows():
                params = {'Emp_id': row['Emp_id'], 'Emp_dept': row['Emp_dept']}
                connection.execute(text(upsert_query), **params)
        print("Differing data loaded into emp_table1 in PostgreSQL database.")
    except Exception as e:
        print("Error occurred during upsert operation:", e)
else:
    print("No differing data found in Oracle. Nothing to load.")

