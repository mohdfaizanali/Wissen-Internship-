import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, DateTime

# Oracle database connection details
oracle_username = 'system'
oracle_password = '5394'
oracle_host = 'localhost'
oracle_port = '1521'
oracle_service = 'xe'

oracle_engine = create_engine(
    f'oracle+cx_oracle://{oracle_username}:{oracle_password}@{oracle_host}:{oracle_port}/{oracle_service}')

# PostgreSQL database connection details
postgres_username = 'postgres'
postgres_password = 'Postgres'
postgres_host = 'localhost'
postgres_port = '5432'
postgres_database = 'demo'

postgres_engine = create_engine(
    f'postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

query = "SELECT * FROM emp_table1"
df_pg = pd.read_sql(query, postgres_engine)
df_pg.info()


df_oracle = pd.read_sql(query, oracle_engine)
df_oracle.info()

# Assuming dfpg is your dataframe from PostgreSQL and dfora is your dataframe from Oracle

# Merge the dataframes
merged_df = pd.merge(df_pg, df_oracle, how='outer', indicator=True)

# Select rows only present in PostgreSQL dataframe
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

