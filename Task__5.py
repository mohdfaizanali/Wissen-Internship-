import cx_Oracle
import psycopg2
import os

# or_user = os.environ['oracle_username']
# or_pwd = os.environ['oracle_password']
or_host = os.environ['oracle_host']
or_port = os.environ['oracle_port']
or_service = os.environ['oracle_service']


# Connect to Oracle database
oracle_connection = cx_Oracle.connect(
    user=os.environ['oracle_username'],
    password=os.environ['oracle_password'],
    dsn=f"{or_host}:{or_port}/{or_service}"
)

oracle_cursor = oracle_connection.cursor()

# Connect to PostgreSQL database
postgres_connection = psycopg2.connect(
    user=os.environ['postgres_username'],
    password=os.environ['postgres_password'],
    host=os.environ['postgres_host'],
    port=os.environ['postgres_port'],
    database=os.environ['postgres_database']
)

postgres_cursor = postgres_connection.cursor()

# Define table name
table_name = 'emp_table1'

# Fetch table schema from Oracle
oracle_cursor.execute(f"SELECT * FROM {table_name} WHERE 1=0")
columns = [desc[0] for desc in oracle_cursor.description]
column_types = [desc[1] for desc in oracle_cursor.description]

# Map Oracle data types to equivalent PostgreSQL data types
postgres_data_types = {
    cx_Oracle.DB_TYPE_NUMBER: 'BIGINT',  # Maps Oracle NUMBER(19) to PostgreSQL NUMERIC(19)
    cx_Oracle.STRING: 'VARCHAR',
    cx_Oracle.DATETIME: 'TIMESTAMP',
    cx_Oracle.FIXED_CHAR: 'CHAR',
    cx_Oracle.FIXED_NCHAR: 'CHAR',
    cx_Oracle.LONG_STRING: 'TEXT',
    cx_Oracle.CLOB: 'TEXT',
    cx_Oracle.TIMESTAMP: 'timestamp without time zone'
}


# Generate CREATE TABLE Zquery for PostgreSQL with correct data types
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
for col, col_type in zip(columns, column_types):
    postgres_type= postgres_data_types.get(col_type, 'VARCHAR')  # Default to VARCHAR if type not found
    create_table_query += f"{col} {postgres_type}, "
create_table_query = create_table_query[:-2] + ")"
postgres_cursor.execute(create_table_query)


# Fetch data from Oracle and insert into PostgreSQL
oracle_cursor.execute(f"SELECT * FROM {table_name}")
for row in oracle_cursor.fetchall():
    cleaned_row = []
    for val, val_type in zip(row, column_types):
        if val_type in [cx_Oracle.NUMBER, cx_Oracle.STRING]:
            cleaned_row.append(val)
        elif val_type == cx_Oracle.DATETIME:
            cleaned_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
        else:
            # Handle other data types accordingly
            cleaned_row.append(str(val))
    postgres_cursor.execute(f"INSERT INTO {table_name} VALUES ({','.join(['%s' for _ in range(len(columns))])})", cleaned_row)

# Commit changes and close connections
postgres_connection.commit()
postgres_connection.close()
oracle_connection.close()

print("Data exported successfully from Oracle to PostgreSQL.")