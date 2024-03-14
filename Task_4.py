import sqlalchemy
from sqlalchemy import create_engine, TIMESTAMP
import pandas as pd
import os

user = os.environ['oracle_username']
pwd = os.environ['oracle_password']
host = os.environ['oracle_host']
port = os.environ['oracle_port']
service = os.environ['oracle_service']

oracle_engine = create_engine(f'oracle://{user}:{pwd}@{host}:{port}/{service}')

file_path1 = os.environ.get('file_path')
df = pd.read_excel(file_path1)

df['Emp_joining_date'] = pd.to_datetime(df['Emp_joining_date'], format='%Y-%m-%d %H:%M:%S')
df['Last_updated'] = pd.to_datetime(df['Last_updated'], format='%Y-%m-%d %H:%M:%S')
df['Emp_id'] = pd.to_numeric(df['Emp_id'],)

dtype_mapping = {
    'Emp_joining_date': TIMESTAMP,
    'Last_updated': TIMESTAMP
}

df.to_sql("emp_table56", oracle_engine, if_exists='replace', index=False, dtype=dtype_mapping)

print(df)