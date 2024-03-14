import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import os

user = os.environ['postgres_username']
pwd = os.environ['postgres_password']
host = os.environ['postgres_host']
port = os.environ['postgres_port']
database = os.environ['postgres_database']

engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{database}')

file_path1 = os.environ.get('file_path')
df = pd.read_excel(file_path1)

dtype_mapping = {'Emp_joining_date': sqlalchemy.TIMESTAMP,
                 'Last_updated': sqlalchemy.TIMESTAMP}

df.to_sql("emp_table8",engine,if_exists='replace', index=False, dtype=dtype_mapping)

print(df)