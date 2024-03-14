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
print("File path:", file_path1)  # Print the value of the environmental variable

df = pd.read_excel(file_path1)
df.to_sql("emp_table6",engine,if_exists='replace', index=False)

print(df)
