import sqlalchemy
from sqlalchemy import create_engine, TIMESTAMP
import pandas as pd

oracle_engine = create_engine('oracle://system:5394@localhost:1521/xe')

file_path = 'C:\\Users\\Faizan Ali\\Downloads\\DATETIME.xlsx'
df = pd.read_excel(file_path)

df['Emp_joining_date'] = pd.to_datetime(df['Emp_joining_date'], format='%Y-%m-%d %H:%M:%S')
df['Last_updated'] = pd.to_datetime(df['Last_updated'], format='%Y-%m-%d %H:%M:%S')
df['Emp_id'] = pd.to_numeric(df['Emp_id'],)

dtype_mapping = {
    'Emp_joining_date': TIMESTAMP,
    'Last_updated': TIMESTAMP
}

df.to_sql("emp_table4", oracle_engine, if_exists='replace', index=False, dtype=dtype_mapping)

print(df)