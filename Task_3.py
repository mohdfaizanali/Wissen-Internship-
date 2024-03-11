import sqlalchemy
from sqlalchemy import create_engine, TIMESTAMP
import pandas as pd

oracle_engine = create_engine('oracle://system:5394@localhost:1521/xe')

file_path = 'C:\\Users\\Faizan Ali\\Downloads\\DATETIME.xlsx'
df = pd.read_excel(file_path)
df.to_sql("emp_table4", oracle_engine, if_exists='replace', index=False)

print(df)