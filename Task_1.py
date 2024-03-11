import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:Postgres@localhost:5432/demo')

file_path = 'C:\\Users\\Faizan Ali\\Downloads\\DATETIME.xlsx'
df = pd.read_excel(file_path)

df.to_sql("emp_table1",engine,if_exists='replace', index=False)

print(df)