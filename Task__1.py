# import sqlalchemy
# from sqlalchemy import create_engine
# import pandas as pd
# import os
#
# user = os.environ['postgres_username']
# pwd = os.environ['postgres_password']
# host = os.environ['postgres_host']
# port = os.environ['postgres_port']
# database = os.environ['postgres_database']
#
# engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{database}')
#
# file_path1 = os.environ.get('file_path')
# df = pd.read_excel(file_path1)
#
# df.to_sql("emp_table6",engine,if_exists='replace', index=False)
#
# print(df)
# After Adding functions , try and except block and oops
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import os

class DatabaseHandler:
    def __init__(self):
        self.user = os.environ.get('postgres_username')
        self.pwd = os.environ.get('postgres_password')
        self.host = os.environ.get('postgres_host')
        self.port = os.environ.get('postgres_port')
        self.database = os.environ.get('postgres_database')
        self.engine = self.create_engine()

    def create_engine(self):
        try:
            engine = create_engine(f'postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}')
            print("Database engine created successfully.")
            return engine
        except Exception as e:
            print(f"Error creating engine: {e}")
            return None

    def load_excel(self, file_path):
        try:
            df = pd.read_excel(file_path)
            print("Excel file loaded successfully.")
            return df
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            return None

    def upload_to_db(self, df, table_name):
        if self.engine is not None:
            try:
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                print(f"Data uploaded to {table_name} successfully.")
            except Exception as e:
                print(f"Error uploading data to database: {e}")
        else:
            print("No database engine available. Upload aborted.")

if __name__ == "__main__":
    # Initialize the database handler
    db_handler = DatabaseHandler()

    # Define the file path
    file_path = os.environ.get('file_path')

    # Load the Excel file
    df = db_handler.load_excel(file_path)

    # If the dataframe is loaded successfully, upload it to the database
    if df is not None:
        db_handler.upload_to_db(df, "emp_table6")

    # Print the dataframe (if loaded)
    if df is not None:
        print(df)
