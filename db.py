import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

def create_mssql_connection():
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = "Utkrishtsa"
    password = os.getenv("PASSWORD")
    
    connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    
    try:
        connection = pyodbc.connect(connection_string)
        print("Connection to MSSQL database established successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to MSSQL database: {e}")
        return None


#create_mssql_connection()        
