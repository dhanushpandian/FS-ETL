# modules/validator.py
import psycopg2
import pymysql
import pyodbc
import sqlite3
import pymongo
import pandas as pd
from typing import Tuple, List, Dict, Any, Union

def validate_db_connection(db_type: str, creds: Dict[str, Any]) -> Tuple[bool, Union[List, str]]:
    """
    Validates connection to the source or target database based on the provided credentials.
    Returns a tuple of (success, result) where result is either data or error message.
    """
    try:
        if db_type == "PostgreSQL":
            connection = psycopg2.connect(
                host=creds.get('host', ''),
                port=creds.get('port', '5432'),
                user=creds.get('user', ''),
                password=creds.get('password', ''),
                database=creds.get('database', '')
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {creds.get('table', '')} LIMIT 2;")
            result = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            cursor.close()
            connection.close()
            return True, (column_names, result)

        elif db_type == "MySQL":
            connection = pymysql.connect(
                host=creds.get('host', ''),
                port=int(creds.get('port', '3306')),
                user=creds.get('user', ''),
                password=creds.get('password', ''),
                database=creds.get('database', '')
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {creds.get('table', '')} LIMIT 2;")
            result = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            cursor.close()
            connection.close()
            return True, (column_names, result)

        elif db_type == "MSSQL":
            connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={creds.get("host", "")};'
                f'PORT={creds.get("port", "1433")};UID={creds.get("user", "")};'
                f'PWD={creds.get("password", "")};DATABASE={creds.get("database", "")}'
            )
            cursor = connection.cursor()
            cursor.execute(f"SELECT TOP 2 * FROM {creds.get('table', '')};")
            result = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            cursor.close()
            connection.close()
            return True, (column_names, result)

        elif db_type == "MongoDB":
            client = pymongo.MongoClient(creds.get('uri', ''))
            db = client[creds.get('database', '')]
            collection = db[creds.get('collection', '')]
            result = list(collection.find().limit(2))
            
            # Get field names from first document
            fields = []
            if result and len(result) > 0:
                fields = list(result[0].keys())
            
            client.close()
            return True, (fields, result)

        elif db_type == "SQLite":
            connection = sqlite3.connect(creds.get('file_path', ''))
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {creds.get('table', '')} LIMIT 2;")
            result = cursor.fetchall()
            
            # Get column names
            cursor.execute(f"PRAGMA table_info({creds.get('table', '')});")
            column_names = [info[1] for info in cursor.fetchall()]
            
            cursor.close()
            connection.close()
            return True, (column_names, result)

        else:
            return False, f"Unsupported database type: {db_type}"
    
    except Exception as e:
        return False, str(e)


def validate_and_fetch_schema(db_type: str, creds: Dict[str, Any]) -> Tuple[str, Any]:
    """
    Validates the database connection and fetches the schema (table/collection preview).
    Returns a tuple of (status message, schema preview).
    """
    try:
        success, result = validate_db_connection(db_type, creds)
        
        if success:
            if isinstance(result, tuple) and len(result) == 2:
                # Format data for API response
                column_names, data = result
                
                # Convert to pandas DataFrame for easier handling
                if db_type == "MongoDB":
                    # MongoDB data might need special handling
                    if data and len(data) > 0:
                        # Convert ObjectId and other non-serializable types to strings
                        import json
                        data_str = json.loads(json.dumps(data, default=str))
                        df = pd.DataFrame(data_str)
                    else:
                        df = pd.DataFrame(columns=column_names)
                else:
                    # SQL databases
                    df = pd.DataFrame(data, columns=column_names)
                
                # Convert DataFrame to dict for JSON serialization
                preview_data = df.to_dict(orient='records')
                
                status = f"Connected to {db_type} successfully!"
                return status, (column_names, preview_data)
            else:
                return "Connected but couldn't fetch schema", []
        else:
            return f"Error: {result}", []
            
    except Exception as e:
        return f"Error: {str(e)}", []