# database/snowflake_connection.py
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus

class SnowflakeConnection:
    @staticmethod
    def get_connection_string():
        config = {
            "account": "rob31655.us-east-1",  # Full account identifier
            "user": "vishal_prasanna",
            "password": "Northeastern@12",
            "database": "SEC_DATA",
            "schema": "PUBLIC",
            "warehouse": "FINANCIAL_WH",
            "role": "ACCOUNTADMIN"
        }
        
        # URL encode the password to handle special characters
        encoded_password = quote_plus(config['password'])
        
        return (
            f"snowflake://{config['user']}:{encoded_password}@"
            f"{config['account']}.snowflakecomputing.com/{config['database']}"
            f"/{config['schema']}?"
            f"warehouse={config['warehouse']}&"
            f"role={config['role']}&"
            f"account={config['account']}"  # Added explicit account parameter
        )

    @staticmethod
    def create_engine():
        connection_string = SnowflakeConnection.get_connection_string()
        return create_engine(
            connection_string,
            connect_args={
                'client_session_keep_alive': True,
                'client_session_keep_alive_heartbeat_frequency': 3600
            }
        )

    @staticmethod
    def test_connection():
        """Test the database connection"""
        try:
            engine = SnowflakeConnection.create_engine()
            with engine.connect() as connection:
                result = connection.execute("SELECT current_version()").fetchone()
                print(f"Successfully connected to Snowflake! Version: {result[0]}")
                return True
        except Exception as e:
            print(f"Connection failed: {str(e)}")
            return False