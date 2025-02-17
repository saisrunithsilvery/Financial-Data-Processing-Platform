import snowflake.connector
import yaml
import os
from pathlib import Path

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def execute_sql_file(cursor, file_path, config):
    with open(file_path, 'r') as f:
        sql = f.read()
        
        # Replace AWS credentials
        sql = sql.replace(':aws_key_id', config['s3_stage']['aws_access_key'])
        sql = sql.replace(':aws_secret_key', config['s3_stage']['aws_secret_key'])
            
        for statement in sql.split(';'):
            if statement.strip():
                try:
                    print(f"Executing SQL: {statement.strip()}")  # For debugging
                    cursor.execute(statement.strip())
                except Exception as e:
                    print(f"Error executing statement: {statement.strip()}")
                    raise e

def main():
    config = load_config()
    
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=config['snowflake']['user'],
        password=config['snowflake']['password'],
        account=config['snowflake']['account'],
        warehouse=config['snowflake']['warehouse'],
        database=config['snowflake']['database'],
        schema=config['snowflake']['schema'],
        role='ACCOUNTADMIN'  # Explicitly use ACCOUNTADMIN role
    )
    
    cursor = conn.cursor()
    
    try:
        print("Initializing database...")
        execute_sql_file(cursor, 'sql/setup/01_init_db.sql', config)
        print("Setup completed successfully!")
        
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()