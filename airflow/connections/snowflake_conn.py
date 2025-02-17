import os
from airflow import settings
from airflow.models import Connection

def create_snowflake_conn():
    conn = Connection(
        conn_id='snowflake_default',
        conn_type='snowflake',
        host=os.getenv('SNOWFLAKE_ACCOUNT'),
        login=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        schema='PUBLIC',
        extra={
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT')
        }
    )
    
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == 'snowflake_default').first()
    if existing_conn:
        session.delete(existing_conn)
        session.commit()
    session.add(conn)
    session.commit()
    session.close()