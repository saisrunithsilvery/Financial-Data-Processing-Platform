from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Retrieve credentials from environment variables
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_FOLDER = os.getenv('S3_FOLDER')

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    's3_to_snowflake_financial_data',
    default_args=default_args,
    description='Extract financial data from S3 and load to Snowflake',
    schedule_interval='@daily',
    catchup=False
)

# **Step 1: Test Snowflake Connection**
test_conn = SnowflakeOperator(
    task_id='test_snowflake_connection',
    snowflake_conn_id='snowflake_default',
    sql="SELECT CURRENT_TIMESTAMP;",
    dag=dag
)

# **Step 2: Create Snowflake Stage**
create_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    CREATE STAGE IF NOT EXISTS financial_stage
    URL='s3://{S3_BUCKET}/{S3_FOLDER}'
    STORAGE_INTEGRATION = my_s3_integration;
    """,
    dag=dag
)

# **Step 3: Create JSON File Format**
create_json_file_format = SnowflakeOperator(
    task_id='create_json_file_format',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE FILE FORMAT IF NOT EXISTS json_format 
    TYPE = 'JSON';
    """,
    dag=dag
)

# **Step 4: Create Snowflake Tables**
create_tables = SnowflakeOperator(
    task_id='create_snowflake_tables',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE TABLE IF NOT EXISTS financial_metadata (
        startDate DATE,
        endDate DATE,
        year INT,
        quarter VARCHAR,
        symbol VARCHAR PRIMARY KEY,
        name VARCHAR,
        country VARCHAR,
        city VARCHAR
    );

    CREATE TABLE IF NOT EXISTS balance_sheet (
        id INTEGER AUTOINCREMENT PRIMARY KEY,
        symbol VARCHAR,
        concept VARCHAR,
        info VARCHAR,
        unit VARCHAR,
        value FLOAT
    );

    CREATE TABLE IF NOT EXISTS cash_flow (
        id INTEGER AUTOINCREMENT PRIMARY KEY,
        symbol VARCHAR,
        concept VARCHAR,
        info VARCHAR,
        unit VARCHAR,
        value FLOAT
    );
    """,
    dag=dag
)

# **Step 5: Load JSON Data into Snowflake**
load_data_to_snowflake = SnowflakeOperator(
    task_id='load_financial_data',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    COPY INTO financial_metadata
    FROM (SELECT 
            data:startDate::DATE, 
            data:endDate::DATE, 
            data:year::INT, 
            data:quarter::STRING, 
            data:symbol::STRING, 
            data:name::STRING, 
            data:country::STRING, 
            data:city::STRING 
          FROM @financial_stage)
    FILE_FORMAT = (TYPE = 'JSON')
    ON_ERROR = 'CONTINUE';

    COPY INTO balance_sheet
    FROM (SELECT 
            data:symbol::STRING, 
            value:concept::STRING, 
            value:info::STRING, 
            value:unit::STRING, 
            value:value::FLOAT 
          FROM @financial_stage, 
          LATERAL FLATTEN(input => data:data.bs))
    FILE_FORMAT = (TYPE = 'JSON')
    ON_ERROR = 'CONTINUE';

    COPY INTO cash_flow
    FROM (SELECT 
            data:symbol::STRING, 
            value:concept::STRING, 
            value:info::STRING, 
            value:unit::STRING, 
            value:value::FLOAT 
          FROM @financial_stage, 
          LATERAL FLATTEN(input => data:data.cf))
    FILE_FORMAT = (TYPE = 'JSON')
    ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

# **Task Dependencies**
test_conn >> create_json_file_format >> create_stage >> create_tables >> load_data_to_snowflake
