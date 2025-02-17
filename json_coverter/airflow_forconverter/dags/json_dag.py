"""
DAG for loading financial data from S3 to Snowflake
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook

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

# Initialize Snowflake Database
init_database = SnowflakeOperator(
    task_id='init_database',
    snowflake_conn_id='snowflake_conn',
    sql="""
    USE ROLE ACCOUNTADMIN;
    CREATE DATABASE IF NOT EXISTS FINANCIAL_DB;
    USE DATABASE FINANCIAL_DB;
    CREATE SCHEMA IF NOT EXISTS FINANCIAL_SCHEMA;
    USE SCHEMA FINANCIAL_SCHEMA;
    """,
    dag=dag
)



create_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id='snowflake_conn',
    sql="""
    CREATE OR REPLACE STAGE my_s3_stage
    STORAGE_INTEGRATION = S3_INT
    URL = 's3://damgassign02/Extracted_json_files/'
    FILE_FORMAT = (TYPE = JSON);
    """,
    dag=dag
)

# Create File Format
create_file_format = SnowflakeOperator(
    task_id="create_file_format",
    snowflake_conn_id="snowflake_conn",
    sql="""
    CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    COMPRESSION = NONE
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE
    STRIP_NULL_VALUES = FALSE
    IGNORE_UTF8_ERRORS = TRUE;
    """,
    dag=dag
)
# Create stage
# create_stage = SnowflakeOperator(
#     task_id="create_s3_stage",
#     snowflake_conn_id="snowflake_conn",
#     sql="""
#     CREATE STAGE IF NOT EXISTS financial_stage
#     URL='s3://damgassign02/Extracted_json_files/2022q4/'
#     CREDENTIALS = (AWS_KEY_ID='AKIA47CR2USMBXVBOSXH' 
#                   AWS_SECRET_KEY='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')
#     FILE_FORMAT = json_format;              
#     """,
#     dag=dag
# )



# Create Tables
create_tables = SnowflakeOperator(
    task_id='create_tables',
    snowflake_conn_id='snowflake_conn',
    sql="""
    -- Metadata table
    -- Metadata table
    DROP TABLE IF EXISTS financial_data;
    DROP TABLE IF EXISTS financial_metadata;
    CREATE TABLE IF NOT EXISTS financial_metadata (
        id INTEGER AUTOINCREMENT PRIMARY KEY,
        startDate DATE,
        endDate DATE,
        year INT,
        quarter VARCHAR,
        symbol VARCHAR NOT NULL,
        name VARCHAR,
        country VARCHAR,
        city VARCHAR,
        CONSTRAINT unique_symbol UNIQUE (symbol)
    );

    -- Financial data table for both balance sheet and cash flow
    CREATE TABLE IF NOT EXISTS financial_data (
        id INTEGER AUTOINCREMENT PRIMARY KEY,
        symbol VARCHAR,
        data_type VARCHAR,  -- 'bs' for balance sheet, 'cf' for cash flow
        concept VARCHAR,
        info VARCHAR,
        unit VARCHAR,
        value FLOAT,
        CONSTRAINT fk_symbol FOREIGN KEY (symbol) 
            REFERENCES financial_metadata(symbol)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );
    """,
    dag=dag
)

# Load Metadata
load_metadata = SnowflakeOperator(
    task_id='load_metadata',
    snowflake_conn_id='snowflake_conn',
    sql="""
    COPY INTO financial_metadata (startDate, endDate, year, quarter, symbol, name, country, city)
    FROM (
        SELECT 
            $1:startDate::DATE,
            $1:endDate::DATE,
            $1:year::INT,
            $1:quarter::STRING,
            $1:symbol::STRING,
            $1:name::STRING,
            $1:country::STRING,
            $1:city::STRING
        FROM @my_s3_stage/2022q4/ (PATTERN => '.*\.json')
    )
    FILE_FORMAT = json_format
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    """,
    dag=dag
)

# Load Balance Sheet Data
# Load Balance Sheet Data
load_balance_sheet = SnowflakeOperator(
    task_id='load_balance_sheet',
    snowflake_conn_id='snowflake_conn',
    sql="""
    -- Create temporary table for raw data
    CREATE OR REPLACE TEMPORARY TABLE tmp_raw_json (
        raw_json VARIANT
    );
    
    -- Load raw JSON data
    COPY INTO tmp_raw_json (raw_json)
    FROM @my_s3_stage/2022q4/
    FILE_FORMAT = json_format
    PATTERN = '.*\.json'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    
    -- Transform and insert data
    INSERT INTO financial_data (symbol, data_type, concept, info, unit, value)
    SELECT 
        raw_json:symbol::STRING as symbol,
        'bs' as data_type,
        bs.value:concept::STRING as concept,
        bs.value:info::STRING as info,
        bs.value:unit::STRING as unit,
        bs.value:value::FLOAT as value
    FROM tmp_raw_json,
    LATERAL FLATTEN(input => raw_json:data:bs) bs;
    
    -- Clean up
    DROP TABLE IF EXISTS tmp_raw_json;
    """,
    dag=dag
)

# Load Cash Flow Data
load_cash_flow = SnowflakeOperator(
    task_id='load_cash_flow',
    snowflake_conn_id='snowflake_conn',
    sql="""
    -- Create temporary table for raw data
    CREATE OR REPLACE TEMPORARY TABLE tmp_raw_json (
        raw_json VARIANT
    );
    
    -- Load raw JSON data
    COPY INTO tmp_raw_json (raw_json)
    FROM @my_s3_stage/2022q4/
    FILE_FORMAT = json_format
    PATTERN = '.*\.json'
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;
    
    -- Transform and insert data
    INSERT INTO financial_data (symbol, data_type, concept, info, unit, value)
    SELECT 
        raw_json:symbol::STRING as symbol,
        'cf' as data_type,
        cf.value:concept::STRING as concept,
        cf.value:info::STRING as info,
        cf.value:unit::STRING as unit,
        cf.value:value::FLOAT as value
    FROM tmp_raw_json,
    LATERAL FLATTEN(input => raw_json:data:cf) cf;
    
    -- Clean up
    DROP TABLE IF EXISTS tmp_raw_json;
    """,
    dag=dag
)
# Validate Data Load
validate_load = SnowflakeOperator(
    task_id='validate_load',
    snowflake_conn_id='snowflake_conn',
    sql="""
    -- Check for proper data loading
    SELECT 
        'Metadata Count' as check_type,
        COUNT(*) as record_count,
        COUNT(DISTINCT symbol) as unique_symbols
    FROM financial_metadata
    UNION ALL
    SELECT 
        'Financial Data Count',
        COUNT(*),
        COUNT(DISTINCT symbol)
    FROM financial_data;
    
    -- Check for data consistency
    SELECT 
        'Orphaned Records' as check_type,
        COUNT(*) as count
    FROM financial_data fd
    LEFT JOIN financial_metadata fm ON fd.symbol = fm.symbol
    WHERE fm.symbol IS NULL;
    """,
    dag=dag
)

# Set up task dependencies
init_database >>create_stage >> create_tables >> load_metadata >> [load_balance_sheet, load_cash_flow] >> validate_load