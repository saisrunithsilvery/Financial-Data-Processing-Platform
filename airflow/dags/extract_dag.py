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


extractDag = DAG(
    'loading_raw_data_from_S3_to_Snowflake',
    default_args=default_args,
    description='Extract financial raw data from S3 and load to Snowflake',
    schedule_interval=None,
    catchup=False
)

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
    dag=extractDag
)

create_stage = SnowflakeOperator(
    task_id='create_s3_stage',
    snowflake_conn_id='snowflake_conn',
    sql="""
    CREATE OR REPLACE STAGE my_s3_raw_stage
    STORAGE_INTEGRATION = S3_INT
    URL = 's3://damgassign02/unziped_folder/'
    FILE_FORMAT = (TYPE = csv);
    """,
    dag=extractDag
)

create_tables = SnowflakeOperator(
    task_id='create_tables',
    snowflake_conn_id='snowflake_conn',
    sql="""
    CREATE OR REPLACE TABLE TAG (
        tag VARCHAR NOT NULL,
        version VARCHAR NOT NULL,
        custom VARCHAR NOT NULL,  -- Boolean replaced with VARCHAR
        abstract VARCHAR NOT NULL, -- Boolean replaced with VARCHAR
        datatype VARCHAR,
        iord VARCHAR,
        crdr VARCHAR,
        tlabel VARCHAR,
        doc VARCHAR -- TEXT replaced with VARCHAR
    );

    CREATE OR REPLACE TABLE NUM (
        adsh VARCHAR NOT NULL,
        tag VARCHAR NOT NULL,
        version VARCHAR NOT NULL,
        ddate VARCHAR NOT NULL, -- DATE replaced with VARCHAR
        qtrs VARCHAR NOT NULL,  -- INT replaced with VARCHAR
        uom VARCHAR NOT NULL,
        segments VARCHAR, -- TEXT replaced with VARCHAR
        coreg VARCHAR,
        value VARCHAR,  -- DECIMAL replaced with VARCHAR
        footnote VARCHAR -- TEXT replaced with VARCHAR
    );

    CREATE OR REPLACE TABLE PRE (
        adsh VARCHAR,
        report VARCHAR, -- INT replaced with VARCHAR
        line VARCHAR, -- INT replaced with VARCHAR
        stmt VARCHAR,
        inpth VARCHAR, -- INT replaced with VARCHAR
        rfile VARCHAR,
        tag VARCHAR,
        version VARCHAR,
        plabel VARCHAR, -- TEXT replaced with VARCHAR
        negating VARCHAR -- INT replaced with VARCHAR
    );

    CREATE OR REPLACE TABLE SUB (
        adsh VARCHAR,           -- Accession Number
        cik VARCHAR,           -- Central Index Key
        name VARCHAR,         -- Company Name
        sic VARCHAR,           -- Standard Industrial Classification
        countryba VARCHAR,     -- Business Address Country
        stprba VARCHAR,       -- Business Address State/Province
        cityba VARCHAR,       -- Business Address City
        zipba VARCHAR,         -- Business Address ZIP
        bas1 VARCHAR,         -- Business Address Street 1
        bas2 VARCHAR,         -- Business Address Street 2
        baph VARCHAR,         -- Business Address Phone
        countryma VARCHAR,     -- Mailing Address Country
        stprma VARCHAR,       -- Mailing Address State/Province
        cityma VARCHAR,       -- Mailing Address City
        zipma VARCHAR,         -- Mailing Address ZIP
        mas1 VARCHAR,         -- Mailing Address Street 1
        mas2 VARCHAR,         -- Mailing Address Street 2
        countryinc VARCHAR,    -- Country of Incorporation
        stprinc VARCHAR,       -- State/Province of Incorporation
        ein VARCHAR,          -- Employer Identification Number
        former VARCHAR,       -- Former Company Name
        changed VARCHAR,       -- Date Changed
        afs VARCHAR,          -- Accounting Filing Status
        wksi VARCHAR,             -- Boolean replaced with VARCHAR
        fye VARCHAR,           -- Fiscal Year End
        form VARCHAR,         -- Form Type
        period VARCHAR,       -- DATE replaced with VARCHAR
        fy VARCHAR,               -- INT replaced with VARCHAR
        fp VARCHAR,            -- Fiscal Period
        filed VARCHAR,        -- DATE replaced with VARCHAR
        accepted VARCHAR,     -- TIMESTAMP replaced with VARCHAR
        prevrpt VARCHAR,       -- Boolean replaced with VARCHAR
        detail VARCHAR,        -- Boolean replaced with VARCHAR
        instance VARCHAR,    -- Instance Document
        nciks VARCHAR,        -- INT replaced with VARCHAR
        aciks VARCHAR       -- TEXT replaced with VARCHAR
    );
    """,
    dag=extractDag
)


load_tables = SnowflakeOperator(
    task_id='loading_tables',
    snowflake_conn_id='snowflake_conn',
    sql="""
    CREATE OR REPLACE FILE FORMAT TXT_FILE_FORMAT
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = '\t'
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

    COPY INTO SUB
    FROM @my_s3_raw_stage/{{ dag_run.conf.get('s3_folder', 'default_folder') }}/
    FILES = ('sub.txt')
    FILE_FORMAT = TXT_FILE_FORMAT;

    COPY INTO num
    FROM @my_s3_raw_stage/{{ dag_run.conf.get('s3_folder', 'default_folder') }}/
    FILES = ('num.txt')
    FILE_FORMAT = TXT_FILE_FORMAT;


    COPY INTO TAG
    FROM @my_s3_raw_stage/{{ dag_run.conf.get('s3_folder', 'default_folder') }}/
    FILES = ('tag.txt')
    FILE_FORMAT = TXT_FILE_FORMAT;


    COPY INTO pre
    FROM @my_s3_raw_stage/{{ dag_run.conf.get('s3_folder', 'default_folder') }}/
    FILES = ('pre.txt')
    FILE_FORMAT = TXT_FILE_FORMAT;

""",
    dag=extractDag
    
)

init_database >> create_stage >> create_tables >> load_tables
