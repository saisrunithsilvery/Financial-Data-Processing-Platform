from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dbt_dag = DAG(
    dag_id="dbt_dag",
    default_args=default_args,
    description="Run dbt transformations in Snowflake",
    schedule_interval=None,
    catchup=False,
)

# Step 1: Initialize Database and Schema in Snowflake
init_database = SnowflakeOperator(
    task_id="init_database",
    snowflake_conn_id="snowflake_conn",
    sql="""
    USE ROLE ACCOUNTADMIN;
    CREATE DATABASE IF NOT EXISTS FINANCIAL_DB;
    USE DATABASE FINANCIAL_DB;
    CREATE SCHEMA IF NOT EXISTS FINANCIAL_SCHEMA;
    USE SCHEMA FINANCIAL_SCHEMA;
    """,
    dag=dbt_dag,
)

# Step 2: Debug dbt Configuration
# dbt_debug = BashOperator(
#     task_id="dbt_debug",
#     bash_command="dbt debug --profiles-dir /opt/airflow/dags/data_validate --project-dir /opt/airflow/dags/data_validate",
#     dag=dbt_dag,
# )

# Step 3: Run dbt Models
run_dbt_models = BashOperator(
    task_id="run_dbt_models",
    bash_command="set -e; dbt run --profiles-dir /opt/airflow/dags/ --project-dir /opt/airflow/dags/ --target dev",
    dag=dbt_dag,
)

# Step 4: Test dbt Models
test_dbt_models = BashOperator(
    task_id="test_dbt_models",
    bash_command="set -e; dbt test --profiles-dir /opt/airflow/dags/ --project-dir /opt/airflow/dags/ --target dev",
    dag=dbt_dag,
)

# Define Task Dependencies
init_database >>  run_dbt_models >> test_dbt_models
