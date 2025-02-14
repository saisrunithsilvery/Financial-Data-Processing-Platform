from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models import Variable
from datetime import datetime, timedelta
from query_controller import process_extraction
from run_pipeline import PipelineRunner
from models.query_model import QueryRequest

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_process_files(year, quarter, **context):
    """
    Extract and process files from S3
    """
    request = QueryRequest(year=year, quarter=quarter)
    result = process_extraction(request)
    # Store the execution date for downstream tasks
    context['task_instance'].xcom_push(key='execution_date', value=result['execution_date'])
    return result['uploaded_files']

def run_snowflake_pipeline(year, quarter, config_path, **context):
    """
    Run the Snowflake data pipeline
    """
    runner = PipelineRunner(
        config_path=config_path,
        year=year,
        quarter=quarter
    )
    runner.run_data_loading()
    runner.validate_loaded_data()

def create_finance_pipeline_dag(
    dag_id='finance_data_pipeline',
    schedule='0 0 1 */3 *',  # Run quarterly
    config_path='config.yaml'
):
    dag = DAG(
        dag_id,
        default_args=default_args,
        description='Finance data processing pipeline',
        schedule=schedule,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['finance', 'quarterly'],
    )

    with dag:
        # Calculate year and quarter based on execution date
        year = "{{ execution_date.year }}"
        quarter = "{{ (execution_date.month - 1) // 3 + 1 }}"
        
        # Check if source file exists in S3
        check_s3_file = S3KeySensor(
            task_id='check_s3_file',
            bucket_name='damgassign02',
            bucket_key=f'Finance/{year}q{quarter}/',
            poke_interval=300,  # Check every 5 minutes
            timeout=3600,  # Timeout after 1 hour
            mode='poke'
        )

        # Extract and process files
        extract_files = PythonOperator(
            task_id='extract_files',
            python_callable=extract_and_process_files,
            op_kwargs={
                'year': year,
                'quarter': quarter
            }
        )

        # Run Snowflake pipeline
        load_to_snowflake = PythonOperator(
            task_id='load_to_snowflake',
            python_callable=run_snowflake_pipeline,
            op_kwargs={
                'year': year,
                'quarter': quarter,
                'config_path': config_path
            }
        )

        # Define task dependencies
        check_s3_file >> extract_files >> load_to_snowflake

    return dag

# Create the DAG
finance_pipeline_dag = create_finance_pipeline_dag()