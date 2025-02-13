# airflow/dags/s3_processing_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import zipfile
from io import BytesIO
import sys
import os

# Add path for importing snowflake pipeline
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from snowflake.run_pipeline import PipelineRunner

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# S3 Configuration
s3_client = boto3.client('s3')
BUCKET_NAME = 'damgassign02'

def check_files(**context):
    """Check if files exist in S3"""
    year = context['dag_run'].conf.get('year')
    quarter = context['dag_run'].conf.get('quarter')
    if not year or not quarter:
        raise ValueError("Year and quarter must be provided")

    quarter_lower = str(quarter).lower().replace('q', '')
    unzipped_pattern = f"unziped_folder/{year}q{quarter_lower}/"
    finance_pattern = f"Finance/{year}q{quarter_lower}/"
    
    # Check unzipped folder first
    unzipped_response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=unzipped_pattern
    )
    
    if 'Contents' in unzipped_response:
        context['task_instance'].xcom_push(
            key='file_status',
            value={'exists': True, 'location': 'unzipped'}
        )
        return True
    
    # Check Finance folder
    finance_response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=finance_pattern
    )
    
    if 'Contents' in finance_response:
        file_key = finance_response['Contents'][0]['Key']
        context['task_instance'].xcom_push(
            key='file_status',
            value={
                'exists': True,
                'location': 'finance',
                'file_key': file_key
            }
        )
        return True
        
    raise ValueError(f"No files found for {year}Q{quarter}")

def process_files(**context):
    """Process and unzip files if needed"""
    year = context['dag_run'].conf.get('year')
    quarter = str(context['dag_run'].conf.get('quarter'))
    
    file_status = context['task_instance'].xcom_pull(
        key='file_status',
        task_ids='check_files'
    )
    
    if not file_status:
        raise ValueError("No file status found")
        
    if file_status['location'] == 'unzipped':
        return "Files already processed"
    
    quarter_lower = quarter.lower().replace('q', '')
    file_key = file_status['file_key']
    
    # Get and process zip file
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        zip_content = response['Body'].read()
        
        uploaded_files = []
        with BytesIO(zip_content) as zip_buffer:
            with zipfile.ZipFile(zip_buffer) as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith('/'):
                        continue
                        
                    with zip_ref.open(file_name) as file:
                        file_content = file.read()
                        target_key = f'unziped_folder/{year}q{quarter_lower}/{file_name}'
                        
                        s3_client.put_object(
                            Bucket=BUCKET_NAME,
                            Key=target_key,
                            Body=file_content
                        )
                        uploaded_files.append(target_key)
        
        context['task_instance'].xcom_push(
            key='processed_files',
            value=uploaded_files
        )
        return f"Processed {len(uploaded_files)} files"
        
    except Exception as e:
        raise Exception(f"Error processing files: {str(e)}")

def run_snowflake_pipeline(**context):
    """Execute Snowflake pipeline"""
    try:
        year = context['dag_run'].conf.get('year')
        quarter = context['dag_run'].conf.get('quarter')
        quarter_num = int(str(quarter).lower().replace('q', ''))
        
        pipeline = PipelineRunner(
            config_path='snowflake/config.yaml',
            year=year,
            quarter=quarter_num
        )
        
        success = pipeline.run_pipeline()
        if not success:
            raise Exception("Pipeline execution failed")
            
        return "Pipeline executed successfully"
        
    except Exception as e:
        raise Exception(f"Pipeline error: {str(e)}")

# Create DAG
with DAG(
    'process_files_dag',
    default_args=default_args,
    description='Process S3 files and run Snowflake pipeline',
    schedule_interval=None,  # Only triggered manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['s3', 'snowflake'],
) as dag:
    
    # Task 1: Check if files exist
    check_files = PythonOperator(
        task_id='check_files',
        python_callable=check_files,
        provide_context=True,
    )
    
    # Task 2: Process files if needed
    process_files = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        provide_context=True,
    )
    
    # Task 3: Run Snowflake pipeline
    run_pipeline = PythonOperator(
        task_id='run_pipeline',
        python_callable=run_snowflake_pipeline,
        provide_context=True,
    )
    
    # Set task dependencies
    check_files >> process_files >> run_pipeline