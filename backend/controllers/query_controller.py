from fastapi import HTTPException
from pydantic import BaseModel, validator
import os
from datetime import datetime
from typing import Dict, Any
import airflow_client.client
from airflow_client.client.api import dag_api, dag_run_api
from airflow_client.client.exceptions import NotFoundException
import logging
import time

class QueryRequest(BaseModel):
    year: int
    quarter: str
    
    @validator('quarter')
    def validate_quarter(cls, v):
        valid_quarters = ['Q1', 'Q2', 'Q3', 'Q4']
        if v not in valid_quarters:
            raise ValueError(f'Quarter must be one of {valid_quarters}')
        return v

    @validator('year')
    def validate_year(cls, v):
        if not 2000 <= v <= 2100:
            raise ValueError('Year must be between 2000 and 2100')
        return v

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryController:
    def __init__(self):
        """Initialize QueryController with Airflow client configuration"""
        self.airflow_host = os.getenv('AIRFLOW_HOST', 'airflow-webserver')
        self.airflow_port = os.getenv('AIRFLOW_PORT', '8080')
        self.airflow_username = os.getenv('AIRFLOW_USERNAME', 'admin')
        self.airflow_password = os.getenv('AIRFLOW_PASSWORD', 'admin')
        self.dag_id = os.getenv('AIRFLOW_DAG_ID', 's3_processing_dag')

        logger.info(
            "Initializing QueryController",
            extra={
                "airflow_host": f"http://{self.airflow_host}:{self.airflow_port}",
                "dag_id": self.dag_id
            }
        )
        
        try:
            # Configure Airflow client with the full API URL
            api_base_url = f"http://{self.airflow_host}:{self.airflow_port}/api/v1"
            self.configuration = airflow_client.client.Configuration(
                host=api_base_url,
                username=self.airflow_username,
                password=self.airflow_password
            )
            
            # Initialize API client with retry mechanism
            self.api_client = self._create_api_client_with_retry()
            self.dag_api = dag_api.DAGApi(self.api_client)
            self.dag_run_api = dag_run_api.DAGRunApi(self.api_client)
            
            # Verify DAG exists
            self._verify_dag_exists()
            
            logger.info("QueryController initialization successful")
            
        except Exception as e:
            logger.error(
                "Failed to initialize QueryController",
                extra={
                    "error_type": type(e).__name__,
                    "error_message": str(e)
                },
                exc_info=True
            )
            raise

    def _create_api_client_with_retry(self, max_retries=3):
        """Create API client with retry mechanism"""
        for attempt in range(max_retries):
            try:
                api_client = airflow_client.client.ApiClient(self.configuration)
                # Test connection
                test_api = dag_api.DAGApi(api_client)
                test_api.get_dags()
                return api_client
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to create API client after {max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"API client creation attempt {attempt + 1} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff

    def _verify_dag_exists(self):
        """Verify that the configured DAG exists"""
        try:
            self.dag_api.get_dag(dag_id=self.dag_id)
        except NotFoundException:
            error_msg = f"DAG '{self.dag_id}' not found in Airflow. Please ensure it's properly deployed."
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Error verifying DAG existence: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _sanitize_dag_run_id(self, dag_run_id: str) -> str:
        """Sanitize DAG run ID to ensure it's valid"""
        sanitized = dag_run_id.replace(" ", "_").replace(":", "-")
        return sanitized[:250]

    async def process_extraction(self, request: QueryRequest) -> Dict[str, Any]:
        """Trigger Airflow DAG for extraction process"""
        logger.info(
            "Processing extraction request",
            extra={
                "year": request.year,
                "quarter": request.quarter,
                "timestamp": datetime.now().isoformat()
            }
        )
        
        try:
            execution_date = datetime.now().isoformat()
            dag_run_id = self._sanitize_dag_run_id(f"extract_{execution_date}")
            
            logger.debug(
                "Preparing DAG run configuration",
                extra={
                    "dag_id": self.dag_id,
                    "dag_run_id": dag_run_id,
                    "execution_date": execution_date
                }
            )
            
            # Create DAG run with retry mechanism
            for attempt in range(3):
                try:
                    dag_run = self.dag_run_api.post_dag_run(
                        dag_id=self.dag_id,
                        dag_run={
                            'dag_run_id': dag_run_id,
                            'conf': {
                                'year': request.year,
                                'quarter': request.quarter,
                                'execution_date': execution_date
                            }
                        }
                    )
                    break
                except airflow_client.client.ApiException as e:
                    if attempt == 2 or e.status != 404:
                        raise
                    logger.warning(f"DAG run creation attempt {attempt + 1} failed, retrying...")
                    time.sleep(2 ** attempt)
            
            logger.info(
                "Successfully triggered Airflow DAG",
                extra={
                    "dag_run_id": dag_run.dag_run_id,
                    "status": "processing",
                    "execution_date": execution_date
                }
            )
            
            return {
                "message": "Pipeline triggered successfully",
                "status": "processing",
                "run_id": dag_run.dag_run_id,
                "execution_date": execution_date,
                "dag_id": self.dag_id
            }
            
        except airflow_client.client.ApiException as e:
            error_message = str(e)
            if e.status == 404:
                error_message = f"DAG '{self.dag_id}' not found or not accessible"
            elif e.status == 401:
                error_message = "Authentication failed. Please check Airflow credentials"
                
            logger.error(
                "Airflow API error during extraction process",
                extra={
                    "error_status": e.status,
                    "error_message": error_message,
                    "request_year": request.year,
                    "request_quarter": request.quarter
                },
                exc_info=True
            )
            raise HTTPException(
                status_code=e.status,
                detail=f"Airflow API error: {error_message}"
            )
        except Exception as e:
            logger.error(
                "Unexpected error during extraction process",
                extra={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "request_year": request.year,
                    "request_quarter": request.quarter
                },
                exc_info=True
            )
            raise HTTPException(
                status_code=500,
                detail=f"Failed to trigger pipeline: {str(e)}"
            )

# Create controller instance
logger.info("Creating QueryController instance")
query_controller = QueryController()