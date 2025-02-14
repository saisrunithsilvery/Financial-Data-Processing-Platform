from fastapi import HTTPException
from models.query_model import QueryRequest
from datetime import datetime
from typing import Dict, Any
from utils.s3_operations import s3_ops
import airflow_client.client
from airflow_client.client.api import dag_api, dag_run_api
from airflow_client.client.exceptions import NotFoundException
from config import AIRFLOW_CONFIG
import logging
import time

# Configure logging
logger = logging.getLogger(__name__)

class QueryController:
    def __init__(self):
        """Initialize QueryController with Airflow client configuration"""
        logger.info(
            "Initializing QueryController",
            extra={
                "airflow_host": AIRFLOW_CONFIG['host'],
                "dag_id": AIRFLOW_CONFIG['dag_id']
            }
        )
        
        try:
            # Remove trailing slash if present in host URL
            host = AIRFLOW_CONFIG['host'].rstrip('/')
            
            # Configure Airflow client
            self.configuration = airflow_client.client.Configuration(
                host=host,
                username=AIRFLOW_CONFIG['username'],
                password=AIRFLOW_CONFIG['password']
            )
            
            # Initialize API client with retry mechanism
            self.api_client = self._create_api_client_with_retry()
            self.dag_api = dag_api.DAGApi(self.api_client)
            self.dag_run_api = dag_run_api.DAGRunApi(self.api_client)
            self.dag_id = AIRFLOW_CONFIG['dag_id']
            
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
                    raise
                logger.warning(f"API client creation attempt {attempt + 1} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff

    def _verify_dag_exists(self):
        """Verify that the configured DAG exists"""
        try:
            self.dag_api.get_dag(dag_id=self.dag_id)
        except NotFoundException:
            raise ValueError(f"DAG '{self.dag_id}' not found in Airflow. Please ensure it's properly deployed.")
        except Exception as e:
            raise ValueError(f"Error verifying DAG existence: {str(e)}")

    def _sanitize_dag_run_id(self, dag_run_id: str) -> str:
        """Sanitize DAG run ID to ensure it's valid"""
        # Replace invalid characters and ensure proper length
        sanitized = dag_run_id.replace(" ", "_").replace(":", "-")
        return sanitized[:250]  # Airflow typically has a length limit

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

    # ... (rest of the methods remain the same) ...

# Create controller instance
logger.info("Creating QueryController instance")
query_controller = QueryController()