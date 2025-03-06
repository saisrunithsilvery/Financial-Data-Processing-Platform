from enum import Enum
from fastapi import HTTPException
import httpx
from typing import Dict, Any, Optional, List
from datetime import datetime
from backend.config.settings import Settings

class PipelineMode(Enum):
    RAW = "RAW"
    JSON = "JSON"
    NORMALIZED = "NORMALIZED"

class SnowflakeService:
    AIRFLOW_URL: str = "http://airflow-webserver:8080/api/v1/dags/"
    
    def __init__(self):
        self.settings = Settings()
        
        self.dag_mappings = {
            PipelineMode.RAW: ["loading_raw_data_from_S3_to_Snowflake"],
            PipelineMode.JSON: ["s3_to_snowflake_financial_data"],
            PipelineMode.NORMALIZED: [
                "loading_raw_data_from_S3_to_Snowflake",  # Run RAW first
                "dbt_dag"  # Then run DBT normalization
            ]
        }

    def _get_airflow_urls(self, mode: PipelineMode) -> List[str]:
        """
        Get the appropriate Airflow URL(s) based on pipeline mode
        
        Args:
            mode: The pipeline mode to get URLs for
            
        Returns:
            List of Airflow API URLs
            
        Raises:
            ValueError: If an unsupported pipeline mode is provided
        """
        if mode not in self.dag_mappings:
            raise ValueError(f"Unsupported pipeline mode: {mode}")
        return [f"{self.AIRFLOW_URL}{dag_id}/dagRuns" for dag_id in self.dag_mappings[mode]]

    async def trigger_pipeline(self, year: int, quarter: str, ways: str) -> Dict[str, Any]:
        """
        Trigger Snowflake data loading pipeline
        
        Args:
            year: The year for data processing
            quarter: The quarter for data processing
            ways: Pipeline execution mode (RAW/JSON/NORMALIZED)
        
        Returns:
            Dict containing pipeline execution results
            
        Raises:
            HTTPException: For various error conditions (400, 500, 504)
        """
        try:
            print("Year", year)
            print("Quarter", quarter)
            # Validate inputs
            if not isinstance(year, int) or year < 2006 or year > 2100:
                raise ValueError(f"Invalid year: {year}")
            if quarter not in ['Q1', 'Q2', 'Q3', 'Q4']:
                raise ValueError(f"Invalid quarter: {quarter}")
            
            mode = PipelineMode(ways.upper())
            urls = self._get_airflow_urls(mode)
            results = []
            
            print(f"Triggering Snowflake pipeline for {mode} mode")
            
            async with httpx.AsyncClient() as client:
                for idx, url in enumerate(urls):
                    try:
                        # Determine current DAG being triggered
                        current_dag = self.dag_mappings[mode][idx]
                        is_dbt_dag = current_dag == "dbt_dag"
                        
                        # Generate a unique DAG run ID
                        dag_run_id = f"{current_dag}_{year}_{quarter}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        
                        # Prepare payload based on the current DAG
                        if is_dbt_dag:
                            # For DBT DAG, we need to provide additional configuration
                            payload = self._prepare_dbt_payload(year, quarter)
                        else:
                            # For data loading DAGs
                            payload = self._prepare_payload(year, quarter)
                        
                        # Set the DAG run ID
                        payload['dag_run_id'] = dag_run_id
                        
                        print(f"Triggering DAG at {url}")
                        print(f"Payload: {payload}")
                        
                        AIRFLOW_USERNAME = "airflow"
                        AIRFLOW_PASSWORD = "airflow"
                        
                        response = await client.post(
                            url,
                            json=payload,
                            auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
                            timeout=300  # 5 minutes timeout
                        )
                      
                        if response.status_code == 401:
                            raise HTTPException(
                                status_code=401,
                                detail="Authentication failed for Airflow API"
                            )
                        
                        if response.status_code != 200:
                            error_msg = self._parse_error_response(response)
                            raise HTTPException(
                                status_code=response.status_code,
                                detail=f"Airflow API error: {error_msg}"
                            )
                        
                        result = response.json()
                        result.update({
                            'execution_config': {
                                'dag_id': current_dag,
                                'year': year,
                                'quarter': quarter,
                                'dag_run_id': dag_run_id
                            }
                        })
                        results.append(result)
                        
                        # If this is the first DAG in NORMALIZED mode, wait for it to complete
                        # before triggering the DBT DAG (optional, depending on your requirements)
                        if mode == PipelineMode.NORMALIZED and idx == 0:
                            # You might want to add a mechanism to wait for the first DAG to complete
                            # or at least reach a certain state before proceeding
                            # This is just a placeholder for now
                            print("Raw data loading triggered, proceeding to DBT transformation")
                            
                    except httpx.RequestError as e:
                        raise HTTPException(
                            status_code=503,
                            detail=f"Failed to connect to Airflow API: {str(e)}"
                        )
            
            return results if mode == PipelineMode.NORMALIZED else results[0]

        except httpx.TimeoutException:
            raise HTTPException(
                status_code=504,
                detail="Request to Airflow API timed out"
            )
        except ValueError as ve:
            raise HTTPException(
                status_code=400,
                detail=str(ve)
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Internal server error: {str(e)}"
            )

    def _prepare_payload(self, year: int, quarter: str) -> Dict[str, Any]:
        """
        Prepare payload for data loading DAGs
        
        Args:
            year: The year for data processing
            quarter: The quarter for data processing (Q1, Q2, Q3, Q4)
        
        Returns:
            Dict containing the prepared payload with s3_folder in format "year/q1"
        """
        # Convert Q1 to q1, Q2 to q2, etc.
        formatted_quarter = quarter.lower()
        s3_folder = f"{year}{formatted_quarter}"
        
        return {
            "conf": {
                "s3_folder": s3_folder
            }
        }
        
    def _prepare_dbt_payload(self, year: int, quarter: str) -> Dict[str, Any]:
        """
        Prepare payload specifically for DBT DAG
        
        Args:
            year: The year for data processing
            quarter: The quarter for data processing (Q1, Q2, Q3, Q4)
        
        Returns:
            Dict containing the prepared payload with specific DBT configuration
        """
        formatted_quarter = quarter.lower()
        s3_folder = f"{year}{formatted_quarter}"
        
        return {
            "conf": {
                "s3_folder": s3_folder,
                "dbt_vars": {
                    "year": year,
                    "quarter": formatted_quarter,
                    "run_date": datetime.now().strftime('%Y-%m-%d')
                }
            }
        }
        
    def _parse_error_response(self, response: httpx.Response) -> str:
        """Parse error response from Airflow API"""
        try:
            error_data = response.json()
            return error_data.get('detail', error_data.get('message', str(response.text)))
        except Exception:
            return str(response.text)

    async def get_pipeline_status(self, dag_run_id: str) -> Dict[str, Any]:
        """
        Get status of Snowflake pipeline execution
        
        Args:
            dag_run_id: The DAG run ID to check status for
            
        Returns:
            Dict containing the pipeline status
            
        Raises:
            HTTPException: If status check fails
        """
        try:
            # Extract the DAG ID from the dag_run_id (assuming format: "{dag_id}_{year}_{quarter}_{timestamp}")
            dag_id = dag_run_id.split("_")[0]
            
            async with httpx.AsyncClient() as client:
                url = f"{self.AIRFLOW_URL}{dag_id}/dagRuns/{dag_run_id}"
                
                AIRFLOW_USERNAME = "airflow"
                AIRFLOW_PASSWORD = "airflow"
                
                response = await client.get(
                    url,
                    auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
                    timeout=60
                )
                
                if response.status_code == 401:
                    raise HTTPException(
                        status_code=401,
                        detail="Authentication failed for Airflow API"
                    )
                
                if response.status_code != 200:
                    error_msg = self._parse_error_response(response)
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Airflow API error: {error_msg}"
                    )
                
                return response.json()
                
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get pipeline status: {str(e)}"
            )