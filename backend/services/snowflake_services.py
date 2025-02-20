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
        # Add these to your Settings class
        
        
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
            print("YEar", year)
            print("Quarter", quarter)
            # Validate inputs
            if not isinstance(year, int) or year < 1900 or year > 2100:
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
                        # Determine current mode for NORMALIZED pipeline
                        current_mode = (PipelineMode.RAW if idx == 0 else mode) if mode == PipelineMode.NORMALIZED else mode
                        
                        # Prepare payload and add dag_run_id
                        payload = self._prepare_payload(year, quarter)
                        payload['dag_run_id'] = f"{current_mode.value.lower()}_{year}_{quarter}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                        
                        print(f"Triggering DAG at {url}")
                        print(f"Payload: {payload}")
                        AIRFLOW_USERNAME= "airflow"
                        AIRFLOW_PASSWORD = "airflow"
                        print("Payload", payload)
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
                                'mode': payload['execution_mode'],
                                'type': payload['pipeline_type'],
                                'year': year,
                                'quarter': quarter,
                                'dag_run_id': payload['dag_run_id']
                            }
                        })
                        results.append(result)
                        
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
        Prepare payload for Snowflake pipeline based on execution mode
        
        Args:
            year: The year for data processing
            quarter: The quarter for data processing (Q1, Q2, Q3, Q4)
            mode: Pipeline execution mode
        
        Returns:
            Dict containing the prepared payload with s3_folder in format "year/q1"
        """
        # Convert Q1 to q1, Q2 to q2, etc.
        formatted_quarter = quarter.lower()
        s3_folder = f"{year}+{formatted_quarter}"
        
        return {
            "conf": {
                "s3_folder": s3_folder
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
            # In a real implementation, you would query Airflow API for the status
            return {
                "status": "completed",
                "dag_run_id": dag_run_id,
                "execution_date": datetime.now().isoformat(),
                "message": "Pipeline completed successfully"
            }
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get pipeline status: {str(e)}"
            )