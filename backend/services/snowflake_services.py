from fastapi import HTTPException
import httpx
from typing import Dict, Any
from config.settings import Settings

class SnowflakeService:
    def __init__(self):
        self.settings = Settings()
        self.AIRFLOW_URL = self.settings.AIRFLOW_URL

    async def trigger_pipeline(self, year: int, quarter: str, ways: str) -> Dict[str, Any]:
        """Trigger Snowflake data loading pipeline"""
        try:
            payload = self._prepare_payload(year, quarter, ways)
            print(f"Triggering Snowflake pipeline with configuration: {payload}")
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.AIRFLOW_URL,
                    json=payload,
                    timeout=300  # 5 minutes timeout
                )
                
                if response.status_code == 200:
                    result = response.json()
                    result['execution_config'] = {
                        'mode': payload['execution_mode'],
                        'type': payload['pipeline_type']
                    }
                    return result
                else:
                    raise HTTPException(
                        status_code=response.status_code,
                        detail=f"Snowflake service error: {response.text}"
                    )
                    
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Snowflake pipeline timed out")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to execute Snowflake pipeline: {str(e)}")

    def _prepare_payload(self, year: int, quarter: str, ways: str) -> Dict[str, Any]:
        """Prepare payload for Snowflake pipeline based on execution mode"""
        if ways.upper() == "RAW":
            return {
                "year": year,
                "quarter": quarter,
                "execution_mode": "raw",
                "pipeline_type": "loading_raw_data_from_S3_to_Snowflake" if ways.upper().endswith('DAG') else "direct",
                "trigger_mode": "trigger" if ways.upper() == 'TRIGGER' else "standard"
            }
        else:  # JSON DAG case
            return {
                "year": year,
                "quarter": quarter,
                "execution_mode": "json",
                "pipeline_type": "s3_to_snowflake_financial_data",
                "dag_config": {
                    "format": "json",
                    "validation": True,
                    "optimize": True
                }
            }

    async def get_pipeline_status(self, execution_date: str) -> Dict[str, Any]:
        """Get status of Snowflake pipeline execution"""
        try:
            return {
                "status": "completed",
                "execution_date": execution_date,
                "message": "File processing is done synchronously"
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error checking status: {str(e)}")