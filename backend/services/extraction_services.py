from fastapi import HTTPException
from datetime import datetime
from typing import Dict, Any, List, Optional
from services.s3_services import S3Service
from services.snowflake_services import SnowflakeService

class ExtractionService:
    def __init__(self):
        self.s3_service = S3Service()
        self.snowflake_service = SnowflakeService()

    async def check_file_exists(self, prefix: str, year: int, quarter: str) -> Optional[List[str]]:
        """Check if files exist in the given prefix"""
        return await self.s3_service.check_file_exists(prefix, year, quarter)

    async def find_zip_file(self, prefix: str, year: int, quarter: str) -> Optional[str]:
        """Find zip file in the given prefix"""
        return await self.s3_service.find_zip_file(prefix, year, quarter)

    async def unzip_and_upload(self, file_key: str, year: int, quarter: str) -> List[str]:
        """Download zip file, unzip it, and upload contents"""
        return await self.s3_service.unzip_and_upload(file_key, year, quarter)

    async def trigger_snowflake_pipeline(self, year: int, quarter: str, way: str) -> Dict[str, Any]:
        """Trigger Snowflake data loading pipeline"""
        return await self.snowflake_service.trigger_pipeline(year, quarter, way)

    async def get_extraction_status(self, execution_date: str) -> Dict[str, Any]:
        """Get status of extraction process"""
        return await self.snowflake_service.get_pipeline_status(execution_date)

    def _create_response(self, message: str, files: List[str] = None, 
                        source_file: str = None, uploaded_files: List[str] = None,
                        snowflake_status: Dict[str, Any] = None) -> Dict[str, Any]:
        """Create standardized response"""
        response = {
            "message": message,
            "status": "completed",
            "execution_date": datetime.now().isoformat()
        }
        
        if files:
            response["files"] = files
        if source_file:
            response["source_file"] = source_file
        if uploaded_files:
            response["uploaded_files"] = uploaded_files
        if snowflake_status:
            response["snowflake_status"] = snowflake_status
            
        return response