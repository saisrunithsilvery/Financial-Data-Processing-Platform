from fastapi import HTTPException
from datetime import datetime
from typing import Dict, Any
from backend.models.query_model import QueryRequest
from backend.services.extraction_services import ExtractionService

class ExtractionController:
    def __init__(self):
        self.service = ExtractionService()

    async def process_extraction(self, request: QueryRequest) -> Dict[str, Any]:
        try:
            execution_date = datetime.now().isoformat()
            print(f"Processing request: {request}")
            
            if request.way == 'JSON':
                unzipped_files = await self.service.check_file_exists('Extracted_json_files', request.year, request.quarter)
                if unzipped_files:
                    print("Triggering Snowflake pipeline")
                    pipeline_status = await self.service.trigger_snowflake_pipeline(
                    request.year,
                    request.quarter,
                    request.way
                )
                    return {
                        "message": "Files already exist in unzipped folder",
                        "status": "completed",
                        "files": unzipped_files,
                        "execution_date": execution_date
                    }
            
            elif request.way == 'RAW':
                unzipped_files = await self.service.check_file_exists('unziped_folder', request.year, request.quarter)
                if unzipped_files:
                    print("Triggering Snowflake pipeline")
                    pipeline_status = await self.service.trigger_snowflake_pipeline(
                    request.year,
                    request.quarter,
                    request.way
                )
                    return {
                        "message": "Files already exist in unzipped folder",
                        "status": "completed",
                        "files": unzipped_files,
                        "execution_date": execution_date
                    }
            
            zip_file = await self.service.find_zip_file('Finance', request.year, request.quarter)
            
            if not zip_file:
                raise HTTPException(
                    status_code=404,
                    detail=f"Zip file not found for year {request.year}, quarter {request.quarter}"
                )
            
            uploaded_files = await self.service.unzip_and_upload(
                zip_file,
                request.year,
                request.quarter
            )
            
            if uploaded_files:
                print("Triggering Snowflake pipeline")
                pipeline_status = await self.service.trigger_snowflake_pipeline(
                    request.year,
                    request.quarter,
                    request.way
                )
            
            return {
                "message": "Files processed and data loaded to Snowflake",
                "status": "completed",
                "source_file": zip_file,
                "uploaded_files": uploaded_files,
                "snowflake_status": pipeline_status,
                "execution_date": execution_date
            }
            
        except HTTPException as e:
            raise e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    async def get_status(self, execution_date: str) -> Dict[str, Any]:
        return await self.service.get_extraction_status(execution_date)