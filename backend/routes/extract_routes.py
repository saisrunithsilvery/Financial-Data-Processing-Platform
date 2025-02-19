from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from backend.models.query_model import QueryRequest
from backend.controllers.extract_controller import ExtractionController
from typing import Dict, Any
from logging import getLogger

logger = getLogger(__name__)
router = APIRouter()
controller = ExtractionController()

@router.post("/extract")
async def extract_data(request: QueryRequest) -> JSONResponse:
    """
    Trigger data extraction pipeline
    
    Args:
        request: QueryRequest object containing extraction parameters
            - year: int
            - quarter: str
            - way: str ('RAW' or 'JSON')
    
    Returns:
        JSONResponse containing:
            - status: Process status
            - execution_date: Timestamp of execution
            - files: List of processed files (if available)
            - snowflake_status: Snowflake pipeline status (if available)
    """
    try:
        logger.info(f"Received extraction request: {request}")
        result = await controller.process_extraction(request)
        logger.info(f"Extraction process completed successfully: {result}")
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result
        )
    except HTTPException as e:
        logger.error(f"HTTP error in extract_data: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in extract_data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )

@router.get("/status/{execution_date}")
async def check_status(execution_date: str) -> JSONResponse:
    """
    Check status of extraction pipeline
    
    Args:
        execution_date: ISO formatted date string of the execution
    
    Returns:
        JSONResponse containing:
            - status: Current status of the process
            - execution_date: Original execution timestamp
            - message: Status description
    """
    try:
        logger.info(f"Checking status for execution date: {execution_date}")
        result = await controller.get_status(execution_date)
        logger.info(f"Status check completed: {result}")
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=result
        )
    except HTTPException as e:
        logger.error(f"HTTP error in check_status: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in check_status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )