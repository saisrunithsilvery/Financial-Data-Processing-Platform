from fastapi import APIRouter, HTTPException
from models.query_model import QueryRequest
from controllers.query_controller import get_extraction_status, process_extraction
from typing import Dict, Any

router = APIRouter()

@router.post("/extract")
async def extract_data(request: QueryRequest) -> Dict[str, Any]:
    """
    Trigger data extraction pipeline
    
    Returns:
        Dict containing pipeline run information and tracking ID
    """
    try:
        print(f"Received request: {request}")
        
        result = await process_extraction(request)
        print(f"Pipeline result: {result}")
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error in extract_data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{run_id}")
async def check_status(run_id: str) -> Dict[str, Any]:
    """
    Check status of extraction pipeline
    
    Args:
        run_id: Airflow DAG run identifier
    
    Returns:
        Dict containing current pipeline status
    """
    try:
        result = await get_extraction_status(run_id)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error in check_status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))