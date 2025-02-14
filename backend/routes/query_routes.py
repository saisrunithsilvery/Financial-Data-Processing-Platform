from fastapi import APIRouter, HTTPException
from models.query_model import QueryRequest
from controllers.query_controller import query_controller
from typing import Dict, Any

router = APIRouter()

@router.post("/")
async def extract_data(request: QueryRequest) -> Dict[str, Any]:
    """
    Trigger data extraction pipeline
    
    Returns:
        Dict containing pipeline run information and tracking ID
    """
    try:
        print(f"Received request: {request}")
        
        result = await query_controller.process_extraction(request)
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
        result = await query_controller.get_extraction_status(run_id)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error in check_status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/runs")
async def get_active_runs() -> Dict[str, Any]:
    """
    Get all active pipeline runs
    
    Returns:
        Dict containing list of active pipeline runs
    """
    try:
        result = await query_controller.get_active_runs()
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error in get_active_runs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))