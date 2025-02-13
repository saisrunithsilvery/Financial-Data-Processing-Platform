from fastapi import APIRouter, HTTPException
from models.query_model import QueryRequest
from controllers.query_controller import process_extraction, get_extraction_status

router = APIRouter()

@router.post("/extract")
async def extract_data(request: QueryRequest):
    try:
        result = await process_extraction(request)
        return result
    except Exception as e:
        print(f"Error occurred: {str(e)}")  # Add this line
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/extract/status/{execution_date}")
async def check_status(execution_date: str):
    """
    Endpoint to check extraction status
    """
    try:
        result = await get_extraction_status(execution_date)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
