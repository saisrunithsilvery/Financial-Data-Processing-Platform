from fastapi import APIRouter, HTTPException
from models.query_model import SQLQueryRequest, QueryResponse
from services.query_services import QueryService
import time

query_router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)

@query_router.post("/execute", response_model=QueryResponse)
async def execute_query(request: SQLQueryRequest):
    """Execute a custom SQL query"""
    try:
        start_time = time.time()
        result = QueryService.execute_query(request.query)
        
        return QueryResponse(
            query=request.query,
            data=result["data"],
            columns=result["columns"],
            execution_time=result["execution_time"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))