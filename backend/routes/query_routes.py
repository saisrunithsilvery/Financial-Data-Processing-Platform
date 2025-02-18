from fastapi import APIRouter, HTTPException
from models.query_model import SQLQueryRequest, QueryResponse
from services.query_services import QueryService

query_router = APIRouter(
    prefix="/queries",
    tags=["queries"],
    responses={404: {"description": "Not found"}},
)

@query_router.post("/execute", response_model=SQLQueryRequest)
async def execute_query(request: SQLQueryRequest):
    """Execute a custom SQL query"""
    return QueryService.execute_query(request.query)