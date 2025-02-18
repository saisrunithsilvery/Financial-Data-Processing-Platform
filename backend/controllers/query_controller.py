from fastapi import APIRouter
from models.query_model import SQLQueryRequest, QueryResponse
from services.query_services import QueryService

router = APIRouter()

class QueryController:
    @router.post("/query", response_model=QueryResponse)
    async def execute_query(request: SQLQueryRequest):
        return QueryService.execute_query(request.query)
