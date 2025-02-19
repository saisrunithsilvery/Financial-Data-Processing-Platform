from fastapi import APIRouter
from backend.models.query_model import SQLQueryRequest, QueryResponse
from backend.services.query_services import QueryService

router = APIRouter()

class QueryController:
    @router.post("/query", response_model=QueryResponse)
    async def execute_query(request: SQLQueryRequest):
        return QueryService.execute_query(request.query)
