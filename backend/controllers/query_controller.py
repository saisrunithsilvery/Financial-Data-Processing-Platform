from fastapi import APIRouter
from ..models.query_models import QueryRequest, QueryResponse
from ..services.query_service import QueryService

router = APIRouter()

class QueryController:
    @router.post("/query", response_model=QueryResponse)
    async def execute_query(request: QueryRequest):
        return QueryService.execute_query(request.query)
