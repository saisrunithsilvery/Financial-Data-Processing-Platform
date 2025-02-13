from fastapi import APIRouter
from models.query_model import QueryRequest
from controllers.query_controller import extract_data

router = APIRouter()

@router.post("/extract")
async def extract(request: QueryRequest):
    return extract_data(request)
