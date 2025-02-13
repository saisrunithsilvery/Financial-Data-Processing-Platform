from pydantic import BaseModel

class QueryRequest(BaseModel):
    year: int
    quarter: str
