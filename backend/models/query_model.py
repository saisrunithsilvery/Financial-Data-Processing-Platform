# models/query_model.py
from pydantic import BaseModel, validator
from typing import List, Dict, Any
from datetime import datetime

class QueryRequest(BaseModel):
    year: int
    quarter: str
    way: str
    
    @validator('quarter')
    def validate_quarter(cls, v):
        valid_quarters = ['Q1', 'Q2', 'Q3', 'Q4']
        if v not in valid_quarters:
            raise ValueError(f'Quarter must be one of {valid_quarters}')
        return v

    @validator('year')
    def validate_year(cls, v):
        if not 2000 <= v <= 2100:
            raise ValueError('Year must be between 2000 and 2100')
        return v
    
    @validator('way')
    def validate_way(cls, v):
        valid_ways = ['RAW', 'JSON', 'NORMALIZED']
        if v.upper() not in valid_ways:
            raise ValueError(f'Way must be one of {valid_ways}')
        return v.upper()

class SQLQueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    query: str  # This is required but missing in response
    data: List[Dict]
    columns: List[str]
    execution_time: float

class ExtractionResponse(BaseModel):
    message: str
    status: str = 'success'
    timestamp: datetime = datetime.now()

class HealthCheckResponse(BaseModel):
    status: str
    database_connection: str
    timestamp: datetime = datetime.now()