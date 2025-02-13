from pydantic import BaseModel, validator

class QueryRequest(BaseModel):
    year: int
    quarter: str
    
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