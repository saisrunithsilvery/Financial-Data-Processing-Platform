from models.query_model import QueryRequest

def extract_data(request: QueryRequest):
    return {"message": f"Data extracted for Year: {request.year}, Quarter: {request.quarter}"}
