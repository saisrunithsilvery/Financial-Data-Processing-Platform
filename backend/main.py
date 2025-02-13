from fastapi import FastAPI
from routes import query_routes

app = FastAPI()

# Include routes
app.include_router(query_routes.router)

@app.get("/")
def root():
    return {"message": "FastAPI is running!"}
