# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.query_routes import router as query_router
from typing import Dict

app = FastAPI(
    title="SEC Data Analysis API",
    description="API for SEC XBRL data extraction and analysis",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8501",  # Streamlit's default port
        "http://127.0.0.1:8501"   # Alternative local address
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers with proper prefixes

app.include_router(query_router, prefix="/extract", tags=["extraction"])

# Health check endpoint at root level
@app.get("/", tags=["health"])
async def root() -> Dict[str, str]:
    """Root endpoint for API health check"""
    return {
        "status": "online",
        "service": "SEC Data Analysis API",
        "version": "1.0.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
        log_level="info"
    )