from pydantic import BaseModel
from functools import lru_cache

class Settings(BaseModel):
    """Application settings"""
    S3_BUCKET_NAME: str = "damgassign02"
    AIRFLOW_URL: str = "http://airflow-webserver:8080/api/v1/dags/"

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    """Cache and return settings instance"""
    return Settings()