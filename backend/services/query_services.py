from sqlalchemy.exc import SQLAlchemyError
from fastapi import HTTPException
from datetime import datetime
from backend.database.snowflake_connection import SnowflakeConnection

class QueryService:
    @staticmethod
    def execute_query(query: str):
        try:
            start_time = datetime.now()
            engine = SnowflakeConnection.create_engine()
            
            with engine.connect() as connection:
                result = connection.execute(query)
                data = [dict(row) for row in result]
                columns = list(result.keys())
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "data": data,
                "columns": columns,
                "execution_time": execution_time
            }
        
        except SQLAlchemyError as e:
            raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")