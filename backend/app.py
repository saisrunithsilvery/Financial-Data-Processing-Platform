from fastapi import FastAPI
import requests
import os

app = FastAPI()

# Change this based on your Docker network
AIRFLOW_URL = "http://localhost:8080/api/v1/dags/loading_raw_data_from_S3_to_Snowflake/dagRuns"
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

@app.post("/trigger_dag/")
async def trigger_dag(s3_folder: str):
    """Trigger Airflow DAG and pass a dynamic S3 folder path"""
    
    payload = {
    "conf": {
        "s3_folder": s3_folder  # ✅ Ensure this variable exists before using it
    }
    }


    response = requests.post(
        AIRFLOW_URL,
        json=payload,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)
    )

    if response.status_code in [200, 201, 202]:
        return {"message": "DAG triggered successfully!", "s3_folder": s3_folder}
    else:
        return {"error": "Failed to trigger DAG", "details": response.json()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="8000")