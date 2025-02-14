from diagrams import Cluster, Diagram
from diagrams.onprem.client import Users
from diagrams.onprem.compute import Server
from diagrams.onprem.database import PostgreSQL
from diagrams.aws.storage import S3
from diagrams.saas.analytics import Snowflake
from diagrams.programming.framework import FastAPI
from diagrams.programming.language import Python
from diagrams.onprem.workflow import Airflow  # Corrected import path
from diagrams.custom import Custom

with Diagram("Financial Data Processing Platform", show=False, outformat="png", filename="financial_data_processing_platform"):
    users = Users("User")
    frontend = FastAPI("Streamlit UI")
    backend = FastAPI("FastAPI Backend")
    data_pipeline = Airflow("Apache Airflow ETL")
    validation = Custom("DBT", "./dvt_icon.png")  # Ensure correct path for icon
    storage = S3("AWS S3")
    database = Snowflake("Snowflake DB")
    data_source = Custom("SEC Financial Data", "./sec_icon.png")  # Ensure correct path for icon
    
    users >> frontend >> backend
    backend >> database
    backend >> storage
    backend >> data_pipeline
    data_pipeline >> database
    data_pipeline >> storage
    data_pipeline >> validation
    validation >> database
    data_source >> data_pipeline
