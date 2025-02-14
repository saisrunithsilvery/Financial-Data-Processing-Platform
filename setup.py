from setuptools import setup, find_packages

setup(
    name="finance-data-pipeline",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "uvicorn",
        "apache-airflow",
        "boto3",
        # Add other dependencies
    ]
)