from fastapi import HTTPException
from models.query_model import QueryRequest
import boto3
from botocore.exceptions import ClientError
import zipfile
from io import BytesIO
from datetime import datetime
from typing import Optional, List, Dict, Any

s3_client = boto3.client('s3')
BUCKET_NAME = 'damgassign02'

async def check_file_exists(prefix: str, year: int, quarter: str) -> Optional[List[str]]:
    """
    Check if files exist in the given S3 prefix
    
    Args:
        prefix (str): S3 prefix to search in
        year (int): Year to search for
        quarter (str): Quarter to search for
    
    Returns:
        Optional[List[str]]: List of file keys if found, None otherwise
    """
    try:
        quarter_lower = quarter.lower().replace('q', '')
        file_pattern = f"{prefix}/{year}q{quarter_lower}"
        print(f"Checking for files in: {file_pattern}")
        
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=file_pattern
        )
        
        contents = response.get('Contents', [])
        if not contents:
            return None
            
        # Return all files found in the directory
        return [obj['Key'] for obj in contents]
        
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")

async def find_zip_file(prefix: str, year: int, quarter: str) -> Optional[str]:
    """
    Find zip file in the given S3 prefix
    
    Args:
        prefix (str): S3 prefix to search in
        year (int): Year to search for
        quarter (str): Quarter to search for
    
    Returns:
        Optional[str]: Zip file key if found, None otherwise
    """
    try:
        quarter_lower = quarter.lower().replace('q', '')
        file_pattern = f"{prefix}/{year}q{quarter_lower}"
        
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=file_pattern
        )
        
        contents = response.get('Contents', [])
        for obj in contents:
            if obj['Key'].endswith('.zip'):
                return obj['Key']
        return None
        
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")

async def unzip_and_upload(file_key: str, year: int, quarter: str) -> List[str]:
    """
    Download zip file from S3, unzip it, and upload contents
    
    Args:
        file_key (str): S3 key of the zip file
        year (int): Year for target path
        quarter (str): Quarter for target path
    
    Returns:
        List[str]: List of uploaded file keys
    """
    try:
        print(f"Unzipping file: {file_key}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        zip_content = response['Body'].read()
        uploaded_files = []
        quarter_lower = quarter.lower().replace('q', '')
        
        with BytesIO(zip_content) as zip_buffer:
            with zipfile.ZipFile(zip_buffer) as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith('/'):  # Skip directories
                        continue
                        
                    with zip_ref.open(file_name) as file:
                        file_content = file.read()
                        target_key = f'unziped_folder/{year}q{quarter_lower}/{file_name}'
                        
                        s3_client.put_object(
                            Bucket=BUCKET_NAME,
                            Key=target_key,
                            Body=file_content
                        )
                        uploaded_files.append(target_key)
                        
        return uploaded_files
        
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Error with S3: {str(e)}")
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Invalid zip file")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error during file processing: {str(e)}")
async def trigger_snowflake_pipeline(year: int, quarter: str) -> Dict[str, Any]:
    """
    Trigger Snowflake data loading pipeline via REST API
    
    Args:
        year (int): Year to process
        quarter (str): Quarter to process (Q1-Q4)
    
    Returns:
        Dict[str, Any]: Pipeline execution status
    """
    try:
        snowflake_url = "http://localhost:8001/load-data"  # Configure in environment/config
        
        
        payload = {
            "year": year,
            "quarter": quarter,
         
        }
        
        print(f"Triggering Snowflake pipeline: {payload}")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                snowflake_url,
                json=payload,
                timeout=300  # 5 minutes timeout for long-running operations
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Snowflake service error: {response.text}"
                )
                
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=504,
            detail="Snowflake pipeline timed out"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute Snowflake pipeline: {str(e)}"
        )

async def process_extraction(request: QueryRequest) -> Dict[str, Any]:
    """
    Process the extraction request
    
    Args:
        request (QueryRequest): Request containing year and quarter
    
    Returns:
        Dict[str, Any]: Response containing status and file information
    """
    try:
        execution_date = datetime.now().isoformat()
        print(f"Processing request: {request}")
        
        # Step 1: Check unzipped folder first
        unzipped_files = await check_file_exists('unziped_folder', request.year, request.quarter)
        if unzipped_files:
            return {
                "message": "Files already exist in unzipped folder",
                "status": "completed",
                "files": unzipped_files,
                "execution_date": execution_date
            }
        
        # Step 2: Find zip file in Finance folder
        zip_file = await find_zip_file('Finance', request.year, request.quarter)
        if not zip_file:
            raise HTTPException(
                status_code=404,
                detail=f"Zip file not found for year {request.year}, quarter {request.quarter}"
            )
        
        # Step 3: Process and upload file
        print(f"Processing zip file: {zip_file}")
        print(f"Year: {request.year}, Quarter: {request.quarter}")
        
        uploaded_files = await unzip_and_upload(
            zip_file,
            request.year,
            request.quarter
        )
        
        if uploaded_files:
            # Trigger Snowflake pipeline
            print("Triggering Snowflake pipeline")
            pipeline_status = await trigger_snowflake_pipeline(
                request.year,
                request.quarter
        )
        
        return {
                "message": "Files processed and data loaded to Snowflake",
                "status": "completed",
                "source_file": zip_file,
                "uploaded_files": uploaded_files,
                "snowflake_status": pipeline_status,
                "execution_date": execution_date
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

async def get_extraction_status(execution_date: str) -> Dict[str, Any]:
    """
    Get status of extraction
    
    Args:
        execution_date (str): Execution date to check status for
    
    Returns:
        Dict[str, Any]: Status information
    """
    try:
        return {
            "status": "completed",
            "execution_date": execution_date,
            "message": "File processing is done synchronously"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking status: {str(e)}")