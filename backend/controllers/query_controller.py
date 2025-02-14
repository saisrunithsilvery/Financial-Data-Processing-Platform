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
async def check_file_exists(prefix: str, year: int, quarter: str) -> Optional[str]:
    """
    Check if file exists in the given S3 prefix
    
    Args:
        prefix (str): S3 prefix to search in
        year (int): Year to search for
        quarter (str): Quarter to search for
    
    Returns:
        Optional[str]: File key if found, None otherwise
    """
    try:
        quarter_lower = quarter.lower().replace('q', '')
        file_pattern = f"{prefix}/{year}q{quarter_lower}/"
        
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=file_pattern
        )
        
        contents = response.get('Contents', [])
        for obj in contents:
            if obj['Key'].startswith(file_pattern):
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
        
        # Check unzipped folder first
        unzipped_file = await check_file_exists('unziped_folder', request.year, request.quarter)
        if unzipped_file:
            return {
                "message": "File already exists in unzipped folder",
                "status": "completed",
                "file": unzipped_file,
                "execution_date": execution_date
            }
        
        # Check Finance folder
        finance_file = await check_file_exists('Finance', request.year, request.quarter)
        if not finance_file:
            raise HTTPException(
                status_code=404,
                detail=f"File not found for year {request.year}, quarter {request.quarter}"
            )
        
        # Process and upload file
        uploaded_files = await unzip_and_upload(
            finance_file,
            request.year,
            request.quarter
        )
        
        return {
            "message": "Files successfully processed and uploaded",
            "status": "completed",
            "source_file": finance_file,
            "uploaded_files": uploaded_files,
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