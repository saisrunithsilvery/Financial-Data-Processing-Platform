from fastapi import HTTPException
import boto3
from botocore.exceptions import ClientError
import zipfile
from io import BytesIO
from typing import Optional, List
from backend.config.settings import Settings

class S3Service:
    def __init__(self):
        self.settings = Settings()
        self.s3_client = boto3.client('s3')
        self.BUCKET_NAME = self.settings.S3_BUCKET_NAME

    async def check_file_exists(self, prefix: str, year: int, quarter: str) -> Optional[List[str]]:
        """Check if files exist in the given S3 prefix"""
        try:
            quarter_lower = quarter.lower().replace('q', '')
            file_pattern = f"{prefix}/{year}q{quarter_lower}"
            print(f"Checking for files in: {file_pattern}")
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.BUCKET_NAME,
                Prefix=file_pattern
            )
            
            contents = response.get('Contents', [])
            return [obj['Key'] for obj in contents] if contents else None
            
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")

    async def find_zip_file(self, prefix: str, year: int, quarter: str) -> Optional[str]:
        """Find zip file in the given S3 prefix"""
        try:
            quarter_lower = quarter.lower().replace('q', '')
            file_pattern = f"{prefix}/{year}q{quarter_lower}"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.BUCKET_NAME,
                Prefix=file_pattern
            )
            
            contents = response.get('Contents', [])
            for obj in contents:
                if obj['Key'].endswith('.zip'):
                    return obj['Key']
            return None
            
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")

    async def unzip_and_upload(self, file_key: str, year: int, quarter: str) -> List[str]:
        """Download zip file from S3, unzip it, and upload contents"""
        try:
            print(f"Unzipping file: {file_key}")
            response = self.s3_client.get_object(Bucket=self.BUCKET_NAME, Key=file_key)
            zip_content = response['Body'].read()
            uploaded_files = []
            quarter_lower = quarter.lower().replace('q', '')
            
            with BytesIO(zip_content) as zip_buffer:
                with zipfile.ZipFile(zip_buffer) as zip_ref:
                    for file_name in zip_ref.namelist():
                        if file_name.endswith('/'):
                            continue
                            
                        with zip_ref.open(file_name) as file:
                            file_content = file.read()
                            target_key = f'unziped_folder/{year}q{quarter_lower}/{file_name}'
                            
                            self.s3_client.put_object(
                                Bucket=self.BUCKET_NAME,
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