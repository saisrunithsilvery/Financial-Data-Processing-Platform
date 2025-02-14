from fastapi import HTTPException
import boto3
from botocore.exceptions import ClientError
import zipfile
from io import BytesIO
from typing import Optional, List, Dict, Any
# Move to config file

class S3Operations:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = 'damgassign02'

    async def check_file_exists(self, prefix: str, year: int, quarter: str) -> Optional[str]:
        """
        Check if file exists in the given S3 prefix
        """
        try:
            quarter_lower = str(quarter).lower().replace('q', '')
            file_pattern = f"{prefix}/{year}q{quarter_lower}/"
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=file_pattern
            )
            
            contents = response.get('Contents', [])
            for obj in contents:
                if obj['Key'].startswith(file_pattern):
                    return obj['Key']
            return None
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"Error accessing S3: {str(e)}")

    async def unzip_and_upload(self, file_key: str, year: int, quarter: str) -> List[str]:
        """
        Download zip file from S3, unzip it, and upload contents
        """
        try:
            # Download zip file
            response = self.s3_client.get_object(
                Bucket=self.bucket_name, 
                Key=file_key
            )
            zip_content = response['Body'].read()
            uploaded_files = []
            quarter_lower = str(quarter).lower().replace('q', '')
            
            # Process zip file
            with BytesIO(zip_content) as zip_buffer:
                with zipfile.ZipFile(zip_buffer) as zip_ref:
                    uploaded_files = await self._process_zip_contents(
                        zip_ref, year, quarter_lower
                    )
                    
            return uploaded_files

        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"Error with S3: {str(e)}")
        except zipfile.BadZipFile:
            raise HTTPException(status_code=400, detail="Invalid zip file")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error during file processing: {str(e)}")

    async def _process_zip_contents(self, zip_ref: zipfile.ZipFile, year: int, quarter: str) -> List[str]:
        """
        Process contents of zip file and upload to S3
        """
        uploaded_files = []
        
        for file_name in zip_ref.namelist():
            if file_name.endswith('/'):  # Skip directories
                continue
                
            with zip_ref.open(file_name) as file:
                file_content = file.read()
                target_key = f'unziped_folder/{year}q{quarter}/{file_name}'
                
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=target_key,
                    Body=file_content
                )
                uploaded_files.append(target_key)
        
        return uploaded_files

# Create a singleton instance
s3_ops = S3Operations()