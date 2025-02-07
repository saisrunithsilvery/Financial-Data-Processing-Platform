import boto3
import os
import uuid
from datetime import datetime
from pathlib import Path

# Initialize S3 client using AWS CLI credentials
s3_client = boto3.client('s3')
bucket_name = "damgassign02"  # Your S3 bucket name

def upload_zip_files_with_unique_folders(local_folder='import_folders'):
    uploaded_files_urls = {}

    # Ensure 'import_folders' exists in the current working directory
    current_dir = os.getcwd()
    import_folder_path = Path(current_dir) / local_folder

    if not import_folder_path.exists():
        raise FileNotFoundError(f"'import_folders' directory not found in {current_dir}")

    # Filter .zip files in 'import_folders'
    zip_files = [f for f in import_folder_path.iterdir() if f.suffix == '.zip']

    if not zip_files:
        print("No ZIP files found in 'import_folders'.")
        return uploaded_files_urls

    # Create a unique folder name for this upload session
    unique_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    process_folder = f"Finance_Upload_{timestamp}_{unique_id}"

    # Define base S3 path for this batch upload
    base_s3_path = f"Finance"

    # Upload each ZIP file and generate full S3 URLs
    for zip_file in zip_files:
        s3_key = f"{base_s3_path}/{zip_file.name}"

        try:
            # Upload file to S3
            s3_client.upload_file(str(zip_file), bucket_name, s3_key)
            print(f"Uploaded {zip_file} to s3://{bucket_name}/{s3_key}")

            # Generate full S3 URL
            s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
            uploaded_files_urls[zip_file.name] = s3_url

        except Exception as e:
            print(f"Failed to upload {zip_file.name}: {e}")

    return uploaded_files_urls

# Execute upload and display URLs
uploaded_urls = upload_zip_files_with_unique_folders()

# Display the URLs for downloaded files
for filename, url in uploaded_urls.items():
    print(f"Download link for {filename}: {url}")
