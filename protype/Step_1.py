#####downloading the zip files from sec.gov

import requests
import os
from datetime import datetime, timedelta
import time  # Added time import

def download_sec_files():
    # Create imports directory
    current_dir = os.getcwd()
    import_dir = os.path.join(current_dir, 'import_folders')
    if not os.path.exists(import_dir):
        os.makedirs(import_dir)
    
    # Define headers required by SEC
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://www.sec.gov',
        'Referer': 'https://www.sec.gov/'
    }
    
    # Generate quarters to download (from 2009 to current)
    current_year = datetime.now().year
    years = range(2009, current_year + 1)
    quarters = ['q1', 'q2', 'q3', 'q4']
    
    # Base URL for SEC files
    base_url = "https://www.sec.gov/files/dera/data/financial-statement-data-sets"
    
    session = requests.Session()
    session.headers.update(headers)
    
    # Download files
    for year in years:
        for quarter in quarters:
            filename = f"{year}{quarter}.zip"
            url = f"{base_url}/{filename}"
            
            filepath = os.path.join(import_dir, filename)
            
            if os.path.exists(filepath):
                print(f"File {filename} already exists, skipping...")
                continue
                
            try:
                print(f"Downloading {filename}...")
                time.sleep(5)  # Increased sleep time to 5 seconds before each request
                
                response = session.get(url)
                
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    print(f"Successfully downloaded {filename}")
                    time.sleep(5)  # Added sleep after successful download
                elif response.status_code == 403:
                    print(f"Access denied for {filename}. Waiting longer...")
                    time.sleep(10)  # Wait longer if we get a 403
                    # Try one more time
                    response = session.get(url)
                    if response.status_code == 200:
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        print(f"Successfully downloaded {filename} on second attempt")
                else:
                    print(f"Failed to download {filename}. Status code: {response.status_code}")
                    
            except Exception as e:
                print(f"Error downloading {filename}: {str(e)}")
                time.sleep(5)  # Sleep even after errors

if __name__ == "__main__":
    download_sec_files()