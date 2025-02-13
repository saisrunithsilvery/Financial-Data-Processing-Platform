from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import boto3

def configure_driver(download_path):
    """
    Configures and initializes the Selenium WebDriver with specified download settings.
    """
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def get_zip_urls(driver, url):
    """
    Extracts ZIP file URLs from the SEC website.
    """
    driver.get(url)
    time.sleep(5)  # Allow time for the page to load
    zip_links = driver.find_elements("xpath", "//table//a[contains(text(), 'Q')]")
    return [link.get_attribute("href") for link in zip_links]

def download_upload_delete(driver, zip_urls, download_path, s3_bucket, s3_folder):
    """
    Downloads ZIP files, uploads them to an S3 bucket, and deletes them locally after upload.
    """
    s3_client = boto3.client("s3")
    for zip_url in zip_urls:
        filename = zip_url.split("/")[-1]
        local_file_path = os.path.join(download_path, filename)
        
        print(f"Downloading: {zip_url}")
        driver.get(zip_url)
        
        # Wait until the file appears in the directory
        max_wait_time, elapsed_time = 60, 0
        while not os.path.exists(local_file_path) and elapsed_time < max_wait_time:
            time.sleep(2)
            elapsed_time += 2
        
        if os.path.exists(local_file_path):
            print(f"Uploading {filename} to S3...")
            s3_client.upload_file(local_file_path, s3_bucket, f"{s3_folder}/{filename}")
            print(f"Deleting {filename} from local storage...")
            os.remove(local_file_path)
        else:
            print(f"Download failed or took too long for {filename}")

def main():
    """
    Main function to orchestrate the downloading, uploading, and cleanup process.
    """
    download_path = "/Users/saisr/Downloads"
    sec_url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
    s3_bucket, s3_folder = "damgassign02", "zipped_folders"
    
    driver = configure_driver(download_path)
    try:
        zip_urls = get_zip_urls(driver, sec_url)
        download_upload_delete(driver, zip_urls, download_path, s3_bucket, s3_folder)
    finally:
        driver.quit()
    print("Process completed successfully.")

if __name__ == "__main__":
    main()
