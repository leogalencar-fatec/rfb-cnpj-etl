import requests
import zipfile
import os
from io import BytesIO

def extract_data(url, download_path, extract_path):
    # Download the zip file from the given URL
    response = requests.get(url)
    if response.status_code == 200:
        # Save the zip file to the specified download path
        with open(download_path, 'wb') as f:
            f.write(response.content)
        
        # Extract the contents of the zip file
        with zipfile.ZipFile(download_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        # Optionally, remove the zip file after extraction
        os.remove(download_path)
    else:
        raise Exception(f"Failed to download data: {response.status_code}")