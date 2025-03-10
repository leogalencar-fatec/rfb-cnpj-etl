import os
import re
import requests
import zipfile
from bs4 import BeautifulSoup
from utils.helpers import load_config
from urllib.parse import urljoin

"""

TODO : generate logs for each operation
TODO : clear extracted filename for each file inside a zip
TODO : Handle exceptions with try / except

"""


# Configuration
config = load_config()
BASE_URL = config["data_source"]["base_url"]
DOWNLOAD_PATH = config["paths"]["download_path"]
EXTRACT_PATH = config["paths"]["extract_path"]


def get_available_months() -> list[str]:
    """Fetch available year-month directories from the Receita Federal website."""
    response = requests.get(BASE_URL, verify=False)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    links = [a["href"] for a in soup.find_all("a", href=True)]

    # Match only folders with the format YYYY-MM/
    months = sorted(
        [link.strip("/") for link in links if re.match(r"\d{4}-\d{2}/", link)],
        reverse=True,  # Sort descending order
    )

    return months


def get_zip_files(month: str) -> list[str]:
    """Fetch all ZIP file URLs from a given month folder."""
    month_url = urljoin(BASE_URL, month + "/")
    response = requests.get(month_url, verify=False)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    zip_files = [
        a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".zip")
    ]

    return [urljoin(month_url, file) for file in zip_files]


def download_zip_file(url: str, save_path: str):
    """Download a ZIP file from the given URL."""
    response = requests.get(url, stream=True, verify=False)
    response.raise_for_status()

    with open(save_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)


def download_all_zips(month: str):
    """Download all ZIP files from a given month."""
    zip_urls = get_zip_files(month)[:1]
    month_dir = os.path.join(DOWNLOAD_PATH, month)
    os.makedirs(month_dir, exist_ok=True)

    downloaded_files = []

    for url in zip_urls:
        filename = os.path.join(month_dir, os.path.basename(url))
        if os.path.exists(filename):
            print(f"Skipping {filename}, already exists.")
        else:
            print(f"Downloading {filename}...")
            download_zip_file(url, filename)
        downloaded_files.append(filename)

    return downloaded_files


def extract_zip_files(zip_files, month) -> list[str]:
    """Extract all ZIP files and return the list of extracted files."""
    extract_path = os.path.join(EXTRACT_PATH, month)
    os.makedirs(extract_path, exist_ok=True)

    extracted_files = []

    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            zip_ref.extractall(extract_path)
            extracted_files.extend(
                [os.path.join(extract_path, name) for name in zip_ref.namelist()]
            )
            print(f"Extracted {zip_file} to {extract_path}")

    return extracted_files


def extract_data() -> list[str]:
    """
    Extracts data by downloading and extracting ZIP files for a selected month.

    This function performs the following steps:
    1. Retrieves the available months for download.
    2. Prompts the user to select a month or automatically selects the latest month.
    3. Downloads the ZIP files for the selected month.
    4. Extracts the downloaded ZIP files.

    Returns:
      list: A list of paths to the extracted files.

    Raises:
      Exception: If no available months are found.
    """

    # Downloading data
    months = get_available_months()
    if not months:
        print("No available months found.")
        raise Exception("No available months found.")

    # Let user choose or pick the latest month automatically
    print("Available months:")
    for i, month in enumerate(months):
        print(f"{i + 1}. {month}")

    choice = input(
        f"Enter the number of the month to download (1-{len(months)}), or press Enter for latest: "
    )

    if choice.isdigit() and 1 <= int(choice) <= len(months):
        selected_month = months[int(choice) - 1]
    else:
        selected_month = months[0]

    print(f"Downloading data for: {selected_month}")
    downloaded_files = download_all_zips(selected_month)
    print("Download complete!")

    # Extracting zip files
    print("Extracting ZIP files...")
    extracted_files = extract_zip_files(downloaded_files, selected_month)
    print("Extraction complete!")

    return extracted_files
