import logging
import os
import re
import requests
import zipfile
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from utils.helpers import ask_month, create_logfile, load_config
from urllib.parse import urljoin

"""

TODO : generate logs for each operation

"""


# Configuration
config = load_config()
BASE_URL = config["data_source"]["base_url"]
DOWNLOAD_PATH = config["paths"]["download_path"]
EXTRACT_PATH = config["paths"]["extract_path"]


# Create a session
session = requests.Session()
session_retries = 3


def request_retry_get(
    url: str, show_attempts: bool = False, **kwargs
) -> requests.Response:
    """
    Performs a GET request with retry logic.
    Args:
        url (str): The URL to send the GET request to.
        show_attempts (bool, optional): If True, logs the attempt number and status. Defaults to False.
        **kwargs: Additional arguments to pass to the `requests.get` method.
    Returns:
        requests.Response: The response object from the GET request.
    Raises:
        requests.RequestException: If the request fails after the specified number of retries.
    """
    for attempt in range(1, session_retries + 1):
        try:
            response = session.get(url, **kwargs)
            response.raise_for_status()
            if show_attempts:
                logging.info(f"Attempt {attempt} successful: {url}")
            return response
        except requests.RequestException as e:
            logging.warning(f"Attempt {attempt} failed: {e}")
            if attempt == session_retries:
                logging.error(f"Max retries reached for {url}")
                raise


def get_available_months() -> list[str]:
    """
    Fetches available year-month directories from the Receita Federal website.
    Returns:
        list[str]: A sorted list of available year-month directories.
    Raises:
        requests.RequestException: If the request to fetch available months fails.
    """
    try:
        response = request_retry_get(BASE_URL)
    except requests.RequestException as e:
        logging.error(f"Failed to fetch available months: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    months = sorted(
        [
            a["href"].strip("/")
            for a in soup.find_all("a", href=True)
            if re.match(r"\d{4}-\d{2}/", a["href"])
        ],
        reverse=True,
    )
    return months


def get_zip_files(month: str) -> list[str]:
    """
    Fetches all ZIP file URLs from a given month folder.
    Args:
        month (str): The selected year-month directory.
    Returns:
        list[str]: A list of ZIP file URLs.
    Raises:
        requests.RequestException: If the request to fetch ZIP files fails.
    """
    month_url = urljoin(BASE_URL, f"{month}/")
    try:
        response = request_retry_get(month_url)
    except requests.RequestException as e:
        logging.error(f"Failed to fetch ZIP files for '{month}': {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    return [
        urljoin(month_url, a["href"])
        for a in soup.find_all("a", href=True)
        if a["href"].endswith(".zip")
    ]


def download_zip_file(url: str, save_path: str):
    """
    Downloads a ZIP file from the given URL and saves it to the specified path.
    Args:
        url (str): The URL of the ZIP file.
        save_path (str): The local file path to save the downloaded ZIP.
    Raises:
        requests.RequestException: If the download fails.
    """
    try:
        response = request_retry_get(url, stream=True)
    except requests.RequestException as e:
        logging.error(f"Failed to download {url}: {e}")
        return

    with open(save_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
            file.write(chunk)
    logging.info(f"Downloaded: {save_path}")


def download_all_zips(month: str) -> list[str]:
    """
    Downloads all ZIP files for a given month.
    Args:
        month (str): The selected year-month directory.
    Returns:
        list[str]: A list of file paths for the downloaded ZIP files.
    """
    zip_urls = get_zip_files(month)
    month_dir = os.path.join(DOWNLOAD_PATH, month)
    os.makedirs(month_dir, exist_ok=True)

    downloaded_files = []
    for url in zip_urls:
        filename = os.path.join(month_dir, os.path.basename(url))
        if os.path.exists(filename):
            logging.info(f"Skipping {filename}, already exists.")
        else:
            logging.info(f"Downloading {filename}...")
            download_zip_file(url, filename)
        downloaded_files.append(filename)

    return downloaded_files


def clean_filename(filename: str, zip_filename: str) -> str:
    """
    Clean the filename by adding .csv if missing, removing special characters, and ensuring uniqueness.

    Args:
        filename (str): The original filename.
        zip_filename (str): The name of the zip file to prepend to the filename.

    Returns:
        str: The cleaned and standardized filename.
    """
    # Ensure .csv extension
    filename = filename if filename.endswith(".csv") else f"{filename}.csv"

    # Prefix with zip filename (without extension) to maintain uniqueness
    zip_prefix = os.path.splitext(os.path.basename(zip_filename))[0]
    filename = f"{zip_prefix}_{filename}"

    # Replace special characters with underscores
    return re.sub(r"[^a-zA-Z0-9_.-]", "_", filename)


def extract_zip_files(zip_files: list[str], month: str) -> list[str]:
    """
    Extract all ZIP files and return a list of cleaned extracted files.

    Args:
        zip_files (list[str]): A list of paths to the ZIP files to extract.
        month (str): The year-month directory to extract files to.

    Returns:
        list[str]: A list of paths to the extracted files.
    """
    extract_path = os.path.join(EXTRACT_PATH, month)
    os.makedirs(extract_path, exist_ok=True)

    log_filename = create_logfile("extract")
    extracted_files = []

    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, "r") as zip_ref:
            for name in zip_ref.namelist():
                cleaned_name = clean_filename(name, zip_ref.filename)
                cleaned_path = os.path.join(extract_path, cleaned_name)

                if os.path.exists(cleaned_path):
                    logging.info(f"Skipping {cleaned_path}, already exists.")
                else:
                    with zip_ref.open(name) as source, open(
                        cleaned_path, "wb"
                    ) as target:
                        # Read and write file in 64KB chunks to handle large files efficiently
                        for chunk in iter(lambda: source.read(65536), b""):
                            target.write(chunk)

                    with open(log_filename, "a") as log_file:
                        log_file.write(f"{cleaned_path} OK\n")

                    logging.info(f"Extracted and cleaned {name} to {cleaned_path}")

                extracted_files.append(cleaned_path)

        # Remove the original zip file after extraction
        os.remove(zip_file)
        logging.info(f"Removed ZIP: {zip_file}")

        logging.info(f"Extracted {zip_file} to {extract_path}")

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
      list[str]: A list of paths to the extracted files.

    Raises:
      Exception: If no available months are found.
    """

    months = get_available_months()
    if not months:
        logging.error("No available months found.")
        raise Exception("No available months found.")

    selected_month = ask_month(months) if config["settings"]["ask_user"] else months[0]

    logging.info(f"Downloading data for: {selected_month}")
    downloaded_files = download_all_zips(selected_month)

    logging.info("Extracting ZIP files...")
    extracted_files = extract_zip_files(downloaded_files, selected_month)
    logging.info("Extraction complete!")

    return extracted_files
