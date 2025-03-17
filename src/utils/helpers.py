import os
import shutil
import subprocess
import yaml
from datetime import datetime

CONFIG_PATH = "config/config.yaml"


def log_message(message):
    print(f"[LOG] {message}")


def validate_data(data):
    if data is None or len(data) == 0:
        log_message("Data validation failed: Data is empty or None.")
        return False
    log_message("Data validation passed.")
    return True


def extract_zip_file(zip_file_path, extract_to):
    import zipfile

    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    log_message(f"Extracted zip file: {zip_file_path} to {extract_to}")


def load_config(config_path=CONFIG_PATH):
    """
    Load configuration data from a YAML config file.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: Configuration data.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


def save_config(config, config_path=CONFIG_PATH):
    """Saves updated configuration back to config.yaml"""
    with open(config_path, "w") as file:
        yaml.safe_dump(config, file)


def is_running_in_wsl() -> bool:
    """Checks if the script is running in a WSL environment."""
    with open("/proc/version", "r") as f:
        return "microsoft" in f.read().lower()


def convert_wsl_to_windows_path(wsl_path: str) -> str:
    """Converts a WSL path to a Windows path."""
    result = subprocess.run(["wslpath", "-m", wsl_path], capture_output=True, text=True)
    return result.stdout.strip()


def copy_to_windows_temp(file_path: str) -> str:
    """Copies a file from WSL to a temporary location on the Windows filesystem."""
    temp_dir = "C:/temp_wsl_files"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    temp_file_path = os.path.join(temp_dir, os.path.basename(file_path))

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    shutil.copy(file_path, temp_file_path)
    return temp_file_path

def create_logfile(filename: str) -> str:
    current_datetime = datetime.now().isoformat(timespec='seconds').replace(':', '-')
    log_filename = f"logs/{filename}/{filename}_{current_datetime}.txt"
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)
    
    return log_filename