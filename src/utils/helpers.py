import yaml

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
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    log_message(f"Extracted zip file: {zip_file_path} to {extract_to}")

def load_config(config_path='config/config.yaml'):
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
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
