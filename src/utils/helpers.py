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