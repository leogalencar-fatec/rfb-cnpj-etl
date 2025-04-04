import csv
import logging
import numpy as np
import pandas as pd
import os
from constants.csv_table_mapping import CSV_TABLE_MAPPING
from extract.extract_data import DOWNLOAD_PATH
from utils.helpers import ask_month, create_logfile, load_config
from constants.table_fields import TABLE_FIELDS
from constants.pandas_dtypes_map import PANDAS_DTYPES_MAP


# Configuration
config = load_config()
EXTRACT_PATH = config["paths"]["extract_path"]
TRANSFORMED_PATH = config["paths"]["transformed_path"]
READ_CHUNK_SIZE = config["performance"]["read_chunk_size"]


def get_table_name(filename: str) -> str | None:
    """
    Extracts the table name from a filename.

    Args:
        filename (str): The name of the CSV file.

    Returns:
        str: The table name if found, otherwise None.
    """

    filename = os.path.basename(filename).lower()

    for prefix, table in CSV_TABLE_MAPPING.items():
        if filename.lower().startswith(prefix):
            return table

    return None


def get_output_file_path(csv_file_path: str, table_name: str) -> str:
    """
    Generates the output file path for a transformed CSV file.
    Args:
        csv_file_path (str): The path to the input CSV file.
        table_name (str): The name of the table to be used in the output file name.
    Returns:
        str: The generated output file path.
    The output file path is constructed by:
    1. Extracting the month from the directory name of the input CSV file.
    2. Extracting the base name of the input CSV file.
    3. Extracting the file number from the base name (if present).
    4. Constructing the output file name using the table name and file number.
    5. Joining the TRANSFORMED_PATH, month, and constructed file name to form the full output file path.
    """

    month = os.path.basename(os.path.dirname(csv_file_path))
    base_name = os.path.basename(csv_file_path)
    file_number = "".join(filter(str.isdigit, base_name.split("_")[0]))

    file_name = f"{table_name}" + (f"_{file_number}" if file_number else "") + ".csv"
    return os.path.join(TRANSFORMED_PATH, month, file_name)


def enforce_dtypes(df: pd.DataFrame, dtype_mapping: dict[str, str]) -> pd.DataFrame:
    """
    Ensures the dataframe columns have the correct types.

    Args:
        df (pd.DataFrame): The dataframe to convert.
        dtype_mapping (Dict[str, str]): Mapping of column names to data types.

    Returns:
        pd.DataFrame: The dataframe with correct data types.
    """
    for col, dtype in dtype_mapping.items():
        pandas_dtype = PANDAS_DTYPES_MAP[dtype]

        try:
            if pandas_dtype == "string":
                df[col] = df[col].astype(str).str.strip()
            elif pandas_dtype == "Int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif pandas_dtype == "float64":
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", ".", regex=False),
                    errors="coerce",
                )
            elif pandas_dtype == "boolean":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.lower()
                    .map({"true": True, "false": False, "1": True, "0": False})
                )
            elif pandas_dtype == "datetime64[ns]":
                df[col] = df[col].replace(["0", "", "0000-00-00"], r"\N")
                
        except Exception as e:
            logging.warning(f"Could not convert {col} to {pandas_dtype}: {e}")

    return df


def filter_estabelecimentos_apta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the DataFrame to include only rows where the 'cod_situacao_cadastral' column has the value '02'.
    Args:
        df (pd.DataFrame): The input DataFrame containing establishment data.
    Returns:
        pd.DataFrame: A DataFrame filtered to include only establishments with 'cod_situacao_cadastral' equal to '02'.
                      If the 'cod_situacao_cadastral' column is not present, the original DataFrame is returned.
    """

    return (
        df[df["cod_situacao_cadastral"] == "02"]
        if "cod_situacao_cadastral" in df.columns
        else df
    )


def clean_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Cleans the given DataFrame by enforcing data types, removing whitespace,
    replacing empty strings with NaN, dropping duplicates, and resetting the index.

    Parameters:
        df (pd.DataFrame): The DataFrame to be cleaned.
        table_name (str): The name of the table to determine the data types to enforce.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """

    # Enforce datatypes
    df = enforce_dtypes(df, TABLE_FIELDS[table_name])

    # Remove leading/trailing whitespace from string columns
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Replace empty strings, NaN, None with \N (NULL in MySQL)
    df = df.replace({"": r"\N", np.nan: r"\N", pd.NA: r"\N", None: r"\N"})
    
    # Drop duplicate rows
    df.drop_duplicates(inplace=True)

    # Reset index
    df.reset_index(drop=True, inplace=True)

    """FILTERS"""

    # Estabelecimentos "APTA" only
    if (
        config["settings"]["estabelecimentos_apta_only"]
        and table_name == "estabelecimento"
    ):
        df = filter_estabelecimentos_apta(df)

    return df


def remove_existing_files(csv_files_paths: list[str]):
    """
    Removes existing output files for the given list of CSV file paths.
    This function checks if an output file already exists for each CSV file path
    in the provided list. If an output file exists, it is deleted.
    Args:
        csv_files_paths (list[str]): A list of paths to CSV files.
    Returns:
        None
    Logs:
        - Info: When checking if an output file exists for a CSV file path.
        - Warning: If the table name cannot be determined for a CSV file path.
        - Info: When an old output file is deleted.
    """

    for csv_file_path in csv_files_paths:
        logging.info(f"Checking if {csv_file_path} already has output file...")

        # Get table name
        table_name = get_table_name(csv_file_path)
        if not table_name:
            logging.warning(f"Could not determine table for {csv_file_path}, skipping.")
            continue

        output_file = get_output_file_path(csv_file_path, table_name)
        if os.path.exists(output_file):
            os.remove(output_file)
            logging.info(f"Deleted old output file {output_file}.")


def process_csv(csv_file_path: str) -> str | None:
    """
    Processes a CSV file by reading it in chunks, cleaning the data, and writing the transformed data to a new CSV file.

    The function performs the following steps:
    1. Determines the table name based on the CSV file path.
    2. Reads the CSV file in chunks, with each chunk being processed separately.
    3. Cleans the data in each chunk using the `clean_dataframe` function.
    4. Writes the cleaned data to a new CSV file in the specified output directory.
    5. Removes the original CSV file after processing is complete.

    Notes:
    - The CSV file is expected to be encoded in "latin-1" and use ";" as the separator.
    - The function handles bad lines by issuing a warning and skipping them.
    - The pandas dtypes are converted later in clean_dataframe function.
    - If the number of columns in a chunk does not match the expected number of columns, the chunk is skipped.
    - The function logs progress and any errors encountered during processing.

    Args:
        csv_file_path (str): The path to the CSV file to be processed.
    Returns:
        str | None: The path to the output file if processing is successful, otherwise None.
    Raises:
        Exception: If an error occurs during processing, logs the error and returns None.
    """

    logging.info(f"Processing {csv_file_path}...")

    table_name = get_table_name(csv_file_path)
    if not table_name:
        logging.warning(
            f"Warning: Could not determine table for {csv_file_path}, skipping."
        )
        return None

    output_file = get_output_file_path(csv_file_path, table_name)
    expected_columns = list(TABLE_FIELDS[table_name].keys())

    try:
        first_chunk = True
        for chunk in pd.read_csv(
            csv_file_path,
            encoding="latin-1",
            sep=";",
            dtype=str,
            header=None,
            on_bad_lines="warn",
            low_memory=False,
            chunksize=READ_CHUNK_SIZE,
        ):
            if len(chunk.columns) != len(expected_columns):
                logging.warning(
                    f"Warning: {csv_file_path} has {len(chunk.columns)} columns but expected {len(expected_columns)}. Skipping chunk."
                )
                continue

            chunk.columns = expected_columns
            chunk = clean_dataframe(chunk, table_name)
            chunk.to_csv(
                output_file,
                mode="a",
                index=False,
                sep=";",
                header=first_chunk,
                encoding="utf-8",
                quoting=csv.QUOTE_NONNUMERIC,
            )
            first_chunk = False

        os.remove(csv_file_path)
        logging.info(f"Finished processing and removed {csv_file_path}.")
        return output_file
    except Exception as e:
        logging.error(f"Error processing {csv_file_path}: {e}")
        return None


def transform_data(csv_files_paths: list[str] = []) -> list[str]:
    """
    Transforms the data from the given CSV file paths.
    If no CSV file paths are provided, it will look for available months in the
    extraction path, ask the user to select a month (if configured to do so),
    and use the CSV files from the selected month.
    Args:
        csv_files_paths (list[str], optional): List of paths to the CSV files to be transformed.
                                               Defaults to an empty list.
    Returns:
        list[str]: List of paths to the transformed data files.
    Raises:
        Exception: If no available months are found in the extraction path.
    Logs:
        Logs the creation of each transformed file and the completion of the transformation process.
    """

    if not len(csv_files_paths) > 0:
        # Getting available months
        months = sorted(os.listdir(EXTRACT_PATH), reverse=True)
        if not months:
            logging.error("No available months found.")
            raise Exception("No available months found.")

        if config["settings"]["ask_user"]:
            month = ask_month(months)
        else:
            month = months[0]

        csv_files_paths = [
            os.path.join(EXTRACT_PATH, month, file)
            for file in sorted(os.listdir(os.path.join(EXTRACT_PATH, month)))
        ]
    else:
        month = os.path.basename(os.path.dirname(csv_files_paths[0]))

    transformed_path = os.path.join(TRANSFORMED_PATH, month)
    os.makedirs(transformed_path, exist_ok=True)
    remove_existing_files(csv_files_paths)

    transformed_data = []

    for csv_file_path in csv_files_paths:
        output_file = process_csv(csv_file_path)
        if output_file:
            logging.info(f"{output_file} created.")
            transformed_data.append(output_file)

    logging.info("Transformation completed!")
    return transformed_data
