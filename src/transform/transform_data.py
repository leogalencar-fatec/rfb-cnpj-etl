import csv
import pandas as pd
import os
from constants.csv_table_mapping import CSV_TABLE_MAPPING
from utils.helpers import create_logfile, load_config
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
    """Constructs the output file path based on input file structure."""
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
                df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif pandas_dtype == "boolean":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.lower()
                    .map({"true": True, "false": False, "1": True, "0": False})
                )
        except Exception as e:
            print(f"Warning: Could not convert {col} to {pandas_dtype}: {e}")

    return df


def filter_estabelecimentos_apta(df: pd.DataFrame) -> pd.DataFrame:
    """Filters 'estabelecimento' rows where 'cod_situacao_cadastral' is '02' (APTA)."""
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

    # Replace empty strings with NaN
    df.replace("", pd.NA, inplace=True)

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
    for csv_file_path in csv_files_paths:
        print(f"Checking if {csv_file_path} already has output file...")

        # Get table name
        table_name = get_table_name(csv_file_path)
        if not table_name:
            print(f"Warning: Could not determine table for {csv_file_path}, skipping.")
            continue

        output_file = get_output_file_path(csv_file_path, table_name)

        if os.path.exists(output_file):
            os.remove(output_file)
            print(f"Deleted old output file {output_file}.")


def process_csv(csv_file_path: str) -> str:
    """
    Processes a CSV file by reading it in chunks, cleaning the data, and writing the transformed data to a new CSV file.

    The function performs the following steps:
    1. Determines the table name based on the CSV file path.
    2. Reads the CSV file in chunks, with each chunk being processed separately.
    3. Cleans the data in each chunk using the `clean_dataframe` function.
    4. Writes the cleaned data to a new CSV file in the specified output directory.

    Notes:
    - The CSV file is expected to be encoded in "latin-1" and use ";" as the separator.
    - The function handles bad lines by issuing a warning and skipping them.
    - The pandas dtypes are converted later in clean_dataframe function.
    - If the number of columns in a chunk does not match the expected number of columns, the chunk is skipped.
    - The function logs progress and any errors encountered during processing.

    Args:
        csv_file_path (str): The path to the CSV file to be processed.
    Returns:
        str or None: The path to the output file if processing is successful, otherwise None.
    """

    print(f"Processing {csv_file_path}...")

    table_name = get_table_name(csv_file_path)
    if not table_name:
        print(f"Warning: Could not determine table for {csv_file_path}, skipping.")
        return None

    output_file = get_output_file_path(csv_file_path, table_name)
    expected_columns = list(TABLE_FIELDS[table_name].keys())

    first_chunk = True
    try:
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
                print(
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
        print(f"Finished processing and removed {csv_file_path}.")
        return output_file
    except Exception as e:
        print(f"Error processing {csv_file_path}: {e}")
        return None


def transform_data(csv_files_paths: list[str]) -> list[str]:
    """
    Transforms a list of CSV files by processing each file and saving the transformed data.

    Args:
        csv_files_paths (list[str]): A list of file paths to the CSV files to be transformed.

    Returns:
        list[str]: A list of file paths to the transformed CSV files.

    Raises:
        OSError: If there is an issue creating the output directory or removing existing files.

    Example:
        transformed_files = transform_data(['/path/to/file1.csv', '/path/to/file2.csv'])
    """

    month = os.path.basename(os.path.dirname(csv_files_paths[0]))
    transformed_path = os.path.join(TRANSFORMED_PATH, month)
    os.makedirs(transformed_path, exist_ok=True)

    remove_existing_files(csv_files_paths)

    log_filename = create_logfile("cleaned")

    transformed_data = []

    for csv_file_path in csv_files_paths:
        output_file = process_csv(csv_file_path)
        if output_file:
            with open(log_filename, "a") as log_file:
                log_file.write(f"{output_file} OK\n")
            transformed_data.append(output_file)

    print("Transformation completed!")
    return transformed_data
