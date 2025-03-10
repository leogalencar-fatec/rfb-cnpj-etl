import pandas as pd
import os
from utils.helpers import load_config

# Configuration
config = load_config()
EXTRACT_PATH = config["paths"]["extract_path"]
TRANSFORMED_PATH = config["paths"]["transformed_path"]
READ_CHUNK_SIZE = 10000


# Constants
FIELDS = {
    "empresa": {
        "cnpj": "str",
        "razao_social": "str",
        "codigo_natureza_juridica": "str",
        "qualificacao_do_responsavel": "str",
        "capital_social": "float",
        "porte": "str",
        "ente_federativo_responsavel": "str",
    },
    "estabelecimento": {
        "cnpj_basico": "str",
        "cnpj_ordem": "str",
        "cnpj_dv": "str",
        "identificador_matriz_filial": "str",
        "nome_fantasia": "str",
        "situacao_cadastral": "str",
        "data_situacao_cadastral": "str",
        "motivo_situacao_cadastral": "str",
        "nome_cidade_exterior": "str",
        "pais": "str",
        "data_inicio_atividade": "str",
        "cnae_fiscal": "str",
        "cnae_fiscal_secundario": "str",
        "tipo_logradouro": "str",
        "logradouro": "str",
        "numero": "str",
        "complemento": "str",
        "bairro": "str",
        "cep": "str",
        "uf": "str",
        "municipio": "str",
        "ddd_1": "str",
        "telefone_1": "str",
        "ddd_2": "str",
        "telefone_2": "str",
        "ddd_fax": "str",
        "telefone_fax": "str",
        "correio_eletronico": "str",
        "situacao_especial": "str",
        "data_situacao_especial": "str",
    },
    "simples": {
        "cnpj_basico": "str",
        "opcao_pelo_simples": "str",
        "data_opcao_pelo_simples": "str",
        "data_exclusao_pelo_simples": "str",
        "opcao_pelo_mei": "str",
        "data_opcao_pelo_mei": "str",
        "data_exclusao_pelo_mei": "str",
    },
    "socio": {
        "cnpj_basico": "str",
        "identificador_socio": "str",
        "razao_social": "str",
        "cnpj_cpf_socio": "str",
        "codigo_qualificacao_socio": "str",
        "data_entrada_sociedade": "str",
        "codigo_pais_socio_estrangeiro": "str",
        "numero_cpf_representante_legal": "str",
        "nome_representante_legal": "str",
        "codigo_qualificacao_representante_legal": "str",
        "faixa_etaria": "str",
    },
    "pais": {"codigo": "str", "descricao": "str"},
    "municipio": {"codigo": "str", "descricao": "str"},
    "qualificacao_socio": {"codigo": "str", "descricao": "str"},
    "natureza_juridica": {"codigo": "str", "descricao": "str"},
    "cnae": {"codigo": "str", "descricao": "str"},
    "motivo": {"codigo": "str", "descricao": "str"},
}

PANDAS_DTYPES = {
    "str": "string",
    "int": "Int64",
    "float": "float64",
    "bool": "boolean",
}


def get_table_name(filename: str) -> str | None:
    """
    Extracts the table name from a filename.

    Args:
        filename (str): The name of the CSV file.

    Returns:
        str: The table name if found, otherwise None.
    """

    filename = os.path.basename(filename).lower()

    for table in FIELDS.keys():
        if table in filename:
            return table

    return None


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
        pandas_dtype = PANDAS_DTYPES[dtype]

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
    df = enforce_dtypes(df, FIELDS[table_name])

    # Remove leading/trailing whitespace from string columns
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Replace empty strings with NaN
    df.replace("", pd.NA, inplace=True)

    # Drop duplicate rows
    df.drop_duplicates(inplace=True)

    # Reset index
    df.reset_index(drop=True, inplace=True)

    return df


def transform_data(csv_files_paths: list[str]) -> list[str]:
    """
    Reads large CSV files in chunks, applies headers, and saves transformed CSVs.

    Args:
        csv_paths (list[str]): List of file paths to CSV files.
    """

    print(csv_files_paths)

    os.makedirs(TRANSFORMED_PATH, exist_ok=True)

    transformed_data = []

    for csv_file_path in csv_files_paths:
        print(f"Processing {csv_file_path}...")

        # Get table name
        table_name = get_table_name(csv_file_path)
        if not table_name:
            print(f"Warning: Could not determine table for {csv_file_path}, skipping.")
            continue

        # Check if file already exists
        output_file = os.path.join(TRANSFORMED_PATH, f"{table_name}.csv")
        if os.path.exists(output_file):
            print(f"Output file {output_file} already exists. Skipping transformation.")
            transformed_data.append(output_file)
            continue

        # Get expected columns
        expected_columns = list(FIELDS[table_name].keys())

        # Get columns types
        dtypes = FIELDS[table_name]

        # Read CSV in chunks
        first_chunk = True
        try:
            for chunk in pd.read_csv(
                csv_file_path,
                encoding="latin-1",
                sep=";",
                dtype=dtypes,
                header=None,
                on_bad_lines="warn",
                low_memory=False,
                chunksize=READ_CHUNK_SIZE,
            ):

                # Validate column count
                if len(chunk.columns) != len(expected_columns):
                    print(
                        f"Warning: {csv_file_path} has {len(chunk.columns)} columns but expected {len(expected_columns)}. Skipping chunk."
                    )
                    continue

                # Assign column names
                chunk.columns = expected_columns

                # Clean chunk
                # chunk = clean_dataframe(chunk, table_name)

                # Append chunk to output file
                chunk.to_csv(
                    output_file,
                    mode="a",
                    index=False,
                    sep=";",
                    header=first_chunk,
                    encoding="latin-1",
                )
                first_chunk = False

        except Exception as e:
            print(f"Error processing {csv_file_path}: {e}")

        # Append file to processed files
        if output_file not in transformed_data:
            transformed_data.append(output_file)

        print(f"Finished processing {csv_file_path}")

    print("Transformation completed!")

    return transformed_data
