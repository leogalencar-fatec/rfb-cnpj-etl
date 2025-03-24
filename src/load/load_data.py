import logging
import os

from constants.table_fields import TABLE_FIELDS
from transform.transform_data import TRANSFORMED_PATH
from utils.helpers import ask_month, create_logfile, load_config
from utils.database.conn import MYSQL_CONN, SQL_ALCHEMY_MYSQL_CONN

# Configuration
config = load_config()
READ_CHUNK_SIZE = config["performance"]["read_chunk_size"]
WRITE_CHUNK_SIZE = config["performance"]["write_chunk_size"]
TABLES_TO_LOAD = {
    "cnae": "cnae",
    "motivo": "motivo",
    "municipio": "municipio",
    "natureza": "natureza_juridica",
    "pais": "pais",
    "qualifica": "qualificacao_socio",
    "empresa": "empresa",
    "estabelecimento": "estabelecimento",
    "socio": "socio",
    "simples": "simples",
}

# Database connections
mysql_conn = MYSQL_CONN.get_connection()
sql_alchemy_conn = SQL_ALCHEMY_MYSQL_CONN.get_session()

def get_separated_files(name: str, data: list[str]) -> list[str]:
    """
    Filters and returns a list of file paths from the given data that start with the specified name.

    Args:
        name (str): The prefix to filter the file names.
        data (list[str]): A list of file paths.
    Returns:
        list[str]: A list of file paths that start with the specified name.
    """

    return [file for file in data if file.lower().split("/")[-1].startswith(name)]


def drop_and_recreate_tables():
    """
    Drops existing tables and recreates them based on the SQL script.

    This function performs the following steps:
    1. Disables foreign key checks.
    2. Drops existing tables listed in the `TABLE_FIELDS` dictionary and additional tables.
    3. Re-enables foreign key checks.
    4. Reads and executes SQL statements from the `create_tables.sql` script to recreate the tables.

    Note:
        The function assumes that `mysql_conn` is a valid MySQL connection object and `TABLE_FIELDS` is a dictionary
        containing table names as keys.
    Raises:
        mysql.connector.Error: If any MySQL error occurs during the execution of SQL statements.
    """

    cursor = mysql_conn.cursor()

    logging.info("Dropping existing tables...")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    # tables = ["socio", "simples", "estabelecimento", "empresa",
    #           "cnae", "motivo", "municipio", "natureza_juridica", "pais", "qualificacao_socio",
    #           "id_matriz_filial", "id_porte_empresa", "id_situacao_cadastral", "id_socio", "id_faixa_etaria"]
    tables = list(TABLE_FIELDS.keys()) + [
        "id_matriz_filial",
        "id_porte_empresa",
        "id_situacao_cadastral",
        "id_socio",
        "id_faixa_etaria",
    ]

    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

    logging.info("Recreating tables...")
    read_sql_file("src/sql/create_tables.sql")

    mysql_conn.commit()
    cursor.close()


def load_csv_to_db(file_paths: list[str], table_name: str):
    """
    Load CSV files into a specified database table.

    This function takes a list of file paths to CSV files and loads their contents
    into a specified database table. The first file in the list is assumed to have
    headers, which will be ignored during the loading process.

    Args:
        file_paths (list[str]): A list of file paths to the CSV files to be loaded.
        table_name (str): The name of the database table into which the data will be loaded.
    Raises:
        Exception: If there is an error during the loading process, an exception will be raised
                   and the error message will be printed.
    Example:
        load_csv_to_db(['/path/to/file1.csv', '/path/to/file2.csv'], 'my_table')
    """

    cursor = mysql_conn.cursor()

    for index, file_path in enumerate(file_paths):
        if not file_path.endswith(".csv"):
            logging.warning(f"Skipping invalid file: {file_path}")
            continue

        logging.info(
            f"{table_name.upper()} ({index + 1}/{len(file_paths)}) - Loading {file_path} into {table_name} table..."
        )

        sql = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ';'
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE 1 LINES;
        """
        try:
            cursor.execute(sql)
            mysql_conn.commit()
            logging.info(
                f"{table_name.upper()} ({index + 1}/{len(file_paths)}) - Successfully loaded {file_path} into {table_name} table."
            )
        except Exception as e:
            mysql_conn.rollback()
            logging.error(
                f"{table_name.upper()} ({index + 1}/{len(file_paths)}) - Failed to load {file_path}: {e}"
            )

    cursor.close()


def read_sql_file(url: str, delimiter: str = ";", multiple: bool = False):
    """
    Reads and executes SQL statements from a file.
    Args:
        url (str): The file path to the SQL file.
        delimiter (str): The delimiter used to separate SQL statements.
        multiple (bool): If True, executes multiple SQL statements in a single call.
    Raises:
        IOError: If the file cannot be opened.
        mysql.connector.Error: If there is an error executing any of the SQL statements.
    """

    cursor = mysql_conn.cursor()

    with open(url, "r") as f:
        sql_script = f.read()
        if not multiple:
            for statement in sql_script.split(delimiter):
                if statement.strip():
                    cursor.execute(statement)
        else:
            cursor.execute(sql_script, multi=True)

    cursor.close()


def get_latest_transformed_data():
    """
    Retrieves the latest transformed data files from the specified directory.
    This function lists the available months in the TRANSFORMED_PATH directory,
    sorts them in reverse order (latest first), and either prompts the user to
    select a month or automatically selects the latest month based on the
    configuration settings. It then returns a list of file paths for the
    transformed data files in the selected month.
    Raises:
        FileNotFoundError: If no available months are found in the TRANSFORMED_PATH directory.
    Returns:
        list: A list of file paths for the transformed data files in the selected month.
    """

    months = sorted(os.listdir(TRANSFORMED_PATH), reverse=True)
    if not months:
        raise FileNotFoundError("No available months found.")

    month = ask_month(months) if config["settings"]["ask_user"] else months[0]
    return [
        os.path.join(TRANSFORMED_PATH, month, file)
        for file in sorted(os.listdir(os.path.join(TRANSFORMED_PATH, month)))
    ]


def load_data(transformed_data: list[str] = None):
    """
    Loads transformed data into the database.

    This function performs the following steps:
    1. Drops and recreates the necessary tables.
    2. Iterates over the tables to load, retrieves the corresponding files from the transformed data,
       and loads the CSV files into the database.
    3. Prints a completion message.
    4. Closes the database connections.

    Args:
        transformed_data (list[str]): A list of transformed data file paths.
    Returns:
        None
    """

    # Get latest transformed data
    transformed_data = transformed_data or get_latest_transformed_data()

    # Resetting database state
    drop_and_recreate_tables()
    
    # Creating triggers
    logging.info("Creating triggers...")
    read_sql_file("src/sql/create_triggers.sql")

    # Insert data for id_tables
    logging.info("Inserting default data...")
    read_sql_file("src/sql/default_insert.sql")

    # Insert missing data on RFB
    logging.info("Inserting missing data...")
    read_sql_file("src/sql/missing_data.sql")

    # Insert data from CSV
    for prefix, table in TABLES_TO_LOAD.items():
        files = get_separated_files(prefix, transformed_data)
        if files:
            load_csv_to_db(files, table)
            logging.info(f"{table} loaded successfully.")

    logging.info("Data loading complete.")

    # Close connections
    mysql_conn.close()
    sql_alchemy_conn.close()
