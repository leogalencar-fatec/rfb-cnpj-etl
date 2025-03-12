import pandas as pd
import psycopg2
from utils.helpers import load_config
from utils.database.conn import MYSQL_CONN, SQL_ALCHEMY_MYSQL_CONN

# Configuration
config = load_config()
READ_CHUNK_SIZE = config["performance"]["read_chunk_size"]
WRITE_CHUNK_SIZE = config["performance"]["write_chunk_size"]


# Database connections
mysql_conn = MYSQL_CONN.get_connection()
sql_alchemy_conn = SQL_ALCHEMY_MYSQL_CONN.get_session()


def get_separated_files(name: str, data: list[str]) -> list[str]:
    print(data)
    return [file for file in data if file.lower().startswith(name)]

def drop_and_recreate_tables():
    """Drops all tables and recreates them using the optimized schema."""
    cursor = mysql_conn.cursor()

    print("Dropping existing tables...")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    tables = ["socio", "simples", "estabelecimento", "empresa", 
              "cnae", "motivo", "municipio", "natureza_juridica", "pais", "qualificacao_socio",
              "id_matriz_filial", "id_porte_empresa", "id_situacao_cadastral", "id_socio", "id_faixa_etaria"]
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

    print("Recreating tables...")
    with open("src/sql/create_tables.sql", "r") as f:
            sql_script = f.read()
            for statement in sql_script.split(';'):
                if statement.strip():
                    cursor.execute(statement)

    mysql_conn.commit()
    cursor.close()


def load_csv_to_db(file_paths: list[str], table_name: str):
    """
    Loads CSV files into MySQL using `LOAD DATA INFILE` for efficiency.
    Assumes first file has headers, others do not.
    """
    cursor = mysql_conn.cursor()

    for index, file_path in enumerate(file_paths):
        has_headers = index == 0  # Only first file has headers
        ignore_lines = 1 if has_headers else 0
        
        print(f"Loading {file_path} into {table_name} (headers: {has_headers})...")

        sql = f"""
        LOAD DATA INFILE '{file_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ';'
        ENCLOSED BY '"'
        LINES TERMINATED BY '\\n'
        IGNORE {ignore_lines} LINES;
        """
        try:
            cursor.execute(sql)
            mysql_conn.commit()
            print(f"Successfully loaded {file_path} into {table_name}.")
        except Exception as e:
            print(f"Failed to load {file_path}: {e}")

    cursor.close()


def load_data(transformed_data: list[str]):
    """Orchestrates the data loading process."""
    drop_and_recreate_tables()

    # Separate files per table
    files_empresa = get_separated_files("empresa", transformed_data)
    files_estabelecimento = get_separated_files("estabelecimento", transformed_data)
    files_socio = get_separated_files("socio", transformed_data)
    files_simples = get_separated_files("simples", transformed_data)
    files_cnae = get_separated_files("cnae", transformed_data)
    files_motivo = get_separated_files("motivo", transformed_data)
    files_municipio = get_separated_files("municipio", transformed_data)
    files_natureza_juridica = get_separated_files("natureza", transformed_data)
    files_pais = get_separated_files("pais", transformed_data)
    files_qualificacao_socio = get_separated_files("qualifica", transformed_data)

    # Load data using optimized `LOAD DATA INFILE`
    if files_empresa:
        load_csv_to_db(files_empresa, "empresa")
    if files_estabelecimento:
        load_csv_to_db(files_estabelecimento, "estabelecimento")
    if files_socio:
        load_csv_to_db(files_socio, "socio")
    if files_simples:
        load_csv_to_db(files_simples, "simples")
    if files_cnae:
        load_csv_to_db(files_cnae, "cnae")
    if files_motivo:
        load_csv_to_db(files_motivo, "motivo")
    if files_municipio:
        load_csv_to_db(files_municipio, "municipio")
    if files_natureza_juridica:
        load_csv_to_db(files_natureza_juridica, "natureza_juridica")
    if files_pais:
        load_csv_to_db(files_pais, "pais")
    if files_qualificacao_socio:
        load_csv_to_db(files_qualificacao_socio, "qualificacao_socio")

    print("Data loading complete.")

    # Close connections
    mysql_conn.close()
    sql_alchemy_conn.close()