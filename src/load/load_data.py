import pandas as pd
import psycopg2
from utils.helpers import load_config
from utils.database.conn import MYSQL_CONN, PG_CONN, SQL_ALCHEMY_CONN

# Configuration
config = load_config()
READ_CHUNK_SIZE = config["performance"]["read_chunk_size"]
WRITE_CHUNK_SIZE = config["performance"]["write_chunk_size"]


# Database connections
mysql_conn = MYSQL_CONN.get_connection()
sql_alchemy_conn = SQL_ALCHEMY_CONN.get_session()


def get_separated_files(name: str, raw_data: list[str]) -> list[str]:
    return [file for file in raw_data if file.lower().startswith(name)]


def load_csv_to_db(file_path, table_name):
    cursor = mysql_conn.cursor()
    
    # Make this


def load_data(transformed_data: list[str]):

    # Separate files
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

    mysql_conn.close()
    sql_alchemy_conn.close()
