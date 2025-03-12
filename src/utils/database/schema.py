from sqlalchemy import Table, Column, String, Integer, Float, Boolean, MetaData
from constants.table_fields import TABLE_FIELDS
from utils.database.conn import SQL_ALCHEMY_CONN

metadata = MetaData()

SQL_TYPE_MAP = {
    "str": String,
    "float": Float,
}


def create_tables():
    """Creates tables in PostgreSQL if they do not exist."""
    tables = {}

    for table_name, fields in TABLE_FIELDS.items():
        columns = [
            Column(column_name, SQL_TYPE_MAP[column_type])
            for column_name, column_type in fields.items()
        ]

        # Add primary key (assumes first column is the PK)
        columns[0].primary_key = True

        tables[table_name] = Table(table_name, metadata, *columns)

    with SQL_ALCHEMY_CONN.get_session() as conn:
        metadata.create_all(conn)
    print("Tables created successfully.")
    
    return tables


# empresa = Table(
#     "empresa", metadata,
#     Column("cnpj", String, primary_key=True),
#     Column("razao_social", String),
#     Column("codigo_natureza_juridica", String),
#     Column("qualificacao_do_responsavel", String),
#     Column("capital_social", Float),
#     Column("porte", String),
#     Column("ente_federativo_responsavel", String),
# )
