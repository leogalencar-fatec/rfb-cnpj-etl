import psycopg2
import mysql.connector
from utils.helpers import load_config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Configuration
config = load_config()

DB_HOST = config["database"]["host"]
DB_USERNAME = config["database"]["username"]
DB_PASSWORD = config["database"]["password"]
DB_NAME = config["database"]["database_name"]
DB_PORT = config["database"]["port"]

SQL_ALCHEMY_MYSQL_URL = (
    f"mysql+mysqlconnector://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
SQL_ALCHEMY_PG_URL = (
    f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
PG_URL = f"dbname={DB_NAME} user={DB_USERNAME} host={DB_HOST} port={DB_HOST} password={DB_PASSWORD}"

# Classes definition

class MySQLConn:
    """
    A class used to manage a connection to a MySQL database using mysql.connector.

    Attributes:
      host (str): The hostname or IP address of the MySQL server.
      user (str): The username used to authenticate with the MySQL server.
      password (str): The password used to authenticate with the MySQL server.
      database (str): The name of the database to connect to.
      port (int): The port number on which the MySQL server is listening.
      conn (mysql.connector.connection_cext.CMySQLConnection or None): The connection object to the MySQL database.


    Methods:
      connect(host, user, password, database, port):
        Establishes a connection to the MySQL database if not already connected.
      get_connection():
        Returns the current connection object, establishing a connection if not already connected.
      close_connection():
        Closes the connection to the MySQL database if it is open.
    """

    def __init__(self, host, user, password, database, port):
        self.conn = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port

    def connect(self, host, user, password, database, port):
        if self.conn is None:
            self.conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database, port=port
            )

    def get_connection(self):
        if self.conn is None:
            self.connect()
        return self.conn

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class Psycopg2Conn:
    """
    A class used to manage a connection to a PostgreSQL database using psycopg2.

    Attributes:
      url (str): The database connection URL.
      conn (psycopg2.extensions.connection or None): The connection object to the PostgreSQL database.

    Methods:
        connect():
          Establishes a connection to the PostgreSQL database if not already connected.
        get_connection():
          Returns the current connection object, establishing a connection if not already connected.
        close_connection():
          Closes the connection to the PostgreSQL database if it is open.
    """

    def __init__(self, url):
        self.conn = None
        self.url = url

    def connect(self):
        if self.conn is None:
            self.conn = psycopg2.connect(self.url)

    def get_connection(self):
        if self.conn is None:
            self.connect()
        return self.conn

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None


class SQLAlchemyConn:
    """
    A class used to manage SQLAlchemy database connections.

    Attributes:
      url (str): The database URL.

    Methods:
      connect():
        Establishes a connection to the database and initializes the session factory.
      get_session():
        Returns a new session object. If the connection is not established, it will call connect() first.
      close_connection():
        Closes the database connection and disposes of the engine.
    """

    def __init__(self, url):
        self.engine = None
        self.Session = None
        self.url = url

    def connect(self):
        if self.engine is None:
            self.engine = create_engine(self.url)
            self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        if self.engine is None:
            self.connect()
        return self.Session()

    def get_engine(self):
        return self.engine

    def close_connection(self):
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            self.Session = None


# Export connections instances
PG_CONN = Psycopg2Conn(PG_URL)
MYSQL_CONN = MySQLConn(
    host=DB_HOST, user=DB_USERNAME, password=DB_PASSWORD, database=DB_NAME, port=DB_PORT
)
SQL_ALCHEMY_MYSQL_CONN = SQLAlchemyConn(SQL_ALCHEMY_MYSQL_URL)
