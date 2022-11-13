import psycopg2
import os
import utils.alter_tables as alter_tables
import utils.create_tables as create_tables


DB = os.environ.get("SQL_DB")
USER = os.environ.get("SQL_USER")
PASSWORD = os.environ.get("SQL_PASSWORD")
PORT = os.environ.get("SQL_PORT")
HOST = os.environ.get("SQL_HOST")

conn_url = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}"

def connect_to_db(conn_url: str = conn_url) -> psycopg2.extensions.connection:
    """Connect to the POSGRESQL database and return the connection object

    :param str conn_url: the connection url to the database, defaults to conn_url
    :return psycopg2.extensions.connection: the connection object
    """
    conn = psycopg2.connect(conn_url)
    conn.autocommit = True
    return conn


def create_db(conn: psycopg2.extensions.connection, db: str = DB) -> None:
    """Create a database

    :param psycopg2.extensions.connection conn: the connection object
    :param str db: the name of the database to create, defaults to DB
    :return None:
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(f"CREATE DATABASE {db}")
            print(f"Database {db} created successfully")
        except psycopg2.errors.DuplicateDatabase:
            print(f"Database {db} already exists")


def create_tables(conn: psycopg2.extensions.connection) -> None:
    """Create the tables in the database

    :param psycopg2.extensions.connection conn: the connection object
    :return None:
    """
    with conn.cursor() as cursor:
        for table_name, table_sql in create_tables.create_tables.items():
            try:
                cursor.execute(table_sql)
                print(f"Table {table_name} created successfully")
            except psycopg2.errors.DuplicateTable:
                print(f"Table {table_name} already exists")


def alter_tables(conn: psycopg2.extensions.connection) -> None:
    """Alter the tables in the database to add foreign keys

    :param psycopg2.extensions.connection conn: the connection object
    :return None:
    """
    with conn.cursor() as cursor:
        for table_name, table_sql in alter_tables.alter_tables.items():
            try:
                cursor.execute(table_sql)
                print(f"Table {table_name} altered successfully")
            except psycopg2.errors.DuplicateTable:
                print(f"Table {table_name} already exists")


def main(conn_url: str = conn_url, db: str = DB) -> None:
    """Main function to create the database and tables

    :param str conn_url: the connection url to the database, defaults to conn_url
    :param str db: the name of the database to create, defaults to DB
    :return None:
    """
    conn = connect_to_db(conn_url)
    create_db(conn, db)
    conn = connect_to_db(conn_url + f"/{db}")
    create_tables(conn)
    alter_tables(conn)

if __name__ == "__main__":
    main()