import psycopg2
import os
import database.utils.create_tables_query as create_tables_query
import database.utils.alter_tables_query as alter_tables_query


def connect_to_db(conn_url: str) -> psycopg2.extensions.connection:
    """Connect to the POSGRESQL database and return the connection object

    :param str conn_url: the connection url to the database, defaults to conn_url
    :return psycopg2.extensions.connection: the connection object
    """
    conn = psycopg2.connect(conn_url)
    conn.autocommit = True
    return conn


def create_db(conn: psycopg2.extensions.connection, db: str) -> None:
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


def create_tables(
    conn: psycopg2.extensions.connection,
    create_tables_query_dict: dict = create_tables_query.create_tables_query_dict,
) -> None:
    """Create the tables in the database

    :param psycopg2.extensions.connection conn: the connection object
    :return None:
    """
    with conn.cursor() as cursor:
        for table_name, table_sql in create_tables_query_dict.items():
            try:
                cursor.execute(table_sql)
                print(f"Table {table_name} created successfully")
            except psycopg2.errors.DuplicateTable:
                print(f"Table {table_name} already exists")


def alter_tables(
    conn: psycopg2.extensions.connection,
    alter_tables_query_dict: dict = alter_tables_query.alter_tables_query_dict,
) -> None:
    """Alter the tables in the database to add foreign keys

    :param psycopg2.extensions.connection conn: the connection object
    :return None:
    """
    with conn.cursor() as cursor:
        for table_name, table_sql in alter_tables_query_dict.items():
            print(table_name)
            try:
                cursor.execute(table_sql)
                print(f"Table {table_name} altered successfully")
            except psycopg2.errors.DuplicateTable:
                print(f"Table {table_name} already exists")


def main(conn_url: str, db: str) -> None:
    """Main function to create the database and tables

    :param str conn_url: the connection url to the database, defaults to conn_url
    :param str db: the name of the database to create, defaults to DB
    :return None:
    """
    conn = connect_to_db(conn_url + f"/{db}")
    create_tables(conn)
    alter_tables(conn)


if __name__ == "__main__":
    DB = os.environ.get("POSTGRES_DB")
    USER = os.environ.get("POSTGRES_USER")
    PASSWORD = os.environ.get("POSTGRES_PASSWORD")
    PORT = os.environ.get("POSTGRES_PORT")
    HOST = os.environ.get("POSTGRES_HOST")

    conn_url = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}"
    main(conn_url, DB)
