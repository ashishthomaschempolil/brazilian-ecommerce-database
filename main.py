import database.create_db as create_db
import transform.transform
import load.insert_db as insert_db
import os
import psycopg2



DB = os.environ.get("POSTGRES_DB")
USER = os.environ.get("POSTGRES_USER")
PASSWORD = os.environ.get("POSTGRES_PASSWORD")
PORT = os.environ.get("POSTGRES_PORT")
HOST = os.environ.get("POSTGRES_HOST")

#conn_url
conn_url = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}"

def pipeline(conn_url:str, db:str, raw_folder:str = "./data/raw",preprocessed_folder:str = "./data/preprocessed")->None:
    """This function creates a database and inserts data into the database

    :param str conn_url: the connection url to the database, defaults to conn_url
    :param str db: the name of the database to create, defaults to DB
    :param str raw_folder: the path to the raw folder, defaults to "./data/preprocessed"
    :param str preprocessed_folder: the path to the preprocessed folder, defaults to "./data/preprocessed"
    :return None:
    """
    #create database
    #get the number of tables in the database
    #if the number of tables is 0, insert data into the database
    #else, print that the database already exists
    create_db.main(conn_url, db)

    #preprocess data
    transform.transform.preprocess(raw_folder, preprocessed_folder)

    #insert data into database
    insert_db.insert_data_into_db(conn_url, db, preprocessed_folder)

if __name__ == "__main__":
    pipeline(conn_url, DB)
