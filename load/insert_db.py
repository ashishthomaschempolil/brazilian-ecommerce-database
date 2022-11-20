import pandas as pd
import os
from sqlalchemy import create_engine

def insert_into_sql(
    df: pd.DataFrame, table_name: str, engine: create_engine, chunksize: int = 1000
):
    """This function inserts a pandas dataframe into a postgres database.

    :param pd.DataFrame df: pandas dataframe to be inserted into the database
    :param str table_name: name of the table where the data will be inserted
    :param create_engine engine: sqlalchemy engine object
    :param int chunksize: number of rows to be inserted at a time, defaults to 1000
    """

    df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=chunksize)
    print(f"Data inserted into {table_name} table.")

#insert data stored in preprocessed folder into database
def insert_data_into_db(conn_url:str, db:str, preprocessed_folder:str = "./data/preprocessed")->None:
    """Insert data into database

    :param str conn_url: the connection url to the database, defaults to conn_url
    :param str db: the name of the database to create, defaults to DB
    :param str preprocessed_folder: the path to the preprocessed folder, defaults to "./data/preprocessed"
    :return None:
    """
    conn_url = f"{conn_url}/{db}"
    #connect to database
    engine = create_engine(conn_url)

    geolocation = pd.read_csv(os.path.join(preprocessed_folder, "geolocation.csv"))
    insert_into_sql(geolocation, "geolocation", engine)

    sellers = pd.read_csv(os.path.join(preprocessed_folder, "sellers.csv"))
    insert_into_sql(sellers, "sellers", engine)

    products = pd.read_csv(os.path.join(preprocessed_folder, "products.csv"))
    insert_into_sql(products, "products", engine)

    customers = pd.read_csv(os.path.join(preprocessed_folder, "customers.csv"))
    insert_into_sql(customers, "customers", engine)

    order_payments = pd.read_csv(os.path.join(preprocessed_folder, "order_payments.csv"))
    insert_into_sql(order_payments, "order_payments", engine)

    order_reviews = pd.read_csv(os.path.join(preprocessed_folder, "order_reviews.csv"))
    insert_into_sql(order_reviews, "order_reviews", engine)

    order_items = pd.read_csv(os.path.join(preprocessed_folder, "order_items.csv"))
    insert_into_sql(order_items, "order_items", engine)

    orders = pd.read_csv(os.path.join(preprocessed_folder, "orders.csv"))
    insert_into_sql(orders, "orders", engine)

    print("Inserting data done!")

