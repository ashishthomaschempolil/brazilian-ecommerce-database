# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: Python 3.9.6 ('base')
#     language: python
#     name: python3
# ---

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import create_db
sns.set()
import psycopg2 #postgres
from sqlalchemy import create_engine #for inserting data from pd dataframe into postgres

# %% [markdown]
# `olist_orders_dataset.csv`: ['order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']
#
#
# `olist_order_reviews_dataset.csv`: ['review_comment_title', 'review_comment_message']
#
# `olist_products_dataset.csv`: ['product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']

# %%
for file in os.listdir("./data/"):
    if file.endswith(".csv"):
        print(file)

# %%
sellers = pd.read_csv("./data/olist_sellers_dataset.csv")
orders = pd.read_csv("./data/olist_orders_dataset.csv")
order_items = pd.read_csv("./data/olist_order_items_dataset.csv")
customers = pd.read_csv("./data/olist_customers_dataset.csv")
geolocation = pd.read_csv("./data/olist_geolocation_dataset.csv")
order_payments = pd.read_csv("./data/olist_order_payments_dataset.csv")
order_reviews = pd.read_csv("./data/olist_order_reviews_dataset.csv")
products = pd.read_csv("./data/olist_products_dataset.csv")
product_category_name_translation = pd.read_csv("./data/product_category_name_translation.csv")

# %%
products.head()

# %% [markdown]
# Here the product_category_name column is in portugese, here we already the correspoding translation in english in the `product_category_name_translation.csv` file. So we will map the values from that file to the `product_category_name` column in the `olist_products_dataset.csv` file.

# %%
#map the values of the product_category_name column to the values of the product_category_name_english column
sellers = pd.read_csv("./data/olist_sellers_dataset.csv")
orders = pd.read_csv("./data/olist_orders_dataset.csv")
order_items = pd.read_csv("./data/olist_order_items_dataset.csv")
customers = pd.read_csv("./data/olist_customers_dataset.csv")
geolocation = pd.read_csv("./data/olist_geolocation_dataset.csv")
order_payments = pd.read_csv("./data/olist_order_payments_dataset.csv")
order_reviews = pd.read_csv("./data/olist_order_reviews_dataset.csv")
products = pd.read_csv("./data/olist_products_dataset.csv")
product_category_name_translation = pd.read_csv("./data/product_category_name_translation.csv")

map_values = dict(zip(product_category_name_translation['product_category_name'], product_category_name_translation['product_category_name_english']))

products['product_category_name'] = products['product_category_name'].map(map_values)

products.head()

# %%
#change the name of the column containeing lenght to length

cols_to_rename = {column: column.replace('lenght', 'length') for column in products.columns if 'lenght' in column}
products.rename(columns=cols_to_rename, inplace=True)
products.head()


# %%
def insert_into_sql(df: pd.DataFrame, table_name: str, engine: create_engine, chunksize: int = 1000):
    """This function inserts a pandas dataframe into a postgres database.

    :param pd.DataFrame df: pandas dataframe to be inserted into the database
    :param str table_name: name of the table where the data will be inserted
    :param create_engine engine: sqlalchemy engine object
    :param int chunksize: number of rows to be inserted at a time, defaults to 1000
    """

    df.to_sql(table_name, engine, if_exists="append", index=False, chunksize=chunksize)
    print(f"Data inserted into {table_name} table.")


# %%
engine = create_engine("postgresql://user:user@localhost:5432/brazil")
# insert_into_sql(products, "products", engine)
insert_into_sql(geolocation, "geolocation", engine)
insert_into_sql(sellers, "sellers", engine)
insert_into_sql(orders, "orders", engine)
insert_into_sql(order_items, "order_items", engine)
insert_into_sql(customers, "customers", engine)

insert_into_sql(order_payments, "order_payments", engine)
insert_into_sql(order_reviews, "order_reviews", engine)


# %%
