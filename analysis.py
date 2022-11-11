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
import mysql.connector
from database import create_db
sns.set()

# %%
#list files in the data folder

os.listdir('data')

# %%
#check missing values for each dataset

#get only the columns with missing values for each values
for file in os.listdir('data'):
    df = pd.read_csv(f'data/{file}')
    print(file)
    print(df.columns[df.isna().any()].tolist())
    print('')
    



# %% [markdown]
# `olist_orders_dataset.csv`: ['order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']
#
#
# `olist_order_reviews_dataset.csv`: ['review_comment_title', 'review_comment_message']
#
# `olist_products_dataset.csv`: ['product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']

# %%
orders = pd.read_csv('data/olist_orders_dataset.csv')

orders.head()

# %%
orders.loc[orders.isna().any(axis=1)]

# %%
order_reviews = pd.read_csv('data/olist_order_reviews_dataset.csv')

order_reviews.head()

# %%
order_reviews.loc[order_reviews.isna().any(axis=1)]

# %%
products = pd.read_csv('data/olist_products_dataset.csv')

products.head()

# %%
translations = pd.read_csv('data/product_category_name_translation.csv')

#combine the portugese and english translations as a dictionary

map_items = dict(zip(translations['product_category_name'].values, translations['product_category_name_english'].values))

products['product_category_name'] = products['product_category_name'].map(map_items)

products.head()

# %%
translations.head(10)

# %%
len('b81ef226f3fe1789b1e8b2acac839d17')

# %%
customers = pd.read_csv('data/olist_customers_dataset.csv')

customers.head()

# %%
DB_NAME = 'brazil'
USER = os.environ.get('MYSQL_USER')
PASSWORD = os.environ.get('MYSQL_PASSWORD')

connection = mysql.connector.connect(user=USER, password=PASSWORD)
cursor = connection.cursor()



# %%
