# -*- coding: utf-8 -*-
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
import os
import database.create_db
import psycopg2  # postgres
from sqlalchemy import (
    create_engine,
)  # for inserting data from pd dataframe into postgres
import unidecode
import difflib
from transform import transform

# %% [markdown]
# `olist_orders_dataset.csv`: ['order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']
#
#
# `olist_order_reviews_dataset.csv`: ['review_comment_title', 'review_comment_message']
#
# `olist_products_dataset.csv`: ['product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']

# %% [markdown]
# Here the product_category_name column is in portugese, here we already the correspoding translation in english in the `product_category_name_translation.csv` file. So we will map the values from that file to the `product_category_name` column in the `olist_products_dataset.csv` file.

# %%
transform.preprocess()

# %%
#read all the preprocess data
customers = pd.read_csv("./data/preprocessed/customers.csv")
geolocation = pd.read_csv("./data/preprocessed/geolocation.csv")
order_items = pd.read_csv("./data/preprocessed/order_items.csv")
orders = pd.read_csv("./data/preprocessed/orders.csv")
products = pd.read_csv("./data/preprocessed/products.csv")
sellers = pd.read_csv("./data/preprocessed/sellers.csv")
order_items = pd.read_csv("./data/preprocessed/order_items.csv")
order_payments = pd.read_csv("./data/preprocessed/order_payments.csv")
order_reviews = pd.read_csv("./data/preprocessed/order_reviews.csv")

# %%
id = 'b81ef226f3fe1789b1e8b2acac839d17'
order_payments.query("order_id == @id")

# %%
orders.query("order_id == @id")

# %% [markdown]
# # Geolocation
#
# Editing geolocation:
# 1. We will remove all special characters and also the spaces in each city name.
# 2. Then we will find which all values are similiar and change them into one common name.
#
# 1. first find a list of all the towns and cities in brazil
# 2. Save it in geolocations database
# 3. Use difflib to find the closest match for each city in the database
# Use difflib to
#
# Remove all words after '\\' or '/'
