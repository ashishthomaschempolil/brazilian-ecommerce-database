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
# from database import create_db
sns.set()
import psycopg2

# %%
import database.utils.create_tables as create_tables

for k in create_tables.create_tables.items():
    print(k)

# %%
#list files in the data folder

os.listdir('data')

# %% [markdown]
# `olist_orders_dataset.csv`: ['order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']
#
#
# `olist_order_reviews_dataset.csv`: ['review_comment_title', 'review_comment_message']
#
# `olist_products_dataset.csv`: ['product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']

# %%
os.environ

# %%
#create a connection to the database
user = 'postgres'
password = 'postgrespw'
host = 'host.docker.internal'
port = 55003
db = 'brazil'
conn_url = (
            "postgres://postgres:postgrespw@localhost:55000"
        )
conn = psycopg2.connect(conn_url)
conn.set_session(autocommit=True) #so that create table & database statements are committed
cur = conn.cursor()


# %%
tables = {}

tables['order_payments'] = """CREATE TABLE IF NOT EXISTS order_payments
(
    order_id character varying(45)[] NOT NULL,
    payment_sequential integer,
    payment_type character varying(32)[],
    payment_installments integer,
    payment_value numeric(3)[],
    PRIMARY KEY (order_id)
);"""
tables['order_reviews'] = """ CREATE TABLE IF NOT EXISTS order_reviews
(
    order_id character varying(45)[] NOT NULL,
    review_id character varying(45)[],
    review_score numeric,
    review_comment_title character varying(45)[],
    review_comment_message character varying(100)[],
    review_creation_date timestamp without time zone[],
    review_answer_timestamp timestamp without time zone,
    PRIMARY KEY (order_id)
);"""
tables['order_items'] = """ CREATE TABLE IF NOT EXISTS order_items
(
    order_id character varying(45)[] NOT NULL,
    order_item_id numeric,
    product_id character varying(45)[],
    seller_id character varying(45)[],
    shipping_limit_date timestamp without time zone,
    price numeric,
    freight_value numeric,
    PRIMARY KEY (order_id)
);"""
tables['orders'] = """ CREATE TABLE IF NOT EXISTS orders
(
    order_id character varying(45)[] NOT NULL,
    customer_id character varying(45)[],
    order_status character varying(10)[],
    order_purchase_timestamp timestamp without time zone,
    order_approved_timestamp timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    PRIMARY KEY (order_id)
);"""
tables['products'] = """ CREATE TABLE IF NOT EXISTS products
(
    product_id character varying(45)[] NOT NULL,
    product_category_name character varying(45)[],
    product_name_length numeric,
    product_description_length numeric,
    product_photos_qty numeric,
    product_weight_g numeric,
    product_length_cm numeric,
    product_height_cm numeric,
    product_width_cm numeric,
    PRIMARY KEY (product_id)
);"""
tables['sellers'] = """ CREATE TABLE IF NOT EXISTS sellers
(
    seller_id character varying(45)[] NOT NULL,
    seller_zip_code_prefix numeric,
    seller_city character varying(45)[],
    seller_state character varying(3)[],
    PRIMARY KEY (seller_id)
);"""
tables['customers'] = """ CREATE TABLE IF NOT EXISTS customers
(
    customer_id character varying(45)[] NOT NULL,
    customer_unique_id character varying(45)[],
    customer_zip_code_prefix numeric,
    customer_city character varying(45),
    customer_state character varying(45)[],
    PRIMARY KEY (customer_id)
);"""
tables['geolocation'] = """ CREATE TABLE IF NOT EXISTS geolocation
(
    geolocation_zipcode_prefix numeric NOT NULL,
    geolocation_lat numeric,
    geolocation_lng numeric,
    geolocation_city character varying(45)[],
    geolocation_state character varying(45)[],
    PRIMARY KEY (geolocation_zipcode_prefix)
);"""

# %%
print(tables[table])

# %%
for table in tables:
    print(f"Creaing table: {table}")
    cur.execute(tables[table])
    conn.commit()

# %%
#show the tables in the database
alter_tables = {}

alter_tables['geolocation'] = """ALTER TABLE IF EXISTS customers
    ADD FOREIGN KEY (customer_zip_code_prefix)
    REFERENCES geolocation (geolocation_zipcode_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables['sellers'] = """ALTER TABLE IF EXISTS sellers
    ADD FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES geolocation (geolocation_zipcode_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables['orders1'] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_payments FOREIGN KEY (order_id)
    REFERENCES order_payments (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables['orders2'] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_reviews FOREIGN KEY (order_id)
    REFERENCES order_reviews (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables['orders3'] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT customers FOREIGN KEY (customer_id)
    REFERENCES customers (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables['orders4'] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_items FOREIGN KEY (order_id)
    REFERENCES order_items (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables['order_items1'] = """ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (product_id)
    REFERENCES products (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables['order_items2'] = """ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (seller_id)
    REFERENCES sellers (seller_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""


# %%
for alter_table in alter_tables:
    print(f"Altering table: {alter_table}")
    cur.execute(alter_tables[alter_table])
    conn.commit()

# %%
cur.execute("""
SELECT *
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND 
    schemaname != 'information_schema';
""")

# %%
cur.execute("""


-- DROP DATABASE IF EXISTS brazil;

CREATE DATABASE brazil
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


CREATE TABLE IF NOT EXISTS public.order_payments
(
    order_id character varying(45)[] NOT NULL,
    payment_sequential integer,
    payment_type character varying(32)[],
    payment_installments integer,
    payment_value numeric(3)[],
    PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS public.products
(
    product_id character varying(45)[] NOT NULL,
    product_category_name character varying(45)[],
    product_name_length numeric,
    product_description_length numeric,
    product_photos_qty numeric,
    product_weight_g numeric,
    product_length_cm numeric,
    product_height_cm numeric,
    product_width_cm numeric,
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS public.order_reviews
(
    order_id character varying(45)[] NOT NULL,
    review_id character varying(45)[],
    review_score numeric,
    review_comment_title character varying(45)[],
    review_comment_message character varying(100)[],
    review_creation_date timestamp without time zone[],
    review_answer_timestamp timestamp without time zone,
    PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS public.customers
(
    customer_id character varying(45)[] NOT NULL,
    customer_unique_id character varying(45)[],
    customer_zip_code_prefix numeric,
    customer_city character varying(45),
    customer_state character varying(45)[],
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS public.geolocation
(
    geolocation_zipcode_prefix numeric NOT NULL,
    geolocation_lat numeric,
    geolocation_lng numeric,
    geolocation_city character varying(45)[],
    geolocation_state character varying(45)[],
    PRIMARY KEY (geolocation_zipcode_prefix)
);

CREATE TABLE IF NOT EXISTS public.sellers
(
    seller_id character varying(45)[] NOT NULL,
    seller_zip_code_prefix numeric,
    seller_city character varying(45)[],
    seller_state character varying(3)[],
    PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS public.order_items
(
    order_id character varying(45)[] NOT NULL,
    order_item_id numeric,
    product_id character varying(45)[],
    seller_id character varying(45)[],
    shipping_limit_date timestamp without time zone,
    price numeric,
    freight_value numeric,
    PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS public.orders
(
    order_id character varying(45)[] NOT NULL,
    customer_id character varying(45)[],
    order_status character varying(10)[],
    order_purchase_timestamp timestamp without time zone,
    order_approved_timestamp timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    PRIMARY KEY (order_id)
);

END;""")

# %%
"""ALTER TABLE IF EXISTS geolocation
    ADD CONSTRAINT customers FOREIGN KEY (geolocation_zipcode_prefix)
    REFERENCES customers (customer_zip_code_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS sellers
    ADD FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES geolocation (geolocation_zipcode_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (product_id)
    REFERENCES products (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (seller_id)
    REFERENCES sellers (seller_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_payments FOREIGN KEY (order_id)
    REFERENCES order_payments (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_reviews FOREIGN KEY (order_id)
    REFERENCES order_reviews (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT customers FOREIGN KEY (customer_id)
    REFERENCES customers (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;


ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_items FOREIGN KEY (order_id)
    REFERENCES order_items (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""
