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
import matplotlib.pyplot as plt
import seaborn as sns
import os
import create_db
sns.set()
import psycopg2 #postgres
from sqlalchemy import create_engine #for inserting data from pd dataframe into postgres
import unidecode
import geocoder
import tqdm
tqdm.tqdm.pandas()
import difflib


# %% [markdown]
# `olist_orders_dataset.csv`: ['order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date']
#
#
# `olist_order_reviews_dataset.csv`: ['review_comment_title', 'review_comment_message']
#
# `olist_products_dataset.csv`: ['product_category_name', 'product_name_lenght', 'product_description_lenght', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']

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

def convert_to_unaccented_string(string: str)->str:
    """Converts a string to an unaccented string.
    Eg: "café" -> "cafe"

    :param str string: The string to convert
    :return str: The converted string
    """
    return unidecode.unidecode(string)

def preprocess_city_column(df:pd.DataFrame, column: str)->pd.DataFrame:
    """Preprocesses the city column of the dataframe.

    :param pd.DataFrame df: The dataframe
    :param str column: The column to preprocess
    :return pd.DataFrame: The preprocessed dataframe
    """
    df[column] = df[column].apply(convert_to_unaccented_string) #convert to unaccented string
    df[column] = df[column].str.lower() #convert to lowercase
    df[column] = df[column].str.replace("_", " ") #replace _ with space
    df[column] = df[column].str.split("/").str[0].str.strip() #remove everything after the first /
    df[column] = df[column].str.split("\\").str[0].str.strip() #remove everything after the first \

    return df

def set_asstr(df: pd.DataFrame, column: str)->pd.DataFrame:
    """Sets the column as a string type.

    :param pd.DataFrame df: The dataframe
    :param str column: The column to set as string
    :return pd.DataFrame: The dataframe with the column set as string
    """
    df[column] = df[column].astype(str)
    return df

def preprocess_geolocation(df: pd.DataFrame)->pd.DataFrame:
    """Preprocesses the geolocation dataframe.

    :param pd.DataFrame df: The geolocation dataframe
    :param list cities: The list of standard city names
    :return pd.DataFrame: The preprocessed dataframe
    """
    df = preprocess_city_column(df, "geolocation_city")
    df.drop(columns = ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"], inplace = True)
    df.drop_duplicates(inplace=True, keep="first")
    df['geolocation_city'] = df['geolocation_city'].astype(str)
    df['geolocation_state'] = df['geolocation_state'].astype(str)
    return df

def append_rows_to_geolocation(df: pd.DataFrame, columns: list, geolocation_df: pd.DataFrame)->pd.DataFrame:
    """Appends (city, state) values that are present in df but not in geolocation_df to geolocation_df.

    :param pd.DataFrame df: The dataframe containing the (city, state) values to be appended
    :param list columns: The list of column names containing the (city, state) values respectively
    :param pd.DataFrame geolocation_df: The geolocation dataframe
    :return pd.DataFrame: The merged geolocation dataframe
    """
    geolocation_df['city_state'] = geolocation_df['geolocation_city'] + "_" + geolocation_df['geolocation_state']
    
    #now for df
    df['city_state'] = df[columns[0]] + "_" + df[columns[1]]
    df.drop_duplicates(subset=["city_state"], inplace=True, keep="first")


    #get the rows that are present in df but not in geolocation_df
    df = df[~df['city_state'].isin(geolocation_df['city_state'])]

    df.rename(columns={columns[0]: "geolocation_city", columns[1]: "geolocation_state"}, inplace=True)
    
    #append the rows to geolocation_df
    geolocation_df = geolocation_df.append(df.loc[:, ['geolocation_city', 'geolocation_state', "city_state"]], ignore_index=True)

    #drop city_state column
    geolocation_df.drop(columns=["city_state"], inplace=True)

    return geolocation_df

    

def make_common_city_values(df: pd.DataFrame, column:str, cities:list)->pd.DataFrame:
    """Makes city values standard across different dataframes

    :param pd.DataFrame df: The dataframe
    :param str column: The city column
    :param list cities: The list of standard city names
    :return pd.DataFrame: The dataframe with standard city names
    """    
    for index, row in df.iterrows():
        if row[column] not in cities:
            closest_match = difflib.get_close_matches(row[column], cities, n=1, cutoff=0.7)
            if closest_match: #if there is a match, assign it otherwise leave it as it is
                df.loc[index, column] = closest_match
    return df


# %%
for file in os.listdir("./data/"):
    if file.endswith(".csv"):
        print(file)

# %% [markdown]
# Here the product_category_name column is in portugese, here we already the correspoding translation in english in the `product_category_name_translation.csv` file. So we will map the values from that file to the `product_category_name` column in the `olist_products_dataset.csv` file.

# %%
#cities
cities = pd.read_csv("./data/cities.csv")
cities = preprocess_city_column(cities, "name").name.values

#sellers
sellers = pd.read_csv("./data/olist_sellers_dataset.csv")
sellers.drop(columns = ["seller_zip_code_prefix"], inplace = True)
sellers = preprocess_city_column(sellers, "seller_city")
sellers = set_asstr(sellers, "seller_city")
sellers = set_asstr(sellers, "seller_state")

#orders
orders = pd.read_csv("./data/olist_orders_dataset.csv")

#order items
order_items = pd.read_csv("./data/olist_order_items_dataset.csv")

#customers
customers = pd.read_csv("./data/olist_customers_dataset.csv")
customers.drop(columns = ["customer_zip_code_prefix"], inplace = True)
customers = preprocess_city_column(customers, "customer_city")
customers = make_common_city_values(customers, "customer_city", cities)
customers = set_asstr(customers, "customer_city")
customers = set_asstr(customers, "customer_state")

#geo location
geolocation = pd.read_csv("./data/olist_geolocation_dataset.csv")
geolocation = preprocess_geolocation(geolocation)
geolocation = make_common_city_values(geolocation, "geolocation_city", cities)
geolocation = set_asstr(geolocation, "geolocation_city")
geolocation = set_asstr(geolocation, "geolocation_state")

#append those cities that are present in customers and sellers
geolocation = append_rows_to_geolocation(sellers, ["seller_city", "seller_state"], geolocation)
geolocation = append_rows_to_geolocation(customers, ["customer_city", "customer_state"], geolocation)
geolocation.reset_index(drop=True, inplace=True)
geolocation['geolocation_id'] = geolocation.index + 1
geolocation = geolocation.loc[:, ["geolocation_id", "geolocation_city", "geolocation_state"]]

#now we map the ids and create a new column for that in sellers and customers
sellers = pd.merge(sellers, geolocation, how="left", left_on=["seller_city", "seller_state"], right_on=["geolocation_city", "geolocation_state"]).drop(columns=["geolocation_city", "geolocation_state", "seller_city", "seller_state"])

customers = pd.merge(customers, geolocation, how="left", left_on=["customer_city", "customer_state"], right_on=["geolocation_city", "geolocation_state"]).drop(columns=["geolocation_city", "geolocation_state", "customer_city", "customer_state"])


#order payments
order_payments = pd.read_csv("./data/olist_order_payments_dataset.csv")

#order reviews
order_reviews = pd.read_csv("./data/olist_order_reviews_dataset.csv")

#products
products = pd.read_csv("./data/olist_products_dataset.csv")

#product category name translation
product_category_name_translation = pd.read_csv("./data/product_category_name_translation.csv")


map_values = dict(zip(product_category_name_translation['product_category_name'], product_category_name_translation['product_category_name_english']))

products['product_category_name'] = products['product_category_name'].map(map_values)

# %%

# %%
sellers['city_state'] = sellers['seller_city'] + "_" + sellers['seller_state']
# geolocation['city_state'] = geolocation['geolocation_city'].str.cat(geolocation['geolocation_state'].str)

# temp = sellers[~sellers['city_state'].isin(geolocation['city_state'])]

# temp

# %%
geolocation.state = geolocation.geolocation_state.astype(str)

# %%
customers['customer_city'] = customers['customer_city'].astype(str)

# %%
geolocation.geolocation_city = geolocation.geolocation_city.astype(str)

# %%
#find all zip_code_prefix not in geolocation but in sellers
sellers['city_state'] = sellers['seller_city'] + '_' + sellers['seller_state']
geolocation['city_state'] = geolocation['geolocation_city'] + '_' + geolocation['geolocation_state']

#get city states values not in geolocation
city_states = [city_state for city_state in sellers['city_state'].unique() if city_state not in geolocation['city_state'].unique()]

temp = sellers.drop_duplicates(subset=['city_state'], keep='first')
temp = temp[temp['city_state'].isin(city_states)]



# %%
sellers.shape

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

# %%
cities = pd.read_csv("cities.csv")

cities.name = cities.name.apply(convert_to_unaccented_string).str.lower()

# print(cities.shape)
cities = cities.name.unique()

# len(cities)
# cities.groupby('name').count().sort_values(by='state', ascending=False).query('state > 1')

# %%
import difflib

# %%
geolocation

# %%
#iter through the geolocation dataframe and get the most similiar city name in the cities dataframe using difflib

for index, row in geolocation.iterrows():
    if row['geolocation_city'] not in cities:
        row['geolocation_city'] = difflib.get_close_matches(row['geolocation_city'], cities, n=1, cutoff=0.8)
    


# %%
geolocation.geolocation_city.unique()

# %%
cities.query('name == "douradina"')

# %%
geolocation[geolocation.geolocation_city == 'douradina']

# %%
difflib.get_close_matches('jacarei / sao paulo', cities, n=1, cutoff=0.6)

# %%

# %%
for index, row in sellers.iterrows():
    if row['seller_city'] not in cities:
        row['seller_city'] = difflib.get_close_matches(row['seller_city'], cities, n=1, cutoff=0.8)

sellers.seller_city.unique()

# %%
sellers[sellers.seller_city == 'jacarei / sao paulo']

# %%
sellers.iloc[3015]

# %%
for index, row in sellers.iterrows():
    if row['seller_city'] not in cities:
        row['seller_city'] = difflib.get_close_matches(row['seller_city'], cities, n=1, cutoff=0.8)

sellers.seller_city.unique()

# %%
#remove special characters from the geolocation_city column
# geolocation['geolocation_city'] = geolocation['geolocation_city'].str.replace('[^a-zA-Z0-9]', '')

geolocation['geolocation_city'].value_counts()


# %%
#zipcode prefix in sellers not in geolocation
sellers[~sellers['seller_zip_code_prefix'].isin(geolocation['geolocation_zip_code_prefix'])]

# %%
geolocation.query('geolocation_city == "brasilia"')

# %%
for i in geolocation['geolocation_city'].unique():
            print(i)

# %%
g = geocoder.osm([-15.790439, -47.880655], method='reverse').json
try:
    g = g['city']
except:
    g = g['town']

# %%
g

# %%
geolocation

# %%
geolocation.drop('geolocation_zip_code_prefix', axis=1, inplace=True) #drop the zip code prefix column

#now remove all duplicate rows
geolocation.drop_duplicates(inplace=True)
geolocation.reset_index(drop=True, inplace=True)

#now add an id column to the geolocation dataframe
geolocation['geolocation_id'] = geolocation.index

geolocation = geolocation[['geolocation_id', 'geolocation_city', 'geolocation_state']]

geolocation.head()

# %%
#now map the geolocation_id to the seller dataframe

geolocation_dict = dict(zip(geolocation['geolocation_city'], geolocation['geolocation_id']))

sellers['geolocation_id'] = sellers['seller_city'].map(geolocation_dict)#.astype('int64')

#drop seller zip code prefix , seller city and seller state columns
# sellers.drop(['seller_zip_code_prefix', 'seller_city', 'seller_state'], axis=1, inplace=True)

# %%
sellers

# %%
sellers.query('geolocation_id.isnull()')

# %%
geolocation.query('geolocation_city.str.contains("[^a-zA-Z0-9 ]")')

# %%
#find the rows where the seller city is same but the format of the string stored is different

#find all rows where the city name have special characters 
sellers.query('seller_city.str.contains("[^a-zA-Z0-9 ]")')

# %%

# %%
sellers['seller_city'] = sellers['seller_city'].str.split(' ').str[0]

# %%
sellers.head()

# %%
#change the name of the column containeing lenght to length

cols_to_rename = {column: column.replace('lenght', 'length') for column in products.columns if 'lenght' in column}
products.rename(columns=cols_to_rename, inplace=True)
products.head()

# %%

geolocation.head()


# %%
geolocation.zip

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
geolocation

# %%
geolocation.geolocation_zip_code_prefix.value_counts()[geolocation.geolocation_zip_code_prefix.value_counts().index == 1046]

# %%
geolocation.query('geolocation_zip_code_prefix == 1037')

# %%
pd.read_sql_query("SELECT * FROM orders", engine)

# %%
#if zipcode not in geolocation append to geolocation
for index, row in sellers.iterrows():
    if row['seller_zip_code_prefix'] not in geolocation.geolocation_zip_code_prefix.values:
        print(row['seller_zip_code_prefix'])
        geolocation = geolocation.append({'geolocation_zip_code_prefix': row['seller_zip_code_prefix'], 'geolocation_city': row['seller_city'], 'geolocation_state': row['seller_state']}, ignore_index=True)

# %%
geolocation

# %%
