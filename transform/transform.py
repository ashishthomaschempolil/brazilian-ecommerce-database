import pandas as pd
import numpy as np
import os
import unidecode
import difflib
# import transform.utils as utils
import utils.utils as utils
import time

def preprocess(raw_folder:str = "./data/raw/", processed_folder:str = "./data/preprocessed")->None:
    """Preprocesses the raw data and saves the preprocessed data to the prepocessed folder inside the data folder

    :param str raw_folder: Path to raw data folder, defaults to "./data/raw/"
    :param str processed_folder: Path to preprocessed data folder, defaults to "./data/preprocessed"
    """

    start_time = time.time()
    # cities
    print("Preprocessing cities...")
    cities = pd.read_csv(os.path.join(raw_folder, "cities.csv"))
    cities = utils.preprocess_city_column(cities, "name").name.values

    # sellers
    print("Preprocessing sellers...")
    sellers = pd.read_csv(os.path.join(raw_folder, "olist_sellers_dataset.csv"))
    sellers.drop(columns=["seller_zip_code_prefix"], inplace=True)
    sellers = utils.preprocess_city_column(sellers, "seller_city")
    sellers = utils.set_asstr(sellers, "seller_city")
    sellers = utils.set_asstr(sellers, "seller_state")

    # orders
    print("Preprocessing orders...")
    orders = pd.read_csv(os.path.join(raw_folder, "olist_orders_dataset.csv"))

    # order items
    print("Preprocessing order items...")
    order_items = pd.read_csv(os.path.join(raw_folder, "olist_order_items_dataset.csv"))

    # customers
    print("Preprocessing customers...")
    customers = pd.read_csv(os.path.join(raw_folder, "olist_customers_dataset.csv"))
    customers.drop(columns=["customer_zip_code_prefix"], inplace=True)
    customers = utils.preprocess_city_column(customers, "customer_city")
    customers = utils.make_common_city_values(customers, "customer_city", cities)
    customers = utils.set_asstr(customers, "customer_city")
    customers = utils.set_asstr(customers, "customer_state")

    # geo location
    print("Preprocessing geolocation...")
    geolocation = pd.read_csv(os.path.join(raw_folder, "olist_geolocation_dataset.csv"))
    geolocation = utils.preprocess_geolocation(geolocation)
    geolocation = utils.make_common_city_values(geolocation, "geolocation_city", cities)
    geolocation = utils.set_asstr(geolocation, "geolocation_city")
    geolocation = utils.set_asstr(geolocation, "geolocation_state")

    # append those cities that are present in customers and sellers
    # but not in geolocation
    print("Appending cities that are present in customers and sellers but not in geolocation...")
    geolocation = utils.append_rows_to_geolocation(
        sellers, ["seller_city", "seller_state"], geolocation
    )
    geolocation = utils.append_rows_to_geolocation(
        customers, ["customer_city", "customer_state"], geolocation
    )
    geolocation.reset_index(drop=True, inplace=True)
    geolocation["geolocation_id"] = geolocation.index + 1
    geolocation = geolocation.loc[
        :, ["geolocation_id", "geolocation_city", "geolocation_state"]
    ]

    # now we map the ids and create a new column for that in sellers and customers
    sellers = pd.merge(
        sellers,
        geolocation,
        how="left",
        left_on=["seller_city", "seller_state"],
        right_on=["geolocation_city", "geolocation_state"],
    ).drop(columns=["geolocation_city", "geolocation_state", "seller_city", "seller_state"])

    customers = pd.merge(
        customers,
        geolocation,
        how="left",
        left_on=["customer_city", "customer_state"],
        right_on=["geolocation_city", "geolocation_state"],
    ).drop(
        columns=["geolocation_city", "geolocation_state", "customer_city", "customer_state"]
    )


    # order payments
    print("Preprocessing order payments...")
    order_payments = pd.read_csv(os.path.join(raw_folder, "olist_order_payments_dataset.csv"))

    # order reviews
    print("Preprocessing order reviews...")
    order_reviews = pd.read_csv(os.path.join(raw_folder, "olist_order_reviews_dataset.csv"))

    # products
    print("Preprocessing products...")
    products = pd.read_csv(os.path.join(raw_folder, "olist_products_dataset.csv"))

    # product category name translation
    product_category_name_translation = pd.read_csv(
        os.path.join(raw_folder,"product_category_name_translation.csv")
    )


    map_values = dict(
        zip(
            product_category_name_translation["product_category_name"],
            product_category_name_translation["product_category_name_english"],
        )
    )

    products["product_category_name"] = products["product_category_name"].map(map_values)

    #save all the dataframes to the processed folder
    print(f"Saving dataframes to {processed_folder}...")
    sellers.to_csv(os.path.join(processed_folder, "sellers.csv"), index=False)
    customers.to_csv(os.path.join(processed_folder, "customers.csv"), index=False)
    geolocation.to_csv(os.path.join(processed_folder, "geolocation.csv"), index=False)
    orders.to_csv(os.path.join(processed_folder, "orders.csv"), index=False)
    order_items.to_csv(os.path.join(processed_folder, "order_items.csv"), index=False)
    order_payments.to_csv(os.path.join(processed_folder, "order_payments.csv"), index=False)
    order_reviews.to_csv(os.path.join(processed_folder, "order_reviews.csv"), index=False)
    products.to_csv(os.path.join(processed_folder, "products.csv"), index=False)

    #time taken
    print(f"Time taken: {time.time() - start_time:.2} seconds")

if __name__ == "__main__":
    preprocess()


