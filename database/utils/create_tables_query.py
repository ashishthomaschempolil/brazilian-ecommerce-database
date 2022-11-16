create_tables_query_dict = {}

create_tables_query_dict[
    "order_payments"
] = """CREATE TABLE IF NOT EXISTS order_payments
(
    order_id text NOT NULL,
    payment_sequential integer,
    payment_type text,
    payment_installments integer,
    payment_value numeric(3),
    PRIMARY KEY (order_id)
);"""
create_tables_query_dict[
    "order_reviews"
] = """ CREATE TABLE IF NOT EXISTS order_reviews
(
    order_id text NOT NULL,
    review_id text,
    review_score numeric,
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp without time zone,
    review_answer_timestamp timestamp without time zone,
    PRIMARY KEY (order_id)
);"""
create_tables_query_dict[
    "order_items"
] = """ CREATE TABLE IF NOT EXISTS order_items
(
    order_id text NOT NULL,
    order_item_id numeric,
    product_id text,
    seller_id text,
    shipping_limit_date timestamp without time zone,
    price numeric,
    freight_value numeric,
    PRIMARY KEY (order_id)
);"""
create_tables_query_dict[
    "orders"
] = """ CREATE TABLE IF NOT EXISTS orders
(
    order_id text NOT NULL,
    customer_id text,
    order_status text,
    order_purchase_timestamp timestamp without time zone,
    order_approved_timestamp timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone,
    PRIMARY KEY (order_id)
);"""
create_tables_query_dict[
    "products"
] = """ CREATE TABLE IF NOT EXISTS products
(
    product_id text NOT NULL,
    product_category_name text,
    product_name_length numeric,
    product_description_length numeric,
    product_photos_qty numeric,
    product_weight_g numeric,
    product_length_cm numeric,
    product_height_cm numeric,
    product_width_cm numeric,
    PRIMARY KEY (product_id)
);"""
create_tables_query_dict[
    "sellers"
] = """ CREATE TABLE IF NOT EXISTS sellers
(
    seller_id text NOT NULL,
    seller_zip_code_prefix numeric,
    seller_city text,
    seller_state text,
    PRIMARY KEY (seller_id)
);"""
create_tables_query_dict[
    "customers"
] = """ CREATE TABLE IF NOT EXISTS customers
(
    customer_id text NOT NULL,
    customer_unique_id text,
    customer_zip_code_prefix numeric,
    customer_city text,
    customer_state text,
    PRIMARY KEY (customer_id)
);"""
create_tables_query_dict[
    "geolocation"
] = """ CREATE TABLE IF NOT EXISTS geolocation
(
    geolocation_zip_code_prefix numeric NOT NULL,
    geolocation_lat numeric,
    geolocation_lng numeric,
    geolocation_city text,
    geolocation_state text,
    PRIMARY KEY (geolocation_zip_code_prefix)
);"""
