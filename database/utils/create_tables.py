create_tables = {}

create_tables[
    "order_payments"
] = """CREATE TABLE IF NOT EXISTS order_payments
(
    order_id character varying(45)[] NOT NULL,
    payment_sequential integer,
    payment_type character varying(32)[],
    payment_installments integer,
    payment_value numeric(3)[],
    PRIMARY KEY (order_id)
);"""
create_tables[
    "order_reviews"
] = """ CREATE TABLE IF NOT EXISTS order_reviews
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
create_tables[
    "order_items"
] = """ CREATE TABLE IF NOT EXISTS order_items
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
create_tables[
    "orders"
] = """ CREATE TABLE IF NOT EXISTS orders
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
create_tables[
    "products"
] = """ CREATE TABLE IF NOT EXISTS products
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
create_tables[
    "sellers"
] = """ CREATE TABLE IF NOT EXISTS sellers
(
    seller_id character varying(45)[] NOT NULL,
    seller_zip_code_prefix numeric,
    seller_city character varying(45)[],
    seller_state character varying(3)[],
    PRIMARY KEY (seller_id)
);"""
create_tables[
    "customers"
] = """ CREATE TABLE IF NOT EXISTS customers
(
    customer_id character varying(45)[] NOT NULL,
    customer_unique_id character varying(45)[],
    customer_zip_code_prefix numeric,
    customer_city character varying(45),
    customer_state character varying(45)[],
    PRIMARY KEY (customer_id)
);"""
create_tables[
    "geolocation"
] = """ CREATE TABLE IF NOT EXISTS geolocation
(
    geolocation_zipcode_prefix numeric NOT NULL,
    geolocation_lat numeric,
    geolocation_lng numeric,
    geolocation_city character varying(45)[],
    geolocation_state character varying(45)[],
    PRIMARY KEY (geolocation_zipcode_prefix)
);"""
