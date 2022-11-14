# show the tables in the database
alter_tables_query_dict = {}

alter_tables_query_dict[
    "geolocation"
] = """ALTER TABLE IF EXISTS customers
    ADD FOREIGN KEY (customer_zip_code_prefix)
    REFERENCES geolocation (geolocation_zipcode_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables_query_dict[
    "sellers"
] = """ALTER TABLE IF EXISTS sellers
    ADD FOREIGN KEY (seller_zip_code_prefix)
    REFERENCES geolocation (geolocation_zipcode_prefix) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables_query_dict[
    "orders1"
] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_payments FOREIGN KEY (order_id)
    REFERENCES order_payments (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables_query_dict[
    "orders2"
] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_reviews FOREIGN KEY (order_id)
    REFERENCES order_reviews (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables_query_dict[
    "orders3"
] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT customers FOREIGN KEY (customer_id)
    REFERENCES customers (customer_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables_query_dict[
    "orders4"
] = """ALTER TABLE IF EXISTS orders
    ADD CONSTRAINT order_items FOREIGN KEY (order_id)
    REFERENCES order_items (order_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables_query_dict[
    "order_items1"
] = """ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (product_id)
    REFERENCES products (product_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""

alter_tables_query_dict[
    "order_items2"
] = """ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (seller_id)
    REFERENCES sellers (seller_id) MATCH SIMPLE
    ON UPDATE NO ACTION
    ON DELETE NO ACTION
    NOT VALID;"""
