# show the tables in the database
alter_tables_query_dict = {}


alter_tables_query_dict[
    "orders1"
] = """ALTER TABLE IF EXISTS order_payments
    ADD FOREIGN KEY (order_id)
    REFERENCES orders (order_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""

alter_tables_query_dict[
    "orders2"
] = """ALTER TABLE IF EXISTS order_reviews
    ADD FOREIGN KEY (order_id)
    REFERENCES orders (order_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""

alter_tables_query_dict[
    "orders3"
] = """ALTER TABLE IF EXISTS orders
    ADD FOREIGN KEY (customer_id)
    REFERENCES customers (customer_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""

alter_tables_query_dict[
    "orders4"
] = """ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (order_id)
    REFERENCES orders (order_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""

alter_tables_query_dict[
    "order_items1"
] = """ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (product_id)
    REFERENCES products (product_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""

alter_tables_query_dict[
    "order_items2"
] = """ALTER TABLE IF EXISTS order_items
    ADD FOREIGN KEY (seller_id)
    REFERENCES sellers (seller_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""

alter_tables_query_dict[
    "geolocation"
] = """ALTER TABLE IF EXISTS customers
    ADD FOREIGN KEY (geolocation_id)
    REFERENCES geolocation (geolocation_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""

alter_tables_query_dict[
    "sellers"
] = """ALTER TABLE IF EXISTS sellers
    ADD FOREIGN KEY (geolocation_id)
    REFERENCES geolocation (geolocation_id) MATCH SIMPLE
    ON UPDATE CASCADE
    ON DELETE CASCADE
    NOT VALID;"""
