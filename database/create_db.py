import mysql.connector
from mysql.connector import errorcode # for error handling
import os

DB_NAME = 'brazil'
USER = os.environ.get('MYSQL_USER')
PASSWORD = os.environ.get('MYSQL_PASSWORD')

TABLES = {}
TABLES['orders'] = (
    """
    CREATE TABLE IF NOT EXISTS `order_payments` (
  `order_id` VARCHAR(45) NOT NULL,
  `payment_sequential` INT NULL,
  `payment_type` VARCHAR(32) NULL,
  `payment_installments` INT NULL,
  `payment_value` DECIMAL(3) NULL,
  PRIMARY KEY (`order_id`),
  UNIQUE INDEX `order_id_UNIQUE` (`order_id` ASC) VISIBLE)
ENGINE = InnoDB;
    """
)

TABLES['products'] = (
"""    CREATE TABLE IF NOT EXISTS `products` (
  `product_id` VARCHAR(45) NOT NULL,
  `product_category_name` VARCHAR(45) NULL,
  `product_name_length` INT NULL,
  `product_description_length` INT NULL,
  `product_photos_qty` INT NULL,
  `product_weight_g` INT NULL,
  `product_length_cm` INT NULL,
  `product_height_cm` INT NULL,
  `product_width_cm` INT NULL,
  PRIMARY KEY (`product_id`))
ENGINE = InnoDB;
"""
)

TABLES['geolocation'] = (
"""    CREATE TABLE IF NOT EXISTS `geolocation` (
  `geolocation_zipcode_prefix` INT NOT NULL,
  `geolocation_lat` DECIMAL(5) NOT NULL,
  `geolocation_lng` DECIMAL(5) NOT NULL,
  `geolocation_city` VARCHAR(45) NOT NULL,
  `geolocation_state` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`geolocation_zipcode_prefix`))
ENGINE = InnoDB;
"""
)


TABLES['products'] = (
"""    CREATE TABLE IF NOT EXISTS `customers` (
  `customer_id` VARCHAR(45) NOT NULL,
  `customer_unique_id` VARCHAR(45) NULL,
  `customer_zip_code_prefix` INT NOT NULL,
  `customer_city` VARCHAR(45) NULL,
  `customer_state` VARCHAR(45) NULL,
  PRIMARY KEY (`customer_id`, `customer_zip_code_prefix`),
  INDEX `geolocation_zipcode_prefix_idx` (`customer_zip_code_prefix` ASC) VISIBLE,
  CONSTRAINT `geolocation_zipcode_prefix`
    FOREIGN KEY (`customer_zip_code_prefix`)
    REFERENCES `geolocation` (`geolocation_zipcode_prefix`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
"""
)

TABLES['order_reviews'] = (
"""    CREATE TABLE IF NOT EXISTS `order_reviews` (
  `order_id` VARCHAR(45) NOT NULL,
  `review_id` VARCHAR(45) NOT NULL,
  `review_score` INT NULL,
  `review_comment_title` VARCHAR(45) NULL,
  `review_comment_message` VARCHAR(100) NULL,
  `review_creation_date` DATETIME NULL,
  `review_answer_timestamp` DATETIME NULL,
  PRIMARY KEY (`order_id`))
ENGINE = InnoDB;
"""
)

TABLES['sellers'] = (
"""   CREATE TABLE IF NOT EXISTS `sellers` (
  `seller_id` VARCHAR(45) NOT NULL,
  `seller_zip_code_prefix` INT NOT NULL,
  `seller_city` VARCHAR(45) NULL,
  `seller_state` VARCHAR(3) NULL,
  PRIMARY KEY (`seller_id`, `seller_zip_code_prefix`),
  INDEX `geolocation_zipcode_prefix_idx` (`seller_zip_code_prefix` ASC) VISIBLE,
  CONSTRAINT `geolocation_zipcode_prefix`
    FOREIGN KEY (`seller_zip_code_prefix`)
    REFERENCES `geolocation` (`geolocation_zipcode_prefix`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
"""
)





TABLES['order_items'] = (
"""    CREATE TABLE IF NOT EXISTS `order_items` (
  `order_id` VARCHAR(45) NOT NULL,
  `order_item_id` INT NULL,
  `product_id` VARCHAR(45) NOT NULL,
  `seller_id` VARCHAR(45) NOT NULL,
  `shipping_limit_date` DATETIME NULL,
  `price` DECIMAL(2) NULL,
  `freight_value` DECIMAL(2) NULL,
  PRIMARY KEY (`order_id`, `seller_id`, `product_id`),
  INDEX `product_id_idx` (`product_id` ASC) VISIBLE,
  INDEX `seller_id_idx` (`seller_id` ASC) VISIBLE,
  CONSTRAINT `product_id`
    FOREIGN KEY (`product_id`)
    REFERENCES `products` (`product_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `seller_id`
    FOREIGN KEY (`seller_id`)
    REFERENCES `sellers` (`seller_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
"""
)

TABLES['orders'] = (
"""    CREATE TABLE IF NOT EXISTS `orders` (
  `order_id` VARCHAR(45) NOT NULL,
  `customer_id` VARCHAR(45) NOT NULL,
  `order_status` VARCHAR(10) NULL,
  `order_purchase_timestamp` DATETIME NULL,
  `order_approved_timestamp` DATETIME NULL,
  `order_delivered_carrier_date` DATETIME NULL,
  `order_delivered_customer_date` DATETIME NULL,
  `order_estimated_delivery_date` DATETIME NULL,
  PRIMARY KEY (`order_id`, `customer_id`),
  INDEX `customer_id_idx` (`customer_id` ASC) VISIBLE,
  CONSTRAINT `order_id`
    FOREIGN KEY (`order_id`)
    REFERENCES `order_payments` (`order_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `customer_id`
    FOREIGN KEY (`customer_id`)
    REFERENCES `customers` (`customer_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `order_id`
    FOREIGN KEY (`order_id`)
    REFERENCES `order_reviews` (`order_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `order_id`
    FOREIGN KEY (`order_id`)
    REFERENCES `order_items` (`order_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
"""
)


def create_database(cursor) -> None:
    """
    Create a new database

    :param cursor: Cursor to execute the query
    """    
    try:
        cursor.execute(
            f"CREATE DATABASE {DB_NAME} DEFAULT CHARACTER SET 'utf8'")
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

def create_tables(cursor) -> None:
    """
    Create all tables

    :param cursor: Cursor to execute the query
    """    
    for name, ddl in TABLES.iteritems():
        try:
            print(f"Creating table {name}: ")
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")



