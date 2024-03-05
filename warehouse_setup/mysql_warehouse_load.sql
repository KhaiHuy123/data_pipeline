

----------------------------------------------------------------------------------------------- DIMENSIONAL TABLE
LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_customers_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_customers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_geolocation_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_geolocation_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_order_payments_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_order_payments_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_products_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_products_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_sellers_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_sellers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/product_category_name_translation.csv'
INTO TABLE brazilian-ecommerce.product_category_name_translation
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_customers_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_customers_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


----------------------------------------------------------------------------------------------- FACT TABLE
LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_orders_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_orders_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


LOAD DATA LOCAL INFILE '/etl_pipeline_e2e/data/raw/olist_order_items_dataset.csv'
INTO TABLE brazilian-ecommerce.olist_order_items_dataset
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;