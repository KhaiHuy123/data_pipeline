DROP DATABASE IF EXISTS brazilian-ecommerce;
CREATE DATABASE brazilian-ecommerce;
USE brazilian-ecommerce;
----------------------------------------------------------------------------------------------- FACT TABLE

DROP TABLE IF EXISTS olist_orders_dataset;
CREATE TABLE IF NOT EXISTS olist_orders_dataset (
order_id varchar(32),
customer_id varchar(32),
order_status varchar(16),
order_purchase_timestamp datetime,
order_approved_at datetime,
order_delivered_carrier_date datetime,
order_delivered_customer_date datetime,
order_estimated_delivery_date datetime,
PRIMARY KEY(order_id)
);

DROP TABLE IF EXISTS olist_order_items_dataset;
CREATE TABLE IF NOT EXISTS olist_order_items_dataset (
order_id varchar(32),
order_item_id int4,
product_id varchar(32),
seller_id varchar(32),
shipping_limit_date datetime,
price float4,
freight_value float4,
PRIMARY KEY (order_id, order_item_id, product_id, seller_id),
FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id),
FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset(seller_id),
FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id),
FOREIGN KEY (order_id) REFERENCES olist_order_payments_dataset(order_id)
);

----------------------------------------------------------------------------------------------- DIMENSIONAL TABLE

DROP TABLE IF EXISTS olist_products_dataset;
CREATE TABLE IF NOT EXISTS olist_products_dataset (
product_id varchar(32),
product_category_name varchar(64),
product_name_length int4,
product_description_length int4,
product_photos_qty int4,
product_weight_g int4,
product_length_cm int4,
product_height_cm int4,
product_width_cm int4,
PRIMARY KEY (product_id)
);


DROP TABLE IF EXISTS olist_geolocation_dataset;
CREATE TABLE IF NOT EXISTS olist_geolocation_dataset(
geolocation_zip_code_prefix int4,
geolocation_lat float4,
geolocation_lng float4,
geolocation_city varchar(48),
geolocation_state varchar(2)
);

DROP TABLE IF EXISTS product_category_name_translation;
CREATE TABLE IF NOT EXISTS product_category_name_translation (
product_category_name varchar(64),
product_category_name_english varchar(64),
PRIMARY KEY (product_category_name)
);

DROP TABLE IF EXISTS olist_order_payments_dataset;
CREATE TABLE IF NOT EXISTS olist_order_payments_dataset (
order_id varchar(32),
payment_sequential int4,
payment_type varchar(16),
payment_installments int4,
payment_value float4,
PRIMARY KEY (order_id, payment_sequential)
);

DROP TABLE IF EXISTS olist_sellers_dataset;
CREATE TABLE IF NOT EXISTS olist_sellers_dataset (
seller_id varchar(32),
seller_zip_code_prefix int4,
seller_city varchar(48),
seller_state varchar(2),
PRIMARY KEY (seller_id)
);

DROP TABLE IF EXISTS olist_customers_dataset;
CREATE TABLE IF NOT EXISTS olist_customers_dataset(
customer_id varchar(32),
customer_unique_id varchar(32),
customer_city varchar(32),
customer_state varchar(2),
PRIMARY KEY (customer_id, customer_unique_id)
);