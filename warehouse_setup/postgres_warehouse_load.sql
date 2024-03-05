

COPY bronze.bronze_olist_customers_dataset(customer_id, customer_unique_id, customer_city, customer_state)
FROM '/etl_pipeline_e2e/data/raw/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

COPY bronze.bronze_olist_geolocation_dataset(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
geolocation_city, geolocation_state)
FROM '/etl_pipeline_e2e/data/raw/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

COPY bronze.bronze_olist_order_payments_dataset(order_id, payment_sequential, payment_type,
payment_installments, payment_value)
FROM '/etl_pipeline_e2e/data/raw/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

COPY bronze.bronze_olist_products_dataset(product_id, product_category_name, product_name_length,
product_description_length, product_photos_qty,
product_weight_g, product_length_cm, product_height_cm, product_width_cm)
FROM '/etl_pipeline_e2e/data/raw/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

COPY bronze.bronze_olist_sellers_dataset(seller_id, seller_zip_code_prefix,
seller_city, seller_state)
FROM '/etl_pipeline_e2e/data/raw/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

COPY bronze.bronze_product_category_name_translation(product_category_name, product_category_name_english)
FROM '/etl_pipeline_e2e/data/raw/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

COPY bronze.bronze_olist_orders_dataset(order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
FROM '/etl_pipeline_e2e/data/raw/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

COPY bronze.bronze_olist_order_items_dataset(order_id, order_item_id, product_id,
seller_id, shipping_limit_date, price, freight_value)
FROM '/etl_pipeline_e2e/data/raw/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

