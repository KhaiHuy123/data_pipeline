import os


def create_mysql_config():
    return {
        "host": os.getenv("HOST_MYSQL"),
        "port": os.getenv("PORT_MYSQL"),
        "database": os.getenv("DB_NAME_MYSQL"),
        "user": os.getenv("USER_MYSQL"),
        "password": os.getenv("PASSWORD_MYSQL"),
    }


def create_minio_config(bucket):
    return {
        "endpoint_url": os.getenv("ENDPOINT_URL_MINIO"),
        "bucket": os.getenv(bucket),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID_MINIO"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KRY_MINIO"),
    }


def create_psql_config(database, schema):
    return {
        "host": os.getenv("HOST_POSTGRES"),
        "port": os.getenv("PORT_POSTGRES"),
        "database": os.getenv(database),
        "user": os.getenv("USER_POSTGRES"),
        "password": os.getenv("PASSWORD_POSTGRES"),
        "schema": os.getenv(schema),
    }


def create_json_layer_config(layer):
    return {
        "layer": os.getenv(layer)
    }


MYSQL_CONFIG = create_mysql_config()
MINIO_CONFIG_BRONZE = create_minio_config("BUCKET_MINIO_BRONZE")
MINIO_CONFIG_SILVER = create_minio_config("BUCKET_MINIO_SILVER")
MINIO_CONFIG_GOLD = create_minio_config("BUCKET_MINIO_GOLD")
PSQL_CONFIG_BRONZE = create_psql_config("DATABASE_POSTGRES", "SCHEMA_BRONZE")
PSQL_CONFIG_SILVER = create_psql_config("DATABASE_POSTGRES", "SCHEMA_SILVER")
PSQL_CONFIG_GOLD = create_psql_config("DATABASE_POSTGRES", "SCHEMA_GOLD")
JSON_LAYER_BRONZE = create_json_layer_config("JSON_LAYER_BRONZE")
JSON_LAYER_SILVER = create_json_layer_config("JSON_LAYER_SILVER")
JSON_LAYER_GOLD = create_json_layer_config("JSON_LAYER_GOLD")

DBT = "dbt"
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
SOURCE = "source"
NOTEBOOK = "notebook"

START_DATE = "2023-01-01"
END_DATE = "2023-04-01"

customer = {
    "metadata": {
        "primary_keys": ["customer_id", "customer_unique_id"],
        "columns": [
            {"customer_id": "varchar(32)"},
            {"customer_unique_id": "varchar(32)"},
            {"customer_zip_code_prefix": "int4"},
            {"customer_city": "varchar(32)"},
            {"customer_state": "varchar(2)"}
        ]
    },
    "fill_values": {
        "customer_id": "unknown",
        "customer_unique_id": "unknown",
        "customer_zip_code_prefix": 0,
        "customer_city": "unknown",
        "customer_state": "unknown"
    }
}

geolocation = {
    "metadata": {
        "primary_keys": [],
        "columns": [
            {"geolocation_zip_code_prefix": "int4"},
            {"geolocation_lat": "float4"},
            {"geolocation_lng": "float4"},
            {"geolocation_city": "varchar(48)"},
            {"geolocation_state": "varchar(2)"}
        ]
    },
    "fill_values": {
        "geolocation_zip_code_prefix": 0,
        "geolocation_lat": 0.0,
        "geolocation_lng": 0.0,
        "geolocation_city": "unknown",
        "geolocation_state": "unknown"
    }
}

product_category = {
    "metadata": {
        "primary_keys": [],
        "columns": [
            {"product_category_name": "text"},
            {"product_category_name_english": "text"}
        ]
    },
    "fill_values": {
        'product_category_name': 'unknown',
        'product_category_name_english': 'unknown',
    }
}

payments = {
    "metadata": {
        "primary_keys": ["order_id", "payment_sequential"],
        "columns": [
            {"order_id": "varchar(32)"},
            {"payment_sequential": "int4"},
            {"payment_type": "varchar(16)"},
            {"payment_installments": "int4"},
            {"payment_value": "float4"}
        ]
    },
    "fill_values": {
        'order_id': 'unknown',
        'payment_sequential': 0,
        'payment_type': 'unknown',
        'payment_installments': 0,
        'payment_value': 0.0
    }
}

sellers = {
    "metadata": {
        "primary_keys": ["seller_id"],
        "columns": [
            {"seller_id": "varchar(32)"},
            {"seller_zip_code_prefix": "int4"},
            {"seller_city": "varchar(48)"},
            {"seller_state": "varchar(2)"}
        ]
    },
    "fill_values": {
        'seller_id': 'unknown',
        'seller_zip_code_prefix': 0,
        'seller_city': 'unknown',
        'seller_state': 'unknown',
    }
}

products = {
    "metadata": {
        "primary_keys": ["product_id"],
        "columns": [
            {"product_id": "varchar(32)"},
            {"product_category_name": "varchar(64)"},
            {"product_name_length": "int4"},
            {"product_description_length": "int4"},
            {"product_photos_qty": "int4"},
            {"product_weight_g": "int4"},
            {"product_length_cm": "int4"},
            {"product_height_cm": "int4"},
            {"product_width_cm": "int4"}
        ]
    },
    "fill_values": {
        'product_id': 'unknown',
        'product_category_name': 'unknown',
        'product_name_length': 0,
        'product_description_length': 0,
        'product_photos_qty': 0,
        'product_weight_g': 0,
        'product_length_cm': 0,
        'product_height_cm': 0,
        'product_width_cm': 0
    }
}

orders = {
    "metadata": {
        "primary_keys": ["order_id", "customer_id"],
        "columns": [
            {"order_id": "varchar(32)"},
            {"customer_id": "varchar(32)"},
            {"order_status": "varchar(32)"},
            {"order_purchase_timestamp": "datetime"},
            {"order_approved_at": "datetime"},
            {"order_delivered_carrier_date": "datetime"},
            {"order_delivered_customer_date": "datetime"},
            {"order_estimated_delivery_date": "datetime"}
        ]
    }
}

order_items = {
    "metadata": {
        "primary_keys": ["order_id", "order_item_id", "product_id", "seller_id"],
        "columns": [
            {"order_id": "varchar(32)"},
            {"order_item_id": "int4"},
            {"product_id": "varchar(32)"},
            {"seller_id": "varchar(32)"},
            {"shipping_limit_date": "datetime"},
            {"price": "float4"},
            {"freight_value": "float4"}
        ]
    }
}

