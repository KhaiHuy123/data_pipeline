
from sqlalchemy import create_engine, MetaData, Table, Column, String, \
    Integer, Float, DateTime, PrimaryKeyConstraint,\
    ForeignKeyConstraint
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy.exc import SQLAlchemyError
import os

postgres_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv('POSTGRES_PORT'),
    "database": os.getenv('POSTGRES_DB'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
}

engine = create_engine(f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@"
                       f"{postgres_config['host']}:{os.getenv('POSTGRES_PORT')}/{postgres_config['database']}")
metadata = MetaData()


def schema_exists(conn, schema_name):
    return conn.dialect.has_schema(conn, schema_name)


def drop_schema(conn, schema_name, cascade=True):
    if schema_exists(conn, schema_name):
        conn.execute(DropSchema(schema_name, cascade=cascade))
        conn.commit()
        print(f'Schema {schema_name} dropped successfully.')
    else:
        print(f'Schema {schema_name} does not exist.')


def create_schema(conn, schema_name):
    if not schema_exists(conn, schema_name):
        conn.execute(CreateSchema(schema_name))
        conn.commit()
        print(f'Schema {schema_name} created successfully.')
    else:
        print(f'Schema {schema_name} already exists.')


try:
    with engine.connect() as connection:
        drop_schema(connection, 'bronze', cascade=True)
        drop_schema(connection, 'silver', cascade=True)
        drop_schema(connection, 'gold', cascade=True)
        create_schema(connection, 'bronze')
        create_schema(connection, 'silver')
        create_schema(connection, 'gold')

        bronze_olist_products_dataset = Table(
            'bronze_olist_products_dataset', metadata,
            Column('product_id', String(32), primary_key=True),
            Column('product_category_name', String(64)),
            Column('product_name_length', Integer),
            Column('product_description_length', Integer),
            Column('product_photos_qty', Integer),
            Column('product_weight_g', Integer),
            Column('product_length_cm', Integer),
            Column('product_height_cm', Integer),
            Column('product_width_cm', Integer),
            PrimaryKeyConstraint('product_id'),
            schema='bronze'
        )
        bronze_olist_products_dataset.create(engine)

        bronze_olist_geolocation_dataset = Table(
            'bronze_olist_geolocation_dataset', metadata,
            Column('geolocation_zip_code_prefix', Integer),
            Column('geolocation_lat', Float),
            Column('geolocation_lng', Float),
            Column('geolocation_city', String(48)),
            Column('geolocation_state', String(2)),
            schema='bronze'
        )
        bronze_olist_geolocation_dataset.create(engine)

        bronze_product_category_name_translation = Table(
            'bronze_product_category_name_translation', metadata,
            Column('product_category_name', String(64), primary_key=True),
            Column('product_category_name_english', String(64)),
            PrimaryKeyConstraint('product_category_name_english'),
            schema='bronze'
        )
        bronze_product_category_name_translation.create(engine)

        bronze_olist_order_payments_dataset = Table(
            'bronze_olist_order_payments_dataset', metadata,
            Column('order_id', String(32)),
            Column('payment_sequential', Integer),
            Column('payment_type', String(16)),
            Column('payment_installments', Integer),
            Column('payment_value', Float),
            schema='bronze'
        )
        bronze_olist_order_payments_dataset.create(engine)

        bronze_olist_sellers_dataset = Table(
            'bronze_olist_sellers_dataset', metadata,
            Column('seller_id', String(32), primary_key=True),
            Column('seller_zip_code_prefix', Integer),
            Column('seller_city', String(48)),
            Column('seller_state', String(2)),
            schema='bronze'
        )
        bronze_olist_sellers_dataset.create(engine)

        bronze_olist_customers_dataset = Table(
            'bronze_olist_customers_dataset', metadata,
            Column('customer_id', String(32)),
            Column('customer_unique_id', String(32)),
            Column('customer_zip_code_prefix', String(32)),
            Column('customer_city', String(32)),
            Column('customer_state', String(2)),
            PrimaryKeyConstraint('customer_id'),
            schema='bronze'
        )
        bronze_olist_customers_dataset.create(engine)

        bronze_olist_orders_dataset = Table(
            'bronze_olist_orders_dataset', metadata,
            Column('order_id', String(32), primary_key=True),
            Column('customer_id', String(32)),
            Column('order_status', String(16)),
            Column('order_purchase_timestamp', DateTime),
            Column('order_approved_at', DateTime),
            Column('order_delivered_carrier_date', DateTime),
            Column('order_delivered_customer_date', DateTime),
            Column('order_estimated_delivery_date', DateTime),
            PrimaryKeyConstraint('order_id'),
            ForeignKeyConstraint(['customer_id'], ['bronze.bronze_olist_customers_dataset.customer_id']),
            schema='bronze'
        )
        bronze_olist_orders_dataset.create(engine)

        bronze_olist_order_items_dataset = Table(
            'bronze_olist_order_items_dataset', metadata,
            Column('order_id', String(32), primary_key=True),
            Column('order_item_id', Integer),
            Column('product_id', String(32)),
            Column('seller_id', String(32)),
            Column('shipping_limit_date', DateTime),
            Column('price', Float),
            Column('freight_value', Float),
            PrimaryKeyConstraint('order_id', 'order_item_id', 'product_id', 'seller_id'),
            ForeignKeyConstraint(['order_id'], ['bronze.bronze_olist_orders_dataset.order_id']),
            ForeignKeyConstraint(['seller_id'], ['bronze.bronze_olist_sellers_dataset.seller_id']),
            ForeignKeyConstraint(['product_id'], ['bronze.bronze_olist_products_dataset.product_id']),
            schema='bronze'
        )
        bronze_olist_order_items_dataset.create(engine)

        # Create tables
        metadata.create_all(engine)
        print('Tables created successfully.')
        connection.close()

except SQLAlchemyError as e:
    print(f'Error occurred: {e}')

