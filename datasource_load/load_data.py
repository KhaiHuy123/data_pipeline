
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
import os
import pandas as pd

postgres_config = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv('POSTGRES_PORT'),
    "database": os.getenv('POSTGRES_DB'),
    "user": os.getenv('POSTGRES_USER'),
    "password": os.getenv('POSTGRES_PASSWORD'),
}

engine = create_engine(f"postgresql+psycopg2://{postgres_config['user']}:{postgres_config['password']}@"
                       f"{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}")
metadata = MetaData()

try:
    with engine.connect() as connection:
        data_files = [
            'olist_customers_dataset',
            'olist_geolocation_dataset',
            'olist_order_payments_dataset',
            'olist_products_dataset',
            'olist_sellers_dataset',
            'product_category_name_translation',
            'olist_orders_dataset',
            'olist_order_items_dataset'
        ]
        path = "./data/raw/"
        source_schema = "bronze"

        for data_file in data_files:
            csv_file_name = f"{data_file}.csv"
            csv_file_path = os.path.join(path, csv_file_name)
            table_name = f"{source_schema}_{data_file}"
            df = pd.read_csv(csv_file_path)
            df.to_sql(table_name, con=engine, schema='bronze', if_exists="append",
                      index=False, method='multi', chunksize=1000)

        print('Tables and data inserted successfully.')
        connection.close()

except SQLAlchemyError as e:
    print(f'Error occurred: {e}')
