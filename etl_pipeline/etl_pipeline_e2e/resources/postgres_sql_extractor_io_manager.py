import os

import pandas as pd
from datetime import datetime
import psycopg2
from dagster import AssetExecutionContext

current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class PostgresExtractorIOManager:
    def __init__(self, config):
        self._config = config

    def extract_data(self, sql: str, context: AssetExecutionContext) -> pd.DataFrame:
        # Connect to PostgresSQL database
        connection = psycopg2.connect(
            host=self._config["host"],
            port=self._config["port"],
            database=self._config["database"],
            user=self._config["user"],
            password=self._config["password"])

        df = pd.read_sql(sql, connection)
        table_name = context.op.name.split('__')[1]
        print("-" * 70, f"Raw Data from PostgresSQL {self._config['schema']}.{table_name}", "-" * 70)
        print(df)

        # Save to JSON
        output_directory = f"./data/temp/{os.getenv('TARGET_DATABASE')}/{self._config['schema']}/{table_name}/"

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        file_name = f'{table_name}_{day_month_year}.json'
        path = os.path.join(output_directory, file_name)

        df.to_json(path, orient='records', date_format='iso', date_unit='s', default_handler=str)

        # Save to CSV
        output_directory = f"./data/{os.getenv('NOTEBOOK')}/"

        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        file_name = f'{table_name}.csv'
        path = os.path.join(output_directory, file_name)

        df.to_csv(path)

        # Close the connection
        connection.close()
        return df


