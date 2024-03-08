import os
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
from psycopg2 import sql
from datetime import datetime

current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class PostgresSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def get_conn(self):
        conn = psycopg2.connect(
            host=self._config["host"],
            port=self._config["port"],
            database=self._config["database"],
            user=self._config["user"],
            password=self._config["password"])
        try:
            return conn
        except Exception as e:
            print(f'ERROR: {e}')

    def drop_database_if_exists(self, schema_name, table_name):
        with self.get_conn() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                drop_table_query = sql.SQL(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE")
                cursor.execute(drop_table_query)

    def create_table_if_not_exists(self, schema_name, table_name, col_types, primary_keys=None):
        with self.get_conn() as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cursor:
                # Check if table exists
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = %s AND table_name = %s
                    );
                    """,
                    (schema_name, table_name)
                )
                table_exists = cursor.fetchone()[0]
                if table_exists:
                    print(f"Table {schema_name}.{table_name} has already exists. Skipping table creation.")
                    return

                # Convert metadata to SQL column definitions
                column_definitions = [
                    sql.SQL("{} {}").format(
                        sql.Identifier(column_name),
                        sql.SQL(column_type)
                    )
                    for column_dict in col_types
                    for column_name, column_type in column_dict.items()
                ]

                # Add primary key information to the column definitions
                if primary_keys:
                    column_definitions.append(
                        sql.SQL("PRIMARY KEY ({})").format(
                            sql.SQL(", ").join(map(sql.Identifier, primary_keys))
                        )
                    )

                create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(column_definitions)
                )

                cursor.execute(create_table_query)
                print(f'Create table {schema_name}.{table_name} successfully')

    def insert_data(self, schema_name, table_name, obj, columns):
        with self.get_conn() as conn:
            with conn.cursor() as cursor:
                # Check if table has existing data
                cursor.execute(
                    sql.SQL("""
                        SELECT COUNT(*)
                        FROM {}.{}
                    """).format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name)
                    )
                )
                existing_data_count = cursor.fetchone()[0]
                if existing_data_count > 0:
                    print(f"Table {schema_name}.{table_name} already has data. Skipping data insertion.")
                    return

                insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({});").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join([sql.SQL("%s")] * len(columns))
                )
                data_values = [tuple(row) for row in obj.itertuples(index=False, name=None)]
                cursor.executemany(insert_query, data_values)
                conn.commit()
        print(f"Inserted data into PostgresSQL {schema_name}.{table_name} successfully")

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        table_name = context.name
        columns_type = context.metadata.get('columns')
        primary_keys = context.metadata.get('primary_keys')

        print("-" * 70, "Insert Data into PostgresSQL", "-" * 70)
        print(obj)
        print("-" * 70, "Columns Type", "-" * 70)
        print(columns_type)

        output_directory = f"./data/{os.getenv('TARGET_DATABASE')}/{self._config['schema']}/{table_name}/"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        file_name = f'{table_name}_{day_month_year}.json'
        path = os.path.join(output_directory, file_name)

        obj.to_json(path, orient='records')

        print("-" * 70, "Target table: ", "-" * 70)
        print(table_name)
        columns = obj.columns.tolist()

        print("-" * 70, "Columns name :", "-" * 70)
        print(columns)

        # Drop database if exists
        self.drop_database_if_exists(self._config["schema"], table_name)
        # Create table
        self.create_table_if_not_exists(self._config["schema"], table_name, columns_type, primary_keys)
        # Insert data into PostgresSQL table
        self.insert_data(self._config["schema"], table_name, obj, columns)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        table_name = context.name

        # Load data from PostgresSQL table directly into a DataFrame
        with self.get_conn() as conn:
            select_query = f"SELECT * FROM {self._config['schema']}.{table_name};"
            with conn.cursor() as cursor:
                cursor.execute(select_query)
                df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        # Save to CSV
        output_directory = f"./data/{os.getenv('NOTEBOOK')}/"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        file_name = f'{table_name}.csv'
        path = os.path.join(output_directory, file_name)
        df.to_csv(path)

        return df
