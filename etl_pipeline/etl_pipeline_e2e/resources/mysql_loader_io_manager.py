from datetime import datetime
import mysql.connector as mc

current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class MySQL_LoaderIOManager:
    def __init__(self, config):
        self._config = config

    def create_connection(self):
        return mc.connect(
            host=self._config["host"],
            user=self._config["user"],
            password=self._config["password"],
            database=self._config["database"]
        )

    def create_table(self, connection, table, col_types, primary_key=None):
        cursor = connection.cursor()

        # Define columns
        columns = [f"{name} {col_type}" for col_dict in col_types for name, col_type in col_dict.items()]

        # Define table creation query
        create_table_query = f"CREATE TABLE {table} ({', '.join(columns)}"
        if primary_key:
            create_table_query += f", PRIMARY KEY ({', '.join(primary_key)})"
        create_table_query += ");"

        try:
            cursor.execute(create_table_query)
            print(f"Table {table} created successfully.")
        except mc.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()

    def drop_table(self, connection, table):
        cursor = connection.cursor()

        # Define table drop query
        drop_table_query = f"DROP TABLE IF EXISTS {table};"

        try:
            cursor.execute(drop_table_query)
            print(f"Table {table} dropped successfully.")
        except mc.Error as err:
            print(f"Error: {err}")
        finally:
            cursor.close()

    def load_data(self, table: str, metadata, df) -> None:
        columns_type = metadata.get('columns')
        primary_keys = metadata.get('primary_keys')
        connection = self.create_connection()
        cursor = connection.cursor()
        try:
            print("-" * 70, f"Load Raw Data to MySQL for table {table}", "-" * 70)
            print(df)
            self.drop_table(connection, table)
            self.create_table(connection, table, columns_type, primary_keys)

            # Insert data into MySQL table
            for _, row in df.iterrows():
                values = ', '.join([f'"{value}"' for value in row])
                insert_query = f"INSERT INTO {table} VALUES ({values});"
                cursor.execute(insert_query)

            # Commit the changes
            connection.commit()
            print('Load data into MySQL finished !')
        except Exception as e:
            print(f"Error: {e}")
        finally:
            cursor.close()
            connection.close()
