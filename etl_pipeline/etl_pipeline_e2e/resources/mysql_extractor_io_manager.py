import pandas as pd
import mysql.connector as mc
from datetime import datetime

current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class MySQL_ExtractorIOManager:
    def __init__(self, config):
        self._config = config

    def extract_data(self, sql: str) -> pd.DataFrame:
        # Connect to MySQL database
        connection = mc.connect(
            host=self._config["host"],
            user=self._config["user"],
            password=self._config["password"],
            database=self._config["database"]
        )

        df = pd.read_sql(sql, connection)
        print("-" * 70, "Raw Data from MySQL", "-" * 70)
        print(df)

        # Close the connection
        connection.close()

        return df


