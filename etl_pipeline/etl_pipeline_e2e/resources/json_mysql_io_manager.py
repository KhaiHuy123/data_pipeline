import pandas as pd
import os
from dagster import IOManager, OutputContext, InputContext
from datetime import datetime

current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class JSON_MYSQL_IOManager(IOManager):

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        object_name = context.asset_key.path[-1]
        object_name = object_name.replace("mysql_", "")

        output_directory = f"./data/{os.getenv('SOURCE_DATABASE')}/{object_name}/"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        file_name = f'{object_name}_{day_month_year}.json'
        path = os.path.join(output_directory, file_name)

        obj.to_json(path, orient='records', date_format='iso', date_unit='s', default_handler=str)
        num_records = len(obj)
        context.add_output_metadata(metadata={"record_count": num_records, "data_path": path})

    def load_input(self, context: InputContext) -> pd.DataFrame:
        object_name = context.asset_key.path[-1]
        object_name = object_name.replace("mysql_", "")

        output_directory = f"./data/{os.getenv('SOURCE_DATABASE')}/{object_name}/"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        file_name = f'{object_name}_{day_month_year}.json'
        path = os.path.join(output_directory, file_name)

        obj = pd.read_json(path, date_unit='s')

        context.add_input_metadata(metadata={"data_path": path})
        return obj
