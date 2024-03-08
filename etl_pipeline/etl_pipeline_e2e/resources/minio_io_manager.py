import os
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
from minio.error import S3Error
from datetime import datetime
import io
import json
import csv
import pandas as pd
import pyarrow.parquet as pq


current_day = datetime.now()
day_month_year = current_day.strftime("%d%m%y")


class MinIOHelper:
    def read(self, minio_client, bucket_name, file_name, file_extension):
        object_path = f"{bucket_name}/{file_name}.{file_extension}"
        file_content = minio_client.get_object(bucket_name, object_path).read().decode("utf-8")

        if file_extension == 'json':
            return json.loads(file_content)
        elif file_extension == 'csv':
            csv_reader = csv.reader(file_content.splitlines())
            return list(csv_reader)
        elif file_extension == 'parquet':
            parquet_object = minio_client.get_object(bucket_name, object_path)
            parquet_content = io.BytesIO(parquet_object.read())
            return pq.read_table(parquet_content)

    def upload(self, minio_client, bucket_name, object_name, file_path, content_type):
        try:
            with open(file_path, 'rb') as file_data:
                file_content = file_data.read()
                minio_client.put_object(bucket_name, object_name, io.BytesIO(file_content),
                                        len(file_content), content_type=content_type)
            print(f"Uploaded {object_name} successfully")
        except S3Error as e:
            print(f"Error uploading {object_name}: {e}")

    def upload_parquet(self, minio_client, bucket_name, object_name, file_path, content_type):
        try:
            parquet_table = pq.read_table(file_path)
            with io.BytesIO() as buffer:
                pq.write_table(parquet_table, buffer)
                buffer.seek(0)
                minio_client.put_object(bucket_name, object_name, buffer,
                                        len(buffer.getvalue()), content_type=content_type)
            print(f"Uploaded {object_name} successfully")
        except S3Error as e:
            print(f"Error uploading {object_name}: {e}")

    def read_from_minio(self, minio_client, bucket_name, file_name, file_extension):
        return self.read(minio_client, bucket_name, file_name, file_extension)

    def upload_to_minio(self, minio_client, bucket_name, object_name, file_path, content_type):
        if content_type == "application/octet-stream":
            self.upload_parquet(minio_client, bucket_name, object_name, file_path, content_type)
        else:
            self.upload(minio_client, bucket_name, object_name, file_path, content_type)


class MinIOMananger(IOManager):
    def __init__(self, config):
        self._config = self.initialize_minio(config=config)
        self.bucket_name = config["bucket"]
        self.create_bucket(self._config, self.bucket_name)
        self._minio_helper = MinIOHelper()

    def initialize_minio(self, config):
        try:
            return Minio(
                config["endpoint_url"],
                access_key=config["aws_access_key_id"],
                secret_key=config["aws_secret_access_key"],
                secure=False
            )
        except Exception as e:
            print(f'FAIL IN CREATE MINIO OBJECT CONNECTOR : {e}')

    def create_bucket(self, minio_client, bucket_name):
        if minio_client.bucket_exists(bucket_name):
            print(f"{bucket_name} exists")
        else:
            print(f"{bucket_name} does not exist")
            print(f"Creating bucket {bucket_name}")
            minio_client.make_bucket(bucket_name)

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:

        """ catch data and upload data to Minio """

        object_name = context.asset_key.path[1]

        print("Upload Raw Data from MySQL to Minio successfully")
        print(obj)

        print("CONTEXT INFO ")
        print(context.asset_key.path)

        object_name = object_name.replace("minio_", "")
        path = f'./data/{os.getenv("SOURCE_DATABASE")}/{object_name}' \
               f'/{object_name}_{day_month_year}.json'
        if not os.path.exists(path):
            path = f'./data/{os.getenv("TARGET_DATABASE")}/{self.bucket_name}/{object_name}' \
                   f'/{object_name}_{day_month_year}.json'
            if not os.path.exists(path):
                path = f'./data/temp/{os.getenv("TARGET_DATABASE")}/{self.bucket_name}/{object_name}' \
                       f'/{object_name}_{day_month_year}.json'

        file_name = path.rsplit('/', 1)[-1]
        file_extension = path.rsplit('.', 1)[-1]
        print("file_name of file object")
        print(file_name)
        print("file_extension of file object")
        print(file_extension)
        file_extension = file_extension.lower()

        if file_extension in ['csv', 'json', 'parquet']:
            self._minio_helper.upload_to_minio(minio_client=self._config, bucket_name=self.bucket_name,
                                               object_name=f"{self.bucket_name}/{file_name}", file_path=path,
                                               content_type=f"application/{file_extension}"
                                               if file_extension == 'parquet' else f"text/{file_extension}")
        else:
            print(f"Unsupported file extension: {file_extension}")
        pass
        context.add_output_metadata(metadata={"data_path": path})

    def load_input(self, context: InputContext) -> pd.DataFrame:

        """ read from Minio and return as Pandas Dataframe """

        path = f'./data/{os.getenv("SOURCE_DATABASE")}/{context.asset_key.path[1]}' \
               f'/{context.asset_key.path[1]}_{day_month_year}.json'
        if not os.path.exists(path):
            path = f'./data/temp/{os.getenv("TARGET_DATABASE")}/{self.bucket_name}/{context.asset_key.path[1]}' \
                   f'/{context.asset_key.path[1]}_{day_month_year}.json'

        context.add_input_metadata(metadata={"data_path": path})

        file_name = path.split('/')[-1].split('.')[0]
        file_extension = path.rsplit('.', 1)[-1]
        if file_extension in ['csv', 'json', 'parquet']:
            results = self._minio_helper.read_from_minio(minio_client=self._config, bucket_name=self.bucket_name,
                                                         file_name=file_name, file_extension=file_extension)
            pd_data = pd.DataFrame(results)

            return pd_data
        else:
            print(f"Unsupported file extension: {file_extension}")
            return pd.DataFrame()
