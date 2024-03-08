import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

olist_geolocation_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="olist_geolocation_dataset_DataFrame",
    columns=[
        PandasColumn.integer_column("geolocation_zip_code_prefix", non_nullable=True),
        PandasColumn.float_column("geolocation_lat", non_nullable=True),
        PandasColumn.float_column("geolocation_lng", non_nullable=True),
        PandasColumn.string_column("geolocation_city", non_nullable=True),
        PandasColumn.string_column("geolocation_state", non_nullable=True),
    ],
)


@asset(
    name="mysql_olist_geolocation_dataset",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["geolocation"],
    compute_kind="MySQL",  # group_name="geolocation",
)
def mysql_olist_geolocation_dataset(context) -> Output[olist_geolocation_dataset_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM olist_geolocation_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_olist_geolocation_dataset",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["geolocation"], required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="geolocation",
    deps=([mysql_olist_geolocation_dataset]),
)
def minio_olist_geolocation_dataset(context)\
        -> Output[olist_geolocation_dataset_DataFrame]:

    """ upload MySQL raw data to Minio """

    sql_stm = "SELECT * FROM olist_geolocation_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_olist_geolocation_dataset",
    ins={"mysql_olist_geolocation_dataset": AssetIn(key_prefix=["geolocation"])},
    outs={"bronze_olist_geolocation_dataset": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"geolocation_zip_code_prefix": "int4"},
                {"geolocation_lat": "float4"},
                {"geolocation_lng": "float4"},
                {"geolocation_city": "varchar"},
                {"geolocation_state": "varchar"}]
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_olist_geolocation_dataset(mysql_olist_geolocation_dataset) \
        -> Output[olist_geolocation_dataset_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_olist_geolocation_dataset),
        metadata={
            "schema": "bronze",
            "table": "bronze_olist_geolocation_dataset",
        },
    )



