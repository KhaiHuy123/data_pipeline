import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

olist_sellers_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="olist_sellers_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("seller_id", non_nullable=True),
        PandasColumn.integer_column("seller_zip_code_prefix", non_nullable=True),
        PandasColumn.string_column("seller_city", non_nullable=True),
        PandasColumn.string_column("seller_state", non_nullable=True)
    ],
)


@asset(
    name="mysql_olist_sellers_dataset",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["sellers"],
    compute_kind="MySQL",  # group_name="sellers",
)
def mysql_olist_sellers_dataset(context) -> Output[olist_sellers_dataset_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM olist_sellers_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_olist_sellers_dataset",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["sellers"], required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="sellers",
    deps=([mysql_olist_sellers_dataset]),
)
def minio_olist_sellers_dataset(context)\
        -> Output[olist_sellers_dataset_DataFrame]:

    """ upload MySQL raw data to Minio """
    sql_stm = "SELECT * FROM olist_sellers_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_olist_sellers_dataset",
    ins={"mysql_olist_sellers_dataset": AssetIn(key_prefix=["sellers"])},
    outs={"bronze_olist_sellers_dataset": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": ["seller_id"],
            "columns": [
                {"seller_id": "varchar"},
                {"seller_zip_code_prefix": "int4"},
                {"seller_city": "varchar"},
                {"seller_state": "varchar"}]
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_olist_sellers_dataset(mysql_olist_sellers_dataset) \
        -> Output[olist_sellers_dataset_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_olist_sellers_dataset),
        metadata={
            "schema": "bronze",
            "table": "bronze_olist_sellers_dataset",
        },
    )




