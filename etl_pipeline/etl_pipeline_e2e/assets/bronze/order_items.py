import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn
from datetime import datetime

olist_order_items_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="olist_order_items_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("order_id", non_nullable=True),
        PandasColumn.integer_column("order_item_id", non_nullable=True),
        PandasColumn.string_column("product_id", non_nullable=True),
        PandasColumn.string_column("seller_id", non_nullable=True),
        PandasColumn.datetime_column("shipping_limit_date", ignore_missing_vals=True,
                                     min_datetime=datetime(year=2014, month=1, day=1)),
        PandasColumn.float_column("price", min_value=0.0),
        PandasColumn.float_column("freight_value", min_value=0.0),
    ],
)

bronze_olist_order_items_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="bronze_olist_order_items_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("order_id", non_nullable=True),
        PandasColumn.integer_column("order_item_id", non_nullable=True),
        PandasColumn.string_column("product_id", non_nullable=True),
        PandasColumn.string_column("seller_id", non_nullable=True),
        PandasColumn.string_column("shipping_limit_date", ignore_missing_vals=True),
        PandasColumn.float_column("price", min_value=0.0),
        PandasColumn.float_column("freight_value", min_value=0.0),
    ],
)


@asset(
    name="mysql_olist_order_items_dataset",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["order_items"],
    compute_kind="MySQL",  # group_name="order_items",
)
def mysql_olist_order_items_dataset(context) -> Output[olist_order_items_dataset_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_olist_order_items_dataset",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["order_items"], required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="order_items",
    deps=([mysql_olist_order_items_dataset]),
)
def minio_olist_order_items_dataset(context)\
        -> Output[olist_order_items_dataset_DataFrame]:

    """ upload MySQL raw data to Minio """

    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_olist_order_items_dataset",
    ins={"mysql_olist_order_items_dataset": AssetIn(key_prefix=["order_items"])},
    outs={"bronze_olist_order_items_dataset": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": ["order_id", "order_item_id", "product_id", "seller_id"],
            "columns": [
                {"order_id": "varchar"},
                {"order_item_id": "varchar"},
                {"product_id": "varchar"},
                {"seller_id": "varchar"},
                {"shipping_limit_date": "timestamp"},
                {"price": "float4"},
                {"freight_value": "float4"}]
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_olist_order_items_dataset(mysql_olist_order_items_dataset) \
        -> Output[bronze_olist_order_items_dataset_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_olist_order_items_dataset),
        metadata={
            "schema": "bronze",
            "table": "bronze_olist_order_items_dataset",
        },
    )



