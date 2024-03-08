import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn
from datetime import datetime

olist_orders_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="olist_orders_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("order_id", non_nullable=True),
        PandasColumn.string_column("customer_id", non_nullable=True),
        PandasColumn.string_column("order_status", non_nullable=True),

        PandasColumn.datetime_column("order_purchase_timestamp", ignore_missing_vals=True,
                                     min_datetime=datetime(year=2014, month=1, day=1)),
        PandasColumn.datetime_column("order_approved_at", ignore_missing_vals=True,
                                     min_datetime=datetime(year=2014, month=1, day=1)),
        PandasColumn.datetime_column("order_delivered_carrier_date", ignore_missing_vals=True,
                                     min_datetime=datetime(year=2014, month=1, day=1)),
        PandasColumn.datetime_column("order_delivered_customer_date", ignore_missing_vals=True,
                                     min_datetime=datetime(year=2014, month=1, day=1)),
        PandasColumn.datetime_column("order_estimated_delivery_date", ignore_missing_vals=True,
                                     min_datetime=datetime(year=2014, month=1, day=1)),
    ],
)

bronze_olist_orders_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="bronze_olist_orders_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("order_id", non_nullable=True),
        PandasColumn.string_column("customer_id", non_nullable=True),
        PandasColumn.string_column("order_status", non_nullable=True),
        PandasColumn.string_column("order_purchase_timestamp", ignore_missing_vals=True),
        PandasColumn.string_column("order_approved_at", ignore_missing_vals=True),
        PandasColumn.string_column("order_delivered_carrier_date", ignore_missing_vals=True),
        PandasColumn.string_column("order_delivered_customer_date", ignore_missing_vals=True),
        PandasColumn.string_column("order_estimated_delivery_date", ignore_missing_vals=True)
    ]
)


@asset(
    name="mysql_olist_orders_dataset",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["orders"],
    compute_kind="MySQL",  # group_name="orders",
)
def mysql_olist_orders_dataset(context) -> Output[olist_orders_dataset_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM olist_orders_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_olist_orders_dataset",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["orders"], required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="orders",
    deps=([mysql_olist_orders_dataset]),
)
def minio_olist_orders_dataset(context)\
        -> Output[olist_orders_dataset_DataFrame]:

    """ upload MySQL raw data to Minio """

    sql_stm = "SELECT * FROM olist_orders_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_olist_orders_dataset",
    ins={"mysql_olist_orders_dataset": AssetIn(key_prefix=["orders"])},
    outs={"bronze_olist_orders_dataset": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": ["order_id", "customer_id"],
            "columns": [
                {"order_id": "varchar"},
                {"customer_id": "varchar"},
                {"order_status": "varchar"},
                {"order_purchase_timestamp": "timestamp"},
                {"order_approved_at": "timestamp"},
                {"order_delivered_carrier_date": "timestamp"},
                {"order_delivered_customer_date": "timestamp"},
                {"order_estimated_delivery_date": "timestamp"}]
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_olist_orders_dataset(mysql_olist_orders_dataset) \
        -> Output[bronze_olist_orders_dataset_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_olist_orders_dataset),
        metadata={
            "schema": "bronze",
            "table": "bronze_olist_orders_dataset",
        },
    )




