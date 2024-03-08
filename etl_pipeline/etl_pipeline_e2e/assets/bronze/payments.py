import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

olist_order_payments_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="olist_order_payments_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("order_id", non_nullable=True),
        PandasColumn.integer_column("payment_sequential", non_nullable=True),
        PandasColumn.string_column("payment_type", non_nullable=True),
        PandasColumn.integer_column("payment_installments", min_value=0),
        PandasColumn.float_column("payment_value", min_value=0.0)
    ],
)


@asset(
    name="mysql_olist_order_payments_dataset",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["order_payments"],
    compute_kind="MySQL",  # group_name="order_payments",
)
def mysql_olist_order_payments_dataset(context) -> Output[olist_order_payments_dataset_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_olist_order_payments_dataset",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["order_payments"], required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="order_payments",
    deps=([mysql_olist_order_payments_dataset]),
)
def minio_olist_order_payments_dataset(context) \
        -> Output[olist_order_payments_dataset_DataFrame]:

    """ upload MySQL raw data to Minio """

    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_olist_order_payments_dataset",
    ins={"mysql_olist_order_payments_dataset": AssetIn(key_prefix=["order_payments"])},
    outs={"bronze_olist_order_payments_dataset": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": ["order_id", "payment_sequential"],
            "columns": [
                {"order_id": "varchar"},
                {"payment_sequential": "int4"},
                {"payment_type": "varchar"},
                {"payment_installments": "int4"},
                {"payment_value": "float4"}]
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_olist_order_payments_dataset(mysql_olist_order_payments_dataset)\
        -> Output[olist_order_payments_dataset_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_olist_order_payments_dataset),
        metadata={
            "schema": "bronze",
            "table": "bronze_olist_order_payments_dataset",
        },
    )



