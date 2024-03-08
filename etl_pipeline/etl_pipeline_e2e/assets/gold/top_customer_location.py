import os
import pandas as pd
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from ..dbt.dbt_asset import dbt_top_customer
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

top_customer_type = create_dagster_pandas_dataframe_type(
    name='top_customer',
    columns=[
        PandasColumn.string_column("customer_id", ignore_missing_vals=True),
        PandasColumn.string_column("customer_city", ignore_missing_vals=True),
        PandasColumn.integer_column("customer_zip_code_prefix", ignore_missing_vals=True),
        PandasColumn.integer_column("num_of_customers", ignore_missing_vals=True),
        PandasColumn.integer_column("num_of_orders", ignore_missing_vals=True),
        PandasColumn.float_column("average_order_value", ignore_missing_vals=True),
        PandasColumn.string_column("order_status", ignore_missing_vals=True),
        PandasColumn.string_column("payment_type", ignore_missing_vals=True),
        PandasColumn.datetime_column("order_purchase_timestamp", ignore_missing_vals=True)
    ]
)


@asset(
    name='top_customer',  # group_name='top_customer',
    key_prefix=["top_customer"], required_resource_keys={'postgres_sql_extractor_gold_io_manager'},
    compute_kind="PostgresSQL", deps=([dbt_top_customer]),
)
def top_customer(context) -> Output[top_customer_type]:
    """ create sale value by category """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.top_customer_location"
    pd_data = context.resources.postgres_sql_extractor_gold_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_top_customer",
    # group_name="top_customer",
    compute_kind="PostgresSQL", key_prefix=["minio_top_customer"],
    io_manager_key="minio_gold_io_manager",
    ins={"top_customer": AssetIn(key_prefix=['top_customer'])},
)
def minio_top_customer(context, top_customer) -> Output[top_customer_type]:
    """ upload data to Minio """
    context.log.info("upload sale_value to Minio")
    return Output(value=top_customer)


@multi_asset(
    # group_name='gold',
    name="gold_top_customer",
    ins={"top_customer": AssetIn(key_prefix=["top_customer"])},
    outs={"gold_top_customer": AssetOut(
        io_manager_key="postgres_sql_gold_io_manager",
        key_prefix=["gold_top_customer"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"customer_id": "varchar"},
                {"customer_city": "varchar"},
                {"customer_zip_code_prefix": "int4"},
                {"num_of_customers": "int4"},
                {"num_of_orders": "int4"},
                {"average_order_value": "float4"},
                {"order_status": "varchar"},
                {"payment_type": "varchar"},
                {"order_purchase_timestamp": "timestamp"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def gold_top_customer(top_customer) -> Output[top_customer_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(top_customer),
        metadata={
            "schema": "gold",
            "table": "gold_top_customer",
        },
    )



