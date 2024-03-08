import pandas as pd
import os
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from ..dbt.dbt_asset import dbt_fact_customers_key
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn
from datetime import datetime

fact_customers_type = create_dagster_pandas_dataframe_type(
    name="fact_customers_type",
    columns=[
        PandasColumn.string_column("customer_id", ignore_missing_vals=True),
        PandasColumn.integer_column("num_of_orders", ignore_missing_vals=True),
        PandasColumn.float_column("value_per_order", ignore_missing_vals=True),
        PandasColumn.float_column("all_payment_value", ignore_missing_vals=True),
        PandasColumn.string_column("order_status", ignore_missing_vals=True),
        PandasColumn.string_column("payment_type", ignore_missing_vals=True),
        PandasColumn.datetime_column(
            "order_purchase_timestamp", ignore_missing_vals=True,
            min_datetime=datetime(year=2014, month=1, day=1),
        )
    ]
)


@asset(
    name='fact_customers',
    key_prefix=["fact_customers"],  # group_name="fact_customers",
    deps=([dbt_fact_customers_key]), required_resource_keys={"postgres_sql_extractor_silver_io_manager"},
    compute_kind='python',
)
def fact_customers(context) -> Output[fact_customers_type]:
    """ create fact customers """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.fact_customers"
    pd_data = context.resources.postgres_sql_extractor_silver_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_fact_customers",
    # group_name="fact_customers",
    compute_kind="PostgresSQL", key_prefix=["minio_fact_customers"],
    io_manager_key="minio_silver_io_manager",
    ins={"fact_customers": AssetIn(key_prefix=['fact_customers'])},
)
def minio_fact_customers(context, fact_customers) -> Output[fact_customers_type]:
    """ upload data to Minio """
    context.log.info("upload fact_customers to Minio")
    return Output(value=fact_customers)


@multi_asset(
    # group_name='silver',
    name="silver_fact_customers",
    ins={"fact_customers": AssetIn(key_prefix=["fact_customers"])},
    outs={"silver_fact_customers": AssetOut(
        io_manager_key="postgres_sql_silver_io_manager",
        key_prefix=["silver_fact_customers"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"customer_id": "varchar"},
                {"num_of_orders": "int4"},
                {"value_per_order": "float4"},
                {"all_payment_value": "float4"},
                {"order_status": "varchar"},
                {"payment_type": "varchar"},
                {"order_purchase_timestamp": "timestamp"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def silver_fact_customers(fact_customers) -> Output[fact_customers_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(fact_customers),
        metadata={
            "schema": "silver",
            "table": "silver_fact_customers",
        },
    )
