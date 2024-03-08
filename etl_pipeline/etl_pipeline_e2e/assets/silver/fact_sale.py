import pandas as pd
import os
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from ..dbt.dbt_asset import dbt_fact_sales_key
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn
from datetime import datetime

fact_sale_type = create_dagster_pandas_dataframe_type(
    name="fact_sale_type",
    columns=[
        PandasColumn.string_column("order_id", ignore_missing_vals=True),
        PandasColumn.string_column("customer_id", ignore_missing_vals=True),
        PandasColumn.string_column("product_id", ignore_missing_vals=True),
        PandasColumn.datetime_column(
            "order_purchase_timestamp", ignore_missing_vals=True,
            min_datetime=datetime(year=2014, month=1, day=1),
        ),
        PandasColumn.float_column("payment_value", ignore_missing_vals=True),
        PandasColumn.string_column("order_status", ignore_missing_vals=True)
    ]
)


@asset(
    name='fact_sale',
    key_prefix=["fact_sale"],  # group_name="fact_sale",
    deps=([dbt_fact_sales_key]), required_resource_keys={"postgres_sql_extractor_silver_io_manager"},
    compute_kind='python',
)
def fact_sale(context) -> Output[fact_sale_type]:
    """ create fact sale """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.fact_sales"
    pd_data = context.resources.postgres_sql_extractor_silver_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_fact_sale",
    # group_name="fact_sale",
    compute_kind="PostgresSQL", key_prefix=["minio_fact_sale"],
    io_manager_key="minio_silver_io_manager",
    ins={"fact_sale": AssetIn(key_prefix=['fact_sale'])},
)
def minio_fact_sale(context, fact_sale) -> Output[fact_sale_type]:
    """ upload data to Minio """
    context.log.info("upload fact_sale to Minio")
    return Output(value=fact_sale)


@multi_asset(
    # group_name='silver',
    name="silver_fact_sale",
    ins={"fact_sale": AssetIn(key_prefix=["fact_sale"])},
    outs={"silver_fact_sale": AssetOut(
        io_manager_key="postgres_sql_silver_io_manager",
        key_prefix=["silver_fact_sale"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"order_id": "varchar"},
                {"customer_id": "varchar"},
                {"product_id": "varchar"},
                {"order_purchase_timestamp": "timestamp"},
                {"payment_value": "float4"},
                {"order_status": "varchar"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def silver_fact_sale(fact_sale) -> Output[fact_sale_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(fact_sale),
        metadata={
            "schema": "silver",
            "table": "silver_fact_sale",
        },
    )
