import os
import pandas as pd
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from ..dbt.dbt_asset import dbt_average_order_value
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

average_order_value_type = create_dagster_pandas_dataframe_type(
    name='average_order_value',
    columns=[
        PandasColumn.float_column("total_orders", ignore_missing_vals=True),
        PandasColumn.float_column("total_revenue", ignore_missing_vals=True),
        PandasColumn.float_column("average_order_value", ignore_missing_vals=True),
        PandasColumn.string_column("order_status", ignore_missing_vals=True)
    ]
)


@asset(
    name='average_order_value',  # group_name='average_order_value',
    key_prefix=["average_order_value"], required_resource_keys={'postgres_sql_extractor_gold_io_manager'},
    compute_kind="PostgresSQL", deps=([dbt_average_order_value]),
)
def average_order_value(context) -> Output[average_order_value_type]:
    """ create sale value by category """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.average_order_value"
    pd_data = context.resources.postgres_sql_extractor_gold_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_average_order_value",
    # group_name="average_order_value",
    compute_kind="PostgresSQL", key_prefix=["minio_average_order_value"],
    io_manager_key="minio_gold_io_manager",
    ins={"average_order_value": AssetIn(key_prefix=['average_order_value'])},
)
def minio_average_order_value(context, average_order_value) -> Output[average_order_value_type]:
    """ upload data to Minio """
    context.log.info("upload sale_value to Minio")
    return Output(value=average_order_value)


@multi_asset(
    # group_name='gold',
    name="gold_average_order_value",
    ins={"average_order_value": AssetIn(key_prefix=["average_order_value"])},
    outs={"gold_average_order_value": AssetOut(
        io_manager_key="postgres_sql_gold_io_manager",
        key_prefix=["gold_average_order_value"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"total_orders": "int4"},
                {"total_revenue": "float4"},
                {"average_order_value": "float4"},
                {"order_status": "varchar"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def gold_average_order_value(average_order_value) -> Output[average_order_value_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(average_order_value),
        metadata={
            "schema": "gold",
            "table": "gold_average_order_value",
        },
    )



