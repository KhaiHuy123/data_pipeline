import pandas as pd
import os
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn
from ..dbt.dbt_asset import dbt_dim_orders_condition_key

dim_orders_condition_type = create_dagster_pandas_dataframe_type(
    name="dim_orders_condition_type",
    columns=[
        PandasColumn.integer_column("order_condition_stats", ignore_missing_vals=True),
        PandasColumn.string_column("order_status", ignore_missing_vals=True)
    ]
)


@asset(
    name='dim_orders_condition',
    key_prefix=["dim_orders_condition"],  # group_name="dim_orders_condition",
    deps=([dbt_dim_orders_condition_key]), required_resource_keys={"postgres_sql_extractor_silver_io_manager"},
    compute_kind='python',
)
def dim_orders_condition(context) -> Output[dim_orders_condition_type]:
    """ create dim orders_condition  """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.dim_orders_condition"
    pd_data = context.resources.postgres_sql_extractor_silver_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_dim_orders_condition",
    # group_name="dim_orders_condition",
    compute_kind="PostgresSQL", key_prefix=["minio_dim_orders_condition"],
    io_manager_key="minio_silver_io_manager",
    ins={"dim_orders_condition": AssetIn(key_prefix=['dim_orders_condition'])},
)
def minio_dim_orders_condition(context, dim_orders_condition) -> Output[dim_orders_condition_type]:
    """ upload data to Minio """
    context.log.info("upload dim_orders_condition to Minio")
    return Output(value=dim_orders_condition)


@multi_asset(
    # group_name='silver',
    name="silver_dim_orders_condition",
    ins={"dim_orders_condition": AssetIn(key_prefix=["dim_orders_condition"])},
    outs={"silver_dim_orders_condition": AssetOut(
        io_manager_key="postgres_sql_silver_io_manager",
        key_prefix=["silver_dim_orders_condition"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"order_condition_stats": "int4"},
                {"order_status": "varchar"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def silver_dim_orders_condition(dim_orders_condition) -> Output[dim_orders_condition_type]:
    """ insert raw data into PostgresSQL """
    return Output(
        value=pd.DataFrame(dim_orders_condition),
        metadata={
            "schema": "silver",
            "table": "silver_dim_orders_condition",
        },
    )





