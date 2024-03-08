import os
import pandas as pd
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from ..dbt.dbt_asset import dbt_return_rate
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

return_rate_type = create_dagster_pandas_dataframe_type(
    name='return_rate',
    columns=[
        PandasColumn.integer_column("return_customers", ignore_missing_vals=True),
        PandasColumn.integer_column("all_customers", ignore_missing_vals=True),
        PandasColumn.integer_column("return_rate", ignore_missing_vals=True),
        PandasColumn.string_column("order_status", ignore_missing_vals=True)
    ]
)


@asset(
    name='return_rate',  # group_name='return_rate',
    key_prefix=["return_rate"], required_resource_keys={'postgres_sql_extractor_gold_io_manager'},
    compute_kind="PostgresSQL", deps=([dbt_return_rate]),
)
def return_rate(context) -> Output[return_rate_type]:
    """ create sale value by category """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.return_rate"
    pd_data = context.resources.postgres_sql_extractor_gold_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_return_rate",
    # group_name="return_rate",
    compute_kind="PostgresSQL", key_prefix=["minio_return_rate"],
    io_manager_key="minio_gold_io_manager",
    ins={"return_rate": AssetIn(key_prefix=['return_rate'])},
)
def minio_return_rate(context, return_rate) -> Output[return_rate_type]:
    """ upload data to Minio """
    context.log.info("upload sale_value to Minio")
    return Output(value=return_rate)


@multi_asset(
    # group_name='gold',
    name="gold_return_rate",
    ins={"return_rate": AssetIn(key_prefix=["return_rate"])},
    outs={"gold_return_rate": AssetOut(
        io_manager_key="postgres_sql_gold_io_manager",
        key_prefix=["gold_return_rate"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"return_customers": "int4"},
                {"all_customers": "int4"},
                {"return_rate": "int4"},
                {"order_status": "varchar"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def gold_return_rate(return_rate) -> Output[return_rate_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(return_rate),
        metadata={
            "schema": "gold",
            "table": "gold_return_rate",
        },
    )



