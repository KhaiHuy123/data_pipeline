import os
import pandas as pd
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from ..dbt.dbt_asset import dbt_sale_values_key
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

sale_value_type = create_dagster_pandas_dataframe_type(
    name='sale_value_type',
    columns=[
        PandasColumn.string_column("monthly", ignore_missing_vals=True),
        PandasColumn.string_column("category", ignore_missing_vals=True),
        PandasColumn.float_column("total_sales", ignore_missing_vals=True),
        PandasColumn.float_column("total_orders", ignore_missing_vals=True),
        PandasColumn.float_column("value_per_order", ignore_missing_vals=True)
    ]
)


@asset(
    name='sale_value_by_category',  # group_name='sale_value_by_category',
    key_prefix=["sale_value_by_category"], required_resource_keys={'postgres_sql_extractor_gold_io_manager'},
    compute_kind="PostgresSQL", deps=([dbt_sale_values_key]),
)
def sale_value_by_category(context) -> Output[sale_value_type]:
    """ create sale value by category """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.sale_values_by_category"
    pd_data = context.resources.postgres_sql_extractor_gold_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_sale_value_by_category",
    # group_name="sale_value_by_category",
    compute_kind="PostgresSQL", key_prefix=["minio_sale_value_by_category"],
    io_manager_key="minio_gold_io_manager",
    ins={"sale_value_by_category": AssetIn(key_prefix=['sale_value_by_category'])},
)
def minio_sale_value_by_category(context, sale_value_by_category) -> Output[sale_value_type]:
    """ upload data to Minio """
    context.log.info("upload sale_value to Minio")
    return Output(value=sale_value_by_category)


@multi_asset(
    # group_name='gold',
    name="gold_sale_value_by_category",
    ins={"sale_value_by_category": AssetIn(key_prefix=["sale_value_by_category"])},
    outs={"gold_sale_value_by_category": AssetOut(
        io_manager_key="postgres_sql_gold_io_manager",
        key_prefix=["gold_sale_value_by_category"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"monthly": "varchar"},
                {"category": "varchar"},
                {"total_sales": "float4"},
                {"total_orders": "float4"},
                {"value_per_order": "float4"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def gold_sale_value_by_category(sale_value_by_category) -> Output[sale_value_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(sale_value_by_category),
        metadata={
            "schema": "gold",
            "table": "gold_sale_value_by_category",
        },
    )



