import os
import pandas as pd
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from ..dbt.dbt_asset import dbt_sale_conversion
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

sale_conversion_type = create_dagster_pandas_dataframe_type(
    name='sale_conversion',
    columns=[
        PandasColumn.integer_column("customers_purchased", ignore_missing_vals=True),
        PandasColumn.integer_column("all_customers", ignore_missing_vals=True),
        PandasColumn.string_column("order_status", ignore_missing_vals=True)
    ]
)


@asset(
    name='sale_conversion',  # group_name='sale_conversion',
    key_prefix=["sale_conversion"], required_resource_keys={'postgres_sql_extractor_gold_io_manager'},
    compute_kind="PostgresSQL", deps=([dbt_sale_conversion]),
)
def sale_conversion(context) -> Output[sale_conversion_type]:
    """ create sale value by category """
    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.sale_conversion"
    pd_data = context.resources.postgres_sql_extractor_gold_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_sale_conversion",
    # group_name="sale_conversion",
    compute_kind="PostgresSQL", key_prefix=["minio_sale_conversion"],
    io_manager_key="minio_gold_io_manager",
    ins={"sale_conversion": AssetIn(key_prefix=['sale_conversion'])},
)
def minio_sale_conversion(context, sale_conversion) -> Output[sale_conversion_type]:
    """ upload data to Minio """
    context.log.info("upload sale_value to Minio")
    return Output(value=sale_conversion)


@multi_asset(
    # group_name='gold',
    name="gold_sale_conversion",
    ins={"sale_conversion": AssetIn(key_prefix=["sale_conversion"])},
    outs={"gold_sale_conversion": AssetOut(
        io_manager_key="postgres_sql_gold_io_manager",
        key_prefix=["gold_sale_conversion"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"customers_purchased": "int4"},
                {"all_customers": "int4"},
                {"order_status": "varchar"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def gold_sale_conversion(sale_conversion) -> Output[sale_conversion_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(sale_conversion),
        metadata={
            "schema": "gold",
            "table": "gold_sale_conversion",
        },
    )



