import pandas as pd
import os
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn
from ..dbt.dbt_asset import dbt_dim_product_sales_key

dim_product_sales_type = create_dagster_pandas_dataframe_type(
    name="dim_product_sales_type",
    columns=[
        PandasColumn.string_column("product_id", ignore_missing_vals=True),
        PandasColumn.string_column("product_category_name", ignore_missing_vals=True),
        PandasColumn.string_column("order_id", ignore_missing_vals=True),
        PandasColumn.string_column("order_item_id", ignore_missing_vals=True),
        PandasColumn.string_column("seller_id", ignore_missing_vals=True),
        PandasColumn.integer_column("delivered_within_days", ignore_missing_vals=True),
        PandasColumn.float_column("price", ignore_missing_vals=True),
        PandasColumn.float_column("freight_value", ignore_missing_vals=True),
        PandasColumn.string_column("payment_type", ignore_missing_vals=True),
        PandasColumn.float_column("payment_value", ignore_missing_vals=True)
    ]
)


@asset(
    name='dim_product_sales',
    key_prefix=["dim_product_sales"],  # group_name="dim_product_sales",
    deps=([dbt_dim_product_sales_key]), required_resource_keys={"postgres_sql_extractor_silver_io_manager"},
    compute_kind='python',
)
def dim_product_sales(context) -> Output[dim_product_sales_type]:
    """ create dim product  """

    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.dim_product_sales"
    pd_data = context.resources.postgres_sql_extractor_silver_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_dim_product_sales",
    # group_name="dim_product_sales",
    compute_kind="PostgresSQL", key_prefix=["minio_dim_product_sales"],
    io_manager_key="minio_silver_io_manager",
    ins={"dim_product_sales": AssetIn(key_prefix=['dim_product_sales'])},
)
def minio_dim_product_sales(context, dim_product_sales) -> Output[dim_product_sales_type]:
    """ upload data to Minio """
    context.log.info("upload dim_product_sales to Minio")
    return Output(value=dim_product_sales)


@multi_asset(
    # group_name='silver',
    name="silver_dim_product_sales",
    ins={"dim_product_sales": AssetIn(key_prefix=["dim_product_sales"])},
    outs={"silver_dim_product_sales": AssetOut(
        io_manager_key="postgres_sql_silver_io_manager",
        key_prefix=["silver_dim_product_sales"],
        metadata={
            "primary_keys": [],
            "columns": [
                {"product_id": "varchar"},
                {"product_category_name": "varchar"},
                {"order_id": "varchar"},
                {"order_item_id": "varchar"},
                {"seller_id": "varchar"},
                {"delivered_within_days": "int4"},
                {"price": "float4"},
                {"freight_value": "float4"},
                {"payment_type": "varchar"},
                {"payment_value": "float4"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def silver_dim_product_sales(dim_product_sales) -> Output[dim_product_sales_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(dim_product_sales),
        metadata={
            "schema": "silver",
            "table": "silver_dim_product_sales",
        },
    )





