import pandas as pd
import os
from dagster import asset, AssetIn, Output, multi_asset, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn
from ..dbt.dbt_asset import dbt_dim_products_key

dim_product_type = create_dagster_pandas_dataframe_type(
    name="dim_product_type",
    columns=[
        PandasColumn.string_column("product_id", ignore_missing_vals=True),
        PandasColumn.string_column("product_category_name_english", ignore_missing_vals=True)
    ]
)


@asset(
    name='dim_product',
    key_prefix=["dim_product"],  # group_name="dim_product",
    deps=([dbt_dim_products_key]), required_resource_keys={"postgres_sql_extractor_silver_io_manager"},
    compute_kind='python',
)
def dim_product(context) -> Output[dim_product_type]:
    """ create dim product  """

    sql_stm = f"SELECT * FROM {os.getenv('DEV_BASE')}.dim_products"
    pd_data = context.resources.postgres_sql_extractor_silver_io_manager.extract_data(sql_stm, context)
    return Output(value=pd_data)


@asset(
    name="minio_dim_product",
    # group_name="dim_product",
    compute_kind="PostgresSQL", key_prefix=["minio_dim_product"],
    io_manager_key="minio_silver_io_manager",
    ins={"dim_product": AssetIn(key_prefix=['dim_product'])},
)
def minio_dim_product(context, dim_product) -> Output[dim_product_type]:
    """ upload data to Minio """
    context.log.info("upload dim_product to Minio")
    return Output(value=dim_product)


@multi_asset(
    # group_name='silver',
    name="silver_dim_product",
    ins={"dim_product": AssetIn(key_prefix=["dim_product"])},
    outs={"silver_dim_product": AssetOut(
        io_manager_key="postgres_sql_silver_io_manager",
        key_prefix=["silver_dim_product"],
        metadata={
            "primary_keys": ["product_id"],
            "columns": [
                {"product_id": "varchar"},
                {"product_category_name_english": "varchar"}]
            }
        )
    },
    compute_kind="PostgresSQL",
)
def silver_dim_product(dim_product) -> Output[dim_product_type]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(dim_product),
        metadata={
            "schema": "silver",
            "table": "silver_dim_product",
        },
    )





