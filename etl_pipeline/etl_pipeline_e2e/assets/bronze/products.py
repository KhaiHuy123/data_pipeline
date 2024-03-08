import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

olist_products_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="olist_products_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("product_id", non_nullable=True),
        PandasColumn.string_column("product_category_name", ignore_missing_vals=True),
        PandasColumn.integer_column("product_name_length", ignore_missing_vals=True),
        PandasColumn.integer_column("product_description_length", ignore_missing_vals=True),
        PandasColumn.integer_column("product_photos_qty", ignore_missing_vals=True),
        PandasColumn.integer_column("product_weight_g", ignore_missing_vals=True),
        PandasColumn.integer_column("product_length_cm", ignore_missing_vals=True),
        PandasColumn.integer_column("product_height_cm", ignore_missing_vals=True),
        PandasColumn.integer_column("product_width_cm", ignore_missing_vals=True)
    ],
)


@asset(
    name="mysql_olist_products_dataset",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["products"],
    compute_kind="MySQL",  # group_name="products",
)
def mysql_olist_products_dataset(context) -> Output[olist_products_dataset_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_olist_products_dataset",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["products"], required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="products",
    deps=([mysql_olist_products_dataset]),
)
def minio_olist_products_dataset(context) \
        -> Output[olist_products_dataset_DataFrame]:

    """ upload MySQL raw data to Minio """

    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_olist_products_dataset",
    ins={"mysql_olist_products_dataset": AssetIn(key_prefix=["products"])},
    outs={"bronze_olist_products_dataset": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": ["product_id"],
            "columns": [
                {"product_id": "varchar"},
                {"product_category_name": "varchar"},
                {"product_name_length": "float4"},
                {"product_description_length": "float4"},
                {"product_photos_qty": "float4"},
                {"product_weight_g": "float4"},
                {"product_length_cm": "float4"},
                {"product_height_cm": "float4"},
                {"product_width_cm": "float4"}]
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_olist_products_dataset(mysql_olist_products_dataset)\
        -> Output[olist_products_dataset_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_olist_products_dataset),
        metadata={
            "schema": "bronze",
            "table": "bronze_olist_products_dataset",
        },
    )





