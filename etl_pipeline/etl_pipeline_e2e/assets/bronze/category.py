import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

product_category_name_translation_DataFrame = create_dagster_pandas_dataframe_type(
    name="product_category_name_translation_DataFrame",
    columns=[
        PandasColumn.string_column("product_category_name", unique=True),
        PandasColumn.string_column("product_category_name_english"),
    ],
)


@asset(
    name="mysql_product_category_name_translation",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["product_category_name"],
    compute_kind="MySQL",  # group_name="product_category_name"
)
def mysql_product_category_name_translation(context) -> Output[product_category_name_translation_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM product_category_name_translation"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_product_category_name_translation",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["product_category_name"],  required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="product_category_name",
    deps=([mysql_product_category_name_translation])
)
def minio_product_category_name_translation(context)\
        -> Output[product_category_name_translation_DataFrame]:

    """ upload MySQL raw data to Minio """

    sql_stm = "SELECT * FROM product_category_name_translation"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_product_category_name_translation",
    ins={"mysql_product_category_name_translation": AssetIn(key_prefix=["product_category_name"])},
    outs={"bronze_product_category_name_translation": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": ["product_category_name_english"],
            "columns": [
                    {"product_category_name": "text"},
                    {"product_category_name_english": "text"}
                ],
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_product_category_name_translation(mysql_product_category_name_translation) \
        -> Output[product_category_name_translation_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_product_category_name_translation),
        metadata={
            "schema": "bronze",
            "table": "bronze_product_category_name_translation",
        },
    )


