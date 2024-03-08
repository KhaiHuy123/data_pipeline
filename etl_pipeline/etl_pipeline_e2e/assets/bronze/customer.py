import pandas as pd
from dagster import asset, Output, multi_asset, AssetIn, AssetOut
from dagster_pandas import create_dagster_pandas_dataframe_type, PandasColumn

olist_customers_dataset_DataFrame = create_dagster_pandas_dataframe_type(
    name="olist_customers_dataset_DataFrame",
    columns=[
        PandasColumn.string_column("customer_id", non_nullable=True),
        PandasColumn.string_column("customer_unique_id", non_nullable=True),
        PandasColumn.string_column("customer_city", non_nullable=True),
        PandasColumn.string_column("customer_state", non_nullable=True),
        PandasColumn.integer_column("customer_zip_code_prefix", min_value=0),
    ],
)


@asset(
    name="mysql_olist_customers_dataset",
    io_manager_key="json_mysql_io_manager",
    required_resource_keys={"mysql_extractor_io_manager"},
    key_prefix=["customers"],
    compute_kind="MySQL",  # group_name="customer"
)
def mysql_olist_customers_dataset(context) -> Output[olist_customers_dataset_DataFrame]:

    """ create back up versions (select from MySQL) """

    sql_stm = "SELECT * FROM olist_customers_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@asset(
    name="minio_olist_customers_dataset",
    io_manager_key="minio_bronze_io_manager",
    key_prefix=["customers"], required_resource_keys={"mysql_extractor_io_manager"},
    compute_kind="MinIO",  # group_name="customer"
    deps=([mysql_olist_customers_dataset]),
)
def minio_olist_customers_dataset(context)\
        -> Output[olist_customers_dataset_DataFrame]:

    """ upload MySQL raw data to Minio """

    sql_stm = "SELECT * FROM olist_customers_dataset"
    pd_data = context.resources.mysql_extractor_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    # group_name="bronze",
    name="bronze_olist_customers_dataset",
    ins={"mysql_olist_customers_dataset": AssetIn(key_prefix=["customers"])},
    outs={"bronze_olist_customers_dataset": AssetOut(
        io_manager_key="postgres_sql_bronze_io_manager",
        key_prefix=["warehouse", "public"],
        metadata={
            "primary_keys": ["customer_id", "customer_unique_id"],
            "columns": [
                {"customer_id": "varchar"},
                {"customer_unique_id": "varchar"},
                {"customer_city": "varchar"},
                {"customer_state": "varchar"},
                {"customer_zip_code_prefix": "int4"}]
            }
        ),
    },
    compute_kind="PostgresSQL",
)
def bronze_olist_customers_dataset(mysql_olist_customers_dataset) \
        -> Output[olist_customers_dataset_DataFrame]:

    """ insert raw data into PostgresSQL """

    return Output(
        value=pd.DataFrame(mysql_olist_customers_dataset),
        metadata={
            "schema": "bronze",
            "table": "bronze_olist_customers_dataset",
        },
    )



