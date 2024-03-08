import pandas as pd
from dagster import asset, file_relative_path
from ..constant import customer, product_category, products, payments, geolocation, sellers


@asset(
    name="olist_customers_dataset",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["customers"],
    compute_kind="MySQL",  # group_name="dim_data"
)
def olist_customers_dataset(context) -> None:

    """ Load data into MySQL """

    metadata = customer["metadata"]
    fill_values = customer["fill_values"]
    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\olist_customers_dataset.csv")
    table_name = context.asset_key.path[-1]
    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(value=fill_values, inplace=True)
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)


@asset(
    name="olist_geolocation_dataset",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["geolocation"],
    compute_kind="MySQL",  # group_name="dim_data"
)
def olist_geolocation_dataset(context) -> None:

    """ Load data into MySQL """

    metadata = geolocation["metadata"]
    fill_values = geolocation["fill_values"]
    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\olist_geolocation_dataset.csv")
    table_name = context.asset_key.path[-1]
    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(value=fill_values, inplace=True)
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)


@asset(
    name="product_category_name_translation",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["product_category_name"],
    compute_kind="MySQL",  # group_name="dim_data"
)
def product_category_name_translation(context) -> None:

    """ Load data into MySQL """

    metadata = product_category["metadata"]
    fill_values = product_category["fill_values"]
    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\product_category_name_translation.csv")
    table_name = context.asset_key.path[-1]
    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(value=fill_values, inplace=True)
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)


@asset(
    name="olist_order_payments_dataset",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["order_payments"],
    compute_kind="MySQL",  # group_name="dim_data"
)
def olist_order_payments_dataset(context) -> None:

    """ Load data into MySQL """

    metadata = payments["metadata"]
    fill_values = payments["fill_values"]
    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\olist_order_payments_dataset.csv")
    table_name = context.asset_key.path[-1]
    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(value=fill_values, inplace=True)
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)


@asset(
    name="olist_sellers_dataset",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["sellers"],
    compute_kind="MySQL",  # group_name="dim_data"
)
def olist_sellers_dataset(context) -> None:

    """ Load data into MySQL """

    metadata = sellers["metadata"]
    fill_values = sellers["fill_values"]
    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\olist_sellers_dataset.csv")
    table_name = context.asset_key.path[-1]
    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(value=fill_values, inplace=True)
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)


@asset(
    name="olist_products_dataset",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["products"],
    compute_kind="MySQL",  # group_name="dim_data"
)
def olist_products_dataset(context) -> None:

    """ Load data into MySQL """

    metadata = products["metadata"]
    fill_values = products["fill_values"]
    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\olist_products_dataset.csv")
    table_name = context.asset_key.path[-1]
    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(value=fill_values, inplace=True)
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)
