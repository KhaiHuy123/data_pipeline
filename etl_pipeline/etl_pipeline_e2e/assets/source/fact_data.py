import pandas as pd
from dagster import asset, file_relative_path
from ..constant import orders, order_items


@asset(
    name="olist_order_items_dataset",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["order_items"],
    compute_kind="MySQL",  # group_name="fact_data"
)
def olist_order_items_dataset(context) -> None:

    """ Load data into MySQL """

    metadata = order_items["metadata"]

    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\olist_order_items_dataset.csv")
    table_name = context.asset_key.path[-1]

    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(inplace=True, method="ffill")
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)


@asset(
    name="olist_orders_dataset",
    required_resource_keys={"mysql_loader_io_manager"},
    key_prefix=["orders"],
    compute_kind="MySQL",  # group_name="fact_data"
)
def olist_orders_dataset(context) -> None:

    """ Load data into MySQL """

    metadata = orders["metadata"]

    csv_file = file_relative_path(__file__, "..\\..\\..\\data\\raw\\olist_orders_dataset.csv")
    table_name = context.asset_key.path[-1]
    data = pd.read_csv(csv_file, keep_date_col=True,
                       date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M', errors='coerce'))

    data.fillna(inplace=True, method="ffill")
    context.resources.mysql_loader_io_manager.load_data(table_name, metadata, data)
