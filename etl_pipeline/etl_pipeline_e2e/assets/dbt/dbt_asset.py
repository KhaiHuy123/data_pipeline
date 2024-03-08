
from dagster import file_relative_path, AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource, get_asset_key_for_model

# DBT_PROJECT_PATH = file_relative_path(__file__, "..\\..\\..\\dbt_pipeline_e2e")
# DBT_PROFILES_PATH = file_relative_path(__file__, "..\\..\\..\\dbt_pipeline_e2e")
# MANIFEST_PATH = file_relative_path(__file__, "..\\..\\..\\dbt_pipeline_e2e\\target\\manifest.json")
DBT_PROJECT_PATH = file_relative_path(__file__, "../../../dbt_pipeline_e2e")
DBT_PROFILES_PATH = file_relative_path(__file__, "../../../dbt_pipeline_e2e")
MANIFEST_PATH = file_relative_path(__file__, "../../../dbt_pipeline_e2e/target/manifest.json")


@dbt_assets(
    manifest=MANIFEST_PATH,
    name="dbt_asset"
)
def dbt_asset(context: AssetExecutionContext, dbt: DbtCliResource):
    dbt_run_invocation = dbt.cli(["build", "--select"], context=context)
    yield from dbt_run_invocation.stream()


dbt_raw_category_key = get_asset_key_for_model([dbt_asset], "raw_categories")
dbt_raw_order_items_key = get_asset_key_for_model([dbt_asset], "raw_order_items")
dbt_raw_order_payments_key = get_asset_key_for_model([dbt_asset], "raw_order_payments")
dbt_raw_orders_key = get_asset_key_for_model([dbt_asset], "raw_orders")
dbt_raw_products_key = get_asset_key_for_model([dbt_asset], "raw_products")
dbt_raw_customers_key = get_asset_key_for_model([dbt_asset], "raw_customers")
dbt_raw_geolocation_key = get_asset_key_for_model([dbt_asset], "raw_geolocations")
dbt_raw_sellers_key = get_asset_key_for_model([dbt_asset], "raw_sellers")

dbt_dim_products_key = get_asset_key_for_model([dbt_asset], "dim_products")
dbt_dim_orders_condition_key = get_asset_key_for_model([dbt_asset], "dim_orders_condition")
dbt_dim_product_sales_key = get_asset_key_for_model([dbt_asset], "dim_product_sales")
dbt_fact_sales_key = get_asset_key_for_model([dbt_asset], "fact_sales")
dbt_fact_customers_key = get_asset_key_for_model([dbt_asset], "fact_customers")

dbt_sale_values_key = get_asset_key_for_model([dbt_asset], "sale_values_by_category")
dbt_average_order_value = get_asset_key_for_model([dbt_asset], "average_order_value")
dbt_return_rate = get_asset_key_for_model([dbt_asset], "return_rate")
dbt_sale_conversion = get_asset_key_for_model([dbt_asset], "sale_conversion")
dbt_top_customer = get_asset_key_for_model([dbt_asset], "top_customer_location")
