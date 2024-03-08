from . import bronze, dbt, gold, silver, source, notebook
from dagster import load_assets_from_package_module
from .constant import BRONZE, DBT, SILVER, GOLD, SOURCE, NOTEBOOK

bronze_assets = load_assets_from_package_module(package_module=bronze, group_name=BRONZE)
silver_assets = load_assets_from_package_module(package_module=silver, group_name=SILVER)
gold_assets = load_assets_from_package_module(package_module=gold, group_name=GOLD)
dbt_assets = load_assets_from_package_module(package_module=dbt, group_name=DBT)
source_assets = load_assets_from_package_module(package_module=source, group_name=SOURCE)
notebook_assets = load_assets_from_package_module(package_module=notebook, group_name=NOTEBOOK)

