from dagstermill import ConfigurableLocalOutputNotebookIOManager
from ..assets.constant import MYSQL_CONFIG, MINIO_CONFIG_BRONZE, MINIO_CONFIG_GOLD, MINIO_CONFIG_SILVER,\
    PSQL_CONFIG_BRONZE, PSQL_CONFIG_SILVER, PSQL_CONFIG_GOLD
from . import minio_io_manager, mysql_extractor_io_manager, json_mysql_io_manager,\
    postgres_sql_io_manager, postgres_sql_extractor_io_manager, mysql_loader_io_manager
from ..assets.dbt.dbt_asset import DBT_PROFILES_PATH, DBT_PROJECT_PATH
from dagster_dbt import DbtCliResource

resources = {
    "mysql_loader_io_manager": mysql_loader_io_manager.MySQL_LoaderIOManager(config=MYSQL_CONFIG),
    "mysql_extractor_io_manager": mysql_extractor_io_manager.MySQL_ExtractorIOManager(config=MYSQL_CONFIG),
    "minio_bronze_io_manager": minio_io_manager.MinIOMananger(config=MINIO_CONFIG_BRONZE),
    "minio_silver_io_manager": minio_io_manager.MinIOMananger(config=MINIO_CONFIG_SILVER),
    "minio_gold_io_manager": minio_io_manager.MinIOMananger(config=MINIO_CONFIG_GOLD),
    "postgres_sql_bronze_io_manager": postgres_sql_io_manager.PostgresSQLIOManager(config=PSQL_CONFIG_BRONZE),
    "postgres_sql_silver_io_manager": postgres_sql_io_manager.PostgresSQLIOManager(config=PSQL_CONFIG_SILVER),
    "postgres_sql_gold_io_manager": postgres_sql_io_manager.PostgresSQLIOManager(config=PSQL_CONFIG_GOLD),
    "postgres_sql_extractor_silver_io_manager": postgres_sql_extractor_io_manager.PostgresExtractorIOManager(
        config=PSQL_CONFIG_SILVER),
    "postgres_sql_extractor_gold_io_manager": postgres_sql_extractor_io_manager.PostgresExtractorIOManager(
        config=PSQL_CONFIG_GOLD),
    "json_mysql_io_manager": json_mysql_io_manager.JSON_MYSQL_IOManager(),
    "dbt": DbtCliResource(
        profiles_dir=DBT_PROFILES_PATH,
        project_dir=DBT_PROJECT_PATH
        ),
    "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager()
    }

