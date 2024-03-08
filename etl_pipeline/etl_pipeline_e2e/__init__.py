from .assets import bronze_assets, silver_assets, gold_assets, dbt_assets, source_assets, notebook_assets
from .resources import resources
from .job import reload_data, update_pipeline_job
from .schedule import reload_data_schedule, update_pipeline_schedule
from dagster import Definitions

all_assets = [*bronze_assets, *silver_assets, *gold_assets, *dbt_assets, *source_assets, *notebook_assets]
all_jobs = [reload_data, update_pipeline_job]
all_schedules = [reload_data_schedule, update_pipeline_schedule]


defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=all_jobs,
    schedules=all_schedules,
)




