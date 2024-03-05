
FROM python:3.10-slim

WORKDIR /opt/dagster/app

COPY dagster/dagster.yaml .
COPY dagster/workspace.yaml .
COPY etl_pipeline_e2e/pyproject.toml .
COPY etl_pipeline_e2e/setup.cfg .
COPY etl_pipeline_e2e/setup.py .
COPY etl_pipeline_e2e/data/raw  .
COPY etl_pipeline_e2e/dbt_pipeline_e2e .
COPY etl_pipeline_e2e/etl_pipeline_e2e .
COPY data/raw .
COPY . .
