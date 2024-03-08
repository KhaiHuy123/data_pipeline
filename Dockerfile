
FROM python:3.10-slim

ENV DAGSTER_APP=/opt/dagster/app
WORKDIR $DAGSTER_APP

COPY setup.py .
COPY pyproject.toml .
COPY requirements.txt .
COPY workspace.yaml .
COPY etl_pipeline_e2e .
COPY dbt_pipeline_e2e .
COPY notebooks .

RUN pip install --upgrade pip
RUN pip install -e ".[dev]"
RUN pip install -r requirements.txt

CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline_e2e"]
