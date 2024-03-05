# Dagster libraries to run both dagit and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.10-slim
# Set $DAGSTER_HOME and copy dagster instance and workspace YAML there
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p $DAGSTER_HOME && \
    mkdir -p $DAGSTER_HOME/storage && \
    mkdir -p $DAGSTER_HOME/compute_logs && \
    mkdir -p $DAGSTER_HOME/local_artifact_storage

WORKDIR $DAGSTER_HOME
COPY requirements.txt $DAGSTER_HOME
COPY . .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 3000
CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]
