from setuptools import find_packages, setup

setup(
    name="etl_pipeline_e2e",
    packages=find_packages(exclude=["etl_pipeline_e2e_tests"]),
    install_requires=[
        "dagster", "dagster-postgres", "spark", "scrapy", "selenium",
        "dagster_pandas", "dagster_dbt", "dagstermill", "papermill"
        , "minio", "mysql-connector-python"
        , "psycopg2-binary", "basemap", "geos"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
