"""DAG that loads fixtures from MinIO to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime, parse
import duckdb
import os
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_operators.minio import (
    MinIOListOperator,
    MinIODeleteObjectsOperator,
)

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the fixtures data is ready in MinIO
    schedule=[gv.DS_FIXTURES_DATA_MINIO],
    catchup=False,
    default_args=gv.default_args,
    description="Loads fixtures data from MinIO to DuckDB.",
    tags=["load", "minio", "duckdb"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def d_load_data():


    list_files_fixtures_bucket = MinIOListOperator(
        task_id="list_files_fixtures_bucket", bucket_name=gv.FIXTURES_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_FIXTURES], pool="duckdb")
    def load_fixtures_data(obj):
        """Loads content of one fileobject in the MinIO fixtures bucket
        to DuckDB."""

        # get the object from MinIO and save as a local tmp csv file
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.FIXTURES_BUCKET_NAME, obj, file_path=obj)

        # use read_csv_auto to load data to duckdb
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        cursor.execute(
            f"""DROP TABLE IF EXISTS {gv.FIXTURES_IN_TABLE_NAME};"""
        )
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {gv.FIXTURES_IN_TABLE_NAME} AS
            SELECT * FROM read_csv_auto('{obj}');"""
        )
        cursor.commit()
        cursor.close()

        # delete local tmp csv file
        os.remove(obj)


    fixtures_data = load_fixtures_data.expand(obj=list_files_fixtures_bucket.output)

d_load_data()
