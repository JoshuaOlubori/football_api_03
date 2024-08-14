"""DAG that loads fixtures ingests from local csv files into MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import io

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_INGEST],
    catchup=False,
    default_args=gv.default_args,
    description="Ingests fixtures data from provided csv files to MinIO.",
    tags=["ingestion", "minio"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def c_in_fixtures_data():

    create_bucket_tg = CreateBucket(
        task_id="create_fixtures_bucket", bucket_name=gv.FIXTURES_BUCKET_NAME
    )
    ingest_fixtures_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_fixtures_data",
        bucket_name=gv.FIXTURES_BUCKET_NAME,
        outlets=[gv.DS_FIXTURES_DATA_MINIO],
    ).expand_kwargs(
        [
            {
                "local_file_path": gv.FIXTURES_DATA_PATH,
                "object_name": gv.FIXTURES_DATA_PATH.split("/")[-1],
            },
        ]
    )

    create_bucket_tg >> ingest_fixtures_data


c_in_fixtures_data()
