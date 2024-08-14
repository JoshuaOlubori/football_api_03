# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
from airflow.models import Variable
import logging
import os
from minio import Minio
from pendulum import duration
import json

# ----------------------- #
# Configuration variables #
# ----------------------- #

# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
FIXTURES_BUCKET_NAME = "fixtures"
# ARCHIVE_BUCKET_NAME = "archive"

# Source file path climate data
FIXTURES_DATA_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/fixtures_data/all_fixtures_combined.csv"
)

FIXTURES_DATA_FOLDER = (
    f"{os.environ['AIRFLOW_HOME']}/include/fixtures_data/"
)

RESULTS_DATA_PATH = (
    f"{os.environ['AIRFLOW_HOME']}/include/results/"
)

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
FIXTURES_IN_TABLE_NAME = "in_fixtures"
REPORTING_TABLE_NAME_1 = "reporting_table_1"
REPORTING_TABLE_NAME_2 = "reporting_table_2"
CONN_ID_DUCKDB = "duckdb_default"

# Datasets
DS_FIXTURES_DATA_MINIO = Dataset(f"minio://{FIXTURES_BUCKET_NAME}")
DS_DUCKDB_IN_FIXTURES = Dataset(f"duckdb://{FIXTURES_IN_TABLE_NAME}")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_START = Dataset("start")
DS_INGEST = Dataset("ingest")

# API KEYS

# API_ENDPOINT = Variable.get("API_ENDPOINT")
# API_KEY = Variable.get("API_KEY")
# API_HOST = Variable.get("API_HOST")

API_ENDPOINT = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
API_KEY = "55ab8bcb28msh4d904b12081603fp1d7acejsn623e4694b2e8"
API_HOST = "api-football-v1.p.rapidapi.com"

# LEAGUE IDS
LEAGUE_IDS = [128, 98, 99,
              41, 42, 43, 50, 51, 59, 60,
             78, 79, 83, 84, 85, 86, 61, 62, 135, 136,
             88, 89, 106, 107,  144, 283, 94, 95, 96,
             140, 141, 142, 876, 435, 436, 210, 318, 345,
             203, 204, 271, 272, 235, 373, 307, 308, 301, 303,
             207, 208, 179, 180, 183, 184,
            305, 233, 290, 218, 219, 419, 172,
             119, 120, 236, 288, 128, 129, 134, 71, 72, 383, 382
              ]

CURRENT_SEASON = 2024
TEMP = (
    f"{os.environ['AIRFLOW_HOME']}/include/fixtures_data/all_fixtures_combined.csv"
)

DB = (
    f"{os.environ['AIRFLOW_HOME']}/include/dwh" # when changing this value also change the db name in .env
)


# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": "Edun Joshua",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=1),
}


# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client
