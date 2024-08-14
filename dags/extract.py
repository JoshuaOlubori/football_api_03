"""DAG that extracts fixtures data from an API, and concatenates them into a single file saved to the local disk."""
# --------------- #
# PACKAGE IMPORTS #
# --------------- #
from pendulum import datetime
import io
import os
import pandas as pd
from airflow.decorators import dag
from airflow.operators.python import PythonOperator

# -------------------- #
# Local module imports #
# -------------------- #
from include.global_variables import global_variables as gv
from include.logic import fetch_data


def concatenate_csvs(root_dir, output_file):
    csv_files = []
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                csv_files.append(file_path)

    if not csv_files:
        gv.task_log.warning("No CSV files found in the specified directory.")
        return

    combined_df = pd.concat([pd.read_csv(file) for file in csv_files])
    combined_df.to_csv(os.path.join(root_dir, output_file), index=False)


# --- #
# DAG #
# --- #
@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="extract fixtures data from API.",
    tags=["extract", "API"],
    render_template_as_native_obj=True,
)
def b_extraction():
    api_fetcher = PythonOperator(
            task_id = "fetch_data",
            python_callable= fetch_data,
            op_kwargs = {
                "chosen_season": str(gv.CURRENT_SEASON)
            }
        )


    concatenator = PythonOperator(
            task_id = "concat_csvs",
            python_callable= concatenate_csvs,
            op_kwargs = {
                "root_dir": gv.FIXTURES_DATA_FOLDER,
                "output_file": "all_fixtures_combined.csv"

            },
            outlets=[gv.DS_INGEST]
        )

    # set dependencies
    api_fetcher >> concatenator
b_extraction()
