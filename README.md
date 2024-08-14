# Analyzing Football Data - Part 3
## A 3-part series featuring webscraping, data pipelines, orchestration and containerizarion

Overview
========
This repository contains an Airflow pipeline following an ELT pattern, that can be run in GitHub codespaces (or locally with the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)). The pipeline ingests fixtures data from an API transforms it and displays the resultset on StreamLit. 

This is accomplished by using a set of tools in six Airflow DAGs:

- The [Astro Python SDK](https://astro-sdk-python.readthedocs.io/en/stable/index.html) is used for ELT operations.
- [DuckDB](https://duckdb.org/), a relational database, is used to store tables of the ingested data as well as the resulting tables after transformations.
- [Streamlit](https://streamlit.io/), a python package to create interactive apps is used to display the data as a dashboard. The streamlit app retrieves its data from tables in DuckDB.

All tools used are open source and no additional accounts are needed.

After completing all tasks the streamlit app will look similar to the following screenshots:

![Finished Streamlit App Part 1](src/streamlit_result_1.png)

## Part 1: Run a fully functional pipeline

Follow the [Part 1 Instructions](#part-1-instructions) to get started!

The ready to run Airflow pipeline consists of 5 DAGs and will:

- Trigger the pipeline
- Retrieve the current weather from an API and save to local disk
- Ingest fixtures data from local CSV file and upload to a MinIO instance.
- Load the data from MinIO into DuckDB using the Astro SDK.
- Run a transformation on the data using the Astro SDK to create a reporting table powering a Streamlit App.

## Part 2: Play with it!

Use this repository to explore Airflow best practices, experiment with your own DAGs and as a template for your own projects!

This project was created with :heart: by [Astronomer](https://www.astronomer.io/).

> If you are looking for an entry level written tutorial where you build your own DAG from scratch check out: [Get started with Apache Airflow, Part 1: Write and run your first DAG](https://docs.astronomer.io/learn/get-started-with-airflow).

-------------------------------

How to use this repository
==========================

# Setting up

## Option 1: Use GitHub Codespaces

Run this Airflow project without installing anything locally.

1. Fork this repository.
2. Create a new GitHub codespaces project on your fork. Make sure it uses at least 4 cores!

    ![Fork repo and create a codespaces project](src/fork_and_codespaces.png)

3. After creating the codespaces project the Astro CLI will automatically start up all necessary Airflow components as well as the streamlit app. This can take a few minutes. 
4. Once the Airflow project has started access the Airflow UI by clicking on the **Ports** tab and opening the forward URL for port 8080.

    ![Open Airflow UI URL Codespaces](src/open_airflow_ui_codespaces.png)

5. Once the streamlit app is running you can access it by by clicking on the **Ports** tab and opening the forward URL for port 8501.

## Option 2: Use the Astro CLI

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install.

1. Run `git clone https://github.com/astronomer/airflow-quickstart.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.
5. View the streamlit app at `localhost:8501`. NOTE: The streamlit container can take a few minutes to start up.


How it works
============

## Components and infrastructure

This repository uses a [custom codespaces container](https://github.com/astronomer/devcontainer-features/pkgs/container/devcontainer-features%2Fastro-cli) to install the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli). The GH codespaces post creation command will start up the Astro project by running `astro dev start`. 

5 Docker containers will be created and relevant ports will be forwarded:

- The Airflow scheduler
- The Airflow webserver
- The Airflow metastore
- The Airflow triggerer

Additionally when using codespaces, the command to run the streamlit app is automatically run upon starting the environment.

## Data sources

The fixture data is queried from [API-FOOTBALL](https://rapidapi.com/api-sports/api/api-football/details)

Project Structure
================

This repository contains the following files and folders:

- `.astro`: files necessary for Astro CLI commands.
- `.devcontainer`: the GH codespaces configuration.

-  `dags`: all DAGs in your Airflow environment. Files in this folder will be parsed by the Airflow scheduler when looking for DAGs to add to your environment. DAGS are suffixed with consecutive letters of the alphabet in order of their precedence in the pipeline so the Airflow webserver can order the DAGs appropriately in the UI.
    - `start`: DAG to trigger the pipeline. Set to be triggered manually to prevent overrunning GitHub's free compute allowance.
    - `extract`: DAG to query API and save data to local CSVs.
    - `load_csv_to_minio`: DAG to load fixtures ingests from local csv files into MinIO.
    - `load_minio_to_duckdb`: DAG that loads fxitures from MinIO to DuckDB.
    - `transform_fixtures`: DAG that runs a transformation on data in DuckDB using the Astro SDK.

- `include`: supporting files that will be included in the Airflow environment.
    - `global_variables`: configuration files.
    - `streamlit_app.py`: file defining the streamlit app.
    - `fixtures_data`: stores fresh API ingests
    - `results`: stores transformed data
- `src`: contains images used in this README.
- `tests`: folder to place pytests running on DAGs in the Airflow instance. Contains default tests.
- `.dockerignore`: list of files to ignore for Docker.
- `.env`: environment variables. Contains the definition for the DuckDB connection.
- `.gitignore`: list of files to ignore for git. NOTE that `.env` is not ignored in this project.
- `Dockerfile`: the Dockerfile using the Astro CLI.
- `packages.txt`: system-level packages to be installed in the Airflow environment upon building of the Dockerimage.
- `README.md`: this Readme.
