# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import streamlit as st
import duckdb
import pandas as pd
from datetime import date, datetime
import altair as alt
# from global_variables import global_variables as gv




duck_db_instance_path = (
    "/app/include/dwh"  # when changing this value also change the db name in .env
)

APP_USER = "Mr. Akindele"
# -------------- #
# DuckDB Queries #
# -------------- #


def list_currently_available_tables(db=duck_db_instance_path):
    cursor = duckdb.connect(db)
    tables = cursor.execute("SHOW TABLES;").fetchall()
    cursor.close()
    return [table[0] for table in tables]







def get_fixtures_1(db=duck_db_instance_path):

    cursor = duckdb.connect(db)
    fixtures_data = cursor.execute(
        f"""SELECT * FROM reporting_table_1;"""
    ).fetchall()

    fixtures_data_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = 'reporting_table_1';"""
    ).fetchall()

    df = pd.DataFrame(
        fixtures_data, columns=[x[0] for x in fixtures_data_col_names]
    )
    cursor.close()

    return df

def get_fixtures_2(db=duck_db_instance_path):

    cursor = duckdb.connect(db)
    fixtures_data = cursor.execute(
        f"""SELECT * FROM reporting_table_2;"""
    ).fetchall()

    fixtures_data_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = 'reporting_table_2';"""
    ).fetchall()

    df = pd.DataFrame(
        fixtures_data, columns=[x[0] for x in fixtures_data_col_names]
    )
    cursor.close()

    return df




# ------------ #
# Query DuckDB #
# ------------ #


tables = list_currently_available_tables()


if "reporting_table_1" in tables:
    fixtures_result_table_1 = get_fixtures_1()


if "reporting_table_2" in tables:
    fixtures_result_table_2 = get_fixtures_2()



# ------------- #
# STREAMLIT APP #
# ------------- #

st.title("Fixtures Transformation Results")

st.markdown(f"Hello {APP_USER} :wave: Welcome to your Streamlit App! :blush:")
# Get the DataFrame

# Display the DataFrame as a table in Streamlit
# st.dataframe(fixtures_result_table_1)

with st.expander("Section 1: Fixtures in which either teams have won 3 games or more against common opponents in the league"):
    st.dataframe(fixtures_result_table_1)  # Optional styling

with st.expander("Section 2: Fixtures in which either teams have won all 5 games in their last 5 league matches"):
    st.dataframe(fixtures_result_table_2)



