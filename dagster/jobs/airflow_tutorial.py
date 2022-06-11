import os

import psycopg2
import requests
from dagster import op, job

PSYCOPG_CONNECT_STRING = "postgresql://postgres:postgres@localhost:5436/prefect"
DATA_PATH = "./employees.csv"


@op
def create_employees_table():
    with psycopg2.connect(PSYCOPG_CONNECT_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute("""
             CREATE TABLE IF NOT EXISTS employees (
                 "Serial Number" NUMERIC PRIMARY KEY,
                 "Company Name" TEXT,
                 "Employee Markme" TEXT,
                 "Description" TEXT,
                 "Leave" INTEGER
             );""")
            conn.commit()

    return True


@op
def create_employees_temp_table():
    with psycopg2.connect(PSYCOPG_CONNECT_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute("""
             DROP TABLE IF EXISTS employees_temp;
             CREATE TABLE employees_temp (
                 "Serial Number" NUMERIC PRIMARY KEY,
                 "Company Name" TEXT,
                 "Employee Markme" TEXT,
                 "Description" TEXT,
                 "Leave" INTEGER
             );""")
            conn.commit()

    return True


@op
def get_data(temp_table_created: bool, regular_table_created: bool):
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)

    url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"

    response = requests.request("GET", url)

    with open(DATA_PATH, "w") as file:
        file.write(response.text)

    with psycopg2.connect(PSYCOPG_CONNECT_STRING) as conn:
        with conn.cursor() as cur:
            with open(DATA_PATH, "r") as file:
                cur.copy_expert(
                    "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()

    return True


@op
def merge_data(data_loaded: bool):
    with psycopg2.connect(PSYCOPG_CONNECT_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO employees
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM employees_temp
            ) s1
            ON CONFLICT ("Serial Number") DO UPDATE
            SET "Serial Number" = excluded."Serial Number";
            """)
            conn.commit()



@job
def airflow_job():
    merge_data(
        data_loaded=get_data(
            regular_table_created=create_employees_table(),
            temp_table_created=create_employees_temp_table(),
        )
    )


if __name__ == '__main__':
    airflow_job.execute_in_process()
