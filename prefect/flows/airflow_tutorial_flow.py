import os

import psycopg2
import requests
from prefect import task, Flow

PSYCOPG_CONNECT_STRING = "postgresql://postgres:postgres@localhost:5436/prefect"
DATA_PATH = "./employees.csv"


@task
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


@task
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


@task
def get_data():
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


@task
def merge_data():
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


with Flow("airflow_tutorial_flow") as flow:
    flow.set_dependencies(
        task=get_data,
        upstream_tasks=[create_employees_table, create_employees_temp_table],
    )

    flow.set_dependencies(
        task=merge_data,
        upstream_tasks=[get_data]
    )


if __name__ == '__main__':
    flow.register(
        project_name="tutorial",

        # Only bump the version if the flow has changed, instead of upon every deployment
        idempotency_key=flow.serialized_hash()
    )
