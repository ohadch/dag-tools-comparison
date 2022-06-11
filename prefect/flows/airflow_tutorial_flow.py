import os

import psycopg2
import requests
from prefect import task


@task
def create_employees_table():
    with psycopg2.connect(
        host="localhost",
        port="5436"
    ) as conn:
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
    with psycopg2.connect(
        host="localhost",
        port="5436"
    ) as conn:
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
    # NOTE: configure this as appropriate for your airflow environment
    data_path = "/opt/airflow/dags/files/employees.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)

    url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"

    response = requests.request("GET", url)

    with open(data_path, "w") as file:
        file.write(response.text)

    with psycopg2.connect(
        host="localhost",
        port="5436"
    ) as conn:
        with conn.cursor() as cur:
            with open(data_path, "r") as file:
                cur.copy_expert(
                    "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()


def merge_data():
    with psycopg2.connect(
        host="localhost",
        port="5436"
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO employees
            SELECT *
            FROM (
                SELECT DISTINCT *
                FROM employees_temp
            )
            ON CONFLICT ("Serial Number") DO UPDATE
            SET "Serial Number" = excluded."Serial Number";
            """)
            conn.commit()
