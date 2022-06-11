import psycopg2
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


#
#     create_employees_temp_table = PostgresOperator(
#         task_id="create_employees_temp_table",
#         postgres_conn_id="tutorial_pg_conn",
#         sql="""
#             DROP TABLE IF EXISTS employees_temp;
#             CREATE TABLE employees_temp (
#                 "Serial Number" NUMERIC PRIMARY KEY,
#                 "Company Name" TEXT,
#                 "Employee Markme" TEXT,
#                 "Description" TEXT,
#                 "Leave" INTEGER
#             );""",
#     )
#
#     @task
#     def get_data():
#         # NOTE: configure this as appropriate for your airflow environment
#         data_path = "/opt/airflow/dags/files/employees.csv"
#         os.makedirs(os.path.dirname(data_path), exist_ok=True)
#
#         url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"
#
#         response = requests.request("GET", url)
#
#         with open(data_path, "w") as file:
#             file.write(response.text)
#
#         postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
#         conn = postgres_hook.get_conn()
#         cur = conn.cursor()
#         with open(data_path, "r") as file:
#             cur.copy_expert(
#                 "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
#                 file,
#             )
#         conn.commit()
#
#     @task
#     def merge_data():
#         query = """
#             INSERT INTO employees
#             SELECT *
#             FROM (
#                 SELECT DISTINCT *
#                 FROM employees_temp
#             )
#             ON CONFLICT ("Serial Number") DO UPDATE
#             SET "Serial Number" = excluded."Serial Number";
#         """
#         try:
#             postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
#             conn = postgres_hook.get_conn()
#             cur = conn.cursor()
#             cur.execute(query)
#             conn.commit()
#             return 0
#         except Exception as e:
#             return 1
#
#     [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()
