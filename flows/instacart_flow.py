import os
from pathlib import Path
import subprocess

from dotenv import load_dotenv
from prefect import flow, task

load_dotenv()

PSQL_EXE = os.getenv("PSQL_EXE", "psql")
PYTHON_EXE = os.getenv("PYTHON_EXE", "python")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "instacart_dw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")


@task(name="run_python_script")
def run_python_script(script_path: str):
    print(f"Running Python script: {script_path}")
    subprocess.run(
        [PYTHON_EXE, script_path],
        check=True
    )


@task(name="run_sql_file")
def run_sql_file(sql_file_path: str):
    print(f"Running SQL file: {sql_file_path}")

    env = os.environ.copy()
    env["PGPASSWORD"] = DB_PASSWORD if DB_PASSWORD else ""

    subprocess.run(
        [
            PSQL_EXE,
            "-h",
            DB_HOST,
            "-p",
            DB_PORT,
            "-U",
            DB_USER,
            "-d",
            DB_NAME,
            "-P",
            "pager=off",
            "-f",
            sql_file_path
        ],
        check=True,
        env=env
    )


@flow(name="instacart-analytics-warehouse-pipeline")
def instacart_etl_flow():
    base_sql = Path("sql")

    # RAW
    run_sql_file(str(base_sql / "raw" / "create_raw_tables.sql"))
    run_python_script("src/load_raw.py")
    run_sql_file(str(base_sql / "raw" / "check_raw_counts.sql"))

    # STAGING
    run_sql_file(str(base_sql / "staging" / "stg_orders.sql"))
    run_sql_file(str(base_sql / "staging" / "stg_products.sql"))
    run_sql_file(str(base_sql / "staging" / "stg_order_items.sql"))

    # WAREHOUSE
    run_sql_file(str(base_sql / "warehouse" / "dim_aisles.sql"))
    run_sql_file(str(base_sql / "warehouse" / "dim_departments.sql"))
    run_sql_file(str(base_sql / "warehouse" / "dim_orders.sql"))
    run_sql_file(str(base_sql / "warehouse" / "dim_products.sql"))
    run_sql_file(str(base_sql / "warehouse" / "fact_order_items.sql"))

    # MARTS
    run_sql_file(str(base_sql / "marts" / "mart_product_reorders.sql"))
    run_sql_file(str(base_sql / "marts" / "mart_customer_orders.sql"))
    run_sql_file(str(base_sql / "marts" / "mart_department_trends.sql"))


if __name__ == "__main__":
    instacart_etl_flow()