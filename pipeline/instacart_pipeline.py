from pathlib import Path
import subprocess

from prefect import flow, task


PYTHON_EXE = r"C:\Users\akwin\anaconda3\envs\Ds_explore\python.exe"
DB_NAME = "instacart_dw"
DB_USER = "postgres"


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
    subprocess.run(
        [
            "psql",
            "-U",
            DB_USER,
            "-d",
            DB_NAME,
            "-f",
            sql_file_path,
        ],
        check=True,
        shell=True,
    )


@flow(name="instacart-analytics-warehouse-pipeline")
def instacart_pipeline():
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
    instacart_pipeline()