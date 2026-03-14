import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

DATA_PATH = Path("data/raw")


def truncate_raw_tables():
    tables = [
        "raw_order_products_train",
        "raw_order_products_prior",
        "raw_departments",
        "raw_aisles",
        "raw_products",
        "raw_orders",
    ]

    with engine.begin() as conn:
        for table in tables:
            conn.execute(text(f"TRUNCATE TABLE raw.{table};"))
            print(f"Truncated raw.{table}")


def load_csv_to_table(file_name, table_name):
    file_path = DATA_PATH / file_name

    print(f"Loading {file_name}...")

    df = pd.read_csv(file_path)

    df.to_sql(
        table_name,
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        chunksize=50000
    )

    print(f"Finished loading {table_name}")


if __name__ == "__main__":
    truncate_raw_tables()

    load_csv_to_table("orders.csv", "raw_orders")
    load_csv_to_table("products.csv", "raw_products")
    load_csv_to_table("aisles.csv", "raw_aisles")
    load_csv_to_table("departments.csv", "raw_departments")
    load_csv_to_table("order_products__prior.csv", "raw_order_products_prior")
    load_csv_to_table("order_products__train.csv", "raw_order_products_train")