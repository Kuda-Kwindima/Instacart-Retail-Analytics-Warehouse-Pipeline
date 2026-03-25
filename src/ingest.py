import os
from dotenv import load_dotenv
from src.storage.adls_client import ADLSClient

load_dotenv()


def get_client() -> ADLSClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container = os.getenv("AZURE_CONTAINER")

    if not conn_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING is not set")

    if not container:
        raise ValueError("AZURE_CONTAINER is not set")

    return ADLSClient(conn_str, container)


def ingest_products(client: ADLSClient) -> None:
    products = client.read_csv("raw/products/products.csv")
    products["product_name"] = products["product_name"].astype(str).str.strip()
    client.write_csv(products, "processed/products/products_clean.csv")
    print("Products processed:", products.shape)


def ingest_aisles(client: ADLSClient) -> None:
    aisles = client.read_csv("raw/aisles/aisles.csv")
    aisles["aisle"] = aisles["aisle"].astype(str).str.strip()
    client.write_csv(aisles, "processed/aisles/aisles_clean.csv")
    print("Aisles processed:", aisles.shape)


def ingest_departments(client: ADLSClient) -> None:
    departments = client.read_csv("raw/departments/departments.csv")
    departments["department"] = departments["department"].astype(str).str.strip()
    client.write_csv(departments, "processed/departments/departments_clean.csv")
    print("Departments processed:", departments.shape)


def main() -> None:
    client = get_client()

    ingest_products(client)
    ingest_aisles(client)
    ingest_departments(client)

    print("Ingestion pipeline completed successfully.")


if __name__ == "__main__":
    main()