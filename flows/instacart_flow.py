from prefect import flow, task
from src.ingest import main as ingest_main
from src.transform import main as transform_main


@task(name="Ingest Azure Data", retries=2, retry_delay_seconds=30)
def ingest_data() -> None:
    ingest_main()


@task(name="Build Warehouse and Analytics", retries=2, retry_delay_seconds=30)
def transform_data() -> None:
    transform_main()


@flow(name="instacart-azure-warehouse-pipeline")
def instacart_etl_flow() -> None:
    ingest_data()
    transform_data()


if __name__ == "__main__":
    instacart_etl_flow()