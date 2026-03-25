import os
import pandas as pd
from dotenv import load_dotenv
from src.storage.adls_client import ADLSClient

load_dotenv()

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container = os.getenv("AZURE_CONTAINER")

client = ADLSClient(conn_str, container)

df = pd.DataFrame(
    {
        "order_id": [1, 2, 3],
        "user_id": [10, 20, 30],
        "eval_set": ["prior", "train", "prior"],
        "order_number": [1, 2, 3],
        "order_dow": [0, 1, 2],
        "order_hour_of_day": [8, 12, 18],
        "days_since_prior_order": [0.0, 7.0, 14.0],
    }
)

client.write_csv(df, "processed/orders/orders_sample.csv")

print("Write successful!")
print(df)