import os
from dotenv import load_dotenv
from src.storage.adls_client import ADLSClient

load_dotenv()

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container = os.getenv("AZURE_CONTAINER")

client = ADLSClient(conn_str, container)
df = client.read_csv("raw/orders/orders.csv")

print(df.head())
print(df.shape)