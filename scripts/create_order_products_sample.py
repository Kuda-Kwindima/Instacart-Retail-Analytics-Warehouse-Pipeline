import pandas as pd

# adjust path if needed
df = pd.read_csv("data/raw/order_products__prior.csv")

df_sample = df.head(20000)

df_sample.to_csv("data/raw/order_products_sample.csv", index=False)

print("Sample created:", df_sample.shape)
