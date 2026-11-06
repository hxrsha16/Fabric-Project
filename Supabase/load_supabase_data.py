import pandas as pd
from sqlalchemy import create_engine
import sys

# 1. PASTE YOUR CONNECTION STRING HERE:
DB_URI = "postgresql+psycopg2://postgres.bimartklrhorzctmhubv:My_Fabric123*@aws-0-ap-northeast-1.pooler.supabase.com:5432/postgres"

# Official Olist GitHub raw data URLs
BASE_URL = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets"
DATASETS = [
    ("product_category_name_translation", f"{BASE_URL}/product_category_name_translation.csv"),
    ("customers", f"{BASE_URL}/olist_customers_dataset.csv"),
    ("sellers", f"{BASE_URL}/olist_sellers_dataset.csv"),
    ("products", f"{BASE_URL}/olist_products_dataset.csv"),
    ("orders", f"{BASE_URL}/olist_orders_dataset.csv"),
    ("order_items", f"{BASE_URL}/olist_order_items_dataset.csv"),
    ("order_payments", f"{BASE_URL}/olist_order_payments_dataset.csv"),
    ("order_reviews", f"{BASE_URL}/olist_order_reviews_dataset.csv"),
    ("geolocation", f"{BASE_URL}/olist_geolocation_dataset.csv"),
]

def main():
    print("Connecting to Supabase PostgreSQL...")
    engine = create_engine(DB_URI)

    for table_name, url in DATASETS:
        print(f"\n---> Fetching {table_name} from GitHub...")
        df = pd.read_csv(url)

        # For geolocation, deduplicate by zip code to keep it light and fast (~19k rows vs 1M)
        if table_name == "geolocation":
            df = df.drop_duplicates(subset=["geolocation_zip_code_prefix"])

        print(f"Loading {len(df):,} rows into '{table_name}' table...")
        # if_exists='append' populates the tables we created with our DDL
        df.to_sql(
            table_name,
            engine,
            schema="public",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi"
        )
        print(f"✓ {table_name} loaded successfully!")

    print("\n All 9 tables loaded successfully into Supabase!")

if __name__ == "__main__":
    main()
