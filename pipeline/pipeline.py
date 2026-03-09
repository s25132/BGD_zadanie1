import pandas as pd
from sqlalchemy import create_engine, text
import time
from sqlalchemy.exc import OperationalError
import os

CHUNK_SIZE = os.getenv("CHUNK_SIZE", "100000")
CSV_FILE = os.getenv("DATA_FILE", "transactions.csv")
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/medallion")

engine = None
for i in range(30):  # próbuje przez ~30 sekund
    try:
        engine = create_engine(DB_URL)
        conn = engine.connect()
        conn.close()
        print("Połączono z bazą danych")
        break
    except OperationalError:
        print("Czekam na bazę danych...")
        time.sleep(1)

if engine is None:
    raise Exception("Nie udało się połączyć z bazą danych")

def load_raw():
    batch_no = 0

    for chunk in pd.read_csv(CSV_FILE, chunksize=CHUNK_SIZE):
        batch_no += 1
        print(f"Ładowanie do RAW batch {batch_no}...")

        raw_chunk = chunk.copy()
        raw_chunk["batch_no"] = batch_no

        raw_chunk.to_sql(
            "transactions_raw",
            engine,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

    print("RAW gotowe")


def build_silver():
    with engine.connect() as conn:
        batch_df = pd.read_sql(
            text("SELECT DISTINCT batch_no FROM raw.transactions_raw ORDER BY batch_no"),
            conn
        )

    for batch_no in batch_df["batch_no"]:
        print(f"Buduję SILVER dla batch {batch_no}...")

        with engine.connect() as conn:
            chunk = pd.read_sql(
                text("""
                    SELECT
                        batch_no,
                        transaction_id,
                        customer_id,
                        customer_name,
                        merchant_id,
                        transaction_ts,
                        amount,
                        city,
                        country,
                        payment_method,
                        status
                    FROM raw.transactions_raw
                    WHERE batch_no = :batch_no
                """),
                conn,
                params={"batch_no": int(batch_no)}
            )

        silver = chunk.copy()

        silver["transaction_id"] = silver["transaction_id"].astype(str).str.strip()
        silver["customer_id"] = silver["customer_id"].astype(str).str.strip()
        silver["customer_name"] = silver["customer_name"].astype(str).str.title().str.strip()
        silver["merchant_id"] = silver["merchant_id"].astype(str).str.strip()
        silver["city"] = silver["city"].astype(str).str.title()
        silver["country"] = silver["country"].astype(str).str.upper()
        silver["payment_method"] = silver["payment_method"].astype(str).str.lower()
        silver["status"] = silver["status"].astype(str).str.lower()

        silver["transaction_ts"] = pd.to_datetime(
            silver["transaction_ts"],
            errors="coerce"
        )

        silver["amount"] = pd.to_numeric(
            silver["amount"].astype(str).str.replace(",", ".", regex=False),
            errors="coerce"
        )

        silver["validation_error"] = ""
        silver.loc[
            silver["transaction_id"].isin(["", "nan", "None"]),
            "validation_error"
        ] += "missing transaction_id; "
        silver.loc[silver["transaction_ts"].isna(), "validation_error"] += "bad date; "
        silver.loc[silver["amount"].isna(), "validation_error"] += "bad amount; "
        silver.loc[silver["amount"] < 0, "validation_error"] += "negative amount; "

        silver["is_valid"] = silver["validation_error"] == ""

        silver = silver[
            [
                "batch_no",
                "transaction_id",
                "customer_id",
                "customer_name",
                "merchant_id",
                "transaction_ts",
                "amount",
                "city",
                "country",
                "payment_method",
                "status",
                "is_valid",
                "validation_error"
            ]
        ]

        silver.to_sql(
            "transactions_clean",
            engine,
            schema="silver",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000
        )

    print("SILVER gotowe")


def build_gold():
    with engine.begin() as conn:
        print("Buduję gold.dim_customer...")
        conn.execute(text("""
            INSERT INTO gold.dim_customer (customer_id, customer_name)
            SELECT DISTINCT customer_id, customer_name
            FROM silver.transactions_clean
            WHERE is_valid = true
              AND customer_id IS NOT NULL
              AND customer_id <> ''
            ON CONFLICT (customer_id) DO NOTHING
        """))

        print("Buduję gold.dim_merchant...")
        conn.execute(text("""
            INSERT INTO gold.dim_merchant (merchant_id, city, country)
            SELECT DISTINCT merchant_id, city, country
            FROM silver.transactions_clean
            WHERE is_valid = true
              AND merchant_id IS NOT NULL
              AND merchant_id <> ''
            ON CONFLICT (merchant_id) DO NOTHING
        """))

        print("Buduję gold.dim_date...")
        conn.execute(text("""
            INSERT INTO gold.dim_date (date_id, year, month, day)
            SELECT DISTINCT
                transaction_ts::date AS date_id,
                EXTRACT(YEAR FROM transaction_ts)::int,
                EXTRACT(MONTH FROM transaction_ts)::int,
                EXTRACT(DAY FROM transaction_ts)::int
            FROM silver.transactions_clean
            WHERE is_valid = true
              AND transaction_ts IS NOT NULL
            ON CONFLICT (date_id) DO NOTHING
        """))

        print("Buduję gold.fact_transactions...")
        conn.execute(text("""
            INSERT INTO gold.fact_transactions (
                transaction_id, customer_id, merchant_id,
                date_id, amount, payment_method, status
            )
            SELECT DISTINCT
                transaction_id,
                customer_id,
                merchant_id,
                transaction_ts::date,
                amount,
                payment_method,
                status
            FROM silver.transactions_clean
            WHERE is_valid = true
              AND transaction_id IS NOT NULL
              AND transaction_id <> ''
            ON CONFLICT (transaction_id) DO NOTHING
        """))

    print("GOLD gotowe")


if __name__ == "__main__":
    load_raw()
    build_silver()
    build_gold()
    print("Gotowe")