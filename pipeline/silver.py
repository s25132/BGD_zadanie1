import pandas as pd
from sqlalchemy import text
from raw import get_raw_batches


def get_silver_batches(engine):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT DISTINCT batch_no
                FROM silver.transactions_clean
                ORDER BY batch_no
            """),
            conn
        )
    return set(df["batch_no"].tolist())


def build_silver(engine):
    """
    Builds SILVER only for new RAW batches
    that have not yet been processed.
    """
    raw_batches = get_raw_batches(engine)
    silver_batches = get_silver_batches(engine)

    batches_to_process = sorted(raw_batches - silver_batches)

    if not batches_to_process:
        print("No new batches to process in SILVER")
        return

    for batch_no in batches_to_process:
        print(f"Building SILVER for batch {batch_no}...")

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

        # Text normalization
        silver["transaction_id"] = silver["transaction_id"].astype(str).str.strip()
        silver["customer_id"] = silver["customer_id"].astype(str).str.strip()
        silver["customer_name"] = silver["customer_name"].astype(str).str.strip().str.title()
        silver["merchant_id"] = silver["merchant_id"].astype(str).str.strip()
        silver["city"] = silver["city"].astype(str).str.strip().str.title()
        silver["country"] = silver["country"].astype(str).str.strip().str.upper()
        silver["payment_method"] = silver["payment_method"].astype(str).str.strip().str.lower()
        silver["status"] = silver["status"].astype(str).str.strip().str.lower()

        # Dates
        silver["transaction_ts"] = pd.to_datetime(
            silver["transaction_ts"],
            errors="coerce"
        )

        # Amount
        silver["amount"] = pd.to_numeric(
            silver["amount"].astype(str).str.replace(",", ".", regex=False).str.strip(),
            errors="coerce"
        )

        # Validation
        silver["validation_error"] = ""

        silver.loc[
            silver["transaction_id"].isin(["", "nan", "None", "none", "NaN"]),
            "validation_error"
        ] += "missing transaction_id; "

        silver.loc[
            silver["transaction_ts"].isna(),
            "validation_error"
        ] += "bad date; "

        silver.loc[
            silver["amount"].isna(),
            "validation_error"
        ] += "bad amount; "

        silver.loc[
            silver["amount"] < 0,
            "validation_error"
        ] += "negative amount; "

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

    print("SILVER complete")