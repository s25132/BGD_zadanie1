import os
import pandas as pd
from sqlalchemy import text

def mark_file_as_loaded(engine, file_name: str):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO raw.ingestion_log (file_name)
                VALUES (:file_name)
                ON CONFLICT (file_name) DO NOTHING
            """),
            {"file_name": file_name}
        )


def get_max_raw_batch_no(engine):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COALESCE(MAX(batch_no), 0) FROM raw.transactions_raw")
        ).scalar()
    return int(result or 0)


def get_raw_batches(engine):
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT DISTINCT batch_no
                FROM raw.transactions_raw
                ORDER BY batch_no
            """),
            conn
        )
    return set(df["batch_no"].tolist())


def load_raw(engine, csv_file: str, chunk_size: int):
    """
    Loads new data into RAW.
    batch_no continues from the last batch in the database,
    so consecutive runs do not start from 1.
    """
    start_batch_no = get_max_raw_batch_no(engine)
    batch_no = start_batch_no

    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        batch_no += 1
        print(f"Loading RAW batch {batch_no}...")

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

    mark_file_as_loaded(engine, os.path.basename(csv_file))
    print("RAW complete")