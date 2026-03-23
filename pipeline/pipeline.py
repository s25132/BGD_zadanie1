import os
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from raw import load_raw
from silver import build_silver
from gold import build_gold

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
CSV_FILE = os.getenv("DATA_FILE", "transactions.csv")
DB_URL = os.getenv(
    "DB_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/medallion"
)

print(
    f"CSV file in use: {CSV_FILE}"
    f"\nChunk size: {CHUNK_SIZE}"
    f"\nDatabase URL: {DB_URL}"
)


def get_engine():
    engine = None
    for _ in range(30):
        try:
            engine = create_engine(DB_URL)
            conn = engine.connect()
            conn.close()
            print("Connected to the database")
            break
        except OperationalError:
            print("Waiting for the database...")
            time.sleep(1)

    if engine is None:
        raise Exception("Could not connect to the database")

    return engine

def is_file_already_loaded(engine, file_name: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 1
                FROM raw.ingestion_log
                WHERE file_name = :file_name
                LIMIT 1
            """),
            {"file_name": file_name}
        ).fetchone()

    print(f"Checking whether file {file_name} has already been loaded: {'YES' if result else 'NO'}")
    return result is not None


if __name__ == "__main__":
    engine = get_engine()
    file_name = os.path.basename(CSV_FILE)

    if is_file_already_loaded(engine, file_name):
        print(f"File {file_name} has already been loaded — skipping the ENTIRE pipeline")
    else:
        load_raw(engine, CSV_FILE, CHUNK_SIZE)
        build_silver(engine)
        build_gold(engine)
        print("Done")