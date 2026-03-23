from sqlalchemy import text


def build_gold(engine):
    """
    Builds GOLD incrementally:
    - dimensions are updated only with new records,
    - fact table loads only new transaction_id values,
    - creates a reporting view based on JOINs.
    """
    with engine.begin() as conn:
        print("Building gold.dim_customer...")
        conn.execute(text("""
            INSERT INTO gold.dim_customer (customer_id, customer_name)
            SELECT DISTINCT
                s.customer_id,
                s.customer_name
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.customer_id IS NOT NULL
              AND s.customer_id <> ''
            ON CONFLICT (customer_id) DO NOTHING
        """))

        print("Building gold.dim_merchant...")
        conn.execute(text("""
            INSERT INTO gold.dim_merchant (merchant_id, city, country)
            SELECT DISTINCT
                s.merchant_id,
                s.city,
                s.country
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.merchant_id IS NOT NULL
              AND s.merchant_id <> ''
            ON CONFLICT (merchant_id) DO NOTHING
        """))

        print("Building gold.dim_date...")
        conn.execute(text("""
            INSERT INTO gold.dim_date (date_id, year, month, day)
            SELECT DISTINCT
                s.transaction_ts::date AS date_id,
                EXTRACT(YEAR FROM s.transaction_ts)::int AS year,
                EXTRACT(MONTH FROM s.transaction_ts)::int AS month,
                EXTRACT(DAY FROM s.transaction_ts)::int AS day
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.transaction_ts IS NOT NULL
            ON CONFLICT (date_id) DO NOTHING
        """))

        print("Building gold.fact_transactions...")
        conn.execute(text("""
            INSERT INTO gold.fact_transactions (
                transaction_id,
                customer_id,
                merchant_id,
                date_id,
                amount,
                payment_method,
                status
            )
            SELECT DISTINCT
                s.transaction_id,
                s.customer_id,
                s.merchant_id,
                s.transaction_ts::date AS date_id,
                s.amount,
                s.payment_method,
                s.status
            FROM silver.transactions_clean s
            WHERE s.is_valid = true
              AND s.transaction_id IS NOT NULL
              AND s.transaction_id <> ''
            ON CONFLICT (transaction_id) DO NOTHING
        """))

        print("Creating view gold.v_transaction_report...")
        conn.execute(text("""
            CREATE OR REPLACE VIEW gold.v_transaction_report AS
            SELECT
                f.transaction_id,
                d.year,
                d.month,
                d.day,
                c.customer_name,
                m.city,
                m.country,
                f.amount,
                f.payment_method,
                f.status
            FROM gold.fact_transactions f
            JOIN gold.dim_customer c
                ON f.customer_id = c.customer_id
            JOIN gold.dim_merchant m
                ON f.merchant_id = m.merchant_id
            JOIN gold.dim_date d
                ON f.date_id = d.date_id;
        """))

    print("GOLD complete")