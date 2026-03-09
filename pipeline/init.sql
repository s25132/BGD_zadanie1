CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.fact_transactions;
DROP TABLE IF EXISTS gold.dim_date;
DROP TABLE IF EXISTS gold.dim_merchant;
DROP TABLE IF EXISTS gold.dim_customer;
DROP TABLE IF EXISTS silver.transactions_clean;
DROP TABLE IF EXISTS raw.transactions_raw;

CREATE TABLE raw.transactions_raw (
    batch_no INT,
    transaction_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    merchant_id TEXT,
    transaction_ts TEXT,
    amount TEXT,
    city TEXT,
    country TEXT,
    payment_method TEXT,
    status TEXT
);

CREATE TABLE silver.transactions_clean (
    batch_no INT,
    transaction_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    merchant_id TEXT,
    transaction_ts TIMESTAMP,
    amount NUMERIC,
    city TEXT,
    country TEXT,
    payment_method TEXT,
    status TEXT,
    is_valid BOOLEAN,
    validation_error TEXT
);

CREATE TABLE gold.dim_customer (
    customer_id TEXT PRIMARY KEY,
    customer_name TEXT
);

CREATE TABLE gold.dim_merchant (
    merchant_id TEXT PRIMARY KEY,
    city TEXT,
    country TEXT
);

CREATE TABLE gold.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT
);

CREATE TABLE gold.fact_transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    merchant_id TEXT,
    date_id DATE,
    amount NUMERIC,
    payment_method TEXT,
    status TEXT
);

CREATE INDEX idx_raw_batch_no ON raw.transactions_raw(batch_no);
CREATE INDEX idx_silver_batch_no ON silver.transactions_clean(batch_no);
CREATE INDEX idx_silver_valid ON silver.transactions_clean(is_valid);