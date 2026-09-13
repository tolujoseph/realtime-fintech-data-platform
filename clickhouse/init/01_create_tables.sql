CREATE DATABASE IF NOT EXISTS fintech;

CREATE TABLE IF NOT EXISTS fintech.dim_customer
(
    customer_key String,
    customer_id String
)
ENGINE = MergeTree
ORDER BY customer_key;


CREATE TABLE IF NOT EXISTS fintech.dim_merchant
(
    merchant_key String,
    merchant_id String
)
ENGINE = MergeTree
ORDER BY merchant_key;


CREATE TABLE IF NOT EXISTS fintech.dim_date
(
    date_key UInt32,
    date Date,
    year UInt16,
    month UInt8,
    day UInt8,
    day_of_week UInt8
)
ENGINE = MergeTree
ORDER BY date_key;


CREATE TABLE IF NOT EXISTS fintech.fact_payment
(
    event_id String,
    customer_id String,
    merchant_id String,
    date_key UInt32,
    amount Decimal(12, 2),
    currency LowCardinality(String),
    timestamp DateTime64(6, 'UTC')
)
ENGINE = MergeTree
ORDER BY (merchant_id, date_key);