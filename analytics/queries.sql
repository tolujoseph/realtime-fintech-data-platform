-- Transaction volume by merchant
SELECT
    merchant_id,
    count() AS transaction_count,
    sum(amount) AS total_volume,
    avg(amount) AS average_transaction_value
FROM fintech.fact_payment
GROUP BY merchant_id
ORDER BY total_volume DESC;


-- Daily transaction performance
SELECT
    date_key,
    count() AS transaction_count,
    sum(amount) AS total_volume,
    avg(amount) AS average_transaction_value
FROM fintech.fact_payment
GROUP BY date_key
ORDER BY date_key;


-- Customer transaction activity
SELECT
    customer_id,
    count() AS transaction_count,
    sum(amount) AS total_spend
FROM fintech.fact_payment
GROUP BY customer_id
ORDER BY total_spend DESC;