import urllib.parse
import urllib.request


CLICKHOUSE_HOST = "fintech-clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "fintech"


def execute_query(query):
    params = urllib.parse.urlencode(
        {
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        }
    )

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{params}"

    request = urllib.request.Request(
        url,
        data=query.encode("utf-8"),
        method="POST",
    )

    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8")


def main():

    queries = [

        """
        TRUNCATE TABLE fintech.dim_customer
        """,

        """
        INSERT INTO fintech.dim_customer
        SELECT *
        FROM file(
            '/var/lib/clickhouse/user_files/gold/dim_customer/*.parquet',
            Parquet
        )
        """,

        """
        TRUNCATE TABLE fintech.dim_merchant
        """,

        """
        INSERT INTO fintech.dim_merchant
        SELECT *
        FROM file(
            '/var/lib/clickhouse/user_files/gold/dim_merchant/*.parquet',
            Parquet
        )
        """,

        """
        TRUNCATE TABLE fintech.dim_date
        """,

        """
        INSERT INTO fintech.dim_date
        SELECT *
        FROM file(
            '/var/lib/clickhouse/user_files/gold/dim_date/*.parquet',
            Parquet
        )
        """,

        """
        TRUNCATE TABLE fintech.fact_payment
        """,

        """
        INSERT INTO fintech.fact_payment
        SELECT
            event_id,
            customer_id,
            merchant_id,
            date_key,
            amount,
            currency,
            parseDateTime64BestEffort(timestamp, 6, 'UTC')
        FROM file(
            '/var/lib/clickhouse/user_files/gold/fact_payment/*.parquet',
            Parquet
        )
        """,
    ]

    for query in queries:
        execute_query(query)

    print("ClickHouse load completed successfully.")


if __name__ == "__main__":
    main()