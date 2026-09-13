from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    month,
    to_date,
    year,
)


SILVER_PATH = "data/processed/silver"
GOLD_PATH = "data/processed/gold"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverToGold")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    # Read cleaned Silver data
    silver_df = spark.read.parquet(SILVER_PATH)

    print("Silver record count:", silver_df.count())

    # ---------------------------------------------------------
    # DIM_CUSTOMER
    # ---------------------------------------------------------

    dim_customer = (
        silver_df
        .select("customer_id")
        .dropDuplicates()
        .withColumn(
            "customer_key",
            # Temporary surrogate key for the local project
            # We will improve this later.
            col("customer_id")
        )
        .select(
            "customer_key",
            "customer_id",
        )
    )

    # ---------------------------------------------------------
    # DIM_MERCHANT
    # ---------------------------------------------------------

    dim_merchant = (
        silver_df
        .select("merchant_id")
        .dropDuplicates()
        .withColumn(
            "merchant_key",
            col("merchant_id")
        )
        .select(
            "merchant_key",
            "merchant_id",
        )
    )

    # ---------------------------------------------------------
    # DIM_DATE
    # ---------------------------------------------------------

    payment_dates = (
        silver_df
        .select(
            to_date("timestamp").alias("date")
        )
        .dropDuplicates()
    )

    dim_date = (
        payment_dates
        .withColumn("date_key", date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", year("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("day_of_week", dayofweek("date"))
        .select(
            "date_key",
            "date",
            "year",
            "month",
            "day",
            "day_of_week",
        )
    )

    # ---------------------------------------------------------
    # FACT_PAYMENT
    # ---------------------------------------------------------

    fact_payment = (
        silver_df
        .withColumn(
            "date_key",
            date_format(
                to_date("timestamp"),
                "yyyyMMdd"
            ).cast("int")
        )
        .select(
            "event_id",
            "customer_id",
            "merchant_id",
            "date_key",
            "amount",
            "currency",
            "timestamp",
        )
    )

    # ---------------------------------------------------------
    # Write Gold tables
    # ---------------------------------------------------------

    dim_customer.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_customer"
    )

    dim_merchant.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_merchant"
    )

    dim_date.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/dim_date"
    )

    fact_payment.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/fact_payment"
    )

    # ---------------------------------------------------------
    # Print summary
    # ---------------------------------------------------------

    print("\nGold Layer Summary")
    print("------------------")
    print("Customers:", dim_customer.count())
    print("Merchants:", dim_merchant.count())
    print("Dates:", dim_date.count())
    print("Payments:", fact_payment.count())

    print("\nFACT_PAYMENT")
    fact_payment.show(10, truncate=False)

    print("\nDIM_MERCHANT")
    dim_merchant.show(10, truncate=False)

    print("\nDIM_CUSTOMER")
    dim_customer.show(10, truncate=False)

    print("\nDIM_DATE")
    dim_date.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()