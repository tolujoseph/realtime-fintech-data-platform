from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


BRONZE_PATH = "data/raw/bronze.jsonl"
SILVER_PATH = "data/processed/silver"
QUARANTINE_PATH = "data/processed/quarantine"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    # Define the expected Bronze schema
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    # Read raw Bronze data
    bronze_df = (
        spark.read
        .schema(schema)
        .json(BRONZE_PATH)
    )

    bronze_count = bronze_df.count()

    print("Bronze record count:", bronze_count)

    # ---------------------------------------------------------
    # DATA QUALITY RULES
    # ---------------------------------------------------------

    customer_rule = col("customer_id").isNull()

    merchant_rule = col("merchant_id").isNull()

    amount_rule = (
        col("amount").isNull()
        | (col("amount") <= 0)
    )

    currency_rule = col("currency") != "GBP"

    # ---------------------------------------------------------
    # BUILD REJECTION REASONS
    # ---------------------------------------------------------

    rejection_reason = concat_ws(
        "; ",
        when(
            customer_rule,
            lit("customer_id is null")
        ),
        when(
            merchant_rule,
            lit("merchant_id is null")
        ),
        when(
            amount_rule,
            lit("amount is null or <= 0")
        ),
        when(
            currency_rule,
            lit("invalid currency")
        ),
    )

    validated_df = bronze_df.withColumn(
        "rejection_reason",
        rejection_reason,
    )

    # ---------------------------------------------------------
    # QUARANTINE INVALID RECORDS
    # ---------------------------------------------------------

    quarantine_df = (
        validated_df
        .filter(col("rejection_reason") != "")
    )

    # ---------------------------------------------------------
    # SILVER VALID RECORDS
    # ---------------------------------------------------------

    silver_df = (
        validated_df
        .filter(col("rejection_reason") == "")
        .drop("rejection_reason")
        .dropDuplicates(["event_id"])
    )

    valid_count = silver_df.count()
    quarantine_count = quarantine_df.count()

    # ---------------------------------------------------------
    # DATA QUALITY SUMMARY
    # ---------------------------------------------------------

    print("\nData Quality Summary")
    print("--------------------")
    print("Bronze records:", bronze_count)
    print("Valid records:", valid_count)
    print("Quarantined records:", quarantine_count)

    print("\nRejection reasons:")

    (
        quarantine_df
        .groupBy("rejection_reason")
        .count()
        .orderBy(col("count").desc())
        .show(truncate=False)
    )

    # ---------------------------------------------------------
    # WRITE SILVER
    # ---------------------------------------------------------

    (
        silver_df
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    # ---------------------------------------------------------
    # WRITE QUARANTINE
    # ---------------------------------------------------------

    (
        quarantine_df
        .write
        .mode("overwrite")
        .parquet(QUARANTINE_PATH)
    )

    print(f"Silver data written to {SILVER_PATH}")
    print(f"Quarantine data written to {QUARANTINE_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()