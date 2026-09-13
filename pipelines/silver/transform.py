from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


BRONZE_PATH = "data/raw/bronze.jsonl"
SILVER_PATH = "data/processed/silver"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    bronze_df = (
        spark.read
        .schema(schema)
        .json(BRONZE_PATH)
    )

    print("Bronze schema:")
    bronze_df.printSchema()

    print("Bronze record count:", bronze_df.count())

    silver_df = (
        bronze_df
        .dropDuplicates(["event_id"])
        .filter(col("customer_id").isNotNull())
        .filter(col("merchant_id").isNotNull())
        .filter(col("amount") > 0)
        .filter(col("currency") == "GBP")
    )

    print("Silver record count:", silver_df.count())

    (
        silver_df
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )

    print(f"Silver data written to {SILVER_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()