from kafka import KafkaConsumer
import json
from pathlib import Path

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "payment-transactions"
CONSUMER_GROUP = "fintech-ingestion"

BRONZE_PATH = Path("data/raw/bronze.jsonl")


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=False,  # Commit only after successful Bronze write
    group_id=CONSUMER_GROUP,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


def load_processed_event_ids():
    """
    Load event IDs already written to Bronze.

    This gives us a simple idempotency check so that
    reprocessed Kafka messages don't create duplicates.
    """
    if not BRONZE_PATH.exists():
        return set()

    processed_ids = set()

    with BRONZE_PATH.open("r") as file:
        for line in file:
            event = json.loads(line)
            processed_ids.add(event["event_id"])

    return processed_ids


def write_to_bronze(event):
    """
    Append the raw event to the Bronze layer.
    """
    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

    with BRONZE_PATH.open("a") as file:
        file.write(json.dumps(event) + "\n")


processed_event_ids = load_processed_event_ids()

print("Starting payment event consumer...")

try:
    for message in consumer:
        event = message.value
        event_id = event["event_id"]

        # Idempotency check
        if event_id in processed_event_ids:
            print(f"Duplicate event skipped: {event_id}")

            # Already safely stored, so acknowledge the Kafka message
            consumer.commit()
            continue

        # Write to Bronze first
        write_to_bronze(event)
        processed_event_ids.add(event_id)

        # Only acknowledge Kafka after successful Bronze write
        consumer.commit()

        print(
            f"Bronze write successful | "
            f"partition={message.partition} "
            f"offset={message.offset} "
            f"event_id={event_id}"
        )

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()