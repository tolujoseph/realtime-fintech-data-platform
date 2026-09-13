from kafka import KafkaConsumer
import json


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "payment-transactions"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fintech-ingestion",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


print("Starting payment event consumer...")

try:
    for message in consumer:
        print(
            f"Consumed from partition {message.partition}, "
            f"offset {message.offset}: {message.value}"
        )

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()