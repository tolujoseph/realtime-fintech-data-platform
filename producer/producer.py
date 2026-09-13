import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "payment-transactions"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


MERCHANTS = ["merchant_001", "merchant_002", "merchant_003", "merchant_004"]
CUSTOMERS = ["customer_001", "customer_002", "customer_003", "customer_004"]


def generate_payment_event():
    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "merchant_id": random.choice(MERCHANTS),
        "amount": round(random.uniform(5, 500), 2),
        "currency": "GBP",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print("Starting payment event producer...")

    try:
        while True:
            event = generate_payment_event()

            producer.send(
                TOPIC,
                key=event["event_id"].encode("utf-8"),
                value=event,
            )

            producer.flush()

            print(f"Produced: {event}")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.close()


if __name__ == "__main__":
    main()