from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime, timezone
import hashlib
import json
import os
import random
import time
import uuid

load_dotenv()

fake = Faker("pl_PL")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions.raw")
SLEEP_TIME = float(os.getenv("EVENT_SLEEP_SECONDS", "1"))

CUSTOMER_COUNT = 1000
ACCOUNT_COUNT = 1500
MERCHANT_COUNT = 250

TRANSACTION_TYPES = [
    "card_payment",
    "blik",
    "bank_transfer",
    "atm_withdrawal",
    "online_payment",
]

CHANNELS = [
    "mobile",
    "web",
    "pos",
    "atm",
]

STATUSES = [
    "completed",
    "pending",
    "rejected",
]

MERCHANT_CATEGORIES = [
    "grocery",
    "fuel",
    "electronics",
    "restaurant",
    "travel",
    "online",
    "atm",
    "subscription",
    "entertainment",
    "healthcare",
    "insurance",
    "gaming",
    "education",
]


def get_current_timestamp():
    return datetime.now(timezone.utc).isoformat()


def generate_event_id(payload):
    payload_as_string = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload_as_string.encode("utf-8")).hexdigest()


def generate_fraud_flag():
    return random.choices(
        [0, 1],
        weights=[985, 15],
        k=1,
    )[0]


def generate_amount(is_fraud):
    if is_fraud:
        return round(random.uniform(2000, 25000), 2)

    return round(random.uniform(5, 5000), 2)


def generate_status(is_fraud):
    if is_fraud:
        return random.choices(
            ["completed", "rejected"],
            weights=[70, 30],
            k=1,
        )[0]

    return random.choices(
        STATUSES,
        weights=[94, 3, 3],
        k=1,
    )[0]


def generate_risk_score(is_fraud):
    if is_fraud:
        return random.randint(70, 100)

    return random.randint(1, 69)


def generate_transaction_event():
    is_fraud = generate_fraud_flag()

    payload = {
        "event_time": get_current_timestamp(),
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.randint(1, CUSTOMER_COUNT),
        "account_id": random.randint(1, ACCOUNT_COUNT),
        "merchant_id": random.randint(1, MERCHANT_COUNT),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "channel": random.choice(CHANNELS),
        "amount": generate_amount(is_fraud),
        "currency": "PLN",
        "status": generate_status(is_fraud),
        "is_fraud": is_fraud,
        "risk_score": generate_risk_score(is_fraud),
        "device_id": f"device_{random.randint(1, 5000)}",
        "ip_country": random.choices(
            ["PL", "DE", "CZ", "NL", "UA", "US"],
            weights=[90, 3, 2, 2, 2, 1],
            k=1,
        )[0],
        "city": fake.city(),
        "source_system": "ubuntu_onprem_transaction_simulator",
    }

    payload["event_id"] = generate_event_id(payload)

    return payload


def delivery_report(error, message):
    if error is not None:
        print(f"Delivery failed: {error}")
    else:
        print(
            f"Delivered event | "
            f"topic={message.topic()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "fintech-transaction-producer",
        }
    )

    print("Starting real-time transaction producer...")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Sleep time: {SLEEP_TIME} seconds")
    print("Press CTRL+C to stop.")

    try:
        while True:
            event = generate_transaction_event()
            event_json = json.dumps(event, ensure_ascii=False)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["transaction_id"],
                value=event_json,
                callback=delivery_report,
            )

            producer.poll(0)

            print(json.dumps(event, indent=4, ensure_ascii=False))

            time.sleep(SLEEP_TIME)

    except KeyboardInterrupt:
        print("Producer stopped by user.")

    finally:
        producer.flush()
        print("Producer closed.")


if __name__ == "__main__":
    main()
