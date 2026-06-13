from confluent_kafka import Consumer
from dotenv import load_dotenv
from datetime import datetime, timezone
import boto3
import json
import os
import uuid

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions.raw")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "bronze-minio-consumer")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "bronze")


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def ensure_bucket_exists(s3_client):
    response = s3_client.list_buckets()
    bucket_names = [bucket["Name"] for bucket in response["Buckets"]]

    if MINIO_BUCKET not in bucket_names:
        s3_client.create_bucket(Bucket=MINIO_BUCKET)
        print(f"Created MinIO bucket: {MINIO_BUCKET}")
    else:
        print(f"MinIO bucket already exists: {MINIO_BUCKET}")


def get_partitioned_object_key(event):
    event_time_raw = event.get("event_time")

    try:
        event_time = datetime.fromisoformat(event_time_raw.replace("Z", "+00:00"))
    except Exception:
        event_time = datetime.now(timezone.utc)

    return (
        f"transactions/raw/"
        f"year={event_time.year}/"
        f"month={event_time.month:02d}/"
        f"day={event_time.day:02d}/"
        f"hour={event_time.hour:02d}/"
        f"transaction_{event.get('transaction_id', uuid.uuid4())}.json"
    )


def upload_event_to_minio(s3_client, event):
    object_key = get_partitioned_object_key(event)
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")

    s3_client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=body,
        ContentType="application/json",
    )

    print(f"Uploaded: s3://{MINIO_BUCKET}/{object_key}")


def create_consumer():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def main():
    print("Starting Redpanda to MinIO Bronze consumer...")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Consumer group: {KAFKA_CONSUMER_GROUP}")
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"MinIO bucket: {MINIO_BUCKET}")
    print("Press CTRL+C to stop.")

    s3_client = create_s3_client()
    ensure_bucket_exists(s3_client)

    consumer = create_consumer()

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            raw_value = message.value().decode("utf-8")

            try:
                event = json.loads(raw_value)
            except json.JSONDecodeError as error:
                print(f"Invalid JSON message skipped: {error}")
                continue

            upload_event_to_minio(s3_client, event)

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()
