#!/usr/bin/env python3
"""
Transactional outbox publisher.

Continuously reads unreplayed rows from banking_source.stream_events,
publishes them to Redpanda, and marks them as replayed only after Kafka
delivery confirmation.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import time
from pathlib import Path
from typing import Any

import psycopg2
from confluent_kafka import Producer
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


LOG = logging.getLogger("realtime-outbox-publisher")
STOP_REQUESTED = False


def handle_signal(signum: int, frame: Any) -> None:
    del frame
    global STOP_REQUESTED
    LOG.info("Received signal %s; stopping.", signum)
    STOP_REQUESTED = True


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "fintech"),
        user=os.getenv("POSTGRES_USER", "fintech_user"),
        password=required_env("POSTGRES_PASSWORD"),
        connect_timeout=10,
        application_name="realtime_outbox_publisher",
    )


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "realtime-banking-outbox",
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            "linger.ms": 5,
            "compression.type": "snappy",
        }
    )


def fetch_pending(conn, limit: int) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                event_id,
                event_time,
                event_type,
                source_system,
                entity_type,
                entity_id,
                payload_json
            FROM banking_source.stream_events
            WHERE replayed_flag = FALSE
              AND source_system = 'core_banking_realtime'
            ORDER BY event_time, event_id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (limit,),
        )
        return list(cur.fetchall())


def publish_one(
    producer: Producer,
    topic: str,
    row: dict[str, Any],
    delivery_timeout: float,
) -> None:
    delivered: dict[str, Any] = {"done": False, "error": None}

    payload = row["payload_json"]
    if isinstance(payload, str):
        payload = json.loads(payload)

    envelope = {
        "event_id": row["event_id"],
        "event_time": row["event_time"].isoformat(),
        "event_type": row["event_type"],
        "source_system": row["source_system"],
        "entity": row["entity_type"],
        "entity_id": row["entity_id"],
        "payload": payload,
    }

    def callback(err, msg):
        delivered["done"] = True
        delivered["error"] = err
        delivered["partition"] = msg.partition() if msg else None
        delivered["offset"] = msg.offset() if msg else None

    producer.produce(
        topic=topic,
        key=row["entity_id"].encode("utf-8"),
        value=json.dumps(envelope, separators=(",", ":"), default=str).encode("utf-8"),
        on_delivery=callback,
    )

    deadline = time.monotonic() + delivery_timeout
    while not delivered["done"] and time.monotonic() < deadline:
        producer.poll(0.1)

    if not delivered["done"]:
        raise TimeoutError(
            f"Delivery confirmation timeout for event_id={row['event_id']}"
        )
    if delivered["error"] is not None:
        raise RuntimeError(
            f"Kafka delivery failed for event_id={row['event_id']}: "
            f"{delivered['error']}"
        )

    LOG.info(
        "[PUBLISHED] event_id=%s type=%s partition=%s offset=%s",
        row["event_id"],
        row["event_type"],
        delivered["partition"],
        delivered["offset"],
    )


def mark_replayed(conn, event_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE banking_source.stream_events
            SET replayed_flag = TRUE,
                replayed_at = NOW()
            WHERE event_id = %s
              AND replayed_flag = FALSE
            """,
            (event_id,),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"Expected one updated outbox row for event_id={event_id}, "
                f"updated={cur.rowcount}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously publish PostgreSQL outbox events to Redpanda."
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("REDPANDA_TOPIC", "banking.operational.events"),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--fetch-size", type=int, default=20)
    parser.add_argument("--delivery-timeout", type=float, default=30.0)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(Path(args.env_file), override=False)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    conn = connect()
    conn.autocommit = False
    producer = create_producer(args.bootstrap_servers)
    published = 0

    LOG.info(
        "Outbox publisher started. topic=%s bootstrap=%s",
        args.topic,
        args.bootstrap_servers,
    )

    try:
        while not STOP_REQUESTED:
            try:
                rows = fetch_pending(conn, args.fetch_size)
                if not rows:
                    conn.rollback()
                    producer.poll(0)
                    time.sleep(args.poll_interval)
                    continue

                for row in rows:
                    if STOP_REQUESTED:
                        conn.rollback()
                        break

                    publish_one(
                        producer=producer,
                        topic=args.topic,
                        row=row,
                        delivery_timeout=args.delivery_timeout,
                    )
                    mark_replayed(conn, row["event_id"])
                    conn.commit()
                    published += 1

            except (psycopg2.Error, RuntimeError, TimeoutError):
                conn.rollback()
                LOG.exception("Outbox publishing cycle failed; retrying.")
                producer.poll(0)
                time.sleep(2)

                if conn.closed:
                    conn = connect()
                    conn.autocommit = False
    finally:
        try:
            producer.flush(10)
        finally:
            conn.close()
        LOG.info("Outbox publisher stopped. Published=%d", published)


if __name__ == "__main__":
    main()
