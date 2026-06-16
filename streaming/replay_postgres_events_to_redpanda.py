#!/usr/bin/env python3

import argparse
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor


RUNNING = True


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def signal_handler(signum, frame) -> None:
    global RUNNING
    RUNNING = False
    print("\nOtrzymano sygnał zatrzymania. Kończę bieżącą paczkę...")


def load_configuration() -> None:
    project_root = Path(__file__).resolve().parent.parent
    env_path = project_root / ".env"

    if not env_path.exists():
        raise FileNotFoundError(f"Nie znaleziono pliku .env: {env_path}")

    load_dotenv(dotenv_path=env_path)


def connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "fintech"),
        user=os.getenv("POSTGRES_USER", "fintech_user"),
        password=os.getenv("POSTGRES_PASSWORD", "fintech_pass"),
        connect_timeout=10,
        application_name="postgres_to_redpanda_replay",
    )


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "postgres-banking-replay",
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 10,
            "batch.num.messages": 10000,
            "delivery.timeout.ms": 120000,
        }
    )


def normalize_payload(payload: Any) -> dict:
    if isinstance(payload, dict):
        return dict(payload)

    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
            if isinstance(decoded, dict):
                return decoded
            return {"value": decoded}
        except json.JSONDecodeError:
            return {"raw_payload": payload}

    return {"value": payload}


def build_message(row: dict, fresh_timestamp: bool) -> tuple[dict, str]:
    original_event_time = row["event_time"]

    if isinstance(original_event_time, datetime):
        original_event_time_iso = original_event_time.astimezone(
            timezone.utc
        ).isoformat()
    else:
        original_event_time_iso = str(original_event_time)

    replayed_at = utc_now()
    output_event_time = (
        replayed_at.isoformat()
        if fresh_timestamp
        else original_event_time_iso
    )

    payload = normalize_payload(row["payload_json"])

    payload_original_time = payload.get("event_time")
    payload["event_time"] = output_event_time
    payload["original_event_time"] = (
        payload_original_time or original_event_time_iso
    )

    message = {
        "event_id": row["event_id"],
        "event_time": output_event_time,
        "original_event_time": original_event_time_iso,
        "event_type": row["event_type"],
        "source_system": row["source_system"],
        "entity_type": row["entity_type"],
        "entity_id": row["entity_id"],
        "payload": payload,
        "replay_metadata": {
            "replayed_at": replayed_at.isoformat(),
            "replay_source": "postgresql_banking_source",
            "timestamp_strategy": (
                "fresh_on_send"
                if fresh_timestamp
                else "preserve_original"
            ),
            "target_system": "redpanda",
        },
    }

    partition_key = str(
        payload.get("customer_id")
        or payload.get("account_id")
        or row["entity_id"]
        or row["event_id"]
    )

    return message, partition_key


def mark_replayed(
    connection,
    event_ids: list[str],
    replayed_at: datetime,
) -> None:
    if not event_ids:
        return

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE banking_source.stream_events
            SET
                replayed_flag = TRUE,
                replayed_at = %s
            WHERE event_id = ANY(%s)
              AND replayed_flag = FALSE
            """,
            (replayed_at, event_ids),
        )

    connection.commit()


def fetch_event_count(connection, include_replayed: bool) -> int:
    query = """
        SELECT COUNT(*)
        FROM banking_source.stream_events
    """

    if not include_replayed:
        query += " WHERE replayed_flag = FALSE"

    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchone()[0]


def delivery_callback_factory(
    event_id: str,
    delivered: list[str],
    failures: list[tuple[str, str]],
):
    def delivery_callback(error, message) -> None:
        if error is not None:
            failures.append((event_id, str(error)))
        else:
            delivered.append(event_id)

    return delivery_callback


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay PostgreSQL stream_events to Redpanda."
    )

    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:19092",
        ),
    )
    parser.add_argument(
        "--topic",
        default="banking.operational.events",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maksymalna liczba eventów. 0 oznacza wszystkie.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Opóźnienie pomiędzy wiadomościami.",
    )
    parser.add_argument(
        "--fresh-timestamps",
        action="store_true",
        help="Ustaw event_time na czas wysyłki.",
    )
    parser.add_argument(
        "--include-replayed",
        action="store_true",
        help="Uwzględnij również wcześniej wysłane eventy.",
    )
    parser.add_argument(
        "--mark-replayed",
        action="store_true",
        help="Oznacz potwierdzone eventy w PostgreSQL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pokaż eventy bez wysyłania.",
    )

    args = parser.parse_args()

    if args.batch_size <= 0:
        parser.error("--batch-size musi być większe od 0")

    if args.limit < 0:
        parser.error("--limit nie może być ujemny")

    if args.sleep_ms < 0:
        parser.error("--sleep-ms nie może być ujemny")

    load_configuration()

    read_connection = connect_postgres()
    write_connection = connect_postgres()

    total_available = fetch_event_count(
        read_connection,
        include_replayed=args.include_replayed,
    )

    target_count = (
        min(total_available, args.limit)
        if args.limit > 0
        else total_available
    )

    print("PostgreSQL → Redpanda replay")
    print(f"Topic: {args.topic}")
    print(f"Bootstrap servers: {args.bootstrap_servers}")
    print(f"Dostępne eventy: {total_available:,}")
    print(f"Planowane eventy: {target_count:,}")
    print(f"Batch size: {args.batch_size:,}")
    print(f"Sleep: {args.sleep_ms} ms")
    print(f"Fresh timestamps: {args.fresh_timestamps}")
    print(f"Mark replayed: {args.mark_replayed}")
    print(f"Dry run: {args.dry_run}")

    if target_count == 0:
        print("Brak eventów do przetworzenia.")
        read_connection.close()
        write_connection.close()
        return 0

    producer = None if args.dry_run else build_producer(
        args.bootstrap_servers
    )

    where_clause = ""
    if not args.include_replayed:
        where_clause = "WHERE replayed_flag = FALSE"

    limit_clause = ""
    query_parameters: tuple = ()

    if args.limit > 0:
        limit_clause = "LIMIT %s"
        query_parameters = (args.limit,)

    query = f"""
        SELECT
            event_id,
            event_time,
            event_type,
            source_system,
            entity_type,
            entity_id,
            payload_json
        FROM banking_source.stream_events
        {where_clause}
        ORDER BY event_time ASC, event_id ASC
        {limit_clause}
    """

    processed_total = 0
    delivered_total = 0
    failed_total = 0
    started_at = time.monotonic()

    read_cursor = read_connection.cursor(
        name="banking_event_replay_cursor",
        cursor_factory=RealDictCursor,
    )
    read_cursor.itersize = args.batch_size
    read_cursor.execute(query, query_parameters)

    try:
        while RUNNING:
            rows = read_cursor.fetchmany(args.batch_size)

            if not rows:
                break

            delivered_ids: list[str] = []
            failures: list[tuple[str, str]] = []

            for row in rows:
                if not RUNNING:
                    break

                message, partition_key = build_message(
                    row,
                    fresh_timestamp=args.fresh_timestamps,
                )

                if args.dry_run:
                    print(json.dumps(
                        message,
                        ensure_ascii=False,
                        indent=2,
                    ))
                    processed_total += 1
                    continue

                value = json.dumps(
                    message,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")

                while RUNNING:
                    try:
                        producer.produce(
                            topic=args.topic,
                            key=partition_key.encode("utf-8"),
                            value=value,
                            callback=delivery_callback_factory(
                                row["event_id"],
                                delivered_ids,
                                failures,
                            ),
                        )
                        producer.poll(0)
                        break
                    except BufferError:
                        producer.poll(0.5)
                    except KafkaException as exc:
                        failures.append(
                            (row["event_id"], str(exc))
                        )
                        break

                processed_total += 1

                if args.sleep_ms > 0:
                    time.sleep(args.sleep_ms / 1000)

            if not args.dry_run:
                undelivered = producer.flush(120)

                if undelivered:
                    print(
                        f"UWAGA: {undelivered} wiadomości "
                        "pozostało w kolejce producenta."
                    )

                replay_time = utc_now()

                if args.mark_replayed and delivered_ids:
                    mark_replayed(
                        write_connection,
                        delivered_ids,
                        replay_time,
                    )

                delivered_total += len(delivered_ids)
                failed_total += len(failures)

                elapsed = max(time.monotonic() - started_at, 0.001)
                rate = delivered_total / elapsed

                print(
                    f"Postęp: {processed_total:,}/{target_count:,} | "
                    f"dostarczone: {delivered_total:,} | "
                    f"błędy: {failed_total:,} | "
                    f"tempo: {rate:,.1f} eventów/s"
                )

                if failures:
                    print("Przykładowe błędy dostarczenia:")
                    for event_id, error in failures[:5]:
                        print(f"  {event_id}: {error}")

                    print(
                        "Zatrzymuję replay, aby uniknąć "
                        "pominięcia niewysłanych eventów."
                    )
                    return 1

    finally:
        if producer is not None:
            producer.flush(30)

        read_cursor.close()
        read_connection.close()
        write_connection.close()

    elapsed = max(time.monotonic() - started_at, 0.001)

    print("Replay zakończony.")
    print(f"Przetworzono: {processed_total:,}")
    print(f"Dostarczono: {delivered_total:,}")
    print(f"Błędy: {failed_total:,}")
    print(f"Czas: {elapsed:.2f} s")

    return 0 if failed_total == 0 else 1


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        sys.exit(main())
    except Exception as exc:
        print(f"BŁĄD: {exc}", file=sys.stderr)
        sys.exit(1)
