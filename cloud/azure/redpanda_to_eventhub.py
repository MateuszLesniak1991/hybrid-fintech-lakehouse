#!/usr/bin/env python3
"""
Forwards banking events from Redpanda to Azure Event Hub in batches.

Reliability model:
- automatic Kafka commits are disabled,
- events are sent to Event Hub in batches,
- Kafka offsets are committed only after successful batch delivery,
- failed batches are not committed and can be retried,
- a stable consumer group enables restart from the last confirmed offset.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from azure.eventhub import EventData, EventHubProducerClient
from azure.eventhub.exceptions import EventHubError
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    TopicPartition,
)
from dotenv import load_dotenv


LOGGER = logging.getLogger("redpanda_to_eventhub")
STOP_REQUESTED = False


@dataclass
class BridgeStats:
    consumed: int = 0
    delivered: int = 0
    committed: int = 0
    batches_sent: int = 0
    invalid: int = 0
    failed: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Forward Redpanda events to Azure Event Hub in batches."
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after consuming this many events.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="Maximum number of events per batch. Default: 250.",
    )
    parser.add_argument(
        "--batch-timeout",
        type=float,
        default=2.0,
        help="Send a partial batch after this many seconds. Default: 2.",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=0.5,
        help="Kafka poll timeout in seconds. Default: 0.5.",
    )
    parser.add_argument(
        "--idle-timeout",
        type=int,
        default=30,
        help="Stop after this many idle seconds. Use 0 to disable.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate messages without sending or committing offsets.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Use earliest only if the consumer group has no committed offsets.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    # Reduce verbose AMQP connection-state logs.
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("uamqp").setLevel(logging.WARNING)


def handle_signal(signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    LOGGER.warning("Stop requested by signal %s", signum)
    STOP_REQUESTED = True


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def create_consumer(from_beginning: bool) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": required_env(
                "REDPANDA_BOOTSTRAP_SERVERS"
            ),
            "group.id": required_env(
                "REDPANDA_EVENTHUB_CONSUMER_GROUP"
            ),
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": (
                "earliest" if from_beginning else "latest"
            ),
            "client.id": "redpanda-eventhub-batch-bridge",
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 300000,
        }
    )


def create_producer() -> EventHubProducerClient:
    return EventHubProducerClient.from_connection_string(
        conn_str=required_env(
            "AZURE_EVENTHUB_CONNECTION_STRING"
        ),
        eventhub_name=required_env("AZURE_EVENTHUB_NAME"),
        retry_total=5,
        retry_backoff_factor=0.8,
    )


def decode_message(message: Message) -> dict[str, Any]:
    raw_value = message.value()

    if raw_value is None:
        raise ValueError("Kafka message has empty value")

    try:
        event = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid JSON: {exc}") from exc

    if not isinstance(event, dict):
        raise ValueError("Message JSON must be an object")

    if not event.get("event_id"):
        raise ValueError("Message does not contain event_id")

    return event


def prepare_cloud_event(
    event: dict[str, Any],
    message: Message,
) -> dict[str, Any]:
    forwarded = dict(event)

    metadata = dict(forwarded.get("cloud_forwarding_metadata") or {})
    metadata.update(
        {
            "forwarded_at": datetime.now(timezone.utc).isoformat(),
            "forwarded_by": "redpanda_to_eventhub_batch",
            "source_topic": message.topic(),
            "source_partition": message.partition(),
            "source_offset": message.offset(),
            "target_system": "azure_event_hub",
        }
    )

    forwarded["cloud_forwarding_metadata"] = metadata
    return forwarded


def to_event_data(event: dict[str, Any]) -> EventData:
    payload = json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    event_data = EventData(payload)
    event_data.content_type = "application/json"

    if event.get("event_id"):
        event_data.message_id = str(event["event_id"])

    event_data.properties = {
        "event_type": str(event.get("event_type", "")),
        "source_system": str(event.get("source_system", "")),
    }

    return event_data


def commit_messages(
    consumer: Consumer,
    messages: list[Message],
) -> None:
    """
    Commit the highest successfully delivered offset for every partition.
    """
    highest_offsets: dict[tuple[str, int], int] = {}

    for message in messages:
        key = (message.topic(), message.partition())
        highest_offsets[key] = max(
            highest_offsets.get(key, -1),
            message.offset(),
        )

    offsets = [
        TopicPartition(topic, partition, offset + 1)
        for (topic, partition), offset in highest_offsets.items()
    ]

    result = consumer.commit(
        offsets=offsets,
        asynchronous=False,
    )

    if not result:
        raise RuntimeError("Kafka offset commit returned no result")

    for partition in result:
        if partition.error:
            raise KafkaException(partition.error)


def flush_batch(
    producer: EventHubProducerClient,
    consumer: Consumer,
    queued_events: list[EventData],
    queued_messages: list[Message],
    stats: BridgeStats,
) -> None:
    if not queued_events:
        return

    eventhub_batch = producer.create_batch()

    added_messages: list[Message] = []
    added_count = 0

    for event_data, source_message in zip(
        queued_events,
        queued_messages,
        strict=True,
    ):
        try:
            eventhub_batch.add(event_data)
            added_messages.append(source_message)
            added_count += 1
        except ValueError:
            if added_count == 0:
                raise RuntimeError(
                    "Single event exceeds Event Hub batch size limit"
                )

            producer.send_batch(eventhub_batch)
            commit_messages(consumer, added_messages)

            stats.delivered += added_count
            stats.committed += added_count
            stats.batches_sent += 1

            LOGGER.info(
                "[BATCH FORWARDED] events=%s "
                "partition=%s last_offset=%s",
                added_count,
                added_messages[-1].partition(),
                added_messages[-1].offset(),
            )

            eventhub_batch = producer.create_batch()
            eventhub_batch.add(event_data)

            added_messages = [source_message]
            added_count = 1

    if added_count > 0:
        producer.send_batch(eventhub_batch)
        commit_messages(consumer, added_messages)

        stats.delivered += added_count
        stats.committed += added_count
        stats.batches_sent += 1

        LOGGER.info(
            "[BATCH FORWARDED] events=%s "
            "partition=%s last_offset=%s",
            added_count,
            added_messages[-1].partition(),
            added_messages[-1].offset(),
        )


def main() -> int:
    load_dotenv(".env")
    args = parse_args()
    configure_logging(args.log_level)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    if args.batch_size < 1:
        raise ValueError("--batch-size must be at least 1")

    topic = required_env("REDPANDA_SOURCE_TOPIC")
    group = required_env("REDPANDA_EVENTHUB_CONSUMER_GROUP")

    LOGGER.info("Batch bridge starting")
    LOGGER.info("Source topic: %s", topic)
    LOGGER.info("Consumer group: %s", group)
    LOGGER.info("Batch size: %s", args.batch_size)
    LOGGER.info("Batch timeout: %s", args.batch_timeout)
    LOGGER.info("Max events: %s", args.max_events)
    LOGGER.info("Dry run: %s", args.dry_run)

    consumer = create_consumer(args.from_beginning)
    producer = None if args.dry_run else create_producer()

    stats = BridgeStats()
    queued_events: list[EventData] = []
    queued_messages: list[Message] = []

    last_message_time = time.monotonic()
    batch_started_at = time.monotonic()

    try:
        consumer.subscribe([topic])

        if producer is not None:
            producer.__enter__()

        while not STOP_REQUESTED:
            reached_limit = (
                args.max_events is not None
                and stats.consumed >= args.max_events
            )

            batch_expired = (
                queued_events
                and time.monotonic() - batch_started_at
                >= args.batch_timeout
            )

            if reached_limit or batch_expired:
                if args.dry_run:
                    LOGGER.info(
                        "[DRY-RUN BATCH] validated=%s",
                        len(queued_events),
                    )
                else:
                    assert producer is not None
                    flush_batch(
                        producer,
                        consumer,
                        queued_events,
                        queued_messages,
                        stats,
                    )

                queued_events.clear()
                queued_messages.clear()
                batch_started_at = time.monotonic()

                if reached_limit:
                    LOGGER.info("Maximum event count reached")
                    break

            message = consumer.poll(args.poll_timeout)

            if message is None:
                idle_seconds = time.monotonic() - last_message_time

                if queued_events and idle_seconds >= args.batch_timeout:
                    continue

                if (
                    args.idle_timeout > 0
                    and idle_seconds >= args.idle_timeout
                ):
                    LOGGER.info("Idle timeout reached")
                    break

                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            last_message_time = time.monotonic()

            try:
                source_event = decode_message(message)
                cloud_event = prepare_cloud_event(source_event, message)

                queued_events.append(to_event_data(cloud_event))
                queued_messages.append(message)
                stats.consumed += 1

                if len(queued_events) == 1:
                    batch_started_at = time.monotonic()

                if len(queued_events) >= args.batch_size:
                    if args.dry_run:
                        LOGGER.info(
                            "[DRY-RUN BATCH] validated=%s",
                            len(queued_events),
                        )
                    else:
                        assert producer is not None
                        flush_batch(
                            producer,
                            consumer,
                            queued_events,
                            queued_messages,
                            stats,
                        )

                    queued_events.clear()
                    queued_messages.clear()
                    batch_started_at = time.monotonic()

            except ValueError as exc:
                stats.invalid += 1
                LOGGER.error(
                    "[INVALID] partition=%s offset=%s: %s",
                    message.partition(),
                    message.offset(),
                    exc,
                )
                break

            except (
                EventHubError,
                KafkaException,
                RuntimeError,
            ) as exc:
                stats.failed += 1
                LOGGER.error(
                    "[FAILED] partition=%s offset=%s: %s",
                    message.partition(),
                    message.offset(),
                    exc,
                )
                break

        if queued_events:
            if args.dry_run:
                LOGGER.info(
                    "[DRY-RUN BATCH] validated=%s",
                    len(queued_events),
                )
            else:
                assert producer is not None
                flush_batch(
                    producer,
                    consumer,
                    queued_events,
                    queued_messages,
                    stats,
                )

    finally:
        if producer is not None:
            producer.__exit__(None, None, None)

        consumer.close()

        LOGGER.info("")
        LOGGER.info("[SUMMARY]")
        LOGGER.info("Consumed events:  %s", stats.consumed)
        LOGGER.info("Delivered events: %s", stats.delivered)
        LOGGER.info("Committed events: %s", stats.committed)
        LOGGER.info("Batches sent:     %s", stats.batches_sent)
        LOGGER.info("Invalid events:   %s", stats.invalid)
        LOGGER.info("Failed events:    %s", stats.failed)

    return 1 if stats.failed or stats.invalid else 0


if __name__ == "__main__":
    sys.exit(main())
