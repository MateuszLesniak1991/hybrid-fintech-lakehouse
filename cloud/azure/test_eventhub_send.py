#!/usr/bin/env python3
"""
Sends one test banking event to Azure Event Hub.
"""

import json
import os
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv


def main() -> None:
    load_dotenv(".env")

    connection_string = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
    eventhub_name = os.getenv("AZURE_EVENTHUB_NAME")

    if not connection_string:
        raise RuntimeError(
            "AZURE_EVENTHUB_CONNECTION_STRING is missing from .env"
        )

    if not eventhub_name:
        raise RuntimeError(
            "AZURE_EVENTHUB_NAME is missing from .env"
        )

    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
    )

    event = {
        "event_id": f"portfolio-test-{uuid.uuid4()}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": "high_risk_transaction_detected",
        "source_system": "redpanda_bridge_test",
        "entity_type": "transaction",
        "entity_id": "TX-PORTFOLIO-TEST",
        "payload": {
            "amount": 1499.99,
            "currency": "PLN",
            "risk_score": 91,
            "is_fraud_suspected": True,
            "fraud_rule_hit": "high_amount_new_device",
        },
    }

    with producer:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)

    print("[SUCCESS] Test event sent to Azure Event Hub")
    print(json.dumps(event, indent=2))


if __name__ == "__main__":
    main()
