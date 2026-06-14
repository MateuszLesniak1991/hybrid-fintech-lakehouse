from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
from datetime import datetime, timezone
import json
import os
import uuid

load_dotenv()

CONNECTION_STRING = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")
EVENTHUB_NAME = os.getenv("AZURE_EVENTHUB_NAME", "transactions-raw")

if not CONNECTION_STRING:
    raise ValueError("Missing AZURE_EVENTHUB_CONNECTION_STRING in .env")

event = {
    "event_id": str(uuid.uuid4()),
    "event_time": datetime.now(timezone.utc).isoformat(),
    "source_system": "ubuntu_onprem_eventhub_test",
    "transaction_id": str(uuid.uuid4()),
    "customer_id": 1,
    "account_id": 1,
    "merchant_id": 1,
    "transaction_type": "online_payment",
    "channel": "web",
    "amount": 123.45,
    "currency": "PLN",
    "status": "completed",
    "is_fraud": 0,
    "risk_score": 12
}

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING,
    eventhub_name=EVENTHUB_NAME
)

try:
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))

    producer.send_batch(event_data_batch)

    print("Event sent to Azure Event Hub")
    print(json.dumps(event, indent=2))
finally:
    producer.close()
