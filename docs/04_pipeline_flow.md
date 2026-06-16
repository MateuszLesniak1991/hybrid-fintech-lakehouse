# Pipeline Flow

## 1. End-to-end overview

The project contains two parallel pipelines that start from the same PostgreSQL source database.

```text
Python generator
→ PostgreSQL
→ streaming path
→ batch path
```

---

## 2. Historical data generation

Command pattern:

```bash
python generator/generate_historical_banking_postgres.py   --start-date 2026-05-16   --end-date 2026-06-15   --customers 5000   --merchants 800   --atms 250   --transactions 250000   --batch-size 5000   --reset
```

Output:

- 31 days of history,
- 250,000 transactions,
- 259,826 stream events,
- related reference and risk data.

---

## 3. Streaming flow

## 3.1 Source

Source table:

```text
banking_source.stream_events
```

Only events with:

```text
replayed_flag = false
```

are selected for normal replay.

## 3.2 Replay producer

File:

```text
streaming/replay_postgres_events_to_redpanda.py
```

Replay behavior:

1. Read events in ascending event time order.
2. Build a Kafka message.
3. Preserve the source timestamp as `original_event_time`.
4. Assign a fresh replay timestamp to `event_time`.
5. Send the event to Redpanda.
6. Wait for broker delivery confirmation.
7. Mark only successfully delivered events as replayed.

Example message:

```json
{
  "event_id": "example-event-id",
  "event_time": "2026-06-16T06:50:12.442+00:00",
  "original_event_time": "2026-05-23T18:14:07.911+00:00",
  "event_type": "card_payment_authorized",
  "source_system": "payment_gateway",
  "entity_type": "transaction",
  "entity_id": "TX-example",
  "payload": {
    "amount": 499.99,
    "currency": "PLN",
    "risk_score": 82
  },
  "replay_metadata": {
    "replay_source": "postgresql_banking_source",
    "timestamp_strategy": "fresh_on_send",
    "target_system": "redpanda"
  }
}
```

## 3.3 Redpanda

Topic:

```text
banking.operational.events
```

The topic stores operational banking events for downstream consumers.

## 3.4 Event Hub bridge

Status: planned.

Planned flow:

```text
Redpanda consumer group
→ Python bridge
→ Azure Event Hub
```

The bridge will:

- consume messages from Redpanda,
- preserve event identifiers,
- forward messages to Event Hub,
- commit Kafka offsets after successful cloud delivery,
- support restart without losing progress.

## 3.5 Microsoft Fabric

Status: planned.

Planned flow:

```text
Azure Event Hub
→ Fabric Eventstream
→ filtering and routing
→ Real-Time Intelligence
→ Power BI real-time dashboard
```

---

## 4. Batch flow

## 4.1 PostgreSQL extraction

Source tables include:

- transactions,
- customers,
- accounts,
- merchants,
- atms,
- KYC/AML,
- support cases,
- chargebacks,
- risk scores,
- device blacklist,
- daily balances.

## 4.2 Hourly Parquet export

File:

```text
batch/export_postgres_to_minio.py
```

The script:

1. Reads a selected time range.
2. Iterates through hourly windows.
3. Queries records for each hour.
4. Creates Parquet files.
5. Compresses files using Snappy.
6. Uploads files to MinIO.
7. Skips empty hours.
8. Supports overwrite mode.
9. Optionally keeps local copies.

Transaction partition pattern:

```text
banking/transactions/
year=YYYY/
month=MM/
day=DD/
hour=HH/
transactions_YYYYMMDD_HH.parquet
```

## 4.3 Dataset export frequencies

### Hourly

- transactions,
- KYC/AML checks,
- support cases,
- chargebacks,
- customer risk scores,
- merchant risk scores,
- device blacklist.

### Snapshot

- customers,
- accounts,
- merchants,
- ATMs.

### Daily

- account balances.

## 4.4 MinIO

Bucket:

```text
bronze
```

MinIO keeps the local on-premises copy.

## 4.5 ADLS synchronization

Status: planned.

Planned behavior:

- list MinIO objects,
- compare against ADLS target paths,
- upload missing or changed files,
- preserve partition structure,
- log upload status,
- support restart.

## 4.6 Databricks

Status: planned.

Planned flow:

```text
ADLS Bronze Parquet
→ Databricks Bronze Delta
→ Databricks Silver Delta
→ Databricks Gold Delta
→ Power BI
```

---

## 5. Replay versus batch timestamp handling

### Streaming

```text
event_time = replay time
original_event_time = historical source time
```

Purpose:

- fresh real-time demonstration,
- preserved historical audit timestamp.

### Batch

```text
event_time = original historical time
```

Purpose:

- monthly historical analysis,
- correct hourly and daily partitions.

---

## 6. Restart behavior

### Streaming

The source table includes:

- `replayed_flag`,
- `replayed_at`.

A restarted replay job reads only unreplayed events.

### Batch

Deterministic object paths allow:

- skip existing files,
- overwrite files when required,
- restart from the same date range.

---

## 7. Current validated results

Streaming replay:

- processed: 259,826,
- delivered: 259,826,
- errors: 0,
- duration: approximately 47 minutes.

Batch export:

- 744 hourly transaction files,
- 31 daily balance files,
- full source table coverage,
- local MinIO Bronze layer created successfully.
