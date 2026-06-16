# Architecture

## 1. Architecture overview

The platform uses a hybrid architecture with local infrastructure and Microsoft Azure services.

```text
Simulated banking systems
        ↓
Python historical generator
        ↓
PostgreSQL
   ┌───────────────┴───────────────┐
   ↓                               ↓
Streaming path                 Batch path
   ↓                               ↓
Redpanda                       Hourly Parquet
   ↓                               ↓
Azure Event Hub                MinIO Bronze
   ↓                               ↓
Microsoft Fabric               ADLS Gen2 Bronze
   ↓                               ↓
Real-Time Intelligence         Databricks
   ↓                               ↓
Power BI                       Silver / Gold
                                   ↓
                                Power BI
```

---

## 2. Source layer

### Python historical generator

File:

```text
generator/generate_historical_banking_postgres.py
```

Responsibilities:

- generates customers,
- generates accounts,
- generates merchants,
- generates ATMs,
- generates 31 days of transactions,
- generates KYC/AML records,
- generates customer and merchant risk scores,
- generates support cases,
- generates chargebacks,
- generates device blacklist records,
- generates daily account balances,
- creates operational events for streaming.

### PostgreSQL

PostgreSQL represents the local banking source platform.

Schema:

```text
banking_source
```

PostgreSQL acts as:

- operational data source,
- historical source database,
- source for streaming replay,
- source for hourly batch exports.

It is not treated as the final analytical warehouse.

---

## 3. Streaming architecture

### PostgreSQL `stream_events`

The `stream_events` table stores normalized operational events.

Important fields:

- `event_id`,
- `event_time`,
- `event_type`,
- `source_system`,
- `entity_type`,
- `entity_id`,
- `payload_json`,
- `replayed_flag`,
- `replayed_at`.

### PostgreSQL to Redpanda replay

File:

```text
streaming/replay_postgres_events_to_redpanda.py
```

Features:

- reads unreplayed events,
- preserves original timestamps,
- assigns fresh replay timestamps,
- sends messages asynchronously,
- uses idempotent Kafka producer settings,
- commits replay status only after successful delivery,
- supports restart without resending confirmed events.

### Redpanda

Topic:

```text
banking.operational.events
```

Redpanda is used as the local Kafka-compatible event broker.

Responsibilities:

- decouples source and cloud ingestion,
- buffers operational events,
- supports consumer groups,
- enables local inspection,
- provides replay capability.

### Azure Event Hub

Status: connectivity tested; full Redpanda bridge planned.

Responsibilities:

- cloud event ingestion,
- scalable event buffering,
- integration with Microsoft Fabric,
- handoff from on-premises streaming to cloud analytics.

### Microsoft Fabric Eventstream

Status: planned.

Responsibilities:

- consume Event Hub events,
- filter high-risk transactions,
- route data to Real-Time Intelligence destinations,
- support real-time fraud monitoring.

---

## 4. Batch architecture

### PostgreSQL to Parquet export

File:

```text
batch/export_postgres_to_minio.py
```

The exporter:

- reads PostgreSQL domain tables,
- creates Parquet files,
- compresses them with Snappy,
- partitions by year/month/day/hour,
- uploads directly to MinIO,
- optionally keeps local copies.

### MinIO

Bucket:

```text
bronze
```

MinIO represents the local S3-compatible Bronze data lake.

Example path:

```text
s3://bronze/banking/transactions/
year=2026/month=05/day=16/hour=01/
transactions_20260516_01.parquet
```

MinIO provides:

- local object storage,
- on-premises Bronze copy,
- S3-compatible API,
- validation before cloud upload,
- independent local retention.

### ADLS Gen2

Status: planned.

ADLS Gen2 will store the cloud Bronze copy.

The MinIO and ADLS layers will coexist:

- MinIO = on-premises Bronze,
- ADLS = cloud Bronze.

### Databricks

Status: planned.

Databricks will implement:

- Bronze ingestion,
- Silver cleansing and standardization,
- Gold business models,
- data quality checks,
- Delta Lake tables.

### Power BI / Tableau

Status: planned.

The final analytical dashboards will use Gold datasets.

---

## 5. Technology stack

| Layer | Technology |
|---|---|
| Data generation | Python |
| Source database | PostgreSQL |
| Event broker | Redpanda |
| Local object storage | MinIO |
| Cloud event ingestion | Azure Event Hub |
| Cloud object storage | ADLS Gen2 |
| Real-time analytics | Microsoft Fabric Eventstream |
| Lakehouse processing | Databricks |
| File format | Parquet |
| Compression | Snappy |
| Reporting | Power BI / Tableau |
| Local orchestration | Docker Compose |
| Version control | GitHub |

---

## 6. Network boundaries

### Local environment

- PostgreSQL,
- pgAdmin,
- Redpanda,
- Redpanda Console,
- MinIO,
- Python generators and exporters.

### Cloud environment

- Azure Event Hub,
- ADLS Gen2,
- Microsoft Fabric,
- Databricks,
- Power BI.

---

## 7. Reliability principles

The architecture uses:

- event identifiers,
- replay flags,
- replay timestamps,
- Kafka delivery acknowledgements,
- idempotent producer mode,
- deterministic partition paths,
- immutable Parquet files,
- restartable batch jobs,
- clear separation between local and cloud layers.

---

## 8. Security principles

Secrets are stored locally in `.env`.

The repository contains only:

- `.env.example`,
- source code,
- schemas,
- documentation.

The repository does not contain:

- passwords,
- Event Hub connection strings,
- MinIO credentials,
- database dumps,
- generated data,
- logs.

