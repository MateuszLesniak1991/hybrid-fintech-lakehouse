# Hybrid FinTech Lakehouse Platform

Hybrid FinTech Lakehouse Platform is a Data Engineering portfolio project that simulates realistic banking operations and processes data through batch and streaming pipelines.

The implemented platform combines local infrastructure with Azure Data Lake Storage Gen2 and demonstrates:

* realistic banking data generation,
* historical transaction processing,
* Kafka-compatible event streaming,
* hourly batch processing,
* Parquet-based data lake storage,
* hybrid on-premises and cloud data replication.

---

## Business problem

Banks receive data from multiple operational systems, including:

* payment gateways,
* ATM networks,
* core banking systems,
* CRM platforms,
* KYC/AML systems,
* fraud and risk engines,
* customer support systems,
* chargeback management systems.

Some operational events need to be processed quickly for fraud and risk monitoring. Other datasets are processed in batches for customer analytics, historical reporting and business analysis.

The project simulates these systems using Python and stores generated data in PostgreSQL.

---

## Architecture

![Hybrid FinTech Lakehouse Architecture](images/architecture.png)

The implemented platform currently contains two data processing paths.

### Streaming pipeline

```text
Simulated banking systems
→ PostgreSQL
→ Python historical event replay
→ Redpanda
```

The streaming pipeline processes operational banking events such as:

* card payments,
* BLIK payments,
* online payments,
* bank transfers,
* ATM withdrawals,
* transaction declines,
* suspicious device events,
* customer risk updates,
* merchant risk updates,
* high-risk transaction events.

Historical events are replayed with a fresh `event_time`, while the original source timestamp is retained as `original_event_time`.

This makes it possible to demonstrate real-time event processing while preserving the original historical context.

### Batch pipeline

```text
Simulated banking systems
→ PostgreSQL
→ hourly Parquet export
→ MinIO Bronze
→ MinIO to ADLS synchronization
→ ADLS Gen2 Bronze
```

The batch pipeline supports:

* customer analysis,
* customer segmentation,
* transaction behavior analysis,
* account balance reporting,
* merchant risk analysis,
* chargeback analysis,
* ATM usage analysis,
* KYC/AML analysis,
* customer support analysis.

The same Bronze dataset is stored in two locations:

```text
MinIO Bronze — local on-premises copy
ADLS Gen2 Bronze — cloud copy
```

The original folder and partition structure is preserved during synchronization.

---

## Simulated source systems

The Python data generator simulates multiple banking systems.

| Source system         | Generated data                           |
| --------------------- | ---------------------------------------- |
| Core Banking System   | accounts, balances and bank transfers    |
| Payment Gateway       | card, BLIK and online payments           |
| ATM Network           | cash withdrawals and ATM activity        |
| CRM                   | customer profiles and registrations      |
| KYC/AML System        | verification and compliance checks       |
| Fraud and Risk Engine | customer, merchant and transaction risk  |
| Customer Support      | customer service and fraud-related cases |
| Chargeback System     | disputes and chargeback cases            |

---

## Generated dataset

The generated dataset covers 31 days of simulated banking activity.

| Dataset                 | Records |
| ----------------------- | ------: |
| Customers               |   5,000 |
| Accounts                |   5,820 |
| Merchants               |     800 |
| ATMs                    |     250 |
| Transactions            | 250,000 |
| Streaming events        | 259,826 |
| KYC/AML checks          |   5,000 |
| Customer risk scores    |   5,000 |
| Merchant risk scores    |     800 |
| Chargeback cases        |   2,500 |
| Customer support cases  |   2,039 |
| Device blacklist events |     729 |
| Daily account balances  | 176,745 |

Historical period:

```text
2026-05-16 → 2026-06-15
```

The dataset contains multiple:

* transaction methods,
* merchant categories,
* cities and countries,
* customer segments,
* authorization statuses,
* fraud scenarios,
* risk levels,
* banking channels,
* source systems.

---

## Streaming implementation

Operational events are stored in:

```text
banking_source.stream_events
```

The replay application reads the events from PostgreSQL and publishes them to Redpanda.

Replay script:

```text
streaming/replay_postgres_events_to_redpanda.py
```

Redpanda topic:

```text
banking.operational.events
```

The replay process:

1. reads unreplayed events from PostgreSQL,
2. preserves the historical timestamp,
3. assigns a current replay timestamp,
4. publishes the message to Redpanda,
5. waits for delivery confirmation,
6. marks successfully delivered events as replayed.

Example timestamp strategy:

```text
event_time          = current replay timestamp
original_event_time = historical source timestamp
```

### Streaming results

| Metric           |                   Result |
| ---------------- | -----------------------: |
| Processed events |                  259,826 |
| Delivered events |                  259,826 |
| Delivery errors  |                        0 |
| Replay duration  | approximately 47 minutes |

The replay process completed without delivery errors.

---

## Batch implementation

PostgreSQL datasets are exported to MinIO using:

```text
batch/export_postgres_to_minio.py
```

The exporter creates:

* hourly Parquet files,
* daily balance files,
* full reference snapshots.

File format:

```text
Parquet
```

Compression:

```text
Snappy
```

### Transaction partitioning

```text
s3://bronze/banking/transactions/
year=YYYY/
month=MM/
day=DD/
hour=HH/
transactions_YYYYMMDD_HH.parquet
```

The transaction dataset contains:

```text
31 days × 24 hours = 744 hourly files
```

### Batch export results

| Dataset                | Files | Approximate size |
| ---------------------- | ----: | ---------------: |
| Transactions           |   744 |         43.64 MB |
| Daily account balances |    31 |         15.42 MB |
| Support cases          |   548 |          1.50 MB |
| Chargebacks            |   612 |          2.42 MB |
| Customer risk scores   |     8 |          0.02 MB |
| Merchant risk scores   |   414 |          1.12 MB |
| Device blacklist       |   398 |          0.92 MB |
| Customer snapshot      |     1 |          0.35 MB |
| Account snapshot       |     1 |          0.44 MB |
| Merchant snapshot      |     1 |          0.07 MB |
| ATM snapshot           |     1 |          0.01 MB |

Total:

```text
approximately 2,764 Parquet files
approximately 65.9 MB
```

Low-volume datasets contain files only for hours in which records were generated.

---

## ADLS Gen2 synchronization

The local Bronze layer is synchronized from MinIO to Azure Data Lake Storage Gen2.

Synchronization script:

```text
cloud/azure/minio_to_adls.py
```

Implemented features:

* paginated MinIO object discovery,
* prefix-based synchronization,
* preservation of the original object path,
* Azure container validation,
* existing file detection,
* file-size comparison,
* dry-run mode,
* overwrite mode,
* restartable synchronization,
* post-upload file-size validation,
* upload statistics and error reporting.

Data flow:

```text
MinIO bucket: bronze
→ Python synchronization
→ ADLS Gen2 container: bronze
```

Example source:

```text
s3://bronze/banking/transactions/
year=2026/month=05/day=16/hour=01/
transactions_20260516_01.parquet
```

Corresponding cloud destination:

```text
bronze/banking/transactions/
year=2026/month=05/day=16/hour=01/
transactions_20260516_01.parquet
```

The synchronization preserves the complete folder hierarchy from MinIO.

---

## Local platform evidence

### PostgreSQL source datasets

![PostgreSQL row counts](images/local/postgresql_row_counts.png)

### Redpanda operational event

![Redpanda message](images/streaming/redpanda_message.png)

### MinIO hourly partitions

![MinIO hourly partitions](images/batch/minio_hourly_partitions.png)

---

## Cloud platform evidence

### ADLS Gen2 Bronze structure

![ADLS Bronze structure](images/cloud/adls_bronze_structure.png)

### ADLS Gen2 hourly partitions

![ADLS hourly partitions](images/cloud/adls_hourly_partitions.png)

### Completed synchronization

![ADLS full synchronization summary](images/cloud/adls_full_sync_summary.png)

---

## Technology stack

| Area                 | Technology                   |
| -------------------- | ---------------------------- |
| Data generation      | Python                       |
| Source database      | PostgreSQL                   |
| Streaming broker     | Redpanda                     |
| Streaming API        | Apache Kafka API             |
| Local object storage | MinIO                        |
| Cloud data lake      | Azure Data Lake Storage Gen2 |
| File format          | Apache Parquet               |
| Compression          | Snappy                       |
| Local orchestration  | Docker Compose               |
| Cloud integration    | Azure Storage SDK for Python |
| Version control      | Git and GitHub               |

---

## Implemented components

| Component                                       | Status    |
| ----------------------------------------------- | --------- |
| Historical banking data generator               | Completed |
| PostgreSQL banking source schema                | Completed |
| Realistic 31-day banking dataset                | Completed |
| PostgreSQL event replay mechanism               | Completed |
| Redpanda operational event topic                | Completed |
| Historical event replay to Redpanda             | Completed |
| Hourly Parquet export                           | Completed |
| MinIO Bronze layer                              | Completed |
| ADLS Gen2 Storage Account                       | Completed |
| ADLS Gen2 Bronze container                      | Completed |
| MinIO to ADLS synchronization                   | Completed |
| Restartable and idempotent file synchronization | Completed |
| Project documentation                           | Completed |
| Architecture Decision Records                   | Completed |

---

## Repository structure

```text
hybrid-fintech-lakehouse/
├── batch/
│   └── export_postgres_to_minio.py
├── cloud/
│   ├── __init__.py
│   └── azure/
│       ├── __init__.py
│       └── minio_to_adls.py
├── docs/
│   ├── adr/
│   ├── 01_business_scenario.md
│   ├── 02_architecture.md
│   ├── 03_data_model.md
│   ├── 04_pipeline_flow.md
│   ├── 05_bronze_silver_gold.md
│   ├── 06_local_setup.md
│   ├── 07_cloud_setup.md
│   ├── 08_data_quality.md
│   ├── 09_ci_cd_plan.md
│   └── 10_project_results.md
├── generator/
│   └── generate_historical_banking_postgres.py
├── images/
│   ├── architecture/
│   ├── batch/
│   ├── cloud/
│   ├── dashboards/
│   ├── local/
│   └── streaming/
├── sql/
│   └── source/
│       └── 01_banking_source_schema.sql
├── streaming/
│   └── replay_postgres_events_to_redpanda.py
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Documentation

Detailed documentation is available in the `docs` directory:

* [Business scenario](docs/01_business_scenario.md)
* [Architecture](docs/02_architecture.md)
* [Data model](docs/03_data_model.md)
* [Pipeline flow](docs/04_pipeline_flow.md)
* [Bronze, Silver and Gold architecture](docs/05_bronze_silver_gold.md)
* [Local setup](docs/06_local_setup.md)
* [Cloud setup](docs/07_cloud_setup.md)
* [Data quality](docs/08_data_quality.md)
* [CI/CD design](docs/09_ci_cd_plan.md)
* [Project results](docs/10_project_results.md)
* [Architecture Decision Records](docs/adr/)

---

## Key engineering concepts demonstrated

* hybrid on-premises and cloud data architecture,
* batch and streaming data processing,
* realistic banking data simulation,
* historical event replay,
* event-time and processing-time handling,
* Kafka-compatible event streaming,
* idempotent event delivery,
* restartable data pipelines,
* hourly Parquet partitioning,
* Snappy compression,
* S3-compatible object storage,
* ADLS Gen2 cloud storage,
* local and cloud Bronze layers,
* source-to-data-lake synchronization,
* deterministic object paths,
* data reconciliation,
* data quality validation,
* architecture decision documentation.

---

## Engineering outcomes

The project successfully demonstrates an end-to-end hybrid data flow:

```text
Banking data simulation
→ PostgreSQL source system
→ Redpanda event streaming
→ MinIO local Bronze
→ ADLS Gen2 cloud Bronze
```

The platform contains:

* 250,000 banking transactions,
* 259,826 operational events,
* 31 days of historical activity,
* 744 hourly transaction partitions,
* approximately 2,764 Parquet files,
* complete local and cloud Bronze storage,
* zero Redpanda replay delivery errors.
