# Project Results

## 1. Current status

The local on-premises part of the project is operational.

Completed:

- PostgreSQL source system,
- historical banking generator,
- realistic banking data model,
- Redpanda replay,
- hourly Parquet export,
- MinIO Bronze storage,
- Azure Event Hub connectivity test,
- repository cleanup and documentation.

Planned:

- Redpanda to Event Hub bridge,
- MinIO to ADLS synchronization,
- Fabric Eventstream,
- Databricks Bronze/Silver/Gold,
- Power BI dashboards,
- CI/CD.

---

## 2. Generated source dataset

| Dataset | Row count |
|---|---:|
| Customers | 5,000 |
| Accounts | 5,820 |
| Merchants | 800 |
| ATMs | 250 |
| Transactions | 250,000 |
| Stream events | 259,826 |
| KYC/AML checks | 5,000 |
| Support cases | 2,039 |
| Chargeback cases | 2,500 |
| Customer risk scores | 5,000 |
| Merchant risk scores | 800 |
| Device blacklist | 729 |
| Daily account balances | 176,745 |

---

## 3. Historical coverage

Transaction history:

```text
2026-05-16 00:03:19 UTC
→ 2026-06-15 23:59:40 UTC
```

Coverage:

- 31 days,
- all 24 hours represented,
- random realistic hourly distribution.

---

## 4. Generation performance

Full dataset generation:

```text
250,000 transactions
```

Execution time:

```text
approximately 1 minute 25 seconds
```

Environment:

- PostgreSQL in Docker,
- Python batch inserts,
- local Ubuntu Server.

---

## 5. Streaming replay result

Replay source:

```text
banking_source.stream_events
```

Target:

```text
Redpanda topic: banking.operational.events
```

Result:

| Metric | Value |
|---|---:|
| Processed events | 259,826 |
| Delivered events | 259,826 |
| Delivery errors | 0 |
| Replay duration | 2,822.07 seconds |
| Real time | approximately 47 minutes |

Timestamp strategy:

```text
event_time = current replay time
original_event_time = original historical time
```

This allows:

- fresh real-time event demonstrations,
- preserved monthly historical context.

---

## 6. MinIO Bronze export result

### Transactions

| Metric | Value |
|---|---:|
| Files | 744 |
| Size | 43.64 MB |
| Partitioning | year/month/day/hour |
| Format | Parquet |
| Compression | Snappy |

The file count matches:

```text
31 days × 24 hours = 744
```

### Other datasets

| Dataset | Files | Approximate size |
|---|---:|---:|
| KYC/AML checks | 5 | 0.02 MB |
| Support cases | 548 | 1.50 MB |
| Chargebacks | 612 | 2.42 MB |
| Customer risk scores | 8 | 0.02 MB |
| Merchant risk scores | 414 | 1.12 MB |
| Device blacklist | 398 | 0.92 MB |
| Customer snapshot | 1 | 0.35 MB |
| Account snapshot | 1 | 0.44 MB |
| Merchant snapshot | 1 | 0.07 MB |
| ATM snapshot | 1 | 0.01 MB |
| Daily balances | 31 | 15.42 MB |

Approximate totals:

```text
2,764 Parquet files
65.9 MB
```

---

## 7. Validated end-to-end local flows

### Streaming

```text
PostgreSQL
→ Python replay
→ Redpanda
```

Status:

```text
Completed
```

### Batch

```text
PostgreSQL
→ hourly Parquet
→ MinIO Bronze
```

Status:

```text
Completed
```

### Azure connectivity

```text
Python
→ Azure Event Hub
```

Status:

```text
Tested successfully
```

---

## 8. Evidence available in the project

The project can demonstrate:

- PostgreSQL tables in pgAdmin,
- Redpanda topic and messages,
- fresh and original timestamps,
- MinIO hourly partitions,
- Parquet files,
- source schemas,
- replay status,
- generated data volumes,
- reproducible Python scripts.

---

## 9. Key engineering outcomes

The project demonstrates:

- hybrid on-premises/cloud design,
- batch and streaming architecture,
- event replay,
- idempotent Kafka delivery,
- restartable processing,
- hourly partitioning,
- Parquet and Snappy,
- source-to-lake design,
- medallion architecture planning,
- realistic banking data simulation.

---

## 10. Next milestones

1. Implement Redpanda to Event Hub bridge.
2. Implement MinIO to ADLS synchronization.
3. Configure Fabric Eventstream.
4. Build Databricks Bronze ingestion.
5. Build Silver transformations.
6. Build Gold business models.
7. Create Power BI dashboards.
8. Add automated quality checks.
9. Add GitHub Actions CI/CD.
