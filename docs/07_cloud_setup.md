# Cloud Setup

## 1. Status

This document describes the planned cloud implementation.

Current status:

| Component | Status |
|---|---|
| Azure Event Hub namespace | Tested |
| Event Hub message send | Tested |
| Redpanda to Event Hub bridge | Planned |
| ADLS Gen2 | Planned |
| Microsoft Fabric Eventstream | Planned |
| Databricks | Planned |
| Power BI | Planned |

---

## 2. Azure Event Hub

Recommended initial settings for a portfolio environment:

- pricing tier: Basic,
- throughput units: 1,
- auto-inflate: disabled,
- retention: 1 day,
- Capture: disabled,
- partitions: 2.

The target Event Hub can be named:

```text
banking-operational-events
```

Required `.env` values:

```env
AZURE_EVENTHUB_CONNECTION_STRING=...
AZURE_EVENTHUB_NAME=banking-operational-events
```

Connectivity test:

```bash
python cloud/azure/test_eventhub_send.py
```

Validation in Azure Portal:

- Incoming Messages,
- Incoming Requests,
- Incoming Bytes,
- Successful Requests.

---

## 3. Redpanda to Event Hub bridge

Planned file:

```text
cloud/azure/redpanda_to_eventhub.py
```

Planned behavior:

1. Consume from `banking.operational.events`.
2. Deserialize the JSON message.
3. Preserve `event_id`.
4. Add cloud forwarding metadata.
5. Send to Event Hub.
6. Commit Redpanda offset only after successful Event Hub delivery.
7. Support restart using a dedicated consumer group.

Consumer group:

```text
eventhub-forwarder
```

---

## 4. Microsoft Fabric Eventstream

Planned configuration:

1. Create a Fabric workspace.
2. Create an Eventstream.
3. Add Azure Event Hub as a source.
4. Select `banking-operational-events`.
5. Inspect incoming JSON schema.
6. Add filters for fraud-related events.
7. Route results to a real-time destination.

Recommended filters:

```text
event_type = high_risk_transaction_detected
OR payload.is_fraud_suspected = true
OR payload.risk_score >= 75
```

Potential destinations:

- Eventhouse/KQL database,
- Lakehouse,
- custom endpoint,
- Power BI real-time report.

---

## 5. ADLS Gen2

Planned account configuration:

- hierarchical namespace: enabled,
- container: `bronze`,
- private access,
- least-privilege service principal or managed identity.

Planned structure:

```text
bronze/
  banking/
    transactions/
    customer/
    account/
    compliance/
    support/
    fraud/
    risk/
    reference/
```

The MinIO partition paths should be preserved.

---

## 6. MinIO to ADLS synchronization

Planned file:

```text
cloud/azure/minio_to_adls.py
```

Planned features:

- paginated MinIO object listing,
- ADLS target path creation,
- multipart-safe uploads,
- skip existing objects,
- optional overwrite,
- progress logging,
- retry policy,
- checksum or size validation,
- resumable execution.

---

## 7. Databricks

Planned workspace tasks:

### Bronze

- read Parquet from ADLS,
- add ingestion metadata,
- write Delta tables,
- preserve source fields.

### Silver

- enforce schemas,
- cast data types,
- deduplicate,
- validate references,
- normalize statuses,
- derive date and fraud fields.

### Gold

- create customer 360,
- create transaction summaries,
- create fraud summaries,
- create merchant risk summaries,
- create balance summaries.

Potential notebook structure:

```text
databricks/
  01_bronze_ingestion.py
  02_silver_customers.py
  03_silver_transactions.py
  04_silver_risk.py
  05_gold_customer_360.py
  06_gold_fraud_summary.py
```

---

## 8. Power BI

Planned dashboards:

### Real-time fraud dashboard

Sources:

- Fabric real-time destination.

Metrics:

- high-risk events per minute,
- fraud amount,
- top fraud rules,
- affected customers,
- high-risk countries,
- suspicious devices.

### Customer analytics dashboard

Sources:

- Databricks Gold tables.

Metrics:

- active customers,
- transaction volume,
- customer segments,
- balances,
- chargebacks,
- merchant categories,
- risk distribution.

---

## 9. Cloud security

Planned controls:

- secrets outside source code,
- managed identities where possible,
- scoped access keys,
- restricted network access,
- private storage containers,
- minimum required permissions,
- no credentials in screenshots or logs.

---

## 10. Cost control

Portfolio recommendations:

- smallest practical service tiers,
- short retention,
- disabled Event Hub Capture initially,
- limited Databricks cluster runtime,
- automatic cluster termination,
- delete unused resources,
- monitor Azure Cost Management.
