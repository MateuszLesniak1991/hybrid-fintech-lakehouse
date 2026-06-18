# Business Scenario

## 1. Project objective

The Hybrid FinTech Lakehouse Platform simulates a realistic banking data environment in which multiple operational systems generate transactional, customer, account, risk and compliance data.

The project demonstrates two parallel data processing patterns:

1. **Continuous near real-time event processing** for fraud detection and operational monitoring.
2. **Automated hourly batch processing** for customer analytics, risk analysis and business reporting.

The architecture combines local infrastructure, Microsoft Azure services and Microsoft Fabric to demonstrate a hybrid on-premises/cloud data platform.

---

## 2. Simulated banking environment

The Python generators represent several independent banking and payment systems.

### Core Banking System

Responsible for:

* customer accounts,
* account opening,
* account status,
* transaction limits,
* balances,
* account currencies,
* bank transfers.

### Payment Gateway

Generates:

* card payments,
* BLIK payments,
* online payments,
* transaction authorization results,
* declines,
* device and channel information.

### ATM Network

Generates:

* ATM withdrawals,
* ATM identifiers,
* ATM operator information,
* location data,
* withdrawal authorization results.

### CRM

Generates:

* customer registrations,
* customer attributes,
* segment information,
* contact information,
* customer status.

### KYC/AML System

Generates:

* KYC verification results,
* AML risk scores,
* sanctions screening results,
* politically exposed person flags,
* compliance review outcomes.

### Fraud and Risk Engine

Generates:

* customer risk scores,
* merchant risk scores,
* high-risk transaction events,
* fraud rule matches,
* suspicious geolocation events,
* device blacklist events,
* transaction velocity alerts.

### Customer Support System

Generates:

* customer service cases,
* fraud reports,
* transaction disputes,
* account access problems,
* limit change requests.

### Chargeback Management System

Generates:

* chargeback cases,
* dispute reasons,
* case statuses,
* disputed amounts,
* links to original transactions.

---

## 3. Simulated customer activity

Customers perform realistic banking operations such as:

* opening accounts,
* making card payments,
* paying with BLIK,
* making online purchases,
* sending bank transfers,
* withdrawing cash from ATMs,
* reporting suspicious transactions,
* opening support cases,
* submitting chargebacks.

Transactions are generated across:

* different cities,
* different merchants,
* different merchant categories,
* different currencies,
* different payment channels,
* different devices,
* different IP countries,
* different customer segments.

The platform supports two data generation modes:

### Historical generation

A 31-day historical dataset is generated for repeatable batch processing and analytical workloads.

### Continuous real-time generation

New banking transactions are generated individually every few seconds and permanently stored in PostgreSQL.

Each real-time transaction creates:

* one record in `banking_source.transactions`,
* one matching event in `banking_source.stream_events`.

This allows the platform to continuously produce fresh operational data without resetting or replacing the historical dataset.

---

## 4. Business processing paths

## 4.1 Streaming path — real-time fraud detection

The streaming path supports both historical event replay and continuously generated banking events.

The final real-time path is:

```text
Banking transaction generator
→ PostgreSQL transactions
→ PostgreSQL transactional outbox
→ Redpanda
→ Python Event Hub bridge
→ Azure Event Hubs
→ Microsoft Fabric Eventstream
→ high-risk transaction filter
→ Fabric Eventhouse
→ KQL fraud analysis
→ Power BI
```

The transactional outbox is implemented using:

```text
banking_source.stream_events
```

The generator stores the banking transaction and its corresponding outbox event in PostgreSQL. The outbox publisher reads only new real-time events with:

```text
source_system = core_banking_realtime
replayed_flag = false
```

After Redpanda confirms successful delivery, the outbox record is updated:

```text
replayed_flag = true
replayed_at   = delivery timestamp
```

Historical unreplayed events are ignored by the continuous publisher.

Typical streaming events include:

* `transaction_authorized`,
* `transaction_declined`,
* `high_risk_transaction_detected`,
* `device_blacklist_updated`,
* `customer_risk_score_updated`,
* `merchant_risk_score_updated`,
* `chargeback_created`.

The purpose is to detect suspicious behavior quickly, including:

* unusually large transactions,
* transactions from unusual countries,
* high-risk merchant categories,
* large ATM withdrawals,
* repeated declines,
* new or suspicious devices,
* rapid transaction velocity,
* suspected account takeover,
* impossible travel scenarios.

Example fraud rules include:

* `HIGH_VALUE_TRANSACTION`,
* `IMPOSSIBLE_TRAVEL`,
* `BLACKLISTED_DEVICE`,
* `VELOCITY_BREACH`,
* `UNUSUAL_GEOLOCATION`.

### Microsoft Fabric event processing

Azure Event Hubs is connected to Microsoft Fabric Eventstream.

The implemented Eventstream flow is:

```text
Azure Event Hubs source
→ Eventstream
→ event_type filter
→ Eventhouse destination
```

The filter forwards only:

```text
event_type = high_risk_transaction_detected
```

All incoming events remain visible in Eventstream, while only high-risk transactions are written to:

```text
fraud_events_realtime
```

The Eventhouse data is queried using KQL and can be used by Power BI for real-time fraud monitoring.

---

## 4.2 Batch path — customer and risk analytics

Data used for historical and analytical workloads is exported from PostgreSQL to Parquet files and stored in MinIO.

The batch path is:

```text
PostgreSQL domain tables
→ scheduled hourly Parquet exporter
→ MinIO Bronze
→ Python MinIO to ADLS synchronization
→ ADLS Gen2 Bronze
```

The hourly process exports only the previous completed UTC hour.

Example:

```text
Execution time: 10:05 UTC
Exported range: 09:00:00–09:59:59 UTC
```

The process is triggered by cron:

```cron
5 * * * * /home/dataeng/portfolio/hybrid-fintech-lakehouse/scripts/run_hourly_minio_export.sh
```

The hourly wrapper:

1. calculates the previous completed UTC hour,
2. prevents overlapping executions using a file lock,
3. executes the PostgreSQL-to-MinIO exporter,
4. exports only the closed hourly window,
5. skips historical snapshots and daily balances,
6. avoids overwriting existing valid partitions,
7. stores execution logs.

Generated partition structure:

```text
s3://bronze/banking/transactions/
year=YYYY/
month=MM/
day=DD/
hour=HH/
transactions_YYYYMMDD_HH.parquet
```

The batch path supports:

* customer segmentation,
* customer 360 analysis,
* account portfolio analysis,
* balance analysis,
* transaction behavior analysis,
* merchant risk analysis,
* ATM usage analysis,
* KYC/AML analysis,
* chargeback analysis,
* support case analysis.

The next cloud processing layers can extend the path into:

```text
ADLS Gen2 Bronze
→ Silver transformations
→ Gold analytical models
→ Power BI
```

---

## 5. Business questions

The platform is designed to answer questions such as:

* Which customers generate the highest transaction volume?
* Which customer segments have the highest average balances?
* Which merchant categories produce the most fraud alerts?
* Which countries and devices are associated with suspicious transactions?
* Which merchants have high chargeback rates?
* Which customers show unusual transaction behavior?
* Which ATMs generate the most high-risk withdrawals?
* How many transactions are authorized or declined per hour?
* What is the relationship between customer risk and transaction fraud?
* Which fraud rules are triggered most frequently?
* What is the total value of high-risk transactions?
* How quickly can high-risk events be delivered to Microsoft Fabric?
* How many new transactions are exported into each hourly Bronze partition?
* Are streaming and batch pipelines processing the same operational source data consistently?

---

## 6. Initial historical load and continuous processing

The project uses an initial historical load instead of waiting one month for data collection.

The historical generator created:

* 31 days of activity,
* realistic event timestamps,
* random hourly distribution,
* realistic transaction types,
* multiple business entities,
* fraud, risk and compliance scenarios.

For historical streaming demonstrations:

* `event_time` is replaced with the replay timestamp,
* `original_event_time` preserves the historical timestamp,
* the original monthly history remains available for analysis.

The continuous generator then adds new transactions without resetting the historical dataset.

This creates two complementary operating modes:

```text
Historical mode
→ repeatable replay and batch analytics
```

```text
Continuous mode
→ fresh operational transactions and real-time fraud detection
```

New transactions are stored permanently in PostgreSQL and are processed by both paths:

```text
Streaming:
PostgreSQL → Redpanda → Event Hubs → Fabric
```

```text
Batch:
PostgreSQL → hourly Parquet → MinIO
```

---

## 7. Business value

The project demonstrates how a bank can use the same operational source data for two different objectives.

### Operational value

* faster fraud detection,
* continuous transaction monitoring,
* rapid alerting,
* reduced response time,
* near real-time visibility,
* fraud rule analysis,
* immediate Eventhouse ingestion.

### Analytical value

* better customer understanding,
* historical trend analysis,
* risk reporting,
* customer segmentation,
* hourly transaction reporting,
* reproducible analytical datasets,
* data-driven business decisions.

### Engineering value

* transactional outbox reliability,
* separation of historical and real-time workloads,
* Kafka-compatible event streaming,
* restartable Event Hub delivery,
* cloud-based stream processing,
* incremental hourly batch processing,
* idempotent Parquet partition creation,
* hybrid local and cloud data architecture.
