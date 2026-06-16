# Business Scenario

## 1. Project objective

The Hybrid FinTech Lakehouse Platform simulates a realistic banking data environment in which multiple operational systems generate transactional, customer, account, risk and compliance data.

The project demonstrates two parallel processing patterns:

1. **Near real-time event processing** for fraud detection and operational monitoring.
2. **Hourly batch processing** for customer analytics, risk analysis and business reporting.

The architecture combines local infrastructure with Microsoft Azure services to demonstrate a hybrid on-premises/cloud data platform.

---

## 2. Simulated banking environment

The Python generator represents several independent banking and payment systems.

### Core Banking System

Responsible for:

- customer accounts,
- account opening,
- account status,
- transaction limits,
- balances,
- account currencies.

### Payment Gateway

Generates:

- card payments,
- BLIK payments,
- online payments,
- transaction authorization results,
- declines,
- device and channel information.

### ATM Network

Generates:

- ATM withdrawals,
- ATM identifiers,
- ATM operator information,
- location data,
- withdrawal authorization results.

### CRM

Generates:

- customer registrations,
- customer attributes,
- segment information,
- contact information,
- customer status.

### KYC/AML System

Generates:

- KYC verification results,
- AML risk scores,
- sanctions screening results,
- politically exposed person flags,
- compliance review outcomes.

### Fraud and Risk Engine

Generates:

- customer risk scores,
- merchant risk scores,
- high-risk transaction events,
- fraud rule matches,
- device blacklist events.

### Customer Support System

Generates:

- customer service cases,
- fraud reports,
- transaction disputes,
- account access problems,
- limit change requests.

### Chargeback Management System

Generates:

- chargeback cases,
- dispute reasons,
- case statuses,
- disputed amounts,
- links to original transactions.

---

## 3. Simulated customer activity

Customers perform realistic banking operations such as:

- opening accounts,
- making card payments,
- paying with BLIK,
- making online purchases,
- sending bank transfers,
- withdrawing cash from ATMs,
- reporting suspicious transactions,
- opening support cases,
- submitting chargebacks.

Transactions are generated across:

- different cities,
- different merchants,
- different merchant categories,
- different currencies,
- different payment channels,
- different devices,
- different IP countries,
- different customer segments.

---

## 4. Business processing paths

## 4.1 Streaming path — fraud detection

Operational events that require fast analysis are stored in PostgreSQL and replayed to Redpanda.

The streaming path is:

```text
PostgreSQL stream_events
→ Python replay producer
→ Redpanda
→ Python Event Hub bridge
→ Azure Event Hub
→ Microsoft Fabric Eventstream
→ Real-Time Intelligence
→ Fraud monitoring dashboard
```

Typical streaming events include:

- `card_payment_authorized`,
- `card_payment_declined`,
- `blik_payment_authorized`,
- `online_payment_declined`,
- `atm_withdrawal_authorized`,
- `bank_transfer_declined`,
- `high_risk_transaction_detected`,
- `device_blacklist_updated`,
- `customer_risk_score_updated`,
- `chargeback_created`.

The purpose is to detect suspicious behavior quickly, including:

- unusually large transactions,
- transactions from unusual countries,
- high-risk merchant categories,
- large ATM withdrawals,
- repeated declines,
- new or suspicious devices,
- rapid transaction velocity,
- suspected account takeover.

---

## 4.2 Batch path — customer and risk analytics

Data used for historical and analytical workloads is exported from PostgreSQL to hourly Parquet files and stored in MinIO.

The batch path is:

```text
PostgreSQL domain tables
→ Python hourly Parquet exporter
→ MinIO Bronze
→ Python MinIO to ADLS synchronization
→ ADLS Gen2 Bronze
→ Databricks Bronze
→ Databricks Silver
→ Databricks Gold
→ Power BI / Tableau
```

The batch path supports:

- customer segmentation,
- customer 360 analysis,
- account portfolio analysis,
- balance analysis,
- transaction behavior analysis,
- merchant risk analysis,
- ATM usage analysis,
- KYC/AML analysis,
- chargeback analysis,
- support case analysis.

---

## 5. Business questions

The platform is designed to answer questions such as:

- Which customers generate the highest transaction volume?
- Which customer segments have the highest average balances?
- Which merchant categories produce the most fraud alerts?
- Which countries and devices are associated with suspicious transactions?
- Which merchants have high chargeback rates?
- Which customers show unusual transaction behavior?
- Which ATMs generate the most high-risk withdrawals?
- How many transactions are authorized or declined per hour?
- What is the relationship between customer risk and transaction fraud?
- How quickly can high-risk events be delivered to a real-time analytics platform?

---

## 6. Initial historical load and replay

The project uses an initial historical load instead of waiting one month for data collection.

The generator created:

- 31 days of activity,
- realistic event timestamps,
- random hourly distribution,
- realistic transaction types,
- multiple business entities.

For streaming demonstrations:

- `event_time` is replaced with the replay timestamp,
- `original_event_time` preserves the historical timestamp,
- the original monthly history remains available for analysis.

This allows the same dataset to support both:

- historical analytics,
- fresh-looking real-time streaming demonstrations.

---

## 7. Business value

The project demonstrates how a bank can use the same operational source data for two different objectives:

### Operational value

- faster fraud detection,
- real-time monitoring,
- rapid alerting,
- reduced response time.

### Analytical value

- better customer understanding,
- historical trend analysis,
- risk reporting,
- customer segmentation,
- data-driven business decisions.

