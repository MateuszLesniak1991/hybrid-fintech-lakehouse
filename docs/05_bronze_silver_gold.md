# Bronze, Silver and Gold Architecture

## 1. Objective

The project follows a medallion architecture to separate raw ingestion, standardized data and business-ready datasets.

```text
Bronze
→ Silver
→ Gold
```

---

## 2. Bronze layer

## 2.1 Local Bronze

Technology:

```text
MinIO
```

Format:

```text
Parquet with Snappy compression
```

Purpose:

- preserve source data,
- retain hourly partitions,
- provide local on-premises copy,
- enable reprocessing,
- validate before cloud upload.

Example:

```text
s3://bronze/banking/transactions/
year=2026/month=05/day=16/hour=01/
transactions_20260516_01.parquet
```

## 2.2 Cloud Bronze

Technology:

```text
ADLS Gen2
```

Status: planned.

Purpose:

- cloud copy of raw source data,
- Databricks ingestion source,
- scalable storage,
- separation between storage and compute.

Bronze rules:

- preserve original values,
- preserve original timestamps,
- do not perform destructive transformations,
- add ingestion metadata where required,
- retain partition structure.

---

## 3. Silver layer

Technology:

```text
Databricks Delta Lake
```

Status: planned.

Silver processing will include:

- schema enforcement,
- type casting,
- timestamp normalization,
- deduplication,
- null handling,
- status standardization,
- reference validation,
- currency validation,
- risk score validation,
- enrichment through joins.

### Planned Silver transaction rules

- `transaction_id` must be unique,
- `amount` must be greater than zero,
- `currency` must be supported,
- `risk_score` must be between 0 and 100,
- `customer_id` must exist,
- `account_id` must exist,
- merchant must exist for merchant payments,
- ATM must exist for ATM withdrawals,
- authorization status must use approved values.

### Planned Silver tables

```text
silver_customers
silver_accounts
silver_transactions
silver_merchants
silver_atms
silver_kyc_aml_checks
silver_support_cases
silver_chargeback_cases
silver_customer_risk_scores
silver_merchant_risk_scores
silver_device_blacklist
silver_daily_account_balances
```

---

## 4. Gold layer

Gold contains business-ready aggregates and dimensional models.

### `gold_customer_360`

Possible fields:

- customer attributes,
- account count,
- active account count,
- total transaction count,
- total transaction value,
- average transaction value,
- fraud alert count,
- chargeback count,
- current risk level,
- average balance,
- support case count.

### `gold_daily_transaction_summary`

Dimensions:

- date,
- payment method,
- channel,
- currency,
- authorization status,
- merchant category.

Measures:

- transaction count,
- transaction value,
- average amount,
- decline count,
- fraud alert count.

### `gold_fraud_alert_summary`

Dimensions:

- date,
- hour,
- fraud rule,
- payment method,
- country,
- city,
- merchant category.

Measures:

- high-risk event count,
- suspected fraud amount,
- declined high-risk count,
- affected customer count.

### `gold_merchant_risk_summary`

Measures:

- transaction count,
- transaction value,
- fraud alert count,
- chargeback count,
- chargeback rate,
- average risk score.

### `gold_account_balance_summary`

Measures:

- opening balance,
- closing balance,
- available balance,
- average balance,
- balance change.

### `gold_atm_usage_summary`

Measures:

- withdrawal count,
- withdrawal value,
- declined withdrawal count,
- high-risk withdrawal count.

---

## 5. Partitioning strategy

### Bronze

Partitioning:

```text
year/month/day/hour
```

for hourly event datasets.

Daily balances use:

```text
year/month/day
```

Snapshots use:

```text
snapshot_date
```

### Silver

Planned partitioning:

- transactions by `event_date`,
- support and chargebacks by event date,
- balances by `balance_date`.

### Gold

Partition only where it improves query performance. Avoid excessive small partitions.

---

## 6. File size considerations

The current portfolio dataset intentionally creates one file per source hour.

In a larger production environment, small files would be compacted in Databricks using:

- Delta optimization,
- auto compaction,
- optimized writes,
- scheduled compaction jobs.

---

## 7. Lineage

Planned lineage example:

```text
PostgreSQL banking_source.transactions
→ MinIO hourly Parquet
→ ADLS Bronze
→ bronze_transactions
→ silver_transactions
→ gold_daily_transaction_summary
→ Power BI
```

---

## 8. Reprocessing

The Bronze layer is immutable and retains the original data.

This allows:

- replay of failed transformations,
- rule changes,
- schema evolution,
- historical backfills,
- audit and validation.
