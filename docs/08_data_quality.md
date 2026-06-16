# Data Quality

## 1. Objective

Data quality controls ensure that data is complete, valid, unique, consistent and suitable for analytics.

Controls are divided into:

- source validation,
- batch file validation,
- streaming validation,
- Silver transformation validation,
- business rule validation.

---

## 2. Source-level checks

## 2.1 Transaction uniqueness

```sql
SELECT transaction_id, COUNT(*)
FROM banking_source.transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
```

Expected result:

```text
0 rows
```

## 2.2 Event uniqueness

```sql
SELECT event_id, COUNT(*)
FROM banking_source.stream_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

Expected result:

```text
0 rows
```

## 2.3 Mandatory transaction fields

```sql
SELECT COUNT(*)
FROM banking_source.transactions
WHERE transaction_id IS NULL
   OR event_time IS NULL
   OR customer_id IS NULL
   OR account_id IS NULL
   OR amount IS NULL
   OR currency IS NULL;
```

Expected result:

```text
0
```

## 2.4 Positive transaction amount

```sql
SELECT COUNT(*)
FROM banking_source.transactions
WHERE amount <= 0;
```

Expected result:

```text
0
```

## 2.5 Valid risk score

```sql
SELECT COUNT(*)
FROM banking_source.transactions
WHERE risk_score < 0 OR risk_score > 100;
```

Expected result:

```text
0
```

## 2.6 Valid currencies

```sql
SELECT currency, COUNT(*)
FROM banking_source.transactions
GROUP BY currency
ORDER BY currency;
```

Allowed values:

- PLN,
- EUR,
- USD.

## 2.7 Customer reference integrity

```sql
SELECT COUNT(*)
FROM banking_source.transactions t
LEFT JOIN banking_source.customers c
  ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

Expected result:

```text
0
```

## 2.8 Account reference integrity

```sql
SELECT COUNT(*)
FROM banking_source.transactions t
LEFT JOIN banking_source.accounts a
  ON t.account_id = a.account_id
WHERE a.account_id IS NULL;
```

Expected result:

```text
0
```

---

## 3. Business consistency checks

### Merchant payments

For card, BLIK and online transactions, `merchant_id` should be populated.

```sql
SELECT COUNT(*)
FROM banking_source.transactions
WHERE payment_method IN (
    'card_payment',
    'blik_payment',
    'online_payment'
)
AND merchant_id IS NULL;
```

### ATM withdrawals

For ATM withdrawals, `atm_id` should be populated.

```sql
SELECT COUNT(*)
FROM banking_source.transactions
WHERE payment_method = 'atm_withdrawal'
  AND atm_id IS NULL;
```

### Fraud flag consistency

```sql
SELECT COUNT(*)
FROM banking_source.transactions
WHERE risk_score >= 75
  AND is_fraud_suspected = FALSE;
```

Any result must be reviewed against the simulation rules.

---

## 4. Time-range validation

```sql
SELECT
    MIN(event_time),
    MAX(event_time),
    COUNT(DISTINCT event_time::date)
FROM banking_source.transactions;
```

Expected:

- start: 2026-05-16,
- end: 2026-06-15,
- days: 31.

---

## 5. Streaming replay validation

## 5.1 Replay completion

```sql
SELECT replayed_flag, COUNT(*)
FROM banking_source.stream_events
GROUP BY replayed_flag;
```

After full replay:

```text
true = 259826
false = 0
```

## 5.2 Delivery validation

Validated replay result:

- processed: 259,826,
- delivered: 259,826,
- errors: 0.

## 5.3 Timestamp validation

Each replayed event should contain:

- `event_time`,
- `original_event_time`,
- `replay_metadata.timestamp_strategy`.

---

## 6. Parquet validation

## 6.1 Hourly transaction file count

Expected:

```text
31 days × 24 hours = 744 files
```

## 6.2 Parquet row count

All transaction Parquet files combined should contain:

```text
250,000 rows
```

## 6.3 Partition validation

Expected path fields:

- year,
- month,
- day,
- hour.

## 6.4 Schema validation

Expected transaction columns include:

- transaction_id,
- event_time,
- customer_id,
- account_id,
- payment_method,
- amount,
- currency,
- risk_score,
- is_fraud_suspected.

---

## 7. Planned Silver quality controls

Planned implementation in Databricks:

- unique-key checks,
- null checks,
- accepted-value checks,
- referential integrity checks,
- freshness checks,
- row-count reconciliation,
- duplicate detection,
- schema drift detection.

Rejected rows will be written to quarantine tables with:

- rejection reason,
- ingestion timestamp,
- source file,
- source partition.

---

## 8. Reconciliation

Planned reconciliation checks:

```text
PostgreSQL source rows
= MinIO Parquet rows
= ADLS Bronze rows
= Databricks Bronze rows
```

For transactions:

```text
250,000 = 250,000 = 250,000 = 250,000
```

---

## 9. Monitoring metrics

Recommended metrics:

- source row count,
- exported row count,
- file count,
- failed file count,
- duplicate count,
- null-key count,
- replay lag,
- replay failure count,
- Event Hub delivery count,
- rejected Silver row count.
