# Data Model

## 1. Overview

The source model is stored in PostgreSQL under:

```text
banking_source
```

The model contains:

- reference entities,
- transactional entities,
- compliance entities,
- risk entities,
- support entities,
- event entities,
- daily analytical snapshots.

---

## 2. Entity relationship overview

```text
customers
   └── accounts
         └── transactions
                ├── merchants
                └── atms

customers
   ├── kyc_aml_checks
   ├── customer_risk_scores
   ├── support_cases
   └── chargeback_cases

merchants
   ├── merchant_risk_scores
   └── chargeback_cases

transactions
   └── chargeback_cases

all operational entities
   └── stream_events
```

---

## 3. Tables

## 3.1 `customers`

Purpose:

Stores simulated bank customers.

Primary key:

```text
customer_id
```

Important columns:

- `registration_time`,
- `first_name`,
- `last_name`,
- `gender`,
- `birth_year`,
- `email`,
- `phone_number`,
- `country`,
- `city`,
- `customer_segment`,
- `customer_status`,
- `source_system`.

Used for:

- customer segmentation,
- customer 360,
- demographic analysis,
- activity analysis,
- risk analysis.

Export strategy:

- full snapshot to MinIO,
- later cloud Bronze snapshot.

---

## 3.2 `accounts`

Purpose:

Stores customer bank accounts.

Primary key:

```text
account_id
```

Important columns:

- `customer_id`,
- `opened_at`,
- `account_type`,
- `currency`,
- `account_status`,
- `daily_card_limit`,
- `daily_transfer_limit`,
- `daily_cash_withdrawal_limit`,
- `source_system`.

Used for:

- account portfolio analysis,
- transaction limit analysis,
- customer-account relationships,
- balance analysis.

Export strategy:

- full snapshot to MinIO.

---

## 3.3 `merchants`

Purpose:

Stores merchant reference data.

Primary key:

```text
merchant_id
```

Important columns:

- `merchant_name`,
- `merchant_category`,
- `city`,
- `country`,
- `terminal_id`,
- `merchant_status`,
- `source_system`.

Used for:

- merchant analysis,
- category analysis,
- location analysis,
- fraud analysis,
- chargeback analysis.

Export strategy:

- full snapshot to MinIO.

---

## 3.4 `atms`

Purpose:

Stores ATM reference data.

Primary key:

```text
atm_id
```

Important columns:

- `operator`,
- `city`,
- `country`,
- `location_type`,
- `atm_status`,
- `source_system`.

Used for:

- ATM withdrawal analysis,
- ATM location analysis,
- ATM risk analysis.

Export strategy:

- full snapshot to MinIO.

---

## 3.5 `transactions`

Purpose:

Stores all generated banking transactions.

Primary key:

```text
transaction_id
```

Important columns:

- `event_time`,
- `customer_id`,
- `account_id`,
- `merchant_id`,
- `atm_id`,
- `payment_method`,
- `channel`,
- `amount`,
- `currency`,
- `authorization_status`,
- `decline_reason`,
- `device_id`,
- `device_type`,
- `ip_country`,
- `city`,
- `risk_score`,
- `is_fraud_suspected`,
- `fraud_rule_hit`,
- `source_system`.

Payment methods:

- card payment,
- BLIK payment,
- online payment,
- bank transfer,
- ATM withdrawal.

Used for:

- transaction analytics,
- fraud analysis,
- customer behavior analysis,
- merchant analytics,
- channel analytics.

Export strategy:

- hourly Parquet files.

---

## 3.6 `kyc_aml_checks`

Purpose:

Stores KYC and AML checks.

Primary key:

```text
kyc_event_id
```

Important columns:

- `customer_id`,
- `event_time`,
- `kyc_status`,
- `aml_risk_score`,
- `aml_risk_level`,
- `pep_flag`,
- `sanctions_screening_status`,
- `source_system`.

Used for:

- compliance analytics,
- customer risk enrichment,
- AML reporting.

Export strategy:

- hourly when records exist.

---

## 3.7 `support_cases`

Purpose:

Stores customer support interactions.

Primary key:

```text
support_case_id
```

Important columns:

- `customer_id`,
- `created_at`,
- `case_type`,
- `case_status`,
- `priority`,
- `source_system`.

Used for:

- support workload analysis,
- fraud report analysis,
- customer experience analysis.

Export strategy:

- hourly when records exist.

---

## 3.8 `chargeback_cases`

Purpose:

Stores chargeback and dispute records.

Primary key:

```text
chargeback_id
```

Important columns:

- `transaction_id`,
- `customer_id`,
- `merchant_id`,
- `case_opened_at`,
- `reason_code`,
- `case_status`,
- `amount`,
- `currency`,
- `source_system`.

Used for:

- dispute analysis,
- merchant quality analysis,
- fraud confirmation,
- financial loss analysis.

Export strategy:

- hourly when records exist.

---

## 3.9 `customer_risk_scores`

Purpose:

Stores customer risk scoring results.

Primary key:

```text
risk_event_id
```

Important columns:

- `customer_id`,
- `score_time`,
- `risk_score`,
- `risk_level`,
- `main_risk_driver`,
- `model_version`,
- `source_system`.

Used for:

- customer risk segmentation,
- fraud enrichment,
- AML enrichment.

Export strategy:

- hourly when records exist.

---

## 3.10 `merchant_risk_scores`

Purpose:

Stores merchant risk scoring results.

Primary key:

```text
merchant_risk_event_id
```

Important columns:

- `merchant_id`,
- `score_time`,
- `merchant_risk_score`,
- `merchant_risk_level`,
- `main_risk_driver`,
- `source_system`.

Used for:

- merchant risk analysis,
- chargeback analysis,
- fraud enrichment.

Export strategy:

- hourly when records exist.

---

## 3.11 `device_blacklist`

Purpose:

Stores suspicious and blocked devices.

Primary key:

```text
blacklist_event_id
```

Important columns:

- `device_id`,
- `event_time`,
- `reason`,
- `severity`,
- `active_flag`,
- `source_system`.

Used for:

- account takeover analysis,
- device risk analysis,
- fraud detection.

Export strategy:

- hourly when records exist.

---

## 3.12 `daily_account_balances`

Purpose:

Stores daily account balance snapshots.

Primary key:

```text
balance_id
```

Important columns:

- `account_id`,
- `balance_date`,
- `opening_balance`,
- `closing_balance`,
- `available_balance`,
- `currency`,
- `source_system`,
- `created_at`.

Used for:

- average balance analysis,
- customer value analysis,
- liquidity reporting,
- daily account reporting.

Export strategy:

- one Parquet file per day.

---

## 3.13 `stream_events`

Purpose:

Stores operational events intended for replay to Redpanda.

Primary key:

```text
event_id
```

Important columns:

- `event_time`,
- `event_type`,
- `source_system`,
- `entity_type`,
- `entity_id`,
- `payload_json`,
- `replayed_flag`,
- `replayed_at`.

Used for:

- streaming replay,
- fraud event simulation,
- Event Hub forwarding,
- Fabric Eventstream input.

Not exported to MinIO because the streaming path is handled by Redpanda.

---

## 4. Data volumes

Current generated dataset:

| Table | Rows |
|---|---:|
| customers | 5,000 |
| accounts | 5,820 |
| merchants | 800 |
| atms | 250 |
| transactions | 250,000 |
| stream_events | 259,826 |
| kyc_aml_checks | 5,000 |
| support_cases | 2,039 |
| chargeback_cases | 2,500 |
| customer_risk_scores | 5,000 |
| merchant_risk_scores | 800 |
| device_blacklist | 729 |
| daily_account_balances | 176,745 |

---

## 5. Planned Silver model

Planned Silver entities:

- `silver_customers`,
- `silver_accounts`,
- `silver_transactions`,
- `silver_merchants`,
- `silver_atms`,
- `silver_kyc_aml`,
- `silver_support_cases`,
- `silver_chargebacks`,
- `silver_customer_risk`,
- `silver_merchant_risk`,
- `silver_daily_balances`.

Silver processing will include:

- schema normalization,
- timestamp normalization,
- deduplication,
- referential integrity checks,
- currency validation,
- standard status values,
- null handling,
- derived fraud indicators.

---

## 6. Planned Gold model

Planned Gold models:

- `gold_customer_360`,
- `gold_daily_transaction_summary`,
- `gold_customer_risk_profile`,
- `gold_fraud_alert_summary`,
- `gold_merchant_risk_summary`,
- `gold_chargeback_summary`,
- `gold_account_balance_summary`,
- `gold_atm_usage_summary`.
