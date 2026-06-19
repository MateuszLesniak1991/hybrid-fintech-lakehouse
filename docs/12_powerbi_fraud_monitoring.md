# Power BI Fraud Monitoring Report

## 1. Purpose

The Power BI Fraud Monitoring Report provides a business-oriented analytical layer for fraud monitoring on top of Microsoft Fabric Eventhouse data.

The report is designed for:

- fraud analysts,
- risk and compliance teams,
- business stakeholders,
- data engineering reviewers,
- portfolio presentation.

It complements the Microsoft Fabric Real-Time Dashboard by providing a more business-friendly reporting view with KPI cards, slicers, trend analysis and detailed transaction tables.

---

## 2. Architecture context

The Power BI report is the final visualization layer of the real-time fraud analytics path:

```text
Real-time banking generator
→ PostgreSQL transactional outbox
→ Redpanda
→ Azure Event Hubs
→ Microsoft Fabric Eventstream
→ high-risk transaction filter
→ Fabric Eventhouse / KQL database
→ Power BI report
```

The report uses high-risk transaction events stored in:

```text
fraud_events_realtime
```

---

## 3. Reporting model

Power BI connects to a KQL reporting function:

```text
fraud_events_powerbi()
```

The function flattens the dynamic Eventhouse payload and exposes a reporting-friendly table.

The model combines two types of fraud events:

| Source category | Description |
|---|---|
| `historical_replay` | High-risk events replayed from the historical simulated banking dataset |
| `core_banking_realtime` | Newly generated real-time fraud events from the continuous generator |

The report uses a unified business timestamp:

```text
business_event_timestamp
```

This timestamp uses:

- `payload.original_event_time` for historical replay events,
- `event_time` for real-time events.

This allows historical and live events to be analyzed together on the same timeline.

---

## 4. KQL reporting function

```kusto
.create-or-alter function fraud_events_powerbi()
{
    fraud_events_realtime
    | extend event_timestamp = todatetime(event_time)
    | extend original_event_timestamp = todatetime(payload.original_event_time)
    | extend business_event_timestamp = iff(isnotempty(original_event_timestamp), original_event_timestamp, event_timestamp)
    | extend source_category = iff(isnotempty(original_event_timestamp), "historical_replay", "core_banking_realtime")
    | extend source_system = iff(isempty(tostring(payload.source_system)), source_category, tostring(payload.source_system))
    | extend event_type = iff(isempty(tostring(payload.event_type)), "high_risk_transaction_detected", tostring(payload.event_type))
    | extend event_hour = bin(event_timestamp, 1h)
    | extend event_minute = bin(event_timestamp, 1m)
    | extend business_event_day = bin(business_event_timestamp, 1d)
    | extend business_event_hour = bin(business_event_timestamp, 1h)
    | extend transaction_id = tostring(payload.transaction_id)
    | extend customer_id = tostring(payload.customer_id)
    | extend account_id = tostring(payload.account_id)
    | extend merchant_id = tostring(payload.merchant_id)
    | extend merchant_name = tostring(payload.merchant_name)
    | extend merchant_category = tostring(payload.merchant_category)
    | extend amount = todouble(payload.amount)
    | extend currency = tostring(payload.currency)
    | extend risk_score = toint(payload.risk_score)
    | extend fraud_rule_raw = tostring(payload.fraud_rule_hit)
    | extend fraud_rule = case(
        fraud_rule_raw == "unusual_country", "UNUSUAL_GEOLOCATION",
        fraud_rule_raw == "high_value_transaction", "HIGH_VALUE_TRANSACTION",
        fraud_rule_raw == "velocity_breach", "VELOCITY_BREACH",
        fraud_rule_raw == "blacklisted_device", "BLACKLISTED_DEVICE",
        fraud_rule_raw == "impossible_travel", "IMPOSSIBLE_TRAVEL",
        toupper(fraud_rule_raw)
    )
    | extend city = tostring(payload.city)
    | extend country = tostring(payload.country)
    | extend ip_country = tostring(payload.ip_country)
    | extend channel = toupper(tostring(payload.channel))
    | extend payment_method = toupper(tostring(payload.payment_method))
    | extend authorization_status = toupper(tostring(payload.authorization_status))
    | extend decline_reason = tostring(payload.decline_reason)
    | extend device_id = tostring(payload.device_id)
    | extend device_type = tostring(payload.device_type)
    | project
        event_timestamp,
        event_hour,
        event_minute,
        business_event_timestamp,
        business_event_day,
        business_event_hour,
        original_event_timestamp,
        source_category,
        source_system,
        event_type,
        transaction_id,
        customer_id,
        account_id,
        merchant_id,
        merchant_name,
        merchant_category,
        amount,
        currency,
        risk_score,
        fraud_rule,
        city,
        country,
        ip_country,
        channel,
        payment_method,
        authorization_status,
        decline_reason,
        device_id,
        device_type
}
```

Validation query:

```kusto
fraud_events_powerbi()
| summarize events = count() by source_category
```

---

## 5. Power BI connection

Power BI Desktop connects to Fabric Eventhouse / KQL database using the Azure Data Explorer connector.

Connection mode:

```text
DirectQuery
```

Reason:

- the report should query current Eventhouse data,
- data continues to arrive through the streaming pipeline,
- the report should avoid static local imports for real-time monitoring.

Source:

```text
Fabric Eventhouse / KQL database
```

Reporting object:

```text
fraud_events_powerbi()
```

---

## 6. Report page

Report name:

```text
pbi-realtime-fraud-monitoring
```

Page name:

```text
Real-Time Fraud Monitoring
```

The page contains:

- KPI cards,
- fraud trend chart,
- fraud-rule analysis,
- suspicious amount analysis,
- latest high-risk transaction table,
- slicers.

---

## 7. Measures

The report uses the following DAX measures.

```DAX
Total_Fraud_Alerts =
COUNTROWS('fraud_events_powerbi')
```

```DAX
Suspicious_Amount =
SUM('fraud_events_powerbi'[amount])
```

```DAX
Average_Risk_Score =
AVERAGE('fraud_events_powerbi'[risk_score])
```

```DAX
Affected_Customers =
DISTINCTCOUNT('fraud_events_powerbi'[customer_id])
```

```DAX
Affected_Merchants =
DISTINCTCOUNT('fraud_events_powerbi'[merchant_id])
```

Optional measures:

```DAX
Historical_Fraud_Alerts =
CALCULATE(
    [Total_Fraud_Alerts],
    'fraud_events_powerbi'[source_category] = "historical_replay"
)
```

```DAX
Realtime_Fraud_Alerts =
CALCULATE(
    [Total_Fraud_Alerts],
    'fraud_events_powerbi'[source_category] = "core_banking_realtime"
)
```

---

## 8. Visuals

## 8.1 KPI cards

KPI cards:

- Total Fraud Alerts,
- Suspicious Amount,
- Average Risk Score,
- Affected Customers,
- Affected Merchants.

Formatting:

| KPI | Format |
|---|---|
| Total Fraud Alerts | Whole number |
| Suspicious Amount | Currency, PLN |
| Average Risk Score | Decimal number, 2 decimal places |
| Affected Customers | Whole number |
| Affected Merchants | Whole number |

---

## 8.2 Fraud alerts over time

Purpose:

- shows fraud activity over business time,
- combines historical replay and real-time events.

Configuration:

```text
Visual: Line chart or column chart
X-axis: business_event_timestamp
Y-axis: Total_Fraud_Alerts
```

The chart uses business time instead of pure processing time so that historical replay events appear according to their original simulated transaction time.

---

## 8.3 Alerts by fraud rule

Purpose:

- compares the number of alerts by fraud detection rule.

Configuration:

```text
Visual: Bar chart / column chart
Axis: fraud_rule
Values: Total_Fraud_Alerts
```

---

## 8.4 Suspicious amount by fraud rule

Purpose:

- shows the financial exposure per fraud rule.

Configuration:

```text
Visual: Bar chart / column chart
Axis: fraud_rule
Values: Suspicious_Amount
```

---

## 8.5 Latest high-risk transactions

Purpose:

- provides detailed transaction-level visibility.

Recommended columns:

```text
business_event_timestamp
event_timestamp
source_category
transaction_id
customer_id
merchant_id
amount
currency
risk_score
fraud_rule
city
ip_country
payment_method
authorization_status
```

Sort order:

```text
business_event_timestamp descending
```

---

## 9. Slicers

Recommended slicers:

- `business_event_timestamp`,
- `fraud_rule`,
- `risk_score`,
- `country`,
- `ip_country`,
- `source_category`.

The time slicer uses:

```text
business_event_timestamp
```

This allows the report to filter both historical replay and real-time fraud events using one consistent business timeline.

---

## 10. Evidence

The following screenshots document the completed Power BI report.

### 10.1 Full report

![Power BI real-time fraud report](../images/dashboards/powerbi_realtime_fraud_report.png)

### 10.2 KPI tiles

![Power BI fraud KPI tiles](../images/dashboards/powerbi_fraud_kpi_tiles.png)

### 10.3 Fraud-rule analysis

![Power BI fraud rules analysis](../images/dashboards/powerbi_fraud_rules_analysis.png)

### 10.4 Fraud alerts over time

![Power BI fraud alerts over time](../images/dashboards/powerbi_fraud_alerts_over_time.png)

### 10.5 Latest high-risk transactions

![Power BI latest high-risk transactions](../images/dashboards/powerbi_latest_high_risk_transactions.png)

### 10.6 DirectQuery / KQL connection

![Power BI DirectQuery KQL connection](../images/dashboards/powerbi_directquery_kql_connection.png)

### 10.7 Power BI data model

![Power BI fraud data model](../images/dashboards/powerbi_fraud_data_model.png)

### 10.8 KQL reporting function

![Power BI KQL reporting function](../images/dashboards/powerbi_kql_reporting_function.png)

---

## 11. Business value

The Power BI report demonstrates how fraud monitoring data can be presented to business stakeholders.

Business value:

- monitoring total fraud alert volume,
- tracking suspicious transaction value,
- identifying the most common fraud rules,
- comparing historical and real-time fraud events,
- filtering events by time, risk level and geography,
- reviewing detailed high-risk transaction records.

---

## 12. Engineering value

The report demonstrates:

- DirectQuery reporting on Fabric Eventhouse data,
- KQL-based reporting model design,
- dynamic JSON payload flattening,
- unified historical and real-time event modeling,
- business-time and processing-time handling,
- Power BI KPI and slicer design,
- end-to-end reporting on a streaming data pipeline.
