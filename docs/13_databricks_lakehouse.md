# Databricks Lakehouse Branch

## Purpose

The Databricks Lakehouse branch extends the batch path of the Hybrid FinTech Lakehouse Platform.

It transforms Bronze Parquet files stored in ADLS Gen2 into Delta Lake Silver and Gold datasets.

## Architecture

```text
PostgreSQL
→ hourly Parquet export
→ MinIO Bronze
→ ADLS Gen2 Bronze
→ Azure Databricks
→ Delta Lake Silver
→ Delta Lake Gold
→ Data Quality
→ Power BI batch analytics
```

## Implemented layers

### Bronze

Raw Parquet files synchronized from MinIO to ADLS Gen2.

### Silver

Cleaned and standardized Delta tables.

Initial table:

```text
silver.transactions
```

### Gold

Business-oriented analytical marts.

Initial marts:

```text
gold.daily_transaction_summary
gold.fraud_risk_summary
gold.channel_payment_summary
```

### Quality

Data quality results stored in:

```text
quality.data_quality_results
```

## Evidence screenshots

Recommended screenshots:

```text
images/databricks/databricks_workspace.png
images/databricks/databricks_cluster.png
images/databricks/databricks_bronze_to_silver_notebook.png
images/databricks/databricks_silver_transactions_table.png
images/databricks/databricks_gold_tables.png
images/databricks/databricks_data_quality_results.png
images/databricks/databricks_workflow_success.png
```
