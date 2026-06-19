# Databricks Lakehouse Branch

This directory contains Azure Databricks notebooks for the batch Lakehouse branch of the Hybrid FinTech Lakehouse Platform.

## Flow

```text
ADLS Gen2 Bronze
→ Databricks
→ Delta Lake Silver
→ Delta Lake Gold
→ Data Quality
→ Power BI batch analytics
```

## Notebooks

| Notebook | Purpose |
|---|---|
| `00_config.py` | Shared paths, catalog names and table names |
| `01_bronze_to_silver_transactions.py` | Reads Bronze Parquet transactions and writes Silver Delta |
| `02_silver_to_gold_marts.py` | Builds Gold analytical marts |
| `03_data_quality_checks.py` | Runs quality checks and stores results |
| `04_optimize_delta_tables.py` | Optimizes Delta tables |

## Required setup

1. Create Azure Databricks workspace.
2. Grant Databricks access to ADLS Gen2.
3. Create ADLS containers: `bronze`, `silver`, `gold`, `quality`.
4. Update `storage_account_name` in `00_config.py`.
5. Run notebooks in order.
