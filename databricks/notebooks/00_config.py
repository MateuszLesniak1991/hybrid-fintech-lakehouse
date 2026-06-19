# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config
# MAGIC Shared configuration for the Hybrid FinTech Lakehouse Databricks branch.
# MAGIC
# MAGIC Before running the notebooks, update the values below.

# COMMAND ----------

# Update these values.
storage_account_name = "REPLACE_WITH_STORAGE_ACCOUNT_NAME"

bronze_container = "bronze"
silver_container = "silver"
gold_container = "gold"
quality_container = "quality"

project_name = "hybrid_fintech_lakehouse"

# Unity Catalog names.
catalog_name = "hybrid_fintech"
bronze_schema = "bronze"
silver_schema = "silver"
gold_schema = "gold"
quality_schema = "quality"

# ADLS paths.
bronze_transactions_path = (
    f"abfss://{bronze_container}@{storage_account_name}.dfs.core.windows.net/"
    "banking/transactions/"
)

silver_transactions_path = (
    f"abfss://{silver_container}@{storage_account_name}.dfs.core.windows.net/"
    "banking/transactions/"
)

gold_daily_transaction_summary_path = (
    f"abfss://{gold_container}@{storage_account_name}.dfs.core.windows.net/"
    "marts/daily_transaction_summary/"
)

gold_fraud_risk_summary_path = (
    f"abfss://{gold_container}@{storage_account_name}.dfs.core.windows.net/"
    "marts/fraud_risk_summary/"
)

gold_channel_payment_summary_path = (
    f"abfss://{gold_container}@{storage_account_name}.dfs.core.windows.net/"
    "marts/channel_payment_summary/"
)

quality_results_path = (
    f"abfss://{quality_container}@{storage_account_name}.dfs.core.windows.net/"
    "data_quality/results/"
)

# Table names.
silver_transactions_table = f"{catalog_name}.{silver_schema}.transactions"
gold_daily_transaction_summary_table = f"{catalog_name}.{gold_schema}.daily_transaction_summary"
gold_fraud_risk_summary_table = f"{catalog_name}.{gold_schema}.fraud_risk_summary"
gold_channel_payment_summary_table = f"{catalog_name}.{gold_schema}.channel_payment_summary"
quality_results_table = f"{catalog_name}.{quality_schema}.data_quality_results"

print("Configuration loaded.")
print(f"Bronze transactions path: {bronze_transactions_path}")
print(f"Silver transactions table: {silver_transactions_table}")
