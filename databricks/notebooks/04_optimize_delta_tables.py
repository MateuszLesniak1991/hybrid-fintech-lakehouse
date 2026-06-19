# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Optimize Delta Tables
# MAGIC
# MAGIC Optimizes Silver and Gold Delta tables after transformation.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

tables = [
    silver_transactions_table,
    gold_daily_transaction_summary_table,
    gold_fraud_risk_summary_table,
    gold_channel_payment_summary_table,
    quality_results_table
]

for table_name in tables:
    print(f"Optimizing {table_name}")
    spark.sql(f"OPTIMIZE {table_name}")

print("Optimization completed.")
