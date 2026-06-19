# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver to Gold — Analytical marts
# MAGIC
# MAGIC Builds Gold Delta marts from `silver.transactions`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")

transactions = spark.table(silver_transactions_table)
print(f"Silver transactions rows: {transactions.count()}")

# COMMAND ----------

daily_summary = (
    transactions
    .groupBy("business_date")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum(F.when(F.col("authorization_status") == "AUTHORIZED", 1).otherwise(0)).alias("authorized_count"),
        F.sum(F.when(F.col("authorization_status") == "DECLINED", 1).otherwise(0)).alias("declined_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.max("amount"), 2).alias("max_amount"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.sum(F.when(F.col("is_fraud_suspected") == True, 1).otherwise(0)).alias("suspected_fraud_count"),
        F.round(F.sum(F.when(F.col("is_fraud_suspected") == True, F.col("amount")).otherwise(F.lit(0.0))), 2).alias("suspected_fraud_amount"),
        F.countDistinct("customer_id").alias("active_customers"),
        F.countDistinct("merchant_id").alias("active_merchants")
    )
    .orderBy("business_date")
)

(
    daily_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_daily_transaction_summary_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_daily_transaction_summary_table}
USING DELTA
LOCATION '{gold_daily_transaction_summary_path}'
""")

display(daily_summary.limit(20))

# COMMAND ----------

fraud_risk_summary = (
    transactions
    .withColumn("fraud_rule", F.coalesce(F.col("fraud_rule_hit"), F.lit("NO_RULE")))
    .groupBy("business_date", "fraud_rule")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum(F.when(F.col("is_fraud_suspected") == True, 1).otherwise(0)).alias("suspected_fraud_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.countDistinct("customer_id").alias("affected_customers")
    )
    .orderBy("business_date", "fraud_rule")
)

(
    fraud_risk_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_fraud_risk_summary_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_fraud_risk_summary_table}
USING DELTA
LOCATION '{gold_fraud_risk_summary_path}'
""")

display(fraud_risk_summary.limit(20))

# COMMAND ----------

channel_payment_summary = (
    transactions
    .groupBy("business_date", "channel", "payment_method", "authorization_status")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(F.sum("amount"), 2).alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
        F.countDistinct("customer_id").alias("active_customers")
    )
    .orderBy("business_date", "channel", "payment_method", "authorization_status")
)

(
    channel_payment_summary
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_channel_payment_summary_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_channel_payment_summary_table}
USING DELTA
LOCATION '{gold_channel_payment_summary_path}'
""")

display(channel_payment_summary.limit(20))

# COMMAND ----------

print("Gold marts created:")
print(f"- {gold_daily_transaction_summary_table}")
print(f"- {gold_fraud_risk_summary_table}")
print(f"- {gold_channel_payment_summary_table}")
