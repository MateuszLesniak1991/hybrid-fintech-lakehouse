# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Data Quality Checks
# MAGIC
# MAGIC Runs data quality checks on the Silver transaction table and stores results as a Delta table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, timezone

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{quality_schema}")

df = spark.table(silver_transactions_table)

run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
run_timestamp = datetime.now(timezone.utc).isoformat()

checks = []

def add_check(check_name, check_type, failed_count, total_count, severity="ERROR"):
    status = "PASSED" if failed_count == 0 else "FAILED"
    checks.append({
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "table_name": silver_transactions_table,
        "check_name": check_name,
        "check_type": check_type,
        "severity": severity,
        "status": status,
        "failed_count": int(failed_count),
        "total_count": int(total_count)
    })

total_count = df.count()

add_check("transaction_id_not_null", "null_check", df.where(F.col("transaction_id").isNull()).count(), total_count)

duplicate_count = df.groupBy("transaction_id").count().where(F.col("count") > 1).count()
add_check("transaction_id_unique", "duplicate_check", duplicate_count, total_count)

add_check("amount_positive", "range_check", df.where((F.col("amount").isNull()) | (F.col("amount") <= 0)).count(), total_count)
add_check("business_timestamp_not_null", "null_check", df.where(F.col("business_timestamp").isNull()).count(), total_count)
add_check("currency_allowed", "allowed_values_check", df.where((F.col("currency").isNull()) | (~F.col("currency").isin("PLN"))).count(), total_count, severity="WARNING")
add_check("risk_score_range", "range_check", df.where((F.col("risk_score").isNotNull()) & ((F.col("risk_score") < 0) | (F.col("risk_score") > 100))).count(), total_count)
add_check("customer_id_not_null", "null_check", df.where(F.col("customer_id").isNull()).count(), total_count)

results_df = spark.createDataFrame(checks)

(
    results_df
    .write
    .format("delta")
    .mode("append")
    .save(quality_results_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {quality_results_table}
USING DELTA
LOCATION '{quality_results_path}'
""")

display(results_df.orderBy("severity", "status", "check_name"))

failed_errors = results_df.where((F.col("status") == "FAILED") & (F.col("severity") == "ERROR")).count()
if failed_errors > 0:
    raise Exception(f"Data quality failed. Failed ERROR checks: {failed_errors}")

print("Data quality checks completed successfully.")
