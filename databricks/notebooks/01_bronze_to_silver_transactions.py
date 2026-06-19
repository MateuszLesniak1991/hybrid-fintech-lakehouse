# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze to Silver — Transactions
# MAGIC
# MAGIC Reads hourly transaction Parquet files from ADLS Bronze, standardizes the schema,
# MAGIC deduplicates transactions and writes a Delta Lake Silver table.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

def has_col(df, name: str) -> bool:
    return name in df.columns


def first_existing_col(df, names, default=None):
    for name in names:
        if has_col(df, name):
            return F.col(name)
    if default is None:
        return F.lit(None)
    return F.lit(default)


def to_timestamp_from_any(df, names):
    exprs = [F.to_timestamp(F.col(name)) for name in names if has_col(df, name)]
    if not exprs:
        return F.lit(None).cast("timestamp")
    return F.coalesce(*exprs)


def ensure_string_col(df, name: str):
    if has_col(df, name):
        return F.col(name).cast("string")
    return F.lit(None).cast("string")


def ensure_double_col(df, name: str):
    if has_col(df, name):
        return F.col(name).cast("double")
    return F.lit(None).cast("double")


def ensure_int_col(df, name: str):
    if has_col(df, name):
        return F.col(name).cast("int")
    return F.lit(None).cast("int")

# COMMAND ----------

bronze_df = (
    spark.read
    .format("parquet")
    .option("recursiveFileLookup", "true")
    .load(bronze_transactions_path)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingestion_timestamp", F.current_timestamp())
)

print("Bronze schema:")
bronze_df.printSchema()
print(f"Bronze rows: {bronze_df.count()}")

# COMMAND ----------

business_ts_expr = to_timestamp_from_any(
    bronze_df,
    [
        "transaction_timestamp",
        "transaction_time",
        "event_time",
        "created_at",
        "timestamp",
        "authorization_time"
    ],
)

silver_df = (
    bronze_df
    .withColumn("transaction_id", ensure_string_col(bronze_df, "transaction_id"))
    .withColumn("customer_id", ensure_string_col(bronze_df, "customer_id"))
    .withColumn("account_id", ensure_string_col(bronze_df, "account_id"))
    .withColumn("merchant_id", ensure_string_col(bronze_df, "merchant_id"))
    .withColumn("merchant_name", ensure_string_col(bronze_df, "merchant_name"))
    .withColumn("merchant_category", ensure_string_col(bronze_df, "merchant_category"))
    .withColumn("atm_id", ensure_string_col(bronze_df, "atm_id"))
    .withColumn("city", ensure_string_col(bronze_df, "city"))
    .withColumn("country", ensure_string_col(bronze_df, "country"))
    .withColumn("currency", F.upper(ensure_string_col(bronze_df, "currency")))
    .withColumn("channel", F.upper(ensure_string_col(bronze_df, "channel")))
    .withColumn("payment_method", F.upper(ensure_string_col(bronze_df, "payment_method")))
    .withColumn("authorization_status", F.upper(ensure_string_col(bronze_df, "authorization_status")))
    .withColumn("decline_reason", ensure_string_col(bronze_df, "decline_reason"))
    .withColumn("fraud_rule_hit", F.upper(ensure_string_col(bronze_df, "fraud_rule_hit")))
    .withColumn("amount", ensure_double_col(bronze_df, "amount"))
    .withColumn("risk_score", ensure_int_col(bronze_df, "risk_score"))
    .withColumn("is_fraud_suspected", first_existing_col(bronze_df, ["is_fraud_suspected"], False).cast("boolean"))
    .withColumn("business_timestamp", business_ts_expr)
    .withColumn("business_date", F.to_date("business_timestamp"))
    .withColumn("year", F.year("business_timestamp"))
    .withColumn("month", F.format_string("%02d", F.month("business_timestamp")))
    .withColumn("day", F.format_string("%02d", F.dayofmonth("business_timestamp")))
)

silver_df = (
    silver_df
    .where(F.col("transaction_id").isNotNull())
    .where(F.col("business_timestamp").isNotNull())
)

window = Window.partitionBy("transaction_id").orderBy(F.col("_ingestion_timestamp").desc(), F.col("_source_file").desc())

silver_df = (
    silver_df
    .withColumn("_row_number", F.row_number().over(window))
    .where(F.col("_row_number") == 1)
    .drop("_row_number")
)

print(f"Silver rows after standardization and deduplication: {silver_df.count()}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("year", "month", "day")
    .save(silver_transactions_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_transactions_table}
USING DELTA
LOCATION '{silver_transactions_path}'
""")

print(f"Silver Delta table created: {silver_transactions_table}")
display(spark.table(silver_transactions_table).limit(20))
