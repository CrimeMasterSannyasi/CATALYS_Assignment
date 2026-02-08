# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver: Data Quality & Cleaning
# MAGIC 
# MAGIC **Purpose**: Validate, clean, and standardize raw data from Bronze layer
# MAGIC 
# MAGIC **Inputs**: 
# MAGIC - `/bronze/sales/` - Raw sales orders
# MAGIC - `/bronze/events/` - Raw event logs
# MAGIC 
# MAGIC **Outputs**:
# MAGIC - `/silver/sales/` - Cleaned sales data
# MAGIC - `/silver/events/` - Cleaned event data
# MAGIC - Audit logs with data quality metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# Configuration
STORAGE_ACCOUNT = "your_storage_account"  # Replace with actual storage account
BRONZE_PATH = f"/mnt/bronze"
SILVER_PATH = f"/mnt/silver"

# Batch tracking
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sales Orders: Bronze to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Read Bronze Sales Data

# COMMAND ----------

# Read raw sales data
df_sales_bronze = spark.read.parquet(f"{BRONZE_PATH}/sales/")

print(f"Bronze sales records: {df_sales_bronze.count()}")
df_sales_bronze.printSchema()
df_sales_bronze.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Data Quality Checks

# COMMAND ----------

# Count initial records
initial_count = df_sales_bronze.count()

# Check for nulls in critical fields
null_checks = {
    "order_id": df_sales_bronze.filter(F.col("order_id").isNull()).count(),
    "customer_id": df_sales_bronze.filter(
        (F.col("customer_id").isNull()) | (F.col("customer_id") == "")
    ).count(),
    "product_id": df_sales_bronze.filter(
        (F.col("product_id").isNull()) | (F.col("product_id") == "")
    ).count(),
    "quantity": df_sales_bronze.filter(
        (F.col("quantity").isNull()) | (F.col("quantity") <= 0)
    ).count(),
    "unit_price": df_sales_bronze.filter(
        (F.col("unit_price").isNull()) | (F.col("unit_price") <= 0)
    ).count(),
}

print("Data Quality Issues Found:")
for field, count in null_checks.items():
    print(f"  {field}: {count} invalid records")

# Check for duplicates
duplicate_count = df_sales_bronze.groupBy("order_id").count().filter(F.col("count") > 1).count()
print(f"  Duplicate order_ids: {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Remove Duplicates

# COMMAND ----------

# Remove duplicates - keep latest record based on last_modified
window_spec = Window.partitionBy("order_id").orderBy(
    F.col("last_modified").desc(), 
    F.col("order_timestamp").desc()
)

df_sales_dedup = df_sales_bronze.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")

print(f"After deduplication: {df_sales_dedup.count()} records")
print(f"Removed {initial_count - df_sales_dedup.count()} duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Clean and Standardize

# COMMAND ----------

df_sales_clean = df_sales_dedup.select(
    F.col("order_id"),
    F.upper(F.trim(F.col("customer_id"))).alias("customer_id"),
    F.upper(F.trim(F.col("product_id"))).alias("product_id"),
    F.to_date(F.col("order_date")).alias("order_date"),
    F.col("order_timestamp").cast(TimestampType()).alias("order_timestamp"),
    F.col("quantity").cast(IntegerType()),
    F.col("unit_price").cast(DecimalType(10, 2)),
    F.col("discount_amount").cast(DecimalType(10, 2)),
    F.col("shipping_cost").cast(DecimalType(10, 2)),
    F.col("line_total").cast(DecimalType(10, 2)),
    F.lower(F.trim(F.col("status"))).alias("status"),
    F.lower(F.trim(F.col("payment_method"))).alias("payment_method"),
    F.col("last_modified").cast(TimestampType())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Apply Business Rules and Validation

# COMMAND ----------

# Filter invalid records
df_sales_valid = df_sales_clean.filter(
    # Critical fields not null
    (F.col("order_id").isNotNull()) &
    (F.col("customer_id").isNotNull()) & (F.col("customer_id") != "") &
    (F.col("product_id").isNotNull()) & (F.col("product_id") != "") &
    
    # Business rule validations
    (F.col("quantity") > 0) &
    (F.col("unit_price") > 0) &
    (F.col("line_total") >= 0) &
    
    # Date validations
    (F.col("order_date") <= F.current_date()) &
    (F.col("order_date") >= F.lit("2023-01-01")) &
    
    # Valid status
    (F.col("status").isin(["completed", "pending", "cancelled", "returned"]))
)

# Count filtered records
valid_count = df_sales_valid.count()
invalid_count = df_sales_clean.count() - valid_count

print(f"Valid records: {valid_count}")
print(f"Invalid records filtered: {invalid_count}")
print(f"Quality rate: {100 * valid_count / df_sales_clean.count():.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Store Invalid Records for Analysis

# COMMAND ----------

# Get invalid records for quarantine
df_sales_invalid = df_sales_clean.join(
    df_sales_valid, 
    on="order_id", 
    how="left_anti"
)

# Add validation flags
df_sales_invalid = df_sales_invalid.withColumn("validation_error", 
    F.when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "Missing customer_id")
     .when(F.col("product_id").isNull() | (F.col("product_id") == ""), "Missing product_id")
     .when(F.col("quantity") <= 0, "Invalid quantity")
     .when(F.col("unit_price") <= 0, "Invalid price")
     .when(F.col("line_total") < 0, "Negative total")
     .otherwise("Unknown validation error")
)

# Save to quarantine
df_sales_invalid.write.mode("overwrite").parquet(f"{SILVER_PATH}/quarantine/sales/{batch_id}/")

print(f"Quarantined {df_sales_invalid.count()} invalid sales records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Write to Silver Layer

# COMMAND ----------

# Add audit columns
df_sales_silver = df_sales_valid.withColumn("processed_timestamp", F.current_timestamp()) \
    .withColumn("batch_id", F.lit(batch_id))

# Write to Silver
df_sales_silver.write.mode("overwrite").partitionBy("order_date").parquet(f"{SILVER_PATH}/sales/")

print(f"✓ Written {df_sales_silver.count()} sales records to Silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Events: Bronze to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Read Bronze Events Data

# COMMAND ----------

# Read raw events (JSON format)
df_events_bronze = spark.read.json(f"{BRONZE_PATH}/events/")

print(f"Bronze event records: {df_events_bronze.count()}")
df_events_bronze.printSchema()
df_events_bronze.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Parse and Flatten JSON

# COMMAND ----------

# Extract and flatten nested metadata
df_events_parsed = df_events_bronze.select(
    F.col("event_id"),
    F.upper(F.trim(F.col("user_id"))).alias("user_id"),
    F.lower(F.trim(F.col("event_type"))).alias("event_type"),
    F.col("timestamp").cast(TimestampType()).alias("event_timestamp"),
    F.col("session_id"),
    F.col("ip_address"),
    
    # Extract common metadata fields
    F.col("metadata.page").alias("page"),
    F.col("metadata.device").alias("device"),
    F.col("metadata.browser").alias("browser"),
    F.col("metadata.referrer").alias("referrer"),
    
    # Keep full metadata as JSON string for flexibility
    F.to_json(F.col("metadata")).alias("metadata_json")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Data Quality Checks

# COMMAND ----------

initial_events_count = df_events_parsed.count()

# Check for issues
event_null_checks = {
    "event_id": df_events_parsed.filter(F.col("event_id").isNull()).count(),
    "user_id": df_events_parsed.filter(
        (F.col("user_id").isNull()) | (F.col("user_id") == "")
    ).count(),
    "event_timestamp": df_events_parsed.filter(F.col("event_timestamp").isNull()).count(),
}

print("Event Data Quality Issues:")
for field, count in event_null_checks.items():
    print(f"  {field}: {count} invalid records")

# Bot traffic count
bot_count = df_events_parsed.filter(
    F.col("user_id").like("BOT%") | F.col("user_id").like("TEST%")
).count()
print(f"  Bot/Test traffic: {bot_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Filter and Clean Events

# COMMAND ----------

df_events_valid = df_events_parsed.filter(
    # Critical fields not null
    (F.col("event_id").isNotNull()) &
    (F.col("user_id").isNotNull()) & (F.col("user_id") != "") &
    (F.col("event_type").isNotNull()) &
    (F.col("event_timestamp").isNotNull()) &
    
    # Filter bot traffic
    (~F.col("user_id").like("BOT%")) &
    (~F.col("user_id").like("TEST%")) &
    
    # Valid timestamp
    (F.col("event_timestamp") <= F.current_timestamp()) &
    (F.col("event_timestamp") >= F.lit("2023-01-01"))
)

valid_events_count = df_events_valid.count()
print(f"Valid events: {valid_events_count}")
print(f"Filtered: {initial_events_count - valid_events_count}")
print(f"Quality rate: {100 * valid_events_count / initial_events_count:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Remove Event Duplicates

# COMMAND ----------

# Remove duplicate events (same event_id)
df_events_dedup = df_events_valid.dropDuplicates(["event_id"])

print(f"After deduplication: {df_events_dedup.count()} events")
print(f"Removed {valid_events_count - df_events_dedup.count()} duplicate events")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Write to Silver Layer

# COMMAND ----------

# Add audit columns
df_events_silver = df_events_dedup.withColumn("processed_timestamp", F.current_timestamp()) \
    .withColumn("batch_id", F.lit(batch_id))

# Add date partition column
df_events_silver = df_events_silver.withColumn(
    "event_date", 
    F.to_date(F.col("event_timestamp"))
)

# Write to Silver (partitioned by date)
df_events_silver.write.mode("overwrite") \
    .partitionBy("event_date") \
    .parquet(f"{SILVER_PATH}/events/")

print(f"✓ Written {df_events_silver.count()} events to Silver layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log Audit Metrics

# COMMAND ----------

# Create audit record
audit_data = [
    {
        "batch_id": batch_id,
        "pipeline_stage": "bronze_to_silver",
        "entity": "sales",
        "records_input": initial_count,
        "records_output": df_sales_silver.count(),
        "records_filtered": invalid_count,
        "records_duplicates": initial_count - df_sales_dedup.count(),
        "processed_timestamp": datetime.now().isoformat()
    },
    {
        "batch_id": batch_id,
        "pipeline_stage": "bronze_to_silver",
        "entity": "events",
        "records_input": initial_events_count,
        "records_output": df_events_silver.count(),
        "records_filtered": initial_events_count - valid_events_count,
        "records_duplicates": valid_events_count - df_events_dedup.count(),
        "processed_timestamp": datetime.now().isoformat()
    }
]

df_audit = spark.createDataFrame(audit_data)
df_audit.write.mode("append").parquet(f"{SILVER_PATH}/audit/quality_metrics/")

print("✓ Audit metrics logged")
df_audit.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC **Bronze to Silver transformation complete!**
# MAGIC 
# MAGIC - ✓ Sales data cleaned and validated
# MAGIC - ✓ Events data parsed and filtered
# MAGIC - ✓ Duplicates removed
# MAGIC - ✓ Invalid records quarantined
# MAGIC - ✓ Audit metrics logged
# MAGIC 
# MAGIC **Next**: Run Silver to Gold transformations (business logic)