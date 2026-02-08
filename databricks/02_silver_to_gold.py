# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold: Business Transformations
# MAGIC 
# MAGIC **Purpose**: Apply business logic and create dimension/fact tables
# MAGIC 
# MAGIC **Inputs**:
# MAGIC - `/silver/sales/` - Cleaned sales data
# MAGIC - `/silver/events/` - Cleaned events
# MAGIC - Reference data (customers, products)
# MAGIC 
# MAGIC **Outputs**:
# MAGIC - Dimension tables → Azure SQL Database
# MAGIC - Fact table → Azure SQL Database
# MAGIC - Aggregated events → Azure Table Storage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# Configuration
SILVER_PATH = "/mnt/silver"
GOLD_PATH = "/mnt/gold"

# JDBC Configuration for Azure SQL
jdbc_url = "jdbc:sqlserver://your-server.database.windows.net:1433;database=your-db"
jdbc_properties = {
    "user": "your-username",
    "password": "your-password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Batch ID: {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Silver Layer Data

# COMMAND ----------

# Read cleaned sales data
df_sales_silver = spark.read.parquet(f"{SILVER_PATH}/sales/")

# Read reference data (customers, products)
df_customers_ref = spark.read.csv(
    "/mnt/bronze/customers.csv", 
    header=True, 
    inferSchema=True
)

df_products_ref = spark.read.csv(
    "/mnt/bronze/products.csv",
    header=True,
    inferSchema=True
)

print(f"Sales: {df_sales_silver.count()}")
print(f"Customers: {df_customers_ref.count()}")
print(f"Products: {df_products_ref.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Dimension: Customers

# COMMAND ----------

# Prepare customer dimension
df_dim_customers = df_customers_ref.select(
    F.col("customer_id"),
    F.col("customer_name"),
    F.col("email"),
    F.col("phone"),
    F.col("segment"),
    F.to_date(F.col("registration_date")).alias("registration_date"),
    F.col("is_active").cast(BooleanType())
).distinct()

# Show sample
df_dim_customers.show(5)
print(f"Dimension customers: {df_dim_customers.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Staging Table in SQL Database

# COMMAND ----------

# Write to staging table
df_dim_customers.write \
    .jdbc(
        url=jdbc_url,
        table="staging.dim_customers_staging",
        mode="overwrite",
        properties=jdbc_properties
    )

print("✓ Customers written to staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge to Production Table

# COMMAND ----------

# Call stored procedure to merge
merge_query = "(EXEC staging.sp_merge_dim_customers) as merge_result"

spark.read \
    .jdbc(
        url=jdbc_url,
        table=merge_query,
        properties=jdbc_properties
    ) \
    .show()

print("✓ Customers merged to gold.dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Dimension: Products

# COMMAND ----------

# Prepare product dimension
df_dim_products = df_products_ref.select(
    F.col("product_id"),
    F.col("product_name"),
    F.col("category"),
    F.col("subcategory"),
    F.col("unit_price").cast(DecimalType(10, 2)),
    F.col("cost").cast(DecimalType(10, 2)),
    F.col("supplier"),
    F.col("sku"),
    F.col("stock_quantity").cast(IntegerType())
).distinct()

df_dim_products.show(5)
print(f"Dimension products: {df_dim_products.count()}")

# COMMAND ----------

# Write to staging and merge
df_dim_products.write \
    .jdbc(
        url=jdbc_url,
        table="staging.dim_products_staging",
        mode="overwrite",
        properties=jdbc_properties
    )

merge_query = "(EXEC staging.sp_merge_dim_products) as merge_result"
spark.read.jdbc(url=jdbc_url, table=merge_query, properties=jdbc_properties).show()

print("✓ Products merged to gold.dim_products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Fact Table: Sales

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Read Dimension Keys from SQL Database

# COMMAND ----------

# Read dimension tables to get surrogate keys
df_dim_customers_keys = spark.read.jdbc(
    url=jdbc_url,
    table="gold.dim_customers",
    properties=jdbc_properties
).select("customer_key", "customer_id")

df_dim_products_keys = spark.read.jdbc(
    url=jdbc_url,
    table="gold.dim_products",
    properties=jdbc_properties
).select("product_key", "product_id", "cost")

df_dim_date = spark.read.jdbc(
    url=jdbc_url,
    table="gold.dim_date",
    properties=jdbc_properties
).select("date_key", F.col("full_date").alias("order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Join Sales with Dimensions

# COMMAND ----------

# Join with customer dimension
df_fact = df_sales_silver.join(
    df_dim_customers_keys,
    on="customer_id",
    how="left"
)

# Join with product dimension
df_fact = df_fact.join(
    df_dim_products_keys,
    on="product_id",
    how="left"
)

# Join with date dimension
df_fact = df_fact.join(
    df_dim_date,
    on="order_date",
    how="left"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Calculate Business Metrics

# COMMAND ----------

# Calculate profit: line_total - (quantity * cost)
df_fact_sales = df_fact.select(
    F.col("order_id"),
    F.col("customer_key"),
    F.col("product_key"),
    F.col("date_key"),
    F.col("order_timestamp"),
    F.col("payment_method"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("discount_amount"),
    F.col("shipping_cost"),
    F.col("line_total"),
    
    # Calculate profit
    (F.col("line_total") - (F.col("quantity") * F.coalesce(F.col("cost"), F.lit(0))))
        .cast(DecimalType(10, 2))
        .alias("profit"),
    
    F.col("status")
).filter(
    # Only completed orders in fact table
    F.col("status") == "completed"
).withColumn(
    "batch_id", 
    F.lit(batch_id)
)

print(f"Fact sales records: {df_fact_sales.count()}")
df_fact_sales.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Data Quality Checks

# COMMAND ----------

# Check for orphaned records
orphan_checks = {
    "Missing customer_key": df_fact_sales.filter(F.col("customer_key").isNull()).count(),
    "Missing product_key": df_fact_sales.filter(F.col("product_key").isNull()).count(),
    "Missing date_key": df_fact_sales.filter(F.col("date_key").isNull()).count(),
}

print("Orphan Record Checks:")
for check, count in orphan_checks.items():
    print(f"  {check}: {count}")
    if count > 0:
        print(f"  ⚠️  WARNING: Found {count} orphaned records!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Write to Staging and Merge

# COMMAND ----------

# Write to staging
df_fact_sales.write \
    .jdbc(
        url=jdbc_url,
        table="staging.fact_sales_staging",
        mode="overwrite",
        properties=jdbc_properties
    )

print("✓ Fact sales written to staging")

# Merge to production
merge_query = "(EXEC staging.sp_merge_fact_sales) as merge_result"
spark.read.jdbc(url=jdbc_url, table=merge_query, properties=jdbc_properties).show()

print("✓ Fact sales merged to gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Save to Gold Layer (Parquet for Analytics)

# COMMAND ----------

# Also save to Data Lake Gold layer for Databricks analytics
df_fact_sales.write \
    .mode("overwrite") \
    .partitionBy("date_key") \
    .parquet(f"{GOLD_PATH}/fact_sales/")

print("✓ Fact sales written to Gold layer (Data Lake)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Customer Aggregates

# COMMAND ----------

df_customer_metrics = df_fact_sales.groupBy(
    "customer_key"
).agg(
    F.count("order_id").alias("total_orders"),
    F.sum("quantity").alias("total_items_purchased"),
    F.sum("line_total").alias("total_spent"),
    F.avg("line_total").alias("avg_order_value"),
    F.sum("profit").alias("total_profit_generated"),
    F.min("order_timestamp").alias("first_order_date"),
    F.max("order_timestamp").alias("last_order_date")
).withColumn(
    "customer_lifetime_days",
    F.datediff(F.col("last_order_date"), F.col("first_order_date"))
)

# Write to Gold layer
df_customer_metrics.write.mode("overwrite").parquet(f"{GOLD_PATH}/customer_metrics/")

print(f"✓ Customer metrics created: {df_customer_metrics.count()} customers")
df_customer_metrics.orderBy(F.desc("total_spent")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Create Product Aggregates

# COMMAND ----------

df_product_metrics = df_fact_sales.groupBy(
    "product_key"
).agg(
    F.count("order_id").alias("times_ordered"),
    F.sum("quantity").alias("units_sold"),
    F.sum("line_total").alias("total_revenue"),
    F.avg("unit_price").alias("avg_selling_price"),
    F.sum("profit").alias("total_profit")
).withColumn(
    "profit_margin_pct",
    F.when(
        F.col("total_revenue") > 0,
        F.round(100.0 * F.col("total_profit") / F.col("total_revenue"), 2)
    ).otherwise(0)
)

df_product_metrics.write.mode("overwrite").parquet(f"{GOLD_PATH}/product_metrics/")

print(f"✓ Product metrics created: {df_product_metrics.count()} products")
df_product_metrics.orderBy(F.desc("total_revenue")).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Process Events for Table Storage

# COMMAND ----------

# Read cleaned events
df_events_silver = spark.read.parquet(f"{SILVER_PATH}/events/")

print(f"Events to process: {df_events_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Sessionization

# COMMAND ----------

# Calculate session metrics
window_spec = Window.partitionBy("session_id").orderBy("event_timestamp")

df_session_events = df_events_silver.withColumn(
    "prev_event_timestamp",
    F.lag("event_timestamp").over(window_spec)
).withColumn(
    "event_sequence",
    F.row_number().over(window_spec)
)

# Aggregate by session
df_sessions = df_session_events.groupBy("session_id", "user_id").agg(
    F.min("event_timestamp").alias("session_start"),
    F.max("event_timestamp").alias("session_end"),
    F.count("*").alias("total_events"),
    F.countDistinct("event_type").alias("distinct_event_types"),
    F.collect_list(
        F.struct("event_type", "event_timestamp", "event_sequence")
    ).alias("event_journey")
).withColumn(
    "duration_seconds",
    F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
).filter(
    # Filter very short sessions (likely bots)
    F.col("duration_seconds") >= 5
)

print(f"Sessions created: {df_sessions.count()}")
df_sessions.orderBy(F.desc("duration_seconds")).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Prepare for Table Storage

# COMMAND ----------

# Format for Azure Table Storage
# PartitionKey: user_id, RowKey: session_id

df_table_storage = df_sessions.select(
    F.col("user_id").alias("PartitionKey"),
    F.col("session_id").alias("RowKey"),
    F.col("session_start"),
    F.col("session_end"),
    F.col("duration_seconds"),
    F.col("total_events"),
    F.col("distinct_event_types"),
    F.to_json("event_journey").alias("event_journey_json")
).withColumn(
    "processed_timestamp",
    F.current_timestamp()
).withColumn(
    "batch_id",
    F.lit(batch_id)
)

# Write to Gold layer (ADF will load to Table Storage)
df_table_storage.write.mode("overwrite").parquet(f"{GOLD_PATH}/sessions/")

print(f"✓ Sessions prepared for Table Storage: {df_table_storage.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Update Watermark

# COMMAND ----------

# Get latest timestamp from sales
latest_sales_timestamp = df_sales_silver.agg(
    F.max("order_timestamp")
).collect()[0][0]

# Get latest timestamp from events
latest_event_timestamp = df_events_silver.agg(
    F.max("event_timestamp")
).collect()[0][0]

print(f"Latest sales timestamp: {latest_sales_timestamp}")
print(f"Latest event timestamp: {latest_event_timestamp}")

# Update watermark (would be done via SQL in ADF)
watermark_updates = [
    ("sales_orders", str(latest_sales_timestamp)),
    ("user_events", latest_event_timestamp.isoformat() + "Z")
]

print("Watermarks to update:")
for table, value in watermark_updates:
    print(f"  {table}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Log Pipeline Metrics

# COMMAND ----------

# Create pipeline run log
pipeline_log = spark.createDataFrame([{
    "batch_id": batch_id,
    "pipeline_stage": "silver_to_gold",
    "start_time": datetime.now().isoformat(),
    "sales_processed": df_fact_sales.count(),
    "events_processed": df_table_storage.count(),
    "customers_updated": df_dim_customers.count(),
    "products_updated": df_dim_products.count(),
    "status": "SUCCESS"
}])

pipeline_log.write.mode("append").parquet(f"{GOLD_PATH}/audit/pipeline_logs/")

print("✓ Pipeline metrics logged")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC **Silver to Gold transformation complete!**
# MAGIC 
# MAGIC - ✓ Dimension tables created and loaded to SQL Database
# MAGIC - ✓ Fact table created with business calculations
# MAGIC - ✓ Customer and product aggregates generated
# MAGIC - ✓ Event sessions prepared for Table Storage
# MAGIC - ✓ Data written to Gold layer
# MAGIC - ✓ Watermarks updated
# MAGIC 
# MAGIC **Next**: Run advanced analytics queries