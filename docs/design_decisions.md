# Design Decisions & Rationale

## 📋 Table of Contents

- [Architecture Patterns](#architecture-patterns)
- [Technology Choices](#technology-choices)
- [Data Modeling](#data-modeling)
- [ETL Design](#etl-design)
- [Data Quality Strategy](#data-quality-strategy)
- [Performance Considerations](#performance-considerations)

---

## 🏗️ Architecture Patterns

### Medallion Architecture (Bronze/Silver/Gold)

**Decision**: Implement 3-layer data lake architecture

**Rationale**:
- **Bronze** (Raw): Immutable copy of source data for auditing and reprocessing
- **Silver** (Cleaned): Quality-checked, validated data ready for consumption
- **Gold** (Analytics-Ready): Business-level aggregates and denormalized tables

**Benefits**:
✅ Clear separation of concerns  
✅ Easy to troubleshoot (can trace back through layers)  
✅ Supports data lineage and governance  
✅ Industry standard pattern (Databricks, Snowflake)

**Alternatives Considered**:
- ❌ **Lambda Architecture** (stream + batch): Too complex for batch-only use case
- ❌ **Flat data lake**: Hard to maintain, poor governance

---

### Separate Pipelines for Transactional vs Event Data

**Decision**: Build two independent ETL pipelines

**Rationale**:

| Aspect | Sales (Transactional) | Events (Logs) |
|--------|----------------------|---------------|
| **Schema** | Fixed, predictable | Semi-structured, varies |
| **Volume** | 10K/day | 50K+/hour |
| **Frequency** | Daily batch | Hourly micro-batch |
| **Validation** | Strict (financial accuracy) | Flexible (best effort) |
| **Error Handling** | Fail fast | Continue with partial data |
| **Target** | SQL Database | NoSQL (Table Storage) |

**Benefits**:
✅ Independent scaling (can increase frequency of events without affecting sales)  
✅ Different error handling strategies appropriate to each data type  
✅ Clear ownership (separate teams could own each pipeline)  
✅ Easier debugging (isolated failures)

**Alternatives Considered**:
- ❌ **Single unified pipeline**: Different schedules and validation rules would conflict
- ❌ **Parallel activities in one pipeline**: Complex dependencies, harder to maintain

---

## 🛠️ Technology Choices

### Azure Data Factory for Orchestration

**Decision**: Use ADF for pipeline scheduling and data movement

**Rationale**:
✅ Native Azure integration (SQL DB, Data Lake, Databricks)  
✅ Visual pipeline designer (low-code)  
✅ Built-in monitoring and alerting  
✅ Supports incremental loading patterns  
✅ Serverless (no infrastructure management)

**Alternatives Considered**:
- ❌ **Apache Airflow**: More powerful but requires infrastructure management
- ❌ **Azure Logic Apps**: Better for event-driven workflows, not batch ETL
- ❌ **Databricks Jobs**: Would work but less suited for orchestration across services

---

### Azure Databricks (Spark) for Transformations

**Decision**: Use Databricks with PySpark for all transformations

**Rationale**:
✅ Scales horizontally (can process millions of records)  
✅ Spark SQL familiar to SQL developers  
✅ Native support for Parquet, Delta Lake  
✅ Interactive notebooks for development  
✅ Can connect to SQL DB via JDBC

**Alternatives Considered**:
- ❌ **SQL Stored Procedures**: Can't scale beyond single SQL instance
- ❌ **Azure Synapse**: Overkill for this data volume
- ❌ **Python pandas**: Doesn't scale beyond single machine memory

---

### Azure SQL Database (Relational Store)

**Decision**: Use Azure SQL Database for dimensional model

**Rationale**:

**Why SQL for Sales Data?**
- Fixed schema (orders always have customer, product, date, amount)
- ACID transactions needed for financial accuracy
- Complex analytical queries (joins across 4+ tables)
- BI tools (Power BI, Tableau) integrate natively with SQL
- Star schema optimized for OLAP queries

**Why Azure SQL specifically?**
✅ Managed service (automatic backups, patching)  
✅ Built-in high availability  
✅ Elastic scaling (can increase DTUs if needed)  
✅ Native integration with ADF, Databricks

**Alternatives Considered**:
- ❌ **Azure Synapse Analytics**: 10x more expensive, designed for petabyte-scale
- ❌ **SQL Server on VM**: Would need to manage OS, patches, backups
- ❌ **PostgreSQL**: Works but less integrated with Azure ecosystem

---

### Azure Table Storage (NoSQL Store)

**Decision**: Use Table Storage for event data instead of Cosmos DB

**Rationale**:

| Criteria | Table Storage | Cosmos DB |
|----------|--------------|-----------|
| **Cost** | ~$0.10/month | ~$25-100/month |
| **Throughput** | 20K ops/sec | 100K+ ops/sec |
| **Latency** | <100ms | <10ms |
| **Global Distribution** | No | Yes |
| **Use Case Fit** | ✅ Perfect for logs | ❌ Overkill |

**Why Table Storage is Sufficient**:
- Event logs don't need <10ms latency
- No need for global distribution
- Query pattern is simple: lookup by user_id or session_id
- 90% cost savings vs Cosmos DB

**When to Use Cosmos DB Instead**:
- Multi-region application with users worldwide
- <10ms latency SLA required
- Complex queries with multiple indexes
- Unlimited scale required

**Alternatives Considered**:
- ❌ **Cosmos DB**: Too expensive for this use case
- ❌ **MongoDB Atlas**: Requires separate subscription, not native Azure
- ❌ **Just use SQL**: Event metadata doesn't fit fixed schema well

---

## 📊 Data Modeling

### Star Schema for Sales Analytics

**Decision**: Implement denormalized star schema

**Schema**:
```
         dim_customers
               |
               |
         dim_products ← fact_sales → dim_date
```

**Rationale**:

**Denormalization Benefits**:
- Faster queries (fewer joins needed)
- Easier for BI tools to understand
- Optimized for read-heavy analytical workload

**Grain**: One row per order (order_id is unique)

**Why Not Snowflake Schema?**
- Sales data volume doesn't justify additional normalization
- More joins = slower queries for end users
- Star schema is simpler to maintain

**SCD Strategy**:
- **Dimensions**: Type 1 (overwrite) - No history tracking needed
- **Fact**: Append-only (never update, only insert new records)

**Alternatives Considered**:
- ❌ **Fully normalized (3NF)**: Good for OLTP, bad for analytics
- ❌ **Snowflake schema**: Unnecessary complexity for this scale
- ❌ **Data Vault**: Too complex for assignment scope

---

### Document Model for Events

**Decision**: Store events as flat documents in Table Storage

**Schema**:
```
PartitionKey: user_id  (distributes data)
RowKey: session_id     (unique within partition)
Properties: session_start, duration_seconds, event_count, event_journey_json
```

**Rationale**:

**Key-Value Structure**:
- Each session is independent (no relationships)
- Lookup by user_id is the primary query pattern
- Partition key provides automatic data distribution

**Why Not Relational?**
- Event metadata varies by event_type (JSON fits better)
- No foreign keys or complex relationships
- Write-heavy workload (append logs) suits NoSQL

**Alternatives Considered**:
- ❌ **Separate table per event type**: Schema explosion, hard to query
- ❌ **EAV (Entity-Attribute-Value)**: Complex queries, poor performance
- ❌ **JSON in SQL**: Works but doesn't leverage NoSQL strengths

---

## 🔄 ETL Design

### Incremental Loading with Watermarks

**Decision**: Track last processed timestamp per source

**Implementation**:
```sql
-- Audit table
CREATE TABLE audit.watermark (
  source_table VARCHAR(100) PRIMARY KEY,
  watermark_value VARCHAR(100),  -- Last processed timestamp
  last_updated DATETIME
);

-- Each pipeline run:
1. Read watermark
2. Filter: WHERE timestamp > watermark
3. Process data
4. Update watermark = MAX(timestamp)
```

**Benefits**:
✅ Only processes new data (not full reload)  
✅ Reduces processing time and cost  
✅ Enables frequent pipeline runs (hourly for events)

**Handling Late-Arriving Data**:
- Use lookback window (process last 24 hours even if watermark higher)
- MERGE statements prevent duplicates

**Alternatives Considered**:
- ❌ **Full load every time**: Wastes time and money
- ❌ **File-based tracking**: Hard to manage, error-prone
- ❌ **Change Data Capture**: Too complex for CSV/JSON sources

---

### Idempotent Pipeline Design

**Decision**: Pipelines can be safely rerun without duplicates

**Techniques**:

1. **MERGE instead of INSERT**:
```sql
MERGE INTO gold.fact_sales AS target
USING staging.fact_sales_staging AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```

2. **Batch ID Tracking**:
```python
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
df.withColumn("batch_id", F.lit(batch_id))
```

3. **Partitioned Overwrites**:
```python
# Overwrite only specific date partition
df.write.mode("overwrite").partitionBy("order_date").parquet()
```

**Benefits**:
✅ Safe to rerun failed pipelines  
✅ Can reprocess specific dates  
✅ No duplicate records in target

---

### Error Handling Strategy

**Decision**: Different strategies for different pipeline types

**Sales Pipeline (Strict)**:
```python
try:
    validate_sales_data(df)
except ValidationError:
    # Log error, send alert, STOP pipeline
    raise PipelineFailure("Invalid sales data")
```

**Events Pipeline (Flexible)**:
```python
try:
    df_valid, df_invalid = validate_events(df)
    # Log invalid count but CONTINUE
    log_metrics(invalid_count=len(df_invalid))
    # Process valid records
    load(df_valid)
except Exception as e:
    # Log but don't fail pipeline
    log_warning(f"Partial failure: {e}")
```

**Rationale**:
- **Sales**: Financial accuracy critical, fail fast
- **Events**: Best effort, partial data acceptable

**Alternatives Considered**:
- ❌ **Always fail on any error**: Too strict for high-volume logs
- ❌ **Never fail, always continue**: Dangerous for financial data

---

## 🛡️ Data Quality Strategy

### Validation Layers

**Bronze Layer**:
- ✅ Schema validation (correct columns present)
- ✅ Duplicate detection
- ❌ No business rule validation yet

**Silver Layer**:
- ✅ Null checks on critical fields
- ✅ Data type validation
- ✅ Range checks (amounts > 0, dates not in future)
- ✅ Referential integrity (customer_id, product_id exist)

**Gold Layer**:
- ✅ Orphan detection (foreign keys match)
- ✅ Completeness checks (all required dimensions present)

**Quarantine Pattern**:
```
Invalid records → /silver/quarantine/{batch_id}/
```

**Benefits**:
✅ Invalid data not lost (can be fixed and reprocessed)  
✅ Audit trail of data quality issues  
✅ Clean pipelines (only valid data downstream)

---

### Audit Logging

**Decision**: Log all pipeline runs with metrics

**Audit Table**:
```sql
CREATE TABLE audit.pipeline_runs (
  run_id INT PRIMARY KEY,
  pipeline_name VARCHAR(100),
  batch_id VARCHAR(100),
  records_read INT,
  records_written INT,
  records_failed INT,
  status VARCHAR(20),  -- SUCCESS, FAILED
  error_message VARCHAR(MAX)
);
```

**Benefits**:
✅ Track data lineage  
✅ Debug pipeline failures  
✅ SLA monitoring (how long did pipeline take?)  
✅ Data quality trends (% of invalid records over time)

---

## ⚡ Performance Considerations

### Data Lake Partitioning

**Decision**: Partition by date at multiple layers

**Silver Layer**:
```
/silver/sales/order_date=2024-01-15/
/silver/sales/order_date=2024-01-16/
```

**Benefits**:
✅ Partition pruning (only read relevant dates)  
✅ Faster queries: `WHERE order_date = '2024-01-15'`  
✅ Can reprocess specific dates without full reload

---

### SQL Database Indexing

**Decision**: Create indexes on foreign keys and common filters

```sql
-- Fact table indexes
CREATE INDEX idx_customer_key ON fact_sales(customer_key);
CREATE INDEX idx_product_key ON fact_sales(product_key);
CREATE INDEX idx_date_key ON fact_sales(date_key);
CREATE INDEX idx_status ON fact_sales(status);
```

**Rationale**:
- 90% of queries join facts to dimensions (need FK indexes)
- Filtering by status is common (completed vs all orders)

**Tradeoff**:
- Indexes slow down writes (MERGE operations)
- For this use case, read speed > write speed (batch loads once/day)

---

### Databricks Cluster Configuration

**Decision**: Use autoscaling cluster with appropriate node types

```json
{
  "cluster_name": "etl-processing",
  "spark_version": "13.3.x-scala2.12",
  "node_type_id": "Standard_DS3_v2",
  "autoscale": {
    "min_workers": 2,
    "max_workers": 8
  }
}
```

**Rationale**:
- Autoscaling handles variable data volumes
- Start small (2 nodes) and scale up if needed
- DS3_v2 good balance of CPU/memory for SQL workloads

---

## 📈 Scalability Considerations

### Current Scale (Assignment Scope)
- 10K sales orders/day
- 50K events/hour
- Processes in <30 minutes on single Databricks cluster

### Future Scale (Production)
- **10x growth** (100K orders, 500K events): Same architecture, increase cluster size
- **100x growth** (1M orders, 5M events): 
  - Add streaming ingestion (Event Hub)
  - Use Delta Lake instead of Parquet
  - Partition Bronze/Silver by hour instead of day
  - Consider Synapse for SQL warehouse

### Breaking Points
- **Table Storage**: 20K ops/sec limit (handle 72M events/hour)
- **Databricks**: Horizontal scaling (just add more nodes)
- **SQL Database**: Vertical scaling (increase DTUs), then shard if needed

---

## 🔒 Security & Governance

### Current Implementation (Assignment Scope)
- Basic authentication (username/password)
- No encryption at rest
- No data masking

### Production Recommendations
- **Azure Key Vault** for secrets
- **Managed Identities** for service-to-service auth
- **Encryption at rest** for sensitive data (PII)
- **Column-level masking** in SQL for PII fields
- **RBAC** (Role-Based Access Control) for data lake folders
- **Data lineage** tracking with Azure Purview

---

## 📊 Monitoring & Alerting

### Current Implementation
- ADF built-in monitoring
- Audit tables in SQL database
- Manual error checking

### Production Recommendations
- **Azure Monitor** for centralized logging
- **Application Insights** for performance metrics
- **Alerts** on pipeline failures (email/SMS)
- **Dashboards** showing:
  - Pipeline success rate
  - Data quality metrics (% invalid records)
  - Processing time trends
  - Cost per pipeline run

---

## 💡 Lessons Learned

### What Worked Well
✅ Separate pipelines for different data types  
✅ Medallion architecture (clear layers)  
✅ Table Storage instead of Cosmos DB (cost-effective)  
✅ MERGE statements for idempotency  
✅ Detailed audit logging

### What Could Be Improved
⚠️ Add data quality framework (Great Expectations)  
⚠️ Implement SCD Type 2 for dimension history  
⚠️ Add automated testing (unit tests for transformations)  
⚠️ Use Delta Lake instead of Parquet (ACID, time travel)  
⚠️ Add CI/CD pipeline for deployment

---

## 📚 References

- [Medallion Architecture - Databricks](https://www.databricks.com/glossary/medallion-architecture)
- [Star Schema - Kimball Group](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/)
- [Azure Data Factory Best Practices](https://docs.microsoft.com/azure/data-factory/concepts-pipelines-activities)
- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

---

**Document Version**: 1.0  
**Last Updated**: February 2024  
**Author**: [Your Name]
