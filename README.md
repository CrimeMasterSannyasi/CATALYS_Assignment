# Data Engineering ETL Assignment

> **End-to-end ETL pipeline using Azure Data Factory, Databricks, Azure SQL Database, and Azure Table Storage**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Data Sources](#data-sources)
- [Pipeline Flow](#pipeline-flow)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Design Decisions](#design-decisions)
- [Data Quality](#data-quality)
- [SQL Transformations](#sql-transformations)
- [Assumptions & Limitations](#assumptions--limitations)
- [Sample Outputs](#sample-outputs)
- [Project Structure](#project-structure)

---

## 🎯 Overview

This project demonstrates a **production-grade ETL pipeline** that:

- **Ingests** data from multiple sources (CSV, JSON)
- **Transforms** data through Bronze → Silver → Gold layers (Medallion Architecture)
- **Loads** to appropriate data stores (SQL Database for analytics, Table Storage for operational queries)
- Implements **incremental loading**, **data quality checks**, and **idempotent operations**

### Key Features

✅ **Separate pipelines** for transactional vs event data  
✅ **Medallion architecture** (Bronze/Silver/Gold layers)  
✅ **SQL transformations** with window functions and complex joins  
✅ **Star schema** design for dimensional modeling  
✅ **NoSQL storage** for semi-structured event data  
✅ **Data quality** validation and error handling  
✅ **Incremental loads** with watermark pattern  
✅ **Idempotent** pipeline design (safe to rerun)

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────┐         ┌──────────────────┐         ┌─────────────────┐
│   Data Sources  │         │  Orchestration   │         │  Target Stores  │
├─────────────────┤         ├──────────────────┤         ├─────────────────┤
│                 │         │                  │         │                 │
│  CSV Files      │────────▶│  Azure Data      │────────▶│  Azure SQL DB   │
│  (Sales)        │         │  Factory         │         │  (Star Schema)  │
│                 │         │                  │         │                 │
│  JSON Files     │────────▶│  +               │────────▶│  Azure Table    │
│  (Events)       │         │                  │         │  Storage        │
│                 │         │  Azure           │         │  (Key-Value)    │
└─────────────────┘         │  Databricks      │         └─────────────────┘
                            │  (Spark SQL)     │
                            └──────────────────┘
                                     │
                                     ▼
                            ┌──────────────────┐
                            │  Azure Data Lake │
                            │  Gen2 (Storage)  │
                            │                  │
                            │  Bronze Layer    │
                            │  Silver Layer    │
                            │  Gold Layer      │
                            └──────────────────┘
```

### Detailed Pipeline Flow

```
Sales Pipeline (Batch - Daily):
─────────────────────────────────
CSV Files → ADF Ingest → Raw → Bronze → Databricks Clean → Silver 
  → Databricks Transform → Gold → SQL Database (Star Schema)

Events Pipeline (Micro-batch - Hourly):
────────────────────────────────────────
JSON Files → ADF Ingest → Raw → Bronze → Databricks Parse → Silver 
  → Databricks Sessionize → Gold → Table Storage (Documents)
```

See [Architecture Diagram](docs/architecture_diagram.png) for visual representation.

---

## 🛠️ Technologies Used

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Azure Data Factory | Pipeline scheduling, data movement |
| **Transformation** | Azure Databricks (PySpark) | Data cleaning, business logic, SQL |
| **Relational Storage** | Azure SQL Database | Star schema, analytical queries |
| **NoSQL Storage** | Azure Table Storage | Event logs, high-volume key-value |
| **Data Lake** | Azure Data Lake Gen2 | Medallion architecture (Bronze/Silver/Gold) |
| **Languages** | Python, Spark SQL, T-SQL | Data processing and transformations |

---

## 📊 Data Sources

### 1. Transactional Data (CSV)

**File**: `sales_orders.csv`  
**Volume**: ~10,000 orders  
**Schema**:
```
order_id, customer_id, product_id, order_date, order_timestamp,
quantity, unit_price, discount_amount, shipping_cost, line_total,
status, payment_method, last_modified
```

**Sample**:
```csv
order_id,customer_id,product_id,order_date,quantity,unit_price,status
ORD000001,CUST0042,PROD003,2024-01-15,2,19.99,completed
ORD000002,CUST0123,PROD001,2024-01-15,1,1299.99,completed
```

### 2. Event/Log Data (JSON)

**File**: `user_events.json`  
**Volume**: ~50,000 events  
**Format**: Newline-delimited JSON

**Sample**:
```json
{
  "event_id": "EVT00000001",
  "user_id": "USER00123",
  "event_type": "page_view",
  "timestamp": "2024-01-15T10:30:45Z",
  "session_id": "SESS123456",
  "metadata": {
    "page": "/products/electronics",
    "device": "mobile",
    "browser": "Chrome"
  }
}
```

### Reference Data

- **Customers**: 200 customers with segments (Premium/Standard/Budget)
- **Products**: 15 products across Electronics, Furniture, Office Supplies

---

## 🔄 Pipeline Flow

### Phase 1: Ingestion (ADF)

1. **Trigger**: Scheduled (Sales: daily 2 AM, Events: hourly)
2. **Watermark Check**: Query audit table for last processed timestamp
3. **Copy Activity**: Read source files → Write to Raw/Bronze layers
4. **Incremental Logic**: Only load records newer than watermark

### Phase 2: Transformation (Databricks)

**Notebook 1: Bronze → Silver (Data Quality)**
- Remove duplicates
- Validate critical fields
- Standardize formats (UPPER, TRIM, type casting)
- Filter invalid records → Quarantine
- Write clean data to Silver

**Notebook 2: Silver → Gold (Business Logic)**
- Join sales with customer/product dimensions
- Calculate profit = revenue - cost
- Create star schema (dims + facts)
- Generate aggregates (customer/product metrics)
- Sessionize events (group by session_id)

**Notebook 3: Advanced Analytics**
- Window functions (ROW_NUMBER, RANK, moving averages)
- Customer lifetime value
- Product performance rankings
- Conversion funnel analysis

### Phase 3: Loading (ADF + Databricks)

1. **Databricks** writes to Gold layer (Parquet)
2. **ADF** triggers load pipeline
3. **SQL Database**: MERGE into dimensions and facts (idempotent)
4. **Table Storage**: Bulk insert session documents
5. **Update watermarks** in audit table

---

## 🚀 Setup Instructions

### Prerequisites

- Azure subscription (or free tier)
- Azure Data Factory instance
- Azure Databricks workspace (cluster)
- Azure SQL Database
- Azure Storage Account (Data Lake Gen2 enabled)
- Azure Table Storage

### Step 1: Clone Repository

```bash
git clone https://github.com/yourusername/data-engineering-etl-assignment.git
cd data-engineering-etl-assignment
```

### Step 2: Generate Sample Data

```bash
cd sample_data
python generate_sales_data.py
python generate_event_data.py
```

This creates:
- `sales_orders.csv` (10,000+ orders)
- `customers.csv` (200 customers)
- `products.csv` (15 products)
- `user_events.json` (50,000+ events)

### Step 3: Setup Azure Resources

#### 3.1 Create Resource Group

```bash
az group create --name rg-data-engineering --location eastus
```

#### 3.2 Create Storage Account (Data Lake)

```bash
az storage account create \
  --name dlsyourstorage \
  --resource-group rg-data-engineering \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true
```

Create containers: `raw`, `bronze`, `silver`, `gold`

#### 3.3 Create Azure SQL Database

```bash
az sql server create \
  --name sql-dataeng-server \
  --resource-group rg-data-engineering \
  --location eastus \
  --admin-user sqladmin \
  --admin-password YourPassword123!

az sql db create \
  --name sales_analytics_db \
  --server sql-dataeng-server \
  --resource-group rg-data-engineering \
  --service-objective S0
```

Run DDL script:
```bash
sqlcmd -S sql-dataeng-server.database.windows.net -d sales_analytics_db \
  -U sqladmin -P YourPassword123! -i sql/01_ddl_schema.sql
```

#### 3.4 Create Azure Table Storage

Already included in the storage account created above.

#### 3.5 Create Data Factory

```bash
az datafactory create \
  --name adf-data-engineering \
  --resource-group rg-data-engineering \
  --location eastus
```

#### 3.6 Create Databricks Workspace

```bash
az databricks workspace create \
  --name dbw-data-engineering \
  --resource-group rg-data-engineering \
  --location eastus \
  --sku premium
```

### Step 4: Upload Sample Data

```bash
# Upload to storage account
az storage blob upload-batch \
  --account-name dlsyourstorage \
  --destination raw/sales \
  --source sample_data/*.csv

az storage blob upload-batch \
  --account-name dlsyourstorage \
  --destination raw/events \
  --source sample_data/*.json
```

### Step 5: Import ADF Pipelines

1. Open Azure Data Factory Studio
2. Go to Author → Pipelines → Import
3. Upload JSON files from `/adf/` folder
4. Update linked services with your connection strings

### Step 6: Import Databricks Notebooks

1. Open Databricks workspace
2. Go to Workspace → Import
3. Upload `.py` files from `/databricks/` folder

### Step 7: Configure Connections

Update configuration in notebooks:
```python
# Databricks notebooks
STORAGE_ACCOUNT = "dlsyourstorage"
jdbc_url = "jdbc:sqlserver://sql-dataeng-server.database.windows.net:1433;database=sales_analytics_db"
```

---

## ▶️ Running the Pipeline

### Option 1: Manual Execution (Recommended for Testing)

1. **Run Data Generation** (if not done):
   ```bash
   python sample_data/generate_sales_data.py
   python sample_data/generate_event_data.py
   ```

2. **Upload to Raw Layer** (simulate source files)

3. **Run ADF Pipeline 1**: `pipeline_ingest_sales`
   - Copies CSV → Bronze layer

4. **Run ADF Pipeline 2**: `pipeline_ingest_events`
   - Copies JSON → Bronze layer

5. **Run Databricks Notebook 1**: `01_bronze_to_silver.py`
   - Cleans and validates data → Silver layer

6. **Run Databricks Notebook 2**: `02_silver_to_gold.py`
   - Applies business logic → Gold layer + SQL DB

7. **Run ADF Pipeline 3**: `pipeline_load_table_storage`
   - Loads sessions to Azure Table Storage

### Option 2: Scheduled Execution

**Sales Pipeline**: Daily at 2 AM
```json
{
  "recurrence": {
    "frequency": "Day",
    "interval": 1,
    "schedule": { "hours": [2], "minutes": [0] }
  }
}
```

**Events Pipeline**: Every hour
```json
{
  "recurrence": {
    "frequency": "Hour",
    "interval": 1
  }
}
```

### Option 3: Local Simulation (Without Azure)

If you don't have Azure access:

1. Use **local Spark** instead of Databricks
2. Use **SQLite** instead of Azure SQL
3. Use **local filesystem** instead of Data Lake
4. Use **JSON files** instead of Table Storage

See `docs/local_setup.md` for instructions.

---

## 💡 Design Decisions

### Why Separate Pipelines for Sales vs Events?

| Aspect | Sales | Events |
|--------|-------|--------|
| **Schema** | Fixed columns | Semi-structured JSON |
| **Volume** | 10K/day | 50K+/hour |
| **Frequency** | Daily batch | Hourly micro-batch |
| **Validation** | Strict (fail on errors) | Flexible (filter invalid) |
| **Target** | SQL DB (star schema) | Table Storage (documents) |

**Conclusion**: Different data characteristics require different processing patterns.

### Why Azure SQL Database for Sales Data?

✅ **Structured schema** - Sales orders have fixed columns  
✅ **ACID compliance** - Financial data needs consistency  
✅ **Complex joins** - Analytics requires joining customers, products, dates  
✅ **Aggregations** - Business metrics, BI reporting  
✅ **Star schema** - Optimized for analytical queries

### Why Azure Table Storage for Event Data?

✅ **Schema flexibility** - Event metadata varies by type  
✅ **High write throughput** - Millions of events/day  
✅ **Fast key lookups** - Query by user_id (partition key)  
✅ **Cost-effective** - 90% cheaper than Cosmos DB for same workload  
✅ **Simple data model** - No complex relationships

**Why NOT Cosmos DB?**
- ❌ Too expensive for this scale ($25-100/month vs $0.10/month)
- ❌ Overkill - Don't need global distribution or <10ms latency
- ❌ Shows poor cost awareness (important for data engineers!)

### Star Schema Design

```
Dimensions:
  - dim_customers (SCD Type 1)
  - dim_products (SCD Type 1)
  - dim_date (pre-populated)

Fact:
  - fact_sales (grain: one row per order)

Foreign Keys: customer_key, product_key, date_key
```

**Why denormalized?**
- Optimized for query performance (fewer joins)
- Analytical workload (read-heavy, not OLTP)
- Star schema is industry standard for data warehousing

---

## 🛡️ Data Quality

### Validation Rules

**Sales Data**:
- ✅ `order_id` must be unique and not null
- ✅ `customer_id`, `product_id` must exist
- ✅ `quantity` > 0, `unit_price` > 0
- ✅ `line_total` >= 0
- ✅ `order_date` <= current_date
- ✅ `status` in ['completed', 'pending', 'cancelled', 'returned']

**Event Data**:
- ✅ `event_id` must be unique
- ✅ `user_id` must exist (filter out BOT%, TEST%)
- ✅ `timestamp` <= current_timestamp
- ✅ `event_type` must be valid
- ⚠️ Metadata fields can be null (flexible)

### Error Handling

**Invalid Records**:
- Written to **quarantine tables** for analysis
- Logged in audit tables with error reason
- Never silently dropped

**Pipeline Failures**:
- **Sales**: Fail fast (financial data must be accurate)
- **Events**: Best effort (partial data OK for logs)
- Retry logic: 3 attempts with exponential backoff

### Idempotency

Pipelines can be safely rerun:
- **MERGE** statements (UPSERT) instead of INSERT
- **batch_id** tracking prevents duplicates
- Watermark tables track what's been processed
- Gold layer partitioned by date (reprocess specific dates)

---

## 📈 SQL Transformations

### Window Functions

**7-Day Moving Average**:
```sql
SELECT 
  order_date,
  SUM(line_total) as daily_revenue,
  AVG(SUM(line_total)) OVER (
    ORDER BY order_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as moving_avg_7day
FROM gold.fact_sales
GROUP BY order_date;
```

**Customer Ranking**:
```sql
SELECT 
  customer_name,
  total_spent,
  RANK() OVER (ORDER BY total_spent DESC) as customer_rank,
  NTILE(4) OVER (ORDER BY total_spent DESC) as quartile
FROM customer_metrics;
```

### Complex Joins

**Sales with All Dimensions**:
```sql
SELECT 
  f.order_id,
  c.customer_name,
  c.segment,
  p.product_name,
  p.category,
  d.month_name,
  d.year,
  f.line_total,
  f.profit
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
LEFT JOIN gold.dim_date d ON f.date_key = d.date_key
WHERE f.status = 'completed';
```

### Aggregations

**Product Performance**:
```sql
SELECT 
  category,
  COUNT(DISTINCT product_id) as product_count,
  SUM(units_sold) as total_units,
  SUM(revenue) as total_revenue,
  ROUND(100.0 * SUM(profit) / SUM(revenue), 2) as margin_pct
FROM product_metrics
GROUP BY category
ORDER BY total_revenue DESC;
```

See [SQL Transformations](sql/02_transformations.sql) for complete examples.

---

## ⚠️ Assumptions & Limitations

### Assumptions

1. **Data Availability**: Source files arrive daily/hourly as scheduled
2. **Schema Stability**: CSV/JSON schemas don't change frequently
3. **Network**: Stable connection between Azure services
4. **Scale**: Dataset fits single Databricks cluster (not distributed)
5. **Security**: Basic authentication (not enterprise-grade RBAC)
6. **Time Zone**: All timestamps in UTC

### Limitations

1. **No Real-Time Streaming**: Batch/micro-batch only (not Event Hub/Stream Analytics)
2. **SCD Type 1 Only**: Dimensions overwrite (no history tracking)
3. **Single Region**: No geo-replication or disaster recovery
4. **Basic Error Handling**: Manual intervention required for complex failures
5. **No PII Encryption**: Sensitive data not masked/encrypted
6. **Limited Monitoring**: Basic ADF monitoring (not Azure Monitor/App Insights)
7. **Simulated Incrementals**: Real incremental loads would need CDC (Change Data Capture)

### Future Enhancements

- 🔄 Implement SCD Type 2 for customer history
- 🔐 Add data encryption and masking for PII
- 📊 Integrate with Power BI for dashboards
- ⚡ Add real-time streaming with Event Hub
- 🔔 Implement alerting with Azure Monitor
- 🧪 Add data quality framework (Great Expectations)
- 🌍 Multi-region deployment with failover

---

## 📸 Sample Outputs

### Fact Table (SQL Database)

```
sales_key | order_id  | customer_key | product_key | quantity | line_total | profit
----------|-----------|--------------|-------------|----------|------------|--------
1         | ORD000001 | 42           | 3           | 2        | 39.98      | 29.98
2         | ORD000002 | 123          | 1           | 1        | 1299.99    | 400.00
```

### Customer Metrics

```
customer_name | total_orders | total_spent | avg_order_value | customer_tier
--------------|--------------|-------------|-----------------|---------------
John Smith    | 45           | $12,543.21  | $278.74         | Top 25% (VIP)
Jane Doe      | 23           | $6,234.50   | $271.07         | Top 50% (Premium)
```

### Event Sessions (Table Storage)

```json
{
  "PartitionKey": "USER00123",
  "RowKey": "SESS123456",
  "session_start": "2024-01-15T10:30:00Z",
  "session_end": "2024-01-15T10:45:32Z",
  "duration_seconds": 932,
  "total_events": 15,
  "event_journey": [...]
}
```

See [docs/sample_outputs/](docs/sample_outputs/) for more examples.

---

## 📁 Project Structure

```
/data-engineering-etl-assignment
│
├── README.md                          # This file
├── LICENSE
│
├── /sample_data                       # Sample datasets
│   ├── generate_sales_data.py         # Generates 10K sales orders
│   ├── generate_event_data.py         # Generates 50K events
│   ├── sales_orders.csv               # Generated sales data
│   ├── customers.csv                  # Customer dimension
│   ├── products.csv                   # Product dimension
│   └── user_events.json               # Generated events
│
├── /adf                               # Azure Data Factory pipelines
│   ├── pipeline_ingest_sales.json     # Sales ingestion pipeline
│   ├── pipeline_ingest_events.json    # Events ingestion pipeline
│   ├── pipeline_transform_load.json   # Orchestration pipeline
│   └── README.md                      # ADF setup guide
│
├── /databricks                        # Databricks notebooks
│   ├── 01_bronze_to_silver.py         # Data quality & cleaning
│   ├── 02_silver_to_gold.py           # Business transformations
│   ├── 03_advanced_analytics.py       # Window functions & analytics
│   └── README.md
│
├── /sql                               # SQL scripts
│   ├── 01_ddl_schema.sql              # Database schema creation
│   ├── 02_transformations.sql         # All transformation queries
│   ├── 03_sample_queries.sql          # Analytical query examples
│   └── 04_audit.sql                   # Audit table setup
│
├── /docs                              # Documentation
│   ├── architecture_diagram.png       # Visual architecture
│   ├── design_decisions.md            # Detailed design rationale
│   ├── data_quality_rules.md          # Validation rules
│   ├── local_setup.md                 # Run without Azure
│   └── /sample_outputs                # Example outputs
│
└── /configs                           # Configuration files
    ├── databricks_cluster.json        # Cluster config
    └── linked_services.json           # ADF connections
```

---

## 📚 Additional Resources

- [Azure Data Factory Documentation](https://docs.microsoft.com/azure/data-factory/)
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Star Schema Design](https://en.wikipedia.org/wiki/Star_schema)

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file

---

## 👤 Author

**Your Name**  
📧 your.email@example.com  
🔗 [LinkedIn](https://linkedin.com/in/yourprofile)  
🐙 [GitHub](https://github.com/yourusername)

---

## 🙏 Acknowledgments

- Assignment provided by [Institution/Company]
- Inspired by real-world production ETL patterns
- Sample data generated for educational purposes

---

**⭐ If you found this project helpful, please give it a star!**
