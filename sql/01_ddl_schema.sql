-- ============================================================================
-- Azure SQL Database Schema - DDL Script
-- Data Engineering ETL Assignment
-- ============================================================================

-- Drop existing objects if they exist (for clean reruns)
DROP TABLE IF EXISTS audit.pipeline_runs;
DROP TABLE IF EXISTS audit.watermark;
DROP TABLE IF EXISTS gold.fact_sales;
DROP TABLE IF EXISTS gold.dim_date;
DROP TABLE IF EXISTS gold.dim_products;
DROP TABLE IF EXISTS gold.dim_customers;

DROP SCHEMA IF EXISTS audit;
DROP SCHEMA IF EXISTS gold;
DROP SCHEMA IF EXISTS staging;

GO

-- ============================================================================
-- Create Schemas
-- ============================================================================

CREATE SCHEMA gold;  -- Production tables
GO

CREATE SCHEMA staging;  -- Temporary staging tables
GO

CREATE SCHEMA audit;  -- Audit and control tables
GO

-- ============================================================================
-- Dimension Tables
-- ============================================================================

-- Dimension: Customers
CREATE TABLE gold.dim_customers (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    customer_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    segment VARCHAR(50),
    registration_date DATE,
    is_active BIT DEFAULT 1,
    
    -- SCD Type 1 (overwrite) - Audit columns
    created_date DATETIME DEFAULT GETDATE(),
    last_updated DATETIME DEFAULT GETDATE(),
    
    -- Indexes
    INDEX idx_customer_id (customer_id),
    INDEX idx_segment (segment)
);

-- Dimension: Products
CREATE TABLE gold.dim_products (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price DECIMAL(10,2),
    cost DECIMAL(10,2),
    supplier VARCHAR(200),
    sku VARCHAR(100),
    stock_quantity INT,
    
    -- Audit columns
    created_date DATETIME DEFAULT GETDATE(),
    last_updated DATETIME DEFAULT GETDATE(),
    
    -- Indexes
    INDEX idx_product_id (product_id),
    INDEX idx_category (category)
);

-- Dimension: Date
CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT,
    quarter INT,
    month INT,
    month_name VARCHAR(20),
    day_of_month INT,
    day_of_week INT,
    day_name VARCHAR(20),
    week_of_year INT,
    is_weekend BIT,
    is_holiday BIT DEFAULT 0,
    
    INDEX idx_full_date (full_date),
    INDEX idx_year_month (year, month)
);

-- ============================================================================
-- Fact Tables
-- ============================================================================

-- Fact: Sales
CREATE TABLE gold.fact_sales (
    sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    
    -- Business Keys
    order_id VARCHAR(50) NOT NULL,
    
    -- Foreign Keys (Dimension References)
    customer_key INT FOREIGN KEY REFERENCES gold.dim_customers(customer_key),
    product_key INT FOREIGN KEY REFERENCES gold.dim_products(product_key),
    date_key INT FOREIGN KEY REFERENCES gold.dim_date(date_key),
    
    -- Degenerate Dimensions
    order_timestamp DATETIME,
    payment_method VARCHAR(50),
    
    -- Measures
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    line_total DECIMAL(10,2),
    profit DECIMAL(10,2),  -- Calculated: line_total - (cost * quantity)
    
    -- Status
    status VARCHAR(50),
    
    -- Audit
    batch_id VARCHAR(100),
    created_timestamp DATETIME DEFAULT GETDATE(),
    
    -- Indexes for query performance
    INDEX idx_order_id (order_id),
    INDEX idx_customer_key (customer_key),
    INDEX idx_product_key (product_key),
    INDEX idx_date_key (date_key),
    INDEX idx_status (status),
    INDEX idx_order_timestamp (order_timestamp)
);

-- ============================================================================
-- Staging Tables (for MERGE/UPSERT operations)
-- ============================================================================

CREATE TABLE staging.fact_sales_staging (
    order_id VARCHAR(50),
    customer_key INT,
    product_key INT,
    date_key INT,
    order_timestamp DATETIME,
    payment_method VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    shipping_cost DECIMAL(10,2),
    line_total DECIMAL(10,2),
    profit DECIMAL(10,2),
    status VARCHAR(50),
    batch_id VARCHAR(100)
);

CREATE TABLE staging.dim_customers_staging (
    customer_id VARCHAR(50),
    customer_name VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    segment VARCHAR(50),
    registration_date DATE,
    is_active BIT
);

CREATE TABLE staging.dim_products_staging (
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price DECIMAL(10,2),
    cost DECIMAL(10,2),
    supplier VARCHAR(200),
    sku VARCHAR(100),
    stock_quantity INT
);

-- ============================================================================
-- Audit & Control Tables
-- ============================================================================

-- Pipeline Run Metadata
CREATE TABLE audit.pipeline_runs (
    run_id INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    run_date DATE NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    duration_seconds AS DATEDIFF(SECOND, start_time, end_time),
    records_read INT,
    records_written INT,
    records_failed INT,
    status VARCHAR(20),  -- 'RUNNING', 'SUCCESS', 'FAILED'
    error_message VARCHAR(MAX),
    batch_id VARCHAR(100),
    
    INDEX idx_pipeline_date (pipeline_name, run_date),
    INDEX idx_status (status)
);

-- Watermark Table (for incremental loads)
CREATE TABLE audit.watermark (
    source_table VARCHAR(100) PRIMARY KEY,
    watermark_column VARCHAR(100),
    watermark_value VARCHAR(100),
    last_updated DATETIME DEFAULT GETDATE()
);

-- Initialize watermark values
INSERT INTO audit.watermark (source_table, watermark_column, watermark_value)
VALUES 
    ('sales_orders', 'order_timestamp', '2024-01-01 00:00:00'),
    ('user_events', 'timestamp', '2024-01-01T00:00:00Z');

-- ============================================================================
-- Stored Procedures
-- ============================================================================

GO

-- Merge Customers (SCD Type 1 - Overwrite)
CREATE OR ALTER PROCEDURE staging.sp_merge_dim_customers
AS
BEGIN
    SET NOCOUNT ON;
    
    MERGE gold.dim_customers AS target
    USING staging.dim_customers_staging AS source
    ON target.customer_id = source.customer_id
    
    WHEN MATCHED THEN
        UPDATE SET
            customer_name = source.customer_name,
            email = source.email,
            phone = source.phone,
            segment = source.segment,
            registration_date = source.registration_date,
            is_active = source.is_active,
            last_updated = GETDATE()
    
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (customer_id, customer_name, email, phone, segment, registration_date, is_active)
        VALUES (source.customer_id, source.customer_name, source.email, source.phone, 
                source.segment, source.registration_date, source.is_active);
    
    -- Clean staging
    TRUNCATE TABLE staging.dim_customers_staging;
END;
GO

-- Merge Products (SCD Type 1 - Overwrite)
CREATE OR ALTER PROCEDURE staging.sp_merge_dim_products
AS
BEGIN
    SET NOCOUNT ON;
    
    MERGE gold.dim_products AS target
    USING staging.dim_products_staging AS source
    ON target.product_id = source.product_id
    
    WHEN MATCHED THEN
        UPDATE SET
            product_name = source.product_name,
            category = source.category,
            subcategory = source.subcategory,
            unit_price = source.unit_price,
            cost = source.cost,
            supplier = source.supplier,
            sku = source.sku,
            stock_quantity = source.stock_quantity,
            last_updated = GETDATE()
    
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (product_id, product_name, category, subcategory, unit_price, cost, supplier, sku, stock_quantity)
        VALUES (source.product_id, source.product_name, source.category, source.subcategory, 
                source.unit_price, source.cost, source.supplier, source.sku, source.stock_quantity);
    
    -- Clean staging
    TRUNCATE TABLE staging.dim_products_staging;
END;
GO

-- Merge Fact Sales (Idempotent - avoid duplicates)
CREATE OR ALTER PROCEDURE staging.sp_merge_fact_sales
AS
BEGIN
    SET NOCOUNT ON;
    
    MERGE gold.fact_sales AS target
    USING staging.fact_sales_staging AS source
    ON target.order_id = source.order_id
    
    WHEN MATCHED THEN
        UPDATE SET
            customer_key = source.customer_key,
            product_key = source.product_key,
            date_key = source.date_key,
            order_timestamp = source.order_timestamp,
            payment_method = source.payment_method,
            quantity = source.quantity,
            unit_price = source.unit_price,
            discount_amount = source.discount_amount,
            shipping_cost = source.shipping_cost,
            line_total = source.line_total,
            profit = source.profit,
            status = source.status,
            batch_id = source.batch_id
    
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (order_id, customer_key, product_key, date_key, order_timestamp, payment_method,
                quantity, unit_price, discount_amount, shipping_cost, line_total, profit, status, batch_id)
        VALUES (source.order_id, source.customer_key, source.product_key, source.date_key, 
                source.order_timestamp, source.payment_method, source.quantity, source.unit_price,
                source.discount_amount, source.shipping_cost, source.line_total, source.profit, 
                source.status, source.batch_id);
    
    -- Clean staging
    TRUNCATE TABLE staging.fact_sales_staging;
END;
GO

-- Update Watermark
CREATE OR ALTER PROCEDURE audit.sp_update_watermark
    @source_table VARCHAR(100),
    @new_watermark_value VARCHAR(100)
AS
BEGIN
    UPDATE audit.watermark
    SET watermark_value = @new_watermark_value,
        last_updated = GETDATE()
    WHERE source_table = @source_table;
END;
GO

-- Log Pipeline Run
CREATE OR ALTER PROCEDURE audit.sp_log_pipeline_run
    @pipeline_name VARCHAR(100),
    @status VARCHAR(20),
    @records_read INT = NULL,
    @records_written INT = NULL,
    @records_failed INT = NULL,
    @error_message VARCHAR(MAX) = NULL,
    @batch_id VARCHAR(100) = NULL
AS
BEGIN
    IF @status = 'START'
    BEGIN
        INSERT INTO audit.pipeline_runs (pipeline_name, run_date, start_time, status, batch_id)
        VALUES (@pipeline_name, CAST(GETDATE() AS DATE), GETDATE(), 'RUNNING', @batch_id);
        
        SELECT SCOPE_IDENTITY() AS run_id;
    END
    ELSE
    BEGIN
        UPDATE audit.pipeline_runs
        SET end_time = GETDATE(),
            records_read = @records_read,
            records_written = @records_written,
            records_failed = @records_failed,
            status = @status,
            error_message = @error_message
        WHERE batch_id = @batch_id;
    END
END;
GO

-- ============================================================================
-- Populate Date Dimension (2023-2026)
-- ============================================================================

DECLARE @StartDate DATE = '2023-01-01';
DECLARE @EndDate DATE = '2026-12-31';

WHILE @StartDate <= @EndDate
BEGIN
    INSERT INTO gold.dim_date (
        date_key,
        full_date,
        year,
        quarter,
        month,
        month_name,
        day_of_month,
        day_of_week,
        day_name,
        week_of_year,
        is_weekend
    )
    VALUES (
        CAST(FORMAT(@StartDate, 'yyyyMMdd') AS INT),
        @StartDate,
        YEAR(@StartDate),
        DATEPART(QUARTER, @StartDate),
        MONTH(@StartDate),
        DATENAME(MONTH, @StartDate),
        DAY(@StartDate),
        DATEPART(WEEKDAY, @StartDate),
        DATENAME(WEEKDAY, @StartDate),
        DATEPART(WEEK, @StartDate),
        CASE WHEN DATEPART(WEEKDAY, @StartDate) IN (1, 7) THEN 1 ELSE 0 END
    );
    
    SET @StartDate = DATEADD(DAY, 1, @StartDate);
END;

GO

-- ============================================================================
-- Verification Queries
-- ============================================================================

-- Check schema creation
SELECT 
    SCHEMA_NAME(schema_id) AS schema_name,
    name AS table_name,
    type_desc
FROM sys.tables
WHERE SCHEMA_NAME(schema_id) IN ('gold', 'staging', 'audit')
ORDER BY schema_name, table_name;

-- Check date dimension population
SELECT 
    COUNT(*) AS total_dates,
    MIN(full_date) AS min_date,
    MAX(full_date) AS max_date
FROM gold.dim_date;

PRINT 'Database schema created successfully!';
