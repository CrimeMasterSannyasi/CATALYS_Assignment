-- ============================================================================
-- SQL Transformation Queries
-- Used in Databricks notebooks for Bronze → Silver → Gold transformations
-- ============================================================================

-- ============================================================================
-- BRONZE TO SILVER: Data Quality & Cleaning
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sales Orders: Remove Duplicates (keep latest by last_modified)
-- ----------------------------------------------------------------------------
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY order_id 
           ORDER BY last_modified DESC, order_timestamp DESC
         ) as row_num
  FROM bronze.sales_orders
) ranked
WHERE row_num = 1;

-- ----------------------------------------------------------------------------
-- Sales Orders: Validate and Clean
-- ----------------------------------------------------------------------------
SELECT 
  order_id,
  UPPER(TRIM(customer_id)) as customer_id,
  UPPER(TRIM(product_id)) as product_id,
  CAST(order_date AS DATE) as order_date,
  order_timestamp,
  quantity,
  unit_price,
  discount_amount,
  shipping_cost,
  line_total,
  LOWER(TRIM(status)) as status,
  LOWER(TRIM(payment_method)) as payment_method,
  last_modified
FROM bronze.sales_orders
WHERE 
  -- Critical fields must not be null
  order_id IS NOT NULL 
  AND customer_id IS NOT NULL 
  AND customer_id != ''
  AND product_id IS NOT NULL
  AND product_id != ''
  -- Business rule validations
  AND quantity > 0
  AND unit_price > 0
  AND line_total >= 0
  -- Date validations
  AND order_date <= CURRENT_DATE()
  AND order_date >= '2023-01-01'
  -- Status must be valid
  AND status IN ('completed', 'pending', 'cancelled', 'returned');

-- ----------------------------------------------------------------------------
-- Events: Parse JSON and Validate
-- ----------------------------------------------------------------------------
SELECT 
  event_id,
  UPPER(TRIM(user_id)) as user_id,
  LOWER(TRIM(event_type)) as event_type,
  CAST(timestamp AS TIMESTAMP) as event_timestamp,
  session_id,
  ip_address,
  
  -- Extract nested metadata fields
  metadata.page as page,
  metadata.device as device,
  metadata.browser as browser,
  metadata.referrer as referrer,
  
  -- Keep full metadata as JSON string for flexibility
  to_json(metadata) as metadata_json
  
FROM bronze.user_events
WHERE 
  -- Critical fields
  event_id IS NOT NULL 
  AND user_id IS NOT NULL
  AND user_id != ''
  AND event_type IS NOT NULL
  -- Filter out bot traffic
  AND NOT (user_id LIKE 'BOT%')
  AND NOT (user_id LIKE 'TEST%')
  -- Valid timestamp
  AND timestamp IS NOT NULL
  AND CAST(timestamp AS TIMESTAMP) <= CURRENT_TIMESTAMP()
  AND CAST(timestamp AS TIMESTAMP) >= '2023-01-01';

-- ============================================================================
-- SILVER TO GOLD: Business Transformations
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Create Fact Sales with Dimension Lookups
-- ----------------------------------------------------------------------------
WITH sales_with_cost AS (
  SELECT 
    s.*,
    p.cost as product_cost
  FROM silver.sales_orders s
  LEFT JOIN silver.products p ON s.product_id = p.product_id
)
SELECT 
  s.order_id,
  c.customer_key,
  p.product_key,
  d.date_key,
  s.order_timestamp,
  s.payment_method,
  s.quantity,
  s.unit_price,
  s.discount_amount,
  s.shipping_cost,
  s.line_total,
  
  -- Calculate profit
  (s.line_total - (s.quantity * COALESCE(s.product_cost, 0))) as profit,
  
  s.status
  
FROM sales_with_cost s
LEFT JOIN gold.dim_customers c ON s.customer_id = c.customer_id
LEFT JOIN gold.dim_products p ON s.product_id = p.product_id
LEFT JOIN gold.dim_date d ON s.order_date = d.full_date

WHERE s.status = 'completed';  -- Only include completed orders in fact table

-- ----------------------------------------------------------------------------
-- Aggregate Customer Metrics
-- ----------------------------------------------------------------------------
SELECT 
  c.customer_key,
  c.customer_id,
  c.customer_name,
  c.segment,
  
  -- Aggregated metrics
  COUNT(DISTINCT f.order_id) as total_orders,
  SUM(f.quantity) as total_items_purchased,
  SUM(f.line_total) as total_spent,
  AVG(f.line_total) as avg_order_value,
  SUM(f.profit) as total_profit_generated,
  
  MIN(f.order_timestamp) as first_order_date,
  MAX(f.order_timestamp) as last_order_date,
  DATEDIFF(DAY, MIN(f.order_timestamp), MAX(f.order_timestamp)) as customer_lifetime_days

FROM gold.dim_customers c
LEFT JOIN gold.fact_sales f ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.customer_id, c.customer_name, c.segment;

-- ----------------------------------------------------------------------------
-- Product Performance Metrics
-- ----------------------------------------------------------------------------
SELECT 
  p.product_key,
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  
  COUNT(DISTINCT f.order_id) as times_ordered,
  SUM(f.quantity) as units_sold,
  SUM(f.line_total) as total_revenue,
  AVG(f.unit_price) as avg_selling_price,
  SUM(f.profit) as total_profit,
  
  -- Profit margin
  CASE 
    WHEN SUM(f.line_total) > 0 
    THEN ROUND(100.0 * SUM(f.profit) / SUM(f.line_total), 2)
    ELSE 0 
  END as profit_margin_pct

FROM gold.dim_products p
LEFT JOIN gold.fact_sales f ON p.product_key = f.product_key
GROUP BY p.product_key, p.product_id, p.product_name, p.category, p.subcategory;

-- ============================================================================
-- ADVANCED ANALYTICS: Window Functions
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Sales Trend Analysis with Moving Average
-- ----------------------------------------------------------------------------
SELECT 
  d.full_date,
  d.year,
  d.month,
  d.day_name,
  
  -- Daily metrics
  COUNT(DISTINCT f.order_id) as daily_orders,
  SUM(f.line_total) as daily_revenue,
  SUM(f.profit) as daily_profit,
  
  -- 7-day moving average
  AVG(SUM(f.line_total)) OVER (
    ORDER BY d.full_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as revenue_7day_ma,
  
  -- Month-to-date cumulative
  SUM(SUM(f.line_total)) OVER (
    PARTITION BY d.year, d.month 
    ORDER BY d.full_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as revenue_mtd,
  
  -- Year-to-date cumulative
  SUM(SUM(f.line_total)) OVER (
    PARTITION BY d.year 
    ORDER BY d.full_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as revenue_ytd

FROM gold.dim_date d
LEFT JOIN gold.fact_sales f ON d.date_key = f.date_key
WHERE d.full_date >= '2024-01-01'
GROUP BY d.full_date, d.year, d.month, d.day_name
ORDER BY d.full_date;

-- ----------------------------------------------------------------------------
-- Customer Ranking and Segmentation
-- ----------------------------------------------------------------------------
SELECT 
  customer_id,
  customer_name,
  total_spent,
  total_orders,
  
  -- Rank by total spend
  RANK() OVER (ORDER BY total_spent DESC) as customer_rank,
  
  -- Percentile ranking
  PERCENT_RANK() OVER (ORDER BY total_spent) as percentile_rank,
  
  -- Quartile segmentation
  NTILE(4) OVER (ORDER BY total_spent DESC) as spend_quartile,
  
  -- Categorize customers
  CASE 
    WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 1 THEN 'Top 25% (VIP)'
    WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 2 THEN 'Top 50% (Premium)'
    WHEN NTILE(4) OVER (ORDER BY total_spent DESC) = 3 THEN 'Top 75% (Standard)'
    ELSE 'Bottom 25% (Basic)'
  END as customer_tier

FROM (
  SELECT 
    c.customer_id,
    c.customer_name,
    COALESCE(SUM(f.line_total), 0) as total_spent,
    COUNT(f.order_id) as total_orders
  FROM gold.dim_customers c
  LEFT JOIN gold.fact_sales f ON c.customer_key = f.customer_key
  GROUP BY c.customer_id, c.customer_name
) customer_metrics

ORDER BY customer_rank;

-- ----------------------------------------------------------------------------
-- Product Category Ranking within Each Category
-- ----------------------------------------------------------------------------
SELECT 
  category,
  product_name,
  total_revenue,
  units_sold,
  
  -- Rank within category
  ROW_NUMBER() OVER (PARTITION BY category ORDER BY total_revenue DESC) as rank_in_category,
  
  -- Running total within category
  SUM(total_revenue) OVER (
    PARTITION BY category 
    ORDER BY total_revenue DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as cumulative_revenue,
  
  -- Percentage of category revenue
  ROUND(100.0 * total_revenue / SUM(total_revenue) OVER (PARTITION BY category), 2) as pct_of_category

FROM (
  SELECT 
    p.category,
    p.product_name,
    SUM(f.line_total) as total_revenue,
    SUM(f.quantity) as units_sold
  FROM gold.dim_products p
  LEFT JOIN gold.fact_sales f ON p.product_key = f.product_key
  GROUP BY p.category, p.product_name
) product_sales

ORDER BY category, rank_in_category;

-- ----------------------------------------------------------------------------
-- Event Sessionization: Calculate Session Metrics
-- ----------------------------------------------------------------------------
WITH session_events AS (
  SELECT 
    session_id,
    user_id,
    event_type,
    event_timestamp,
    
    -- Get previous event timestamp in session
    LAG(event_timestamp) OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) as prev_event_timestamp,
    
    -- Event sequence number in session
    ROW_NUMBER() OVER (
      PARTITION BY session_id 
      ORDER BY event_timestamp
    ) as event_sequence
    
  FROM silver.user_events
),
session_metrics AS (
  SELECT 
    session_id,
    user_id,
    
    -- Session start and end
    MIN(event_timestamp) as session_start,
    MAX(event_timestamp) as session_end,
    
    -- Session duration in seconds
    TIMESTAMPDIFF(SECOND, MIN(event_timestamp), MAX(event_timestamp)) as duration_seconds,
    
    -- Event counts
    COUNT(*) as total_events,
    COUNT(DISTINCT event_type) as distinct_event_types,
    
    -- Collect event journey
    COLLECT_LIST(
      STRUCT(event_type, event_timestamp, event_sequence)
    ) as event_journey
    
  FROM session_events
  GROUP BY session_id, user_id
)
SELECT 
  session_id,
  user_id,
  session_start,
  session_end,
  duration_seconds,
  total_events,
  distinct_event_types,
  
  -- Events per minute
  CASE 
    WHEN duration_seconds > 0 
    THEN ROUND(total_events * 60.0 / duration_seconds, 2)
    ELSE total_events 
  END as events_per_minute,
  
  event_journey

FROM session_metrics
WHERE duration_seconds >= 5  -- Filter very short sessions (likely bots)
ORDER BY duration_seconds DESC;

-- ----------------------------------------------------------------------------
-- Conversion Funnel Analysis
-- ----------------------------------------------------------------------------
WITH user_events_agg AS (
  SELECT 
    user_id,
    MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as viewed_product,
    MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as added_to_cart,
    MAX(CASE WHEN event_type = 'checkout' THEN 1 ELSE 0 END) as started_checkout,
    
    COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN session_id END) as page_view_sessions,
    COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) as cart_sessions
  FROM silver.user_events
  WHERE event_type IN ('page_view', 'add_to_cart', 'checkout')
  GROUP BY user_id
)
SELECT 
  COUNT(*) as total_users,
  SUM(viewed_product) as users_viewed,
  SUM(added_to_cart) as users_added_to_cart,
  SUM(started_checkout) as users_checked_out,
  
  -- Conversion rates
  ROUND(100.0 * SUM(added_to_cart) / NULLIF(SUM(viewed_product), 0), 2) as view_to_cart_rate,
  ROUND(100.0 * SUM(started_checkout) / NULLIF(SUM(added_to_cart), 0), 2) as cart_to_checkout_rate,
  
  -- Average sessions
  AVG(page_view_sessions) as avg_page_view_sessions,
  AVG(cart_sessions) as avg_cart_sessions
  
FROM user_events_agg;

-- ============================================================================
-- QUERY EXAMPLES FOR BUSINESS INTELLIGENCE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Top 10 Customers by Revenue
-- ----------------------------------------------------------------------------
SELECT TOP 10
  c.customer_name,
  c.segment,
  COUNT(DISTINCT f.order_id) as total_orders,
  SUM(f.line_total) as total_revenue,
  AVG(f.line_total) as avg_order_value
FROM gold.dim_customers c
JOIN gold.fact_sales f ON c.customer_key = f.customer_key
GROUP BY c.customer_name, c.segment
ORDER BY total_revenue DESC;

-- ----------------------------------------------------------------------------
-- Monthly Sales Summary
-- ----------------------------------------------------------------------------
SELECT 
  d.year,
  d.month,
  d.month_name,
  COUNT(DISTINCT f.order_id) as total_orders,
  SUM(f.quantity) as units_sold,
  SUM(f.line_total) as revenue,
  SUM(f.profit) as profit,
  ROUND(100.0 * SUM(f.profit) / NULLIF(SUM(f.line_total), 0), 2) as profit_margin_pct
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- ----------------------------------------------------------------------------
-- Weekend vs Weekday Sales Comparison
-- ----------------------------------------------------------------------------
SELECT 
  CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
  COUNT(DISTINCT f.order_id) as total_orders,
  SUM(f.line_total) as total_revenue,
  AVG(f.line_total) as avg_order_value
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend;

-- ----------------------------------------------------------------------------
-- Product Category Performance
-- ----------------------------------------------------------------------------
SELECT 
  p.category,
  COUNT(DISTINCT p.product_id) as product_count,
  SUM(f.quantity) as units_sold,
  SUM(f.line_total) as revenue,
  SUM(f.profit) as profit,
  ROUND(AVG(f.unit_price), 2) as avg_price
FROM gold.dim_products p
LEFT JOIN gold.fact_sales f ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY revenue DESC;

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================

-- Check for orphaned records (referential integrity)
SELECT 'Orphaned Sales - No Customer' as issue, COUNT(*) as count
FROM gold.fact_sales WHERE customer_key IS NULL
UNION ALL
SELECT 'Orphaned Sales - No Product', COUNT(*)
FROM gold.fact_sales WHERE product_key IS NULL
UNION ALL
SELECT 'Orphaned Sales - No Date', COUNT(*)
FROM gold.fact_sales WHERE date_key IS NULL;

-- Check for duplicate orders
SELECT order_id, COUNT(*) as duplicate_count
FROM gold.fact_sales
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Check for negative amounts
SELECT 'Negative Line Total' as issue, COUNT(*) as count
FROM gold.fact_sales WHERE line_total < 0
UNION ALL
SELECT 'Negative Profit', COUNT(*)
FROM gold.fact_sales WHERE profit < 0;
