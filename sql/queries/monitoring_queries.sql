-- ===============================
-- 1. DATA FRESHNESS CHECK
-- ===============================
SELECT
    MAX(loaded_at) AS latest_staging
FROM staging.transactions;

SELECT
    MAX(created_at) AS latest_production
FROM production.transactions;

SELECT
    MAX(created_at) AS latest_warehouse
FROM warehouse.fact_sales;

-- ===============================
-- 2. DAILY VOLUME (LAST 30 DAYS)
-- ===============================
SELECT
    date_key,
    COUNT(DISTINCT transaction_id) AS daily_transactions
FROM warehouse.fact_sales
WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY date_key
ORDER BY date_key;

-- ===============================
-- 3. DATA QUALITY CHECKS
-- ===============================
-- Orphan records
SELECT COUNT(*) AS orphan_products
FROM warehouse.fact_sales fs
LEFT JOIN warehouse.dim_products dp
ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;

-- Null violations
SELECT COUNT(*) AS null_violations
FROM warehouse.fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL
   OR date_key IS NULL;

-- ===============================
-- 4. DATABASE HEALTH
-- ===============================
SELECT
    COUNT(*) AS active_connections
FROM pg_stat_activity;

-- ===============================
-- 5. TABLE SIZE
-- ===============================
SELECT
    relname AS table_name,
    n_live_tup AS row_count
FROM pg_stat_user_tables;
