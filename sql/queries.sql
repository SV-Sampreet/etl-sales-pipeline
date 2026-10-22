-- Sales ETL Pipeline - Analytics and Validation Queries

-- 1. Total accepted orders
SELECT COUNT(*) as total_orders FROM orders;

-- 2. Total revenue
SELECT SUM(revenue) as total_revenue FROM orders;

-- 3. Revenue by product
SELECT
    product,
    SUM(quantity) as units_sold,
    SUM(revenue) as total_revenue,
    AVG(unit_price) as avg_price
FROM orders
GROUP BY product
ORDER BY total_revenue DESC;

-- 4. Revenue by category
SELECT
    category,
    SUM(quantity) as units_sold,
    SUM(revenue) as total_revenue,
    AVG(unit_price) as avg_price
FROM orders
GROUP BY category
ORDER BY total_revenue DESC;

-- 5. Revenue by region
SELECT
    region,
    SUM(quantity) as units_sold,
    SUM(revenue) as total_revenue,
    COUNT(*) as order_count
FROM orders
GROUP BY region
ORDER BY total_revenue DESC;

-- 6. Monthly revenue
SELECT
    strftime('%Y-%m', order_date) as month,
    SUM(quantity) as units_sold,
    SUM(revenue) as total_revenue,
    COUNT(*) as order_count
FROM orders
GROUP BY month
ORDER BY month;

-- 7. Total units sold
SELECT SUM(quantity) as total_units_sold FROM orders;

-- 8. Duplicate primary-key check (should return 0 if constraint works)
SELECT order_id, COUNT(*) as count
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- 9. Check for missing values (should return 0 rows if data is clean)
SELECT
    COUNT(*) as missing_customer_id
FROM orders
WHERE customer_id IS NULL OR customer_id = '';

SELECT
    COUNT(*) as missing_product
FROM orders
WHERE product IS NULL OR product = '';

SELECT
    COUNT(*) as missing_category
FROM orders
WHERE category IS NULL OR category = '';

SELECT
    COUNT(*) as missing_region
FROM orders
WHERE region IS NULL OR region = '';

-- 10. Check for invalid quantities (should return 0 rows)
SELECT COUNT(*) as invalid_quantity
FROM orders
WHERE quantity <= 0 OR quantity IS NULL;

-- 11. Check for invalid prices (should return 0 rows)
SELECT COUNT(*) as invalid_price
FROM orders
WHERE unit_price < 0 OR unit_price IS NULL;

-- 12. Revenue calculation verification
SELECT
    order_id,
    quantity,
    unit_price,
    revenue,
    quantity * unit_price as calculated_revenue,
    CASE
        WHEN ABS(revenue - (quantity * unit_price)) > 0.01 THEN 'MISMATCH'
        ELSE 'OK'
    END as verification_status
FROM orders
WHERE ABS(revenue - (quantity * unit_price)) > 0.01;

-- 13. Top 10 customers by revenue
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(revenue) as total_revenue
FROM orders
GROUP BY customer_id
ORDER BY total_revenue DESC
LIMIT 10;

-- 14. Average order value
SELECT AVG(revenue) as avg_order_value FROM orders;

-- 15. Orders by date range
SELECT
    order_date,
    COUNT(*) as order_count,
    SUM(revenue) as daily_revenue
FROM orders
GROUP BY order_date
ORDER BY order_date;
