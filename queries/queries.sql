-- Kasi Mart — Gold layer analytical queries
-- Platform: Databricks (Spark SQL), run against the silver_* tables

-- ---------------------------------------------------------------
-- Load verification: expected 50 customers, 20 products, 150 orders
-- ---------------------------------------------------------------
SELECT COUNT(*) AS customers_count FROM silver_customers;
SELECT COUNT(*) AS products_count  FROM silver_products;
SELECT COUNT(*) AS orders_count    FROM silver_orders;


-- ---------------------------------------------------------------
-- Query 1: Every order joined to customer, product and line revenue
-- ---------------------------------------------------------------
SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    p.product_name,
    p.category,
    o.quantity,
    p.unit_price,
    ROUND(o.quantity * p.unit_price, 2) AS line_revenue
FROM silver_orders o
JOIN silver_customers c
    ON o.customer_id = c.customer_id
JOIN silver_products p
    ON o.product_id = p.product_id;


-- ---------------------------------------------------------------
-- Query 2: Total revenue per customer
-- ---------------------------------------------------------------
SELECT
    c.customer_name,
    ROUND(SUM(o.quantity * p.unit_price), 2) AS total_revenue
FROM silver_orders o
JOIN silver_customers c
    ON o.customer_id = c.customer_id
JOIN silver_products p
    ON o.product_id = p.product_id
GROUP BY c.customer_name
ORDER BY total_revenue DESC;


-- ---------------------------------------------------------------
-- Query 3: Total revenue per product category
-- ---------------------------------------------------------------
SELECT
    p.category,
    ROUND(SUM(o.quantity * p.unit_price), 2) AS total_revenue
FROM silver_orders o
JOIN silver_products p
    ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;


-- ---------------------------------------------------------------
-- Query 4: Top 5 customers by total spend
-- ---------------------------------------------------------------
SELECT
    c.customer_name,
    ROUND(SUM(o.quantity * p.unit_price), 2) AS total_spend
FROM silver_orders o
JOIN silver_customers c
    ON o.customer_id = c.customer_id
JOIN silver_products p
    ON o.product_id = p.product_id
GROUP BY c.customer_name
ORDER BY total_spend DESC
LIMIT 5;
