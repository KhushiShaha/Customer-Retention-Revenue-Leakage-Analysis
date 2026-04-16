-- =========================================================
-- Revenue Leakage & Decision Intelligence System
-- SQL KPI Queries
-- =========================================================


-- 1. Total Revenue
SELECT
    ROUND(SUM(price + freight_value), 2) AS total_revenue
FROM olist_order_items_dataset;


-- 2. Total Orders and Unique Customers
SELECT
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT c.customer_unique_id) AS unique_customers
FROM olist_orders_dataset o
JOIN olist_customers_dataset c
    ON o.customer_id = c.customer_id;


-- 3. Monthly Revenue Trend
SELECT
    strftime('%Y-%m', o.order_purchase_timestamp) AS order_month,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS monthly_revenue
FROM olist_orders_dataset o
JOIN olist_order_items_dataset oi
    ON o.order_id = oi.order_id
GROUP BY strftime('%Y-%m', o.order_purchase_timestamp)
ORDER BY order_month;


-- 4. Average Order Value
SELECT
    ROUND(SUM(oi.price + oi.freight_value) / COUNT(DISTINCT o.order_id), 2) AS average_order_value
FROM olist_orders_dataset o
JOIN olist_order_items_dataset oi
    ON o.order_id = oi.order_id;


-- 5. Repeat vs One-Time Customers
SELECT
    customer_type,
    COUNT(*) AS customer_count
FROM (
    SELECT
        c.customer_unique_id,
        CASE
            WHEN COUNT(DISTINCT o.order_id) > 1 THEN 'Repeat'
            ELSE 'One-time'
        END AS customer_type
    FROM olist_orders_dataset o
    JOIN olist_customers_dataset c
        ON o.customer_id = c.customer_id
    GROUP BY c.customer_unique_id
) customer_segments
GROUP BY customer_type;


-- 6. Top 10 Customers by Revenue
SELECT
    c.customer_unique_id,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue
FROM olist_orders_dataset o
JOIN olist_customers_dataset c
    ON o.customer_id = c.customer_id
JOIN olist_order_items_dataset oi
    ON o.order_id = oi.order_id
GROUP BY c.customer_unique_id
ORDER BY total_revenue DESC
LIMIT 10;


-- 7. Revenue by State
SELECT
    c.customer_state,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue
FROM olist_orders_dataset o
JOIN olist_customers_dataset c
    ON o.customer_id = c.customer_id
JOIN olist_order_items_dataset oi
    ON o.order_id = oi.order_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC;


-- 8. Delivered vs Non-Delivered Orders
SELECT
    order_status,
    COUNT(DISTINCT order_id) AS order_count
FROM olist_orders_dataset
GROUP BY order_status
ORDER BY order_count DESC;