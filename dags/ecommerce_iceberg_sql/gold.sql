-- GOLD: business marts built from silver. Meaningful, dashboard-ready outputs.
CREATE SCHEMA IF NOT EXISTS iceberg.gold WITH (location = 's3a://warehouse/iceberg/gold');

-- Daily revenue and order volume by sales channel.
CREATE OR REPLACE TABLE iceberg.gold.daily_revenue AS
SELECT CAST(o.order_ts AS DATE) AS order_date,
       o.channel,
       count(DISTINCT o.order_id) AS orders,
       round(sum(oi.quantity * oi.unit_price), 2) AS revenue
FROM iceberg.silver.orders o
JOIN iceberg.silver.order_items oi ON o.order_id = oi.order_id
WHERE o.status <> 'cancelled'
GROUP BY 1, 2;

-- Top products by revenue, with units and gross margin.
CREATE OR REPLACE TABLE iceberg.gold.top_products AS
SELECT p.product_id,
       p.name,
       p.category,
       sum(oi.quantity) AS units,
       round(sum(oi.quantity * oi.unit_price), 2) AS revenue,
       round(sum(oi.quantity * (oi.unit_price - p.unit_cost)), 2) AS margin
FROM iceberg.silver.order_items oi
JOIN iceberg.silver.products p ON oi.product_id = p.product_id
GROUP BY 1, 2, 3;

-- Customer lifetime value and order count, by segment/country.
CREATE OR REPLACE TABLE iceberg.gold.customer_ltv AS
SELECT c.customer_id,
       c.segment,
       c.country,
       count(DISTINCT o.order_id) AS orders,
       round(coalesce(sum(oi.quantity * oi.unit_price), 0), 2) AS lifetime_value
FROM iceberg.silver.customers c
LEFT JOIN iceberg.silver.orders o
       ON c.customer_id = o.customer_id AND o.status <> 'cancelled'
LEFT JOIN iceberg.silver.order_items oi ON o.order_id = oi.order_id
GROUP BY 1, 2, 3;
