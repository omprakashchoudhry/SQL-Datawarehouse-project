-- ============================================================
-- SQL Data Warehouse - Analytics Queries
-- Author: Omprakash Choudhary
-- Description: Business analytics on Gold layer tables
-- Tables used: gold.dim_customers, gold.dim_products, gold.fact_sales
-- ============================================================

-- ============================================================
-- 1. DATABASE EXPLORATION
-- ============================================================

-- List all tables in the Gold schema
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold';

-- Explore columns in fact_sales
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_sales' AND TABLE_SCHEMA = 'gold';

-- ============================================================
-- 2. KEY METRICS & MEASURES
-- ============================================================

-- Overall business summary
SELECT
    COUNT(DISTINCT customer_key)        AS total_customers,
    COUNT(DISTINCT product_key)         AS total_products,
    COUNT(order_number)                 AS total_orders,
    SUM(sales_amount)                   AS total_revenue,
    SUM(quantity)                       AS total_quantity_sold,
    ROUND(AVG(sales_amount), 2)         AS avg_order_value,
    MIN(order_date)                     AS first_order_date,
    MAX(order_date)                     AS last_order_date,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS months_of_data
FROM gold.fact_sales;

-- ============================================================
-- 3. CHANGES OVER TIME - Sales Trend Analysis
-- ============================================================

-- Monthly revenue trend
SELECT
    YEAR(order_date)                    AS order_year,
    MONTH(order_date)                   AS order_month,
    DATENAME(MONTH, order_date)         AS month_name,
    SUM(sales_amount)                   AS monthly_revenue,
    COUNT(DISTINCT customer_key)        AS unique_customers,
    SUM(quantity)                       AS total_units_sold
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
    YEAR(order_date),
    MONTH(order_date),
    DATENAME(MONTH, order_date)
ORDER BY order_year, order_month;

-- Year-over-year revenue comparison
SELECT
    YEAR(order_date)                    AS order_year,
    SUM(sales_amount)                   AS yearly_revenue,
    LAG(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date)) AS prev_year_revenue,
    SUM(sales_amount) - LAG(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date)) AS revenue_change,
    ROUND(
        100.0 * (SUM(sales_amount) - LAG(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date)))
        / NULLIF(LAG(SUM(sales_amount)) OVER (ORDER BY YEAR(order_date)), 0), 2
    ) AS yoy_growth_pct
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY order_year;

-- ============================================================
-- 4. CUMULATIVE ANALYTICS
-- ============================================================

-- Running total of revenue over time
SELECT
    order_date,
    SUM(sales_amount)                   AS daily_revenue,
    SUM(SUM(sales_amount)) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                   AS running_total_revenue,
    AVG(SUM(sales_amount)) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                   AS moving_avg_7day
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date;

-- ============================================================
-- 5. PRODUCT PERFORMANCE ANALYSIS
-- ============================================================

-- Top 10 products by revenue
SELECT TOP 10
    p.product_name,
    p.category,
    p.subcategory,
    SUM(f.sales_amount)                 AS total_revenue,
    SUM(f.quantity)                     AS total_units_sold,
    COUNT(DISTINCT f.order_number)      AS total_orders,
    ROUND(AVG(f.sales_amount), 2)       AS avg_order_value
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category, p.subcategory
ORDER BY total_revenue DESC;

-- Bottom 10 products by revenue (potential review candidates)
SELECT TOP 10
    p.product_name,
    p.category,
    SUM(f.sales_amount)                 AS total_revenue,
    SUM(f.quantity)                     AS total_units_sold
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category
ORDER BY total_revenue ASC;

-- Revenue by product category
SELECT
    p.category,
    SUM(f.sales_amount)                 AS category_revenue,
    SUM(f.quantity)                     AS units_sold,
    COUNT(DISTINCT f.customer_key)      AS unique_buyers,
    ROUND(AVG(f.sales_amount), 2)       AS avg_order_value
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY category_revenue DESC;

-- ============================================================
-- 6. PART-TO-WHOLE ANALYSIS
-- ============================================================

-- Each category's % contribution to total revenue
WITH category_revenue AS (
    SELECT
        p.category,
        SUM(f.sales_amount) AS revenue
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    GROUP BY p.category
)
SELECT
    category,
    revenue,
    SUM(revenue) OVER ()                AS total_revenue,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS pct_of_total
FROM category_revenue
ORDER BY revenue DESC;

-- ============================================================
-- 7. CUSTOMER ANALYSIS
-- ============================================================

-- Top 10 customers by revenue
SELECT TOP 10
    c.customer_key,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.country,
    SUM(f.sales_amount)                 AS total_spent,
    COUNT(DISTINCT f.order_number)      AS total_orders,
    SUM(f.quantity)                     AS total_items_bought,
    MIN(f.order_date)                   AS first_purchase,
    MAX(f.order_date)                   AS last_purchase,
    DATEDIFF(DAY, MIN(f.order_date), MAX(f.order_date)) AS customer_lifespan_days
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name, c.country
ORDER BY total_spent DESC;

-- Revenue by country
SELECT
    c.country,
    COUNT(DISTINCT c.customer_key)      AS total_customers,
    SUM(f.sales_amount)                 AS total_revenue,
    ROUND(AVG(f.sales_amount), 2)       AS avg_order_value
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_revenue DESC;

-- ============================================================
-- 8. CUSTOMER SEGMENTATION
-- ============================================================

-- Segment customers by total spend: VIP / Regular / New
WITH customer_spend AS (
    SELECT
        c.customer_key,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        c.country,
        SUM(f.sales_amount)             AS total_spent,
        COUNT(DISTINCT f.order_number)  AS total_orders,
        MIN(f.order_date)               AS first_purchase,
        MAX(f.order_date)               AS last_purchase
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
    GROUP BY c.customer_key, c.first_name, c.last_name, c.country
)
SELECT
    customer_key,
    customer_name,
    country,
    total_spent,
    total_orders,
    CASE
        WHEN total_spent >= 5000 THEN 'VIP'
        WHEN total_spent >= 1000 THEN 'Regular'
        ELSE 'New'
    END                                 AS customer_segment,
    DATEDIFF(DAY, first_purchase, last_purchase) AS lifespan_days
FROM customer_spend
ORDER BY total_spent DESC;

-- Count of customers per segment
WITH customer_spend AS (
    SELECT
        f.customer_key,
        SUM(f.sales_amount) AS total_spent
    FROM gold.fact_sales f
    GROUP BY f.customer_key
),
segmented AS (
    SELECT
        CASE
            WHEN total_spent >= 5000 THEN 'VIP'
            WHEN total_spent >= 1000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spend
)
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers
FROM segmented
GROUP BY customer_segment
ORDER BY customer_count DESC;

-- ============================================================
-- 9. PRODUCT PERFORMANCE RANKING
-- ============================================================

-- Rank products within each category by revenue
WITH product_revenue AS (
    SELECT
        p.category,
        p.product_name,
        SUM(f.sales_amount)             AS total_revenue
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    GROUP BY p.category, p.product_name
)
SELECT
    category,
    product_name,
    total_revenue,
    RANK() OVER (
        PARTITION BY category
        ORDER BY total_revenue DESC
    )                                   AS rank_in_category
FROM product_revenue
ORDER BY category, rank_in_category;

-- ============================================================
-- 10. REPORT VIEWS (for dashboard / BI tool connection)
-- ============================================================

-- Customer report view
CREATE OR ALTER VIEW gold.vw_customer_report AS
SELECT
    c.customer_key,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.country,
    c.gender,
    DATEDIFF(YEAR, c.birthdate, GETDATE()) AS age,
    COUNT(DISTINCT f.order_number)      AS total_orders,
    SUM(f.sales_amount)                 AS total_revenue,
    SUM(f.quantity)                     AS total_quantity,
    MAX(f.order_date)                   AS last_order_date,
    DATEDIFF(DAY, MAX(f.order_date), GETDATE()) AS days_since_last_order,
    CASE
        WHEN SUM(f.sales_amount) >= 5000 THEN 'VIP'
        WHEN SUM(f.sales_amount) >= 1000 THEN 'Regular'
        ELSE 'New'
    END                                 AS customer_segment
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales f ON c.customer_key = f.customer_key
GROUP BY
    c.customer_key, c.first_name, c.last_name,
    c.country, c.gender, c.birthdate;

-- Product report view
CREATE OR ALTER VIEW gold.vw_product_report AS
SELECT
    p.product_key,
    p.product_name,
    p.category,
    p.subcategory,
    p.cost,
    COUNT(DISTINCT f.order_number)      AS total_orders,
    SUM(f.quantity)                     AS total_units_sold,
    SUM(f.sales_amount)                 AS total_revenue,
    ROUND(SUM(f.sales_amount) - (p.cost * SUM(f.quantity)), 2) AS total_profit,
    ROUND(
        100.0 * (SUM(f.sales_amount) - (p.cost * SUM(f.quantity)))
        / NULLIF(SUM(f.sales_amount), 0), 2
    )                                   AS profit_margin_pct,
    RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS revenue_rank
FROM gold.dim_products p
LEFT JOIN gold.fact_sales f ON p.product_key = f.product_key
GROUP BY
    p.product_key, p.product_name, p.category,
    p.subcategory, p.cost;
GO
