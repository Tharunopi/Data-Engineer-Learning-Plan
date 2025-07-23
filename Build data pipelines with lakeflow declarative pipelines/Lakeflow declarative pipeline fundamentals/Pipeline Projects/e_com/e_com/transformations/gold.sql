-- 1. Total sales revenue
CREATE OR REFRESH MATERIALIZED VIEW gold_db.monthly_sales_revenue AS 
SELECT 
  order_month AS report_month,
  SUM(amount) AS total_revenue,
  COUNT(*) AS total_orders
FROM silver_db.question_1
GROUP BY order_month;

-- 2. Top product category by revenue
CREATE OR REFRESH MATERIALIZED VIEW gold_db.category_by_revenue AS 
SELECT 
  category AS product_category,
  SUM(amount) AS total_revenue,
  COUNT(DISTINCT product_id) AS unique_products_sold
FROM silver_db.question_2
GROUP BY category;

-- 3. Customer value ranking
CREATE OR REFRESH MATERIALIZED VIEW gold_db.customer_value_ranking AS 
SELECT 
  customer_id AS customer_unique_id,
  SUM(amount) AS total_spend,
  COUNT(order_id) AS total_orders,
  MIN(order_date) AS first_order_date,
  MAX(order_date) AS latest_order_date
FROM silver_db.question_3
GROUP BY customer_id;

-- 4. Sales by region
CREATE OR REFRESH MATERIALIZED VIEW gold_db.sales_by_revenue AS 
SELECT 
  customer_state,
  SUM(amount) AS total_revenue,
  COUNT(customer_id) AS total_customers
FROM silver_db.question_4
GROUP BY customer_state;