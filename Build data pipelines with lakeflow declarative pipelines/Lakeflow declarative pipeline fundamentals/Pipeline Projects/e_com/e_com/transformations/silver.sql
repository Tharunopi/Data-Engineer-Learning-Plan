CREATE OR REFRESH STREAMING TABLE silver_db.order_payments AS 
SELECT 
  order_id, 
  SUM(payment_value) AS amount 
FROM STREAM bronze_db.orders_payments
GROUP BY order_id;

-- 1. Total sales revenue
CREATE OR REFRESH STREAMING TABLE silver_db.question_1 AS 
SELECT 
  o.order_id AS order_id,
  CONCAT(YEAR(o.order_purchase_timestamp), MONTH(o.order_purchase_timestamp)) AS order_month,
  p.amount AS amount
FROM STREAM bronze_db.orders o
INNER JOIN STREAM silver_db.order_payments p
ON o.order_id = p.order_id;

-- 2. Top product category by revenue
CREATE OR REFRESH STREAMING TABLE silver_db.question_2 AS 
SELECT 
  pr.product_category_name AS category,
  op.amount AS amount,
  oi.product_id AS product_id
FROM STREAM bronze_db.order_items oi
INNER JOIN STREAM bronze_db.products pr
ON pr.product_id = oi.product_id
INNER JOIN STREAM silver_db.order_payments op
ON op.order_id = oi.order_id;

-- 3. Customer value ranking
CREATE OR REFRESH STREAMING TABLE silver_db.question_3 AS 
SELECT 
  o.customer_id AS customer_id,
  op.amount AS amount,
  o.order_id AS order_id,
  o.order_purchase_timestamp AS order_date
FROM STREAM bronze_db.orders o
INNER JOIN STREAM silver_db.order_payments op
ON op.order_id = o.order_id;

-- 4. Sales by region
CREATE OR REFRESH STREAMING TABLE silver_db.question_4 AS
SELECT 
  c.customer_id AS customer_id,
  op.amount AS amount,
  c.customer_state AS customer_state
FROM STREAM bronze_db.orders o
INNER JOIN STREAM bronze_db.customers c
ON c.customer_id = o.customer_id
INNER JOIN STREAM silver_db.order_payments op
ON op.order_id = o.order_id;