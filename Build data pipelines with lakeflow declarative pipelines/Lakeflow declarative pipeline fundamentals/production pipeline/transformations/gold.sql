CREATE OR REFRESH MATERIALIZED VIEW `3_gold_db`.status_orders_real AS 
SELECT 
  o.order_id,
  o.order_date,
  o.customer_id,
  s.status,
  s.updated_at
FROM `2_silver_db`.orders_real o
INNER JOIN `2_silver_db`.status_real s
ON s.order_id = o.order_id;

CREATE OR REFRESH MATERIALIZED VIEW `3_gold_db`.order_summary_real AS 
SELECT 
  order_date,
  SUM(amount) AS total_sales
FROM `2_silver_db`.orders_real
GROUP BY order_date;

CREATE OR REFRESH MATERIALIZED VIEW `3_gold_db`.cancelled_real AS 
SELECT 
  order_id, 
  status 
FROM `3_gold_db`.status_orders_real 
WHERE status = 'Cancelled';

CREATE OR REFRESH MATERIALIZED VIEW `3_gold_db`.delivered_real AS 
SELECT 
  order_id, 
  status 
FROM `3_gold_db`.status_orders_real 
WHERE status = 'Delivered';

CREATE OR REFRESH MATERIALIZED VIEW `3_gold_db`.non_active_customers_real AS 
SELECT 
  customer_id,
  name 
FROM `2_silver_db`.customer_real
WHERE __END_AT IS NULL;