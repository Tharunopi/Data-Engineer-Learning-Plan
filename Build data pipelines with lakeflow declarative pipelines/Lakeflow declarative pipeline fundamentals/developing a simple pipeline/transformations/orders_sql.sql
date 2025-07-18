CREATE OR REFRESH STREAMING TABLE 1_bronze_db.orders_bronze_demo
AS SELECT * FROM STREAM read_files(
  '/Volumes/dbacademy/ops/ops_data/orders/',
  format => 'json',
  schema => 'order_id int, customer_id int, order_date date, amount float, items array<string>'
);

CREATE OR REFRESH STREAMING TABLE 2_silver_db.orders_silver_demo 
AS SELECT order_id, order_date, amount FROM STREAM 1_bronze_db.orders_bronze_demo;

CREATE OR REFRESH MATERIALIZED VIEW 3_gold_db.orders_gold_demo
AS SELECT order_date, COUNT(*) AS total_sales_volume, SUM(amount) AS total_sales_amt FROM 2_silver_db.orders_silver_demo
GROUP BY order_date
ORDER BY SUM(amount) DESC, COUNT(*) DESC