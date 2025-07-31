CREATE OR REFRESH STREAMING TABLE `2_silver_db`.orders_real
(
  CONSTRAINT order_id_check EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT customer_id_check EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT amount_validate EXPECT (amount > 0) ON VIOLATION DROP ROW
)
 AS 
SELECT 
  order_id,
  customer_id,
  order_date,
  amount
FROM STREAM `1_bronze_db`.orders_real;

CREATE OR REFRESH STREAMING TABLE `2_silver_db`.status_real
(
  CONSTRAINT order_id_check EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
 AS 
SELECT 
  order_id,
  status,
  updated_at
FROM STREAM `1_bronze_db`.status_real;

CREATE OR REFRESH STREAMING TABLE `2_silver_db`.customer_real;

APPLY CHANGES INTO `2_silver_db`.customer_real
FROM STREAM `1_bronze_db`.customer_final_real
KEYS (customer_id)
APPLY AS DELETE WHEN operations = 'DELETE'
SEQUENCE BY processed_at
COLUMNS * EXCEPT (operations)
STORED AS SCD TYPE 2;