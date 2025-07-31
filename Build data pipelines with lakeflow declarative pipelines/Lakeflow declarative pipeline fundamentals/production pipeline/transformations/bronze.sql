CREATE OR REFRESH STREAMING TABLE `1_bronze_db`.orders_real AS 
SELECT * FROM STREAM read_files(
    '/Volumes/dbacademy/ops/data/orders/',
    format => 'json',
    schema => 'order_id int, customer_id int, order_date date, amount float, items array<string>'
);

CREATE OR REFRESH STREAMING TABLE `1_bronze_db`.status_real AS 
SELECT * FROM STREAM read_files(
    '/Volumes/dbacademy/ops/data/status/',
    format => 'json',
    schema => 'order_id int, status string, updated_at date'
);

CREATE OR REFRESH STREAMING TABLE `1_bronze_db`.customer_real AS 
SELECT *, current_timestamp() AS processed_at FROM STREAM read_files(
    '/Volumes/dbacademy/ops/data/customers/',
    format => 'json',
    schema => 'customer_id int, name string, email string, phone string, city string, operations string'
);

CREATE OR REFRESH STREAMING TABLE `1_bronze_db`.customer_final_real
(
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operations IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operations = 'DELETE'),
    CONSTRAINT valid_email EXPECT (email IS NOT NULL OR operations = 'DELETE'),
    CONSTRAINT valid_phone EXPECT (phone IS NOT NULL OR operations = 'DELETE'),
    CONSTRAINT valid_city EXPECT (city IS NOT NULL OR operations = 'DELETE')
)
 AS 
 SELECT * FROM STREAM `1_bronze_db`.customer_real
