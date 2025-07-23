CREATE OR REFRESH STREAMING TABLE olist_e_commerce.bronze_db.customers 
AS SELECT * FROM STREAM read_files(
    '/Volumes/olist_e_commerce/ops/data/customers_dataset/',
    format => 'csv',
    inferSchema => 'true',
    header => 'true'
);

CREATE OR REFRESH STREAMING TABLE olist_e_commerce.bronze_db.order_items
AS SELECT * FROM STREAM read_files(
    '/Volumes/olist_e_commerce/ops/data/order_items_dataset/',
    format => 'csv',
    inferSchema => 'true',
    header => 'true'
);

CREATE OR REFRESH STREAMING TABLE olist_e_commerce.bronze_db.orders
AS SELECT * FROM STREAM read_files(
    '/Volumes/olist_e_commerce/ops/data/orders_dataset/',
    format => 'csv',
    inferSchema => 'true',
    header => 'true'
);

CREATE OR REFRESH STREAMING TABLE olist_e_commerce.bronze_db.orders_payments
AS SELECT * FROM STREAM read_files(
    '/Volumes/olist_e_commerce/ops/data/orders_payments_dataset/',
    format => 'csv',
    inferSchema => 'true',
    header => 'true'
);

CREATE OR REFRESH STREAMING TABLE olist_e_commerce.bronze_db.products
AS SELECT * FROM STREAM read_files(
    '/Volumes/olist_e_commerce/ops/data/products_dataset/',
    format => 'csv',
    inferSchema => 'true',
    header => 'true'
);