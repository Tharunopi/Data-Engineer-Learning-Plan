import dlt

@dlt.table(
  name="customers_py"
)
def read_1():
  return (spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option("cloudFiles.header", "true") \
          .option("cloudFiles.inferSchema", "true") \
          .option("cloudFiles.validateOptions", "false") \
          .load("/Volumes/olist_e_commerce/ops/data/customers_dataset/")
    )

@dlt.table(
  name="order_items_py"
)
def read_2():
  return (spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option("cloudFiles.header", "true") \
          .option("cloudFiles.inferSchema", "true") \
          .option("cloudFiles.validateOptions", "false") \
          .load("/Volumes/olist_e_commerce/ops/data/order_items_dataset/")
    )
  
@dlt.table(
  name="orders_py"
)
def read_3():
  return (spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option("cloudFiles.header", "true") \
          .option("cloudFiles.inferSchema", "true") \
          .option("cloudFiles.validateOptions", "false") \
          .load("/Volumes/olist_e_commerce/ops/data/orders_dataset/")
    )

@dlt.table(
  name="order_payments_py"
)
def read_4():
  return (spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option("cloudFiles.header", "true") \
          .option("cloudFiles.inferSchema", "true") \
          .option("cloudFiles.validateOptions", "false") \
          .load("/Volumes/olist_e_commerce/ops/data/orders_payments_dataset/")
    )
  
@dlt.table(
  name="products_py"
)
def read_5():
  return (spark.readStream.format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option("cloudFiles.header", "true") \
          .option("cloudFiles.inferSchema", "true") \
          .option("cloudFiles.validateOptions", "false") \
          .load("/Volumes/olist_e_commerce/ops/data/products_dataset/")
    )