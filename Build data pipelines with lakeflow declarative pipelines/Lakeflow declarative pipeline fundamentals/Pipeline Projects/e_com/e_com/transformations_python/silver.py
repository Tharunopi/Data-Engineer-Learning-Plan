import dlt
import pyspark.sql.functions as F

@dlt.table(
  name="order_payments_bronze_py"
)
def transform_1():
  df = spark.readStream.table("order_payments_py")
  df = df.groupBy("order_id").agg(
    F.sum(F.col("payment_value")).alias("amount")
  )
  return df

@dlt.table(
  name="question_1"
)
def transform_2():
  df_1 = spark.readStream.table("orders_py")
  df_2 = spark.readStream.table("order_payments_bronze_py")
  df = df_1.join(df_2, on="order_id", how="inner")
  df = df.select([
    "order_id", 
    "order_purchase_timestamp",
    "amount"
    ]) \
    .withColumn(
      "order_month",
      F.concat(F.year(F.col("order_purchase_timestamp")), F.month(F.col("order_purchase_timestamp")))
    )
  return df

@dlt.table(
  name="question_2"
)
def transform_3():
  df_1 = spark.readStream.table("order_items_py")
  df_2 = spark.readStream.table("products_py")
  df_3 = spark.readStream.table("order_payments_bronze_py")
  df = df_1.join(df_2, on="product_id", how="inner") \
      .join(df_3, on="order_id", how="inner")
  df = df.select(
    [
      "product_category_name",
      "amount",
      "product_id"
    ]
    )
  return df

@dlt.table(
  name="question_3"
)
def transform_4():
  df_1 = spark.readStream.table("orders_py")
  df_2 = spark.readStream.table("order_payments_bronze_py")
  df = df_1.join(df_2, on="order_id", how="inner")
  df = df.select(
    [
      "order_id",
      "customer_id",
      "amount",
      df.order_purchase_timestamp.alias("order_date")
    ]
  )
  return df 

@dlt.table(
  name="question_4"
)
def transform_5():
  df_1 = spark.readStream.table("orders_py")
  df_2 = spark.readStream.table("customers_py")
  df_3 = spark.readStream.table("order_payments_bronze_py")
  df = df_1.join(df_2, on="customer_id", how="inner") \
      .join(df_3, on="order_id", how="inner")
  df = df.select(
    [
      "customer_id",
      "amount",
      "customer_state"
    ]
  )
  return df