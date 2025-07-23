import dlt
import pyspark.sql.functions as F

@dlt.table(
    name="monthly_sales_revenue"
)
def report_1():
    df = spark.read.table("question_1")
    df = df.groupBy("order_month").agg(
        F.sum(F.col("amount")).alias("total_revenue"),
        F.count(F.col("*")).alias("total_orders")
    )
    return df

@dlt.table(
    name="category_by_revenue"
)
def report_2():
    df = spark.read.table("question_2")
    df = df.groupBy("product_category_name").agg(
        F.sum(F.col("amount")).alias("total_revenue"),
        F.count_distinct(F.col("product_id")).alias("unique_products_sold")
    )
    return df

@dlt.table(
    name="customer_value_ranking"
)
def report_3():
    df = spark.read.table("question_3")
    df = df.groupBy("customer_id").agg(
        F.sum(F.col("amount")).alias("total_spend"),
        F.count(F.col("order_id")).alias("total_orders"),
        F.min(F.col("order_date")).alias("first_order_date"),
        F.max(F.col("order_date")).alias("latest_order_date")
    )
    return df

@dlt.table(
    name="sales_by_revenue"
)
def report_4():
    df = spark.read.table("question_4")
    df = df.groupBy("customer_state").agg(
        F.sum(F.col("amount")).alias("total_revenue"),
        F.count_distinct(F.col("customer_id")).alias("total_customers")
    )
    return df