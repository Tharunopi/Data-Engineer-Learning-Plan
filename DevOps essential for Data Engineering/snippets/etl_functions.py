import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("demo-1").getOrCreate()

def read_health_data(catalog="diabetes", schema="v01", volume="cdc-diabetes", filename="diabetes_binary_5050split_BRFSS2015.csv"):
    return (
        spark.read.csv(
        f"/Volumes/{catalog}/{schema}/{volume}/{filename}",
        inferSchema = True,
        header = True)
        )
    
def bmi_mapping(df):
    return (
        df.select(["HighBP","HighChol","CholCheck","BMI"]) \
            .withColumn(
            "BMI",
            F.when(df.BMI < 18.5, "UnderWeight") \
            .when((df.BMI > 18.5) & (df.BMI < 24.9), "UnderWeight") \
            .when((df.BMI > 25) & (df.BMI < 29.9), "UnderWeight") \
            .when((df.BMI > 30) & (df.BMI < 34.9), "UnderWeight") \
            .when((df.BMI > 35) & (df.BMI < 39.9), "UnderWeight") \
            .otherwise("UnderWeight")
        )
    )

def bmi_agg(df):
    return(
        df.groupBy("BMI") \
        .agg(
           F.mean(F.col("HighBP")).alias("avg_bp"),
           F.mean(F.col("HighChol")).alias("avg_chol"),
           F.mean(F.col("CholCheck")).alias("avg_chol_check")
        )
    )

def save_delta_table(df, catalog, schema, table_name):
    df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{catalog}.{schema}.{table_name}")
