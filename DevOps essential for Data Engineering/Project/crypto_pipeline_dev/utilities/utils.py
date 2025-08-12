from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T
from pyspark.testing import assertSchemaEqual

spark = SparkSession.builder.appName("main_mini_project").getOrCreate()

def get_files_incrementally(catalog, schema, volume):
    return spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .option("coludFiles.header", "true") \
                .option("cloudFiles.inferSchema", "true") \
                .load(f"/Volumes/{catalog}/{schema}/{volume}")

def decimal_rounder(df):
    float_cols = [i[0] for i in df.dtypes if i[1] == "double"]
    
    def single_rounder(df, col):
        df = df.withColumn(col, F.round(F.col(col), 4))
        return df 
    
    for i in float_cols:
        df = single_rounder(i)

    return df 

def per_month(df):
    df = df.withColumn("Date", F.month(F.col("Date")))
    return df 

def per_year(df):
    df = df.withColumn("Date", F.year(F.col("Date")))
    return df 

def agg(df):
    df = df.groupBy(["Symbol", "Date"]).agg(
        F.sum("Volume").alias("total_volume"),
        F.abs(((F.max(F.col("Volume")) - F.min(F.col("Volume"))) / (F.min(F.col("Volume")))) * 100).alias("volatile_percentage")
    ) \
    .orderBy(["Symbol", "Date"])

    return df 

def check_schema(df, target_schema):
    source_schema = df.schema
    assertSchemaEqual(source_schema, target_schema, ignoreColumnOrder=True)
    