import dlt
import pyspark.sql.functions as F

# bronze layer
@dlt.table(
    name="guns.bronze.guns_mfs"
)
def ingestion():
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.header", "true") \
        .option("cloudFiles.inferSchema", "true") \
        .option("cloudFiles.validateOptions", "false") \
        .load("/Volumes/guns/default/guns_dataset")
    return df

# silver layer
@dlt.table(
    name="guns.silver.incident_per_year"
)
def table_1():
    df = spark.readStream.table("guns.bronze.guns_mfs")
    df = df.select(["year", "month"])
    return df 

@dlt.table(
    name="guns.silver.incident_by_gender"
)
def table_2():
    df = spark.readStream.table("guns.bronze.guns_mfs")
    df = df.select("sex")
    return df 

@dlt.table(
    name="guns.silver.incident_by_intent"
)
def table_3():
    df = spark.readStream.table("guns.bronze.guns_mfs")
    df = df.select("intent")
    return df

# gold layer 
@dlt.table(
    name="guns.gold.year_view"
)
def view_1():
    df = spark.read.table("guns.silver.incident_per_year")
    df = df.groupBy("year").agg(F.count(F.col("month")).alias("intent_count_year"))
    return df 

@dlt.table(
    name="guns.gold.gender_view"
)
def view_2():
    df = spark.read.table("guns.silver.incident_by_gender")
    df = df.groupBy("sex").agg(F.count(F.col("sex")).alias("gender_count"))
    return df 

@dlt.table(
    name="guns.gold.intent_view"
)
def view_3():
    df = spark.read.table("guns.silver.incident_by_intent")
    df = df.groupBy("intent").agg(F.count(F.col("intent")).alias("intent_count"))
    return df 