from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W

spark = SparkSession.builder.appName("main_mini_project").getOrCreate()

def get_files_incrementally(catalog, schema, volume):
    return spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .option("coludFiles.header", "true") \
                .option("cloudFiles.inferSchema", "true") \
                .load(f"/Volumes/{catalog}/{schema}/{volume}")

def rsi(df):
    # rs = 14 days avg gain / 14 days avg loss, rsi = 100 - (100 / (1 + rs))
    
