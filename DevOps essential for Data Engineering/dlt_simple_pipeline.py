import pyspark.sql.functions as F

# bronze layer 
df = spark.read.csv(
        "/Volumes/diabetes/v01/cdc-diabetes/diabetes_binary_5050split_BRFSS2015.csv",
        inferSchema = True,
        header = True
        )
df.write.mode("overwrite").format("delta").saveAsTable("health_dlt.bronze.brfs")
    
# silver layer 
df = spark.read.table("health_dlt.bronze.brfs")
df = df.select(["HighBP","HighChol","CholCheck","BMI"])
df.write.mode("overwrite").format("delta").saveAsTable("health_dlt.silver.brfs")

#gold layer 
df = spark.read.table("health_dlt.silver.brfs")
df = df.select(["HighBP","HighChol","CholCheck","BMI"]) \
       .groupBy("BMI") \
       .agg(
           F.mean(F.col("HighBP")).alias("avg_bp"),
           F.mean(F.col("HighChol")).alias("avg_chol"),
           F.mean(F.col("CholCheck")).alias("avg_chol_check")
       )
df.write.mode("overwrite").format("delta").saveAsTable("health_dlt.gold.brfs")

df = spark.read.table("health_dlt.gold.brfs")
df.display(100)