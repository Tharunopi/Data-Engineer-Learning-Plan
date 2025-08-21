import dlt
import pyspark.sql.functions as F

@dlt.table(
    name="guns.silver.psudo"
)
def loader():
    df = spark.readStream.table("guns.bronze.stress_level_bronze")
    df = df.withColumn("bullying", F.sha2(F.col("bullying").cast("BINARY"), 256))
    return df

@dlt.table(
    name="guns.silver.anxiety_level_id_gen"
)
def gen_uuid():
    df = spark.readStream.table("guns.silver.psudo")
    df = df.select("anxiety_level") \
            .distinct() \
            .withColumn("anxiety_token", F.expr("uuid()"))
    return df 

@dlt.table(
    name="guns.silver.anxiety_lookup"
)
def anxiety_lookup():
    token_df = spark.readStream.table("guns.silver.anxiety_level_id_gen")
    ori_df = spark.readStream.table("guns.silver.psudo")
    df = token_df.join(ori_df, on="anxiety_level", how="inner").select(["anxiety_token", "anxiety_level"]).distinct()
    return df.select(["anxiety_token", "anxiety_level"])

@dlt.table(
    name="guns.gold.may_be_final"
)
def gold_view():
    token_df = spark.readStream.table("guns.silver.anxiety_level_id_gen")
    ori_df = spark.readStream.table("guns.silver.psudo")
    df = token_df.join(ori_df, on="anxiety_level", how="inner").drop("anxiety_level")
    return df

@dlt.table(
    name="guns.gold.anonymization"
)
def anonymus():
    df = spark.readStream.table("guns.bronze.stress_level_bronze")
    for i in df.columns:
        df = df.withColumn(i, F.col(i) / 100)
    return df 