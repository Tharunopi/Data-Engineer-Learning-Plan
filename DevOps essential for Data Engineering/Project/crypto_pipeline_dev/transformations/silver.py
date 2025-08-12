import dlt
from utilities.utils import decimal_rounder, per_month, per_year

@dlt.table(
    name="dev.silver.per_month"
)
def monthly():
    df = spark.readStream.table("dev.bronze.crypto_data")
    df = decimal_rounder(df)
    df = per_month(df)
    return df 

@dlt.table(
    name="dev.silver.per_year"
)
def yearly():
    df = spark.readStream.table("dev.bronze.crypto_data")
    df = decimal_rounder(df)
    df = per_year(df)
    return df 