import dlt
from pyspark.sql.types import *
from utilities.utils import decimal_rounder, per_month, per_year, check_schema

catalog = spark.conf.get("catalog")

@dlt.table(
    name=f"{catalog}.silver.per_month"
)
def monthly():
    df = spark.readStream.table(f"{catalog}.bronze.crypto_data")
    df = decimal_rounder(df)
    df = per_month(df)
    return df 

@dlt.table(
    name=f"{catalog}.silver.per_year"
)
def yearly():
    df = spark.readStream.table(f"{catalog}.bronze.crypto_data")
    df = decimal_rounder(df)
    df = per_year(df)
    return df 