import dlt
import pyspark.sql.types as T
from utilities.utils import agg, check_schema

catalog = spark.conf.get("catalog")

@dlt.table(
    name=f"{catalog}.gold.per_month_view"
)
def month_mv():
    df = spark.read.table(f"{catalog}.silver.per_month")
    df = agg(df)
    return df 

@dlt.table(
    name=f"{catalog}.gold.per_year_view"
)
def year_mv():
    df = spark.read.table(f"{catalog}.silver.per_year")
    df = agg(df)
    return df 