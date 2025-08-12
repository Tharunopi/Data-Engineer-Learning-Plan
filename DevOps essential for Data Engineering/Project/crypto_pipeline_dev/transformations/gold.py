import dlt
from utilities.utils import agg

@dlt.table(
    name="dev.gold.per_month_view"
)
def month_mv():
    df = spark.read.table("dev.silver.per_month")
    df = agg(df)
    return df 

@dlt.table(
    name="dev.gold.per_year_view"
)
def year_mv():
    df = spark.read.table("dev.silver.per_year")
    df = agg(df)
    return df 