import dlt, sys, os
from utilities.utils import get_files_incrementally

catalog = spark.conf.get("catalog")

@dlt.table(
    name=f"{catalog}.bronze.crypto_data"
)
def ingesting_data():
    df = get_files_incrementally(catalog=catalog, schema="default", volume="crypto")
    return df 