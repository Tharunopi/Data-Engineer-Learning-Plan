import dlt, sys, os
from utilities.utils import *

@dlt.table(
    name="dev.bronze.crypto_data"
)
def ingesting_data():
    df = get_files_incrementally(catalog="dev", schema="default", volume="crypto")
    return df 