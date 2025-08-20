import dlt

rules = {
    "low_bully": "bullying = 0",
    "high_bully": "bullying = 5"
}

inverse_rules = {
    "low_bully": "bullying != 0",
    "high_bully": "bullying != 5"
}

@dlt.table(
    name="guns.silver.bully_filter"
)
@dlt.expect("low_bully", "bullying = 0")
@dlt.expect("high_bully", "bullying = 5")
def check():
    return spark.readStream.table("guns.bronze.stress_level_bronze")

@dlt.table(
    name="guns.silver.qurantine"
)
@dlt.expect_all_or_drop(inverse_rules)
def check():
    return spark.readStream.table("guns.bronze.stress_level_bronze")