import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# ============================================================
# 1. Initialize Glue / Spark
# ============================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("staging_to_silver_methodist", {})


# ============================================================
# 2. Paths
# ============================================================

staging_path = "s3://htp-bronze-bucket/staging/Methodist/charge_data/"
silver_path = "s3://htp-bronze-bucket/silver/Methodist/"


# ============================================================
# 3. Read Staging data
# ============================================================

staging_df = spark.read.parquet(staging_path)

print("========================================")
print("STAGING DATA")
print("========================================")

staging_count = staging_df.count()

print(f"Staging records : {staging_count}")
print(f"Staging columns : {len(staging_df.columns)}")


# ============================================================
# 4. Filter to CPT / HCPCS
# ============================================================
#
# Business requirement:
# Only CPT and HCPCS procedure records are required
# for the MVP.
#
# ============================================================

silver_df = staging_df.filter(
    F.col("code|2|type").isin("CPT", "HCPCS")
)

eligible_count = silver_df.count()

print("\n========================================")
print("CPT / HCPCS FILTER")
print("========================================")

print(f"Eligible records : {eligible_count}")
print(f"Excluded records : {staging_count - eligible_count}")


# ============================================================
# 5. Select Silver columns
# ============================================================
#
# IMPORTANT:
# Code 1, Code 2, Code 3 and Code 4 are retained because
# Gold grain validation showed that these source-level
# identifiers are required to uniquely distinguish reported
# pricing observations in this MRF.
#
# Pricing fields are retained separately as attributes.
#
# ============================================================

silver_columns = [

    # Source / procedure identifiers
    "code|1",
    "code|1|type",

    "code|2",
    "code|2|type",

    "code|3",
    "code|3|type",

    "code|4",
    "code|4|type",

    # Procedure description
    "description",

    # Healthcare setting
    "setting",

    # Pricing
    "standard_charge|gross",
    "standard_charge|discounted_cash",

    "standard_charge|negotiated_dollar",
    "standard_charge|negotiated_percentage",
    "standard_charge|negotiated_algorithm",

    "standard_charge|min",
    "standard_charge|max",

    "standard_charge|methodology",

    # Payer / plan
    "payer_name",
    "plan_name"
]


silver_df = silver_df.select(
    *silver_columns
)


# ============================================================
# 6. Cast pricing fields to Decimal
# ============================================================

decimal_type = DecimalType(18, 2)

decimal_columns = [
    "standard_charge|gross",
    "standard_charge|discounted_cash",
    "standard_charge|negotiated_dollar",
    "standard_charge|negotiated_percentage",
    "standard_charge|min",
    "standard_charge|max"
]


for column_name in decimal_columns:

    silver_df = silver_df.withColumn(
        column_name,
        F.col(column_name).cast(decimal_type)
    )


# ============================================================
# 7. Validate Silver record count before deduplication
# ============================================================

before_dedup_count = silver_df.count()

print("\n========================================")
print("SILVER BEFORE DEDUPLICATION")
print("========================================")

print(f"Records : {before_dedup_count}")


# ============================================================
# 8. Remove exact duplicate records
# ============================================================
#
# We remove only completely identical records.
#
# We DO NOT deduplicate based on the Gold grain.
# Different pricing observations must be preserved.
#
# ============================================================

silver_deduped = silver_df.dropDuplicates()

after_dedup_count = silver_deduped.count()

removed_count = before_dedup_count - after_dedup_count


print("\n========================================")
print("SILVER DEDUPLICATION")
print("========================================")

print(f"Before deduplication : {before_dedup_count}")
print(f"After deduplication  : {after_dedup_count}")
print(f"Exact duplicates removed : {removed_count}")


# ============================================================
# 9. Validate Gold grain on final Silver
# ============================================================
#
# This is the grain validated previously against Staging.
#
# Expected:
# Duplicate grain groups = 0
#
# ============================================================

gold_grain_columns = [

    "code|1",
    "code|1|type",

    "code|2",
    "code|2|type",

    "code|3",
    "code|3|type",

    "code|4",
    "code|4|type",

    "setting",
    "payer_name",
    "plan_name"
]


grain_counts = (
    silver_deduped
    .groupBy(gold_grain_columns)
    .count()
)


duplicate_groups = (
    grain_counts
    .filter(F.col("count") > 1)
)

duplicate_group_count = duplicate_groups.count()


duplicate_record_count = (
    duplicate_groups
    .select(
        F.sum(F.col("count") - 1).alias("duplicate_records")
    )
    .collect()[0]["duplicate_records"]
)

if duplicate_record_count is None:
    duplicate_record_count = 0


print("\n========================================")
print("GOLD GRAIN VALIDATION ON SILVER")
print("========================================")

print(f"Duplicate grain groups : {duplicate_group_count}")
print(
    f"Duplicate records beyond first : "
    f"{duplicate_record_count}"
)


if duplicate_group_count == 0:

    print("GOLD GRAIN IS UNIQUE")

else:

    print("WARNING: GOLD GRAIN IS NOT UNIQUE")

    duplicate_groups.orderBy(
        F.desc("count")
    ).show(
        50,
        truncate=False
    )


# ============================================================
# 10. Basic Silver pricing validation
# ============================================================

print("\n========================================")
print("SILVER PRICING VALIDATION")
print("========================================")

for column_name in decimal_columns:

    null_count = (
        silver_deduped
        .filter(F.col(column_name).isNull())
        .count()
    )

    print(
        f"{column_name} null count : {null_count}"
    )


# ============================================================
# 11. Final Silver schema
# ============================================================

print("\n========================================")
print("FINAL SILVER SCHEMA")
print("========================================")

silver_deduped.printSchema()


# ============================================================
# 12. Write Silver Parquet
# ============================================================

silver_deduped.write \
    .mode("overwrite") \
    .parquet(silver_path)


print("\n========================================")
print("SILVER WRITE COMPLETE")
print("========================================")

print(f"Silver path : {silver_path}")
print(f"Final Silver records : {after_dedup_count}")


# ============================================================
# 13. Commit Glue job
# ============================================================

job.commit()
