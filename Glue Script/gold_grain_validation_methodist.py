import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# ============================================================
# 1. Initialize Glue / Spark
# ============================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("gold_grain_validation_methodist", {})


# ============================================================
# 2. Read Staging data
# ============================================================

staging_path = "s3://htp-bronze-bucket/staging/Methodist/charge_data/"

df = spark.read.parquet(staging_path)


# ============================================================
# 3. Basic dataset validation
# ============================================================

total_count = df.count()
column_count = len(df.columns)

print("========================================")
print("STAGING DATASET")
print("========================================")
print(f"Total records : {total_count}")
print(f"Total columns : {column_count}")


# ============================================================
# 4. Keep only CPT / HCPCS records
# ============================================================

eligible_df = df.filter(
    F.col("code|2|type").isin("CPT", "HCPCS")
)

eligible_count = eligible_df.count()

print("\n========================================")
print("ELIGIBLE CPT / HCPCS RECORDS")
print("========================================")
print(f"Eligible records : {eligible_count}")


# ============================================================
# 5. Candidate Gold Grain
# ============================================================
#
# One row should represent one distinct reported
# pricing observation.
#
# We are progressively adding source identifiers
# to determine what actually distinguishes observations.
#
# Current candidate:
#
# code|1
# code|1|type
# code|2
# code|2|type
# code|3
# code|3|type
# code|4
# code|4|type
# setting
# payer_name
# plan_name
#
# IMPORTANT:
# Pricing fields are intentionally NOT included.
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


# ============================================================
# 6. Check for duplicate Gold grain
# ============================================================

grain_counts = (
    eligible_df
    .groupBy(gold_grain_columns)
    .count()
)


duplicate_groups = (
    grain_counts
    .filter(F.col("count") > 1)
)

duplicate_group_count = duplicate_groups.count()


# Number of records beyond the first occurrence
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
print("GOLD GRAIN VALIDATION")
print("========================================")

print("Candidate grain:")
for col_name in gold_grain_columns:
    print(f"  - {col_name}")

print(f"\nDuplicate grain groups : {duplicate_group_count}")
print(f"Duplicate records beyond first : {duplicate_record_count}")


# ============================================================
# 7. Investigate duplicates if they exist
# ============================================================

if duplicate_group_count > 0:

    print("\n========================================")
    print("DUPLICATE GRAIN GROUPS")
    print("========================================")

    duplicate_groups.orderBy(
        F.desc("count")
    ).show(
        50,
        truncate=False
    )


    # --------------------------------------------------------
    # Join duplicate grain groups back to source data
    # --------------------------------------------------------

    duplicate_records = (
        eligible_df.alias("source")
        .join(
            duplicate_groups
            .select(*gold_grain_columns)
            .alias("duplicates"),
            on=gold_grain_columns,
            how="inner"
        )
    )


    print("\n========================================")
    print("DUPLICATE RECORD DETAILS")
    print("========================================")

    diagnostic_columns = [
        "code|1",
        "code|1|type",
        "code|2",
        "code|2|type",
        "code|3",
        "code|3|type",
        "code|4",
        "code|4|type",
        "code|5",
        "code|5|type",
        "modifiers",
        "description",
        "setting",
        "payer_name",
        "plan_name",
        "standard_charge|gross",
        "standard_charge|discounted_cash",
        "standard_charge|negotiated_dollar",
        "standard_charge|negotiated_percentage",
        "standard_charge|negotiated_algorithm",
        "standard_charge|min",
        "standard_charge|max",
        "standard_charge|methodology",
        "count",
        "median_amount",
        "10th_percentile",
        "90th_percentile",
        "additional_generic_notes"
    ]

    duplicate_records.select(
        *diagnostic_columns
    ).orderBy(
        *gold_grain_columns
    ).show(
        100,
        truncate=False
    )


# ============================================================
# 8. Final result
# ============================================================

print("\n========================================")
print("FINAL RESULT")
print("========================================")

if duplicate_group_count == 0:

    print("GOLD GRAIN IS UNIQUE")
    print("No duplicate records found for the candidate grain.")

else:

    print("GOLD GRAIN IS NOT UNIQUE")
    print("Additional source-level field(s) may be required.")


# ============================================================
# 9. End Glue job
# ============================================================

job.commit()
