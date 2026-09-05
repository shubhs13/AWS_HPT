import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, trim
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# CONFIG — STAGING PATHS
# ============================================================

metadata_path = "s3://htp-bronze-bucket/staging/Methodist/hospital_metadata/"
charges_path = "s3://htp-bronze-bucket/staging/Methodist/charge_data/"

# ============================================================
# STEP 1: READ STAGING PARQUET
# ============================================================

metadata_df = spark.read.parquet(metadata_path)
charges_df = spark.read.parquet(charges_path)

# ============================================================
# Data Validation Check Column Data Type and Null
# ============================================================

# ======== 1 . Check Column Data Types ========
print("===== COLUMN DATA TYPES =====")

for field in charges_df.schema.fields:
    print(field.name, "->", field.dataType)

# ========2. code|2 and code|2|type ========

print("\n===== code|2 VALIDATION =====")

total_records = charges_df.count()

code2_null = charges_df.filter(
    col("code|2").isNull() | (trim(col("code|2")) == "")
).count()

code2_type_null = charges_df.filter(
    col("code|2|type").isNull() | (trim(col("code|2|type")) == "")
).count()

code2_both_null = charges_df.filter(
    (col("code|2").isNull() | (trim(col("code|2")) == "")) &
    (col("code|2|type").isNull() | (trim(col("code|2|type")) == ""))
).count()

# print("Total records:", total_records)
# print("code|2 NULL/empty:", code2_null)
# print("code|2|type NULL/empty:", code2_type_null)
# print("Both code|2 and code|2|type NULL/empty:", code2_both_null)

# -----------------------------------------
# Validation 3: code|2 values by code type
# -----------------------------------------

print("\n===== DISTINCT code|2 VALUES =====")

code2_distinct = charges_df.filter(
    col("code|2").isNotNull() &
    (trim(col("code|2")) != "")
).select(
    "code|2"
).distinct().count()

print("Distinct code|2 values:", code2_distinct)


print("\n===== SAMPLE HCPCS CODES =====")

charges_df.filter(
    col("code|2|type") == "HCPCS"
).select(
    "code|2"
).distinct().show(30, truncate=False)


print("\n===== SAMPLE CPT CODES =====")

charges_df.filter(
    col("code|2|type") == "CPT"
).select(
    "code|2"
).distinct().show(30, truncate=False)

# -----------------------------------------
# Validation 4 : CPT / HCPCS eligibility
# -----------------------------------------

total_records = charges_df.count()

eligible_records = charges_df.filter(
    trim(col("code|2|type")).isin("CPT", "HCPCS")
).count()

excluded_records = total_records - eligible_records

eligible_percentage = (eligible_records / total_records) * 100
excluded_percentage = (excluded_records / total_records) * 100

print("Total records:", total_records)
print("CPT / HCPCS records:", eligible_records)
print("Excluded records:", excluded_records)
print("Eligible percentage:", round(eligible_percentage, 2), "%")
print("Excluded percentage:", round(excluded_percentage, 2), "%")
    
charges_df.groupBy(
    trim(col("code|2|type")).alias("code_type")
).count().orderBy(
    col("count").desc()
).show(20, False)  
# -----------------------------------------
# Validation 6 : Gross charge validation
# -----------------------------------------

gross_col = "standard_charge|gross"

total_records = charges_df.count()

null_or_empty_gross = charges_df.filter(
    col(gross_col).isNull() |
    (trim(col(gross_col)) == "")
).count()

invalid_gross = charges_df.filter(
    col(gross_col).isNotNull() &
    (trim(col(gross_col)) != "") &
    col(gross_col).cast("double").isNull()
).count()

valid_gross = total_records - null_or_empty_gross - invalid_gross

print("Total records:", total_records)
print("NULL / empty gross charges:", null_or_empty_gross)
print("Invalid numeric gross charges:", invalid_gross)
print("Valid numeric gross charges:", valid_gross)

charges_df.select(
    col(gross_col).cast("double").alias("gross_numeric")
).summary("min", "max").show()
    
 # -----------------------------------------
# Validation 7: Standard_Charge Bulk validation
# -----------------------------------------  

from pyspark.sql.functions import (
    col, trim, sum, when,
    min as spark_min,
    max as spark_max
)

pricing_columns = [
    "standard_charge|discounted_cash",
    "standard_charge|negotiated_dollar",
    "standard_charge|negotiated_percentage",
    "standard_charge|min",
    "standard_charge|max"
]

# Create numeric versions of pricing columns
pricing_df = charges_df

for c in pricing_columns:
    pricing_df = pricing_df.withColumn(
        c + "_numeric",
        trim(col(c)).cast("double")
    )

# One aggregation across the entire dataset
metrics = []

for c in pricing_columns:
    numeric_c = c + "_numeric"

    metrics.extend([
        sum(
            when(
                col(c).isNull() | (trim(col(c)) == ""),
                1
            ).otherwise(0)
        ).alias(c + "_null_empty"),

        sum(
            when(
                col(c).isNotNull() &
                (trim(col(c)) != "") &
                col(numeric_c).isNull(),
                1
            ).otherwise(0)
        ).alias(c + "_invalid"),

        spark_min(col(numeric_c)).alias(c + "_min"),
        spark_max(col(numeric_c)).alias(c + "_max")
    ])

pricing_df.select(*metrics).show(truncate=False)

# Min - Max Relationship

# Validate minimum negotiated price <= maximum negotiated price

min_max_check = pricing_df.filter(
    col("standard_charge|min_numeric").isNotNull() &
    col("standard_charge|max_numeric").isNotNull() &
    (
        col("standard_charge|min_numeric") >
        col("standard_charge|max_numeric")
    )
).count()

print("Records where negotiated min > negotiated max:", min_max_check)
    
    
# --------------------------------------------------
# Negotiated Dollar Batch
# --------------------------------------------------
from pyspark.sql.functions import (
    col, trim, when, count, sum
)

neg_dollar = "standard_charge|negotiated_dollar"
neg_pct = "standard_charge|negotiated_percentage"
neg_algo = "standard_charge|negotiated_algorithm"
methodology = "standard_charge|methodology"

# --------------------------------------------------
# Check 1 — Completeness
# --------------------------------------------------

completeness = charges_df.select(
    count("*").alias("total_records"),

    sum(
        when(
            col(neg_dollar).isNull() |
            (trim(col(neg_dollar)) == ""),
            1
        ).otherwise(0)
    ).alias("negotiated_dollar_null"),

    sum(
        when(
            col(neg_pct).isNull() |
            (trim(col(neg_pct)) == ""),
            1
        ).otherwise(0)
    ).alias("negotiated_percentage_null"),

    sum(
        when(
            col(neg_algo).isNull() |
            (trim(col(neg_algo)) == ""),
            1
        ).otherwise(0)
    ).alias("negotiated_algorithm_null"),

    sum(
        when(
            col(methodology).isNull() |
            (trim(col(methodology)) == ""),
            1
        ).otherwise(0)
    ).alias("methodology_null")
)

completeness.show(truncate=False)


# --------------------------------------------------
# Check 2 — Methodology distribution
# --------------------------------------------------

print("=== Methodology Distribution ===")

charges_df.groupBy(
    trim(col(methodology)).alias("methodology")
).count().orderBy(
    col("count").desc()
).show(50, False)


# --------------------------------------------------
# Check 3 — Negotiated algorithm distribution
# --------------------------------------------------

print("=== Negotiated Algorithm Distribution ===")

charges_df.groupBy(
    trim(col(neg_algo)).alias("negotiated_algorithm")
).count().orderBy(
    col("count").desc()
).show(10, False)


# --------------------------------------------------
# Check 4 — Negotiated pricing representation
# --------------------------------------------------

pricing_representation = charges_df.withColumn(
    "pricing_representation",
    when(
        (col(neg_dollar).isNotNull()) &
        (trim(col(neg_dollar)) != "") &
        (col(neg_pct).isNull() | (trim(col(neg_pct)) == "")) &
        (col(neg_algo).isNull() | (trim(col(neg_algo)) == "")),
        "Dollar only"
    ).when(
        (col(neg_dollar).isNull() | (trim(col(neg_dollar)) == "")) &
        (col(neg_pct).isNotNull()) &
        (trim(col(neg_pct)) != "") &
        (col(neg_algo).isNull() | (trim(col(neg_algo)) == "")),
        "Percentage only"
    ).when(
        (col(neg_dollar).isNull() | (trim(col(neg_dollar)) == "")) &
        (col(neg_pct).isNull() | (trim(col(neg_pct)) == "")) &
        (col(neg_algo).isNotNull()) &
        (trim(col(neg_algo)) != ""),
        "Algorithm only"
    ).when(
        (col(neg_dollar).isNotNull()) &
        (trim(col(neg_dollar)) != "") &
        (col(neg_pct).isNotNull()) &
        (trim(col(neg_pct)) != "") &
        (col(neg_algo).isNull() | (trim(col(neg_algo)) == "")),
        "Dollar + Percentage"
    ).when(
        (col(neg_dollar).isNotNull()) &
        (trim(col(neg_dollar)) != "") &
        (col(neg_pct).isNull() | (trim(col(neg_pct)) == "")) &
        (col(neg_algo).isNotNull()) &
        (trim(col(neg_algo)) != ""),
        "Dollar + Algorithm"
    ).when(
        (col(neg_dollar).isNull() | (trim(col(neg_dollar)) == "")) &
        (col(neg_pct).isNotNull()) &
        (trim(col(neg_pct)) != "") &
        (col(neg_algo).isNotNull()) &
        (trim(col(neg_algo)) != ""),
        "Percentage + Algorithm"
    ).when(
        (col(neg_dollar).isNotNull()) &
        (trim(col(neg_dollar)) != "") &
        (col(neg_pct).isNotNull()) &
        (trim(col(neg_pct)) != "") &
        (col(neg_algo).isNotNull()) &
        (trim(col(neg_algo)) != ""),
        "All three"
    ).otherwise(
        "None"
    )
)

print("=== Negotiated Pricing Representation ===")

pricing_representation.groupBy(
    "pricing_representation"
).count().orderBy(
    col("count").desc()
).show(5, False)
    
#
#=== Payer / Plan Relationship ===
#
print("=== Payer / Plan Relationship ===")

charges_df.groupBy(
    trim(col("payer_name")).alias("payer_name"),
    trim(col("plan_name")).alias("plan_name")
).count().orderBy(
    col("payer_name"),
    col("count").desc()
).show(200, False)

print("=== Missing Payer / Plan ===")

charges_df.select(
    sum(
        when(
            col("payer_name").isNull() |
            (trim(col("payer_name")) == ""),
            1
        ).otherwise(0)
    ).alias("payer_null_empty"),

    sum(
        when(
            col("plan_name").isNull() |
            (trim(col("plan_name")) == ""),
            1
        ).otherwise(0)
    ).alias("plan_null_empty")
).show()
#
# Check Duplicates
#
print("Start HER SHubhaz")
from pyspark.sql.functions import col, trim, count

silver_cols = [
    "description",
    "code|2",
    "code|2|type",
    "setting",
    "standard_charge|gross",
    "standard_charge|discounted_cash",
    "payer_name",
    "plan_name",
    "standard_charge|negotiated_dollar",
    "standard_charge|negotiated_percentage",
    "standard_charge|negotiated_algorithm",
    "standard_charge|methodology",
    "standard_charge|min",
    "standard_charge|max"
]

df = spark.read.parquet(
    "s3://htp-bronze-bucket/staging/Methodist/charge_data/"
)

# Silver eligibility
df = df.filter(
    trim(col("code|2|type")).isin("CPT", "HCPCS")
)

print("Silver-eligible records:", df.count())


# --------------------------------------------------
# 1. EXACT DUPLICATES
# --------------------------------------------------

exact_dupes = (
    df.groupBy(silver_cols)
      .count()
      .filter(col("count") > 1)
)

print("\n===== EXACT DUPLICATES =====")
print("Duplicate groups:", exact_dupes.count())

exact_dupes_records = exact_dupes.selectExpr(
    "sum(count - 1) as duplicate_records"
).collect()[0]["duplicate_records"]

print("Duplicate records beyond first occurrence:", exact_dupes_records)


# --------------------------------------------------
# 2. DUPLICATE PROCEDURE IDENTIFICATION
# --------------------------------------------------

procedure_cols = [
    "description",
    "code|2",
    "code|2|type"
]

procedure_dupes = (
    df.groupBy(procedure_cols)
      .count()
      .filter(col("count") > 1)
)

print("\n===== DUPLICATE PROCEDURE IDENTIFICATION =====")
print("Duplicate procedure groups:", procedure_dupes.count())

procedure_dupe_records = procedure_dupes.selectExpr(
    "sum(count - 1) as duplicate_records"
).collect()[0]["duplicate_records"]

print("Duplicate records beyond first occurrence:", procedure_dupe_records)


# --------------------------------------------------
# 3. DUPLICATE PROCEDURE + PAYER/PLAN
# --------------------------------------------------

payer_plan_cols = [
    "description",
    "code|2",
    "code|2|type",
    "payer_name",
    "plan_name"
]

payer_plan_dupes = (
    df.groupBy(payer_plan_cols)
      .count()
      .filter(col("count") > 1)
)

print("\n===== DUPLICATE PROCEDURE + PAYER/PLAN =====")
print("Duplicate groups:", payer_plan_dupes.count())

payer_plan_dupe_records = payer_plan_dupes.selectExpr(
    "sum(count - 1) as duplicate_records"
).collect()[0]["duplicate_records"]

print("Duplicate records beyond first occurrence:", payer_plan_dupe_records)


# --------------------------------------------------
# 4. DUPLICATE PROCEDURE + PAYER/PLAN + PRICING
# --------------------------------------------------

pricing_cols = payer_plan_cols + [
    "standard_charge|negotiated_dollar",
    "standard_charge|negotiated_percentage",
    "standard_charge|negotiated_algorithm",
    "standard_charge|methodology",
    "standard_charge|min",
    "standard_charge|max"
]

pricing_dupes = (
    df.groupBy(pricing_cols)
      .count()
      .filter(col("count") > 1)
)

print("\n===== DUPLICATE PROCEDURE + PAYER/PLAN + PRICING =====")
print("Duplicate groups:", pricing_dupes.count())

pricing_dupe_records = pricing_dupes.selectExpr(
    "sum(count - 1) as duplicate_records"
).collect()[0]["duplicate_records"]

print("Duplicate records beyond first occurrence:", pricing_dupe_records)


# --------------------------------------------------
# SAMPLE DUPLICATE PATTERNS
# --------------------------------------------------

print("\n===== SAMPLE EXACT DUPLICATES =====")
exact_dupes.show(20, truncate=False)

print("\n===== SAMPLE PROCEDURE DUPLICATES =====")
procedure_dupes.show(20, truncate=False)

print("\n===== SAMPLE PAYER/PLAN DUPLICATES =====")
payer_plan_dupes.show(20, truncate=False)

print("\n===== SAMPLE PRICING DUPLICATES =====")
pricing_dupes.show(20, truncate=False)

job.commit()
