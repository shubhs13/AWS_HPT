import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── CONFIG — PATH
bronze_path = "s3://htp-bronze-bucket/raw_bronze/Methodist/hospitalone.csv"
metadata_path = "s3://htp-bronze-bucket/staging/Methodist/hospital_metadata/"
charges_path = "s3://htp-bronze-bucket/staging/Methodist/charge_data/"

# ── STEP 1: Read raw CSV with NO header (all rows treated as data)
raw_df = spark.read \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .csv(bronze_path)

## INSPECT
# raw_df.show(5, truncate=False)

# ============================================================
# STEP 1: Extract hospital metadata
# ============================================================

metadata_headers = raw_df.first()
metadata_values = raw_df.collect()[1]

# Keep only metadata fields that have a real column name
metadata = {
    header: value
    for header, value in zip(metadata_headers, metadata_values)
    if header is not None and header != ""
}

metadata_schema = StructType([
    StructField(column_name, StringType(), True)
    for column_name in metadata.keys()
])

metadata_df = spark.createDataFrame(
    [tuple(metadata.values())],
    schema=metadata_schema
)

print("===== METADATA DATAFRAME START =====")
metadata_df.show(truncate=False)
print("===== METADATA DATAFRAME END =====")

# ============================================================
# STEP 2: Extract charge data
# ============================================================

# Row 3 contains the charge column names
charge_headers = raw_df.collect()[2]

# Rows 4 onward contain the actual charge records
charge_data = raw_df.rdd.zipWithIndex() \
    .filter(lambda x: x[1] >= 3) \
    .map(lambda x: x[0])

charge_schema = StructType([
    StructField(column_name, StringType(), True)
    for column_name in charge_headers
])

charges_df = spark.createDataFrame(
    charge_data,
    schema=charge_schema
)

print("===== CHARGE DATA SAMPLE START =====")
charges_df.show(5, truncate=False)
print("===== CHARGE DATA SAMPLE END =====")

print("Number of charge columns:", len(charges_df.columns))


# ============================================================
# STEP 3: Write parsed data to Staging
# ============================================================
# Write hospital metadata
metadata_df.write \
    .mode("overwrite") \
    .parquet(metadata_path)

# Write charge data
charges_df.write \
    .mode("overwrite") \
    .parquet(charges_path)

print("===== STAGING WRITE COMPLETE =====")
print("Metadata written to:", metadata_path)
print("Charges written to:", charges_path)



job.commit()
