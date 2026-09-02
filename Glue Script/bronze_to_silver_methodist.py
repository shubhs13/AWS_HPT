import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── CONFIG — replace these with your actual paths 
bronze_path = "s3://htp-bronze-bucket/raw_bronze/Methodist/hospitalone.csv"

# Read raw CSV with NO header (all rows treated as data)
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

metadata = dict(
    zip(
        metadata_headers,
        metadata_values
    )
)

print("===== HOSPITAL METADATA START =====")

for key, value in metadata.items():
    print(f"{key} = {value}")

print("===== HOSPITAL METADATA END =====")

# ============================================================
# STEP 2: Extract charge data
# ============================================================

# Row 3 contains the charge column names
charge_headers = raw_df.collect()[2]

# Rows 4 onward contain the actual charge records
charge_data = raw_df.rdd.zipWithIndex() \
    .filter(lambda x: x[1] >= 3) \
    .map(lambda x: x[0])

# Create DataFrame using Row 3 as column names
charges_df = spark.createDataFrame(
    charge_data,
    schema=charge_headers
)

print("===== CHARGE DATA SAMPLE START =====")
charges_df.show(5, truncate=False)
print("===== CHARGE DATA SAMPLE END =====")

print("Number of charge columns:", len(charges_df.columns))



job.commit()
