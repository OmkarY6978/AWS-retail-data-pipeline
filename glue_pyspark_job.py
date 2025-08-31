import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # <-- THIS IS THE NEW LINE WE ADDED
from pyspark.sql.functions import col, from_utc_timestamp, current_timestamp

# --- Job Initialization ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- Data Source ---
# Read data from the "raw-data" folder in your S3 bucket
# Replace 'your-bucket-name' with the actual S3 bucket name you created.
input_path = "s3://omkar-retail-data-pipeline-12345/raw-data/"
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="csv",
    format_options={"withHeader": True, "inferSchema": True},
    transformation_ctx="datasource",
)

# --- Transformations ---
# Convert DynamicFrame to Spark DataFrame for easier manipulation
df = datasource.toDF()

# 1. Cast data types to be more specific
df = df.withColumn("price", col("price").cast("double"))
df = df.withColumn("quantity", col("quantity").cast("int"))
df = df.withColumn("order_date", col("order_date").cast("timestamp"))

# 2. Add a new column for total price
df = df.withColumn("total_price", col("price") * col("quantity"))

# 3. Add a timestamp for when the processing happened
df = df.withColumn("processed_timestamp", current_timestamp())

# Show a sample of the transformed data in the logs
print("Sample of transformed data:")
df.show(5)

# --- Data Sink ---
# Convert back to DynamicFrame before writing
transformed_frame = DynamicFrame.fromDF(df, glueContext, "transformed_frame")

# Write the transformed data to the "processed-data" folder in S3 as Parquet files.
# Parquet is a columnar format which is highly efficient for analytical queries.
# Replace 'your-bucket-name' with the actual S3 bucket name you created.
output_path = "s3://omkar-retail-data-pipeline-12345/processed-data/"
glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    transformation_ctx="datasink",
)

# --- Job Commit ---
job.commit()

