import dags.utils.cleansing_function as cf
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import os

S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")

spark = (
    SparkSession.builder
    .appName("LookUpTable")    
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.connect.extensions.relation.classes","org.apache.spark.sql.connect.delta.DeltaRelationPlugin")
    .config("spark.connect.extensions.command.classes","org.apache.spark.sql.connect.delta.DeltaCommandPlugin")
    .config("spark.sql.parquet.compression.codec", "gzip")  
    .getOrCreate()
)

# 1. Vendor Dimension Table
vendor_data = [
    (1, "Creative Mobile Technologies, LLC"),
    (2, "VeriFone Inc.")
]
vendor_df = spark.createDataFrame(vendor_data, ["vendor_id", "vendor_name"])

# 2. RateCode Dimension Table
rate_code_data = [
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride")
]
rate_code_df = spark.createDataFrame(rate_code_data, ["rate_code_id", "rate_description"])

# 3. Location Dimension Table (assuming PULocationID/DOLocationID maps to Taxi Zones)
lookup_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
spark.sparkContext.addFile(lookup_url)
location_df = spark.read.csv(f'file://{SparkFiles.get("taxi_zone_lookup.csv")}', header=True, inferSchema=True)
location_df = cf.rename_columns_to_snake_case(location_df)

# Paths for saving dimension tables
dim_vendor_path = "s3a://taxi/silver/dim_vendor"
dim_rate_code_path = "s3a://taxi/silver/dim_rate_code"
dim_location_path = "s3a://taxi/silver/dim_location"

# Save dimension tables in Delta format
vendor_df.write.format("delta").mode("overwrite").save(dim_vendor_path)
rate_code_df.write.format("delta").mode("overwrite").save(dim_rate_code_path)
location_df.write.format("delta").mode("overwrite").save(dim_location_path)