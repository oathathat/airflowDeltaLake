import dags.utils.cleansing_function as cf
from pyspark.sql import SparkSession
import os

S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")

spark = (
    SparkSession.builder
    .appName("SilverLayer_green")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.connect.extensions.relation.classes","org.apache.spark.sql.connect.delta.DeltaRelationPlugin")
    .config("spark.connect.extensions.command.classes","org.apache.spark.sql.connect.delta.DeltaCommandPlugin")
    .config("spark.sql.parquet.compression.codec", "gzip")  # Alternative: 'lzo' or 'uncompressed'
    .getOrCreate()
)
bucket_path = "s3a://taxi"
bronze_bucket_path = f"{bucket_path}/bronze"
green_b_path = f'{bronze_bucket_path}/green'
bronze_raw_green_df = spark.read.format('delta').load(green_b_path)

valid_ranges = {
    "VendorID": (1, 2),
    "RatecodeID": (1, 6),
    "payment_type": (1, 5),    
}
# 1 .drop unnecessary columns
bronze_green_df = cf.drop_columns(bronze_raw_green_df, ["ehail_fee"])
# 2. remove duplicate
bronze_green_df = bronze_green_df.distinct()
# 3. filter valid range
bronze_green_df = cf.filter_valid_range(bronze_green_df, valid_ranges)
bronze_green_df = cf.filter_time_in_range(bronze_green_df,"lpep_pickup_datetime")
# 4. filter out negative value
bronze_green_df = cf.filter_positive(bronze_green_df, ["passenger_count","trip_distance"])
# 5. filter outlier
bronze_green_df = cf.filter_iqr_outliers(bronze_green_df, ["trip_distance"])
# 6. fill null and correct value
bronze_green_df = cf.fill_na(bronze_green_df, ["store_and_fwd_flag"], "N")
bronze_green_df = cf.adjust_payment_type(bronze_green_df)
bronze_green_df = cf.correct_negative_values(bronze_green_df, [
    "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", 
    "total_amount", "congestion_surcharge", "Airport_fee"
])
# 7. Standardize schema
bronze_green_df = cf.rename_columns_to_snake_case(bronze_green_df)
cleaned_green_df = cf.change_timestamp_format(bronze_green_df, "lpep_pickup_datetime", "lpep_dropoff_datetime")

# 8. Data Enrichment
enriched_green_df = cf.add_trip_duration(cleaned_green_df)
enriched_green_df = cf.add_fare_per_mile(enriched_green_df)

silver_bucket_path = f"{bucket_path}/silver"
green_s_path = f'{silver_bucket_path}/green'

(
    enriched_green_df
    .write
    .format('delta')
    .mode('append')
    .partitionBy("ingest_date")
    .save(green_s_path)
)