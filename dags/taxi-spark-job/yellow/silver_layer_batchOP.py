import dags.utils.cleansing_function as cf
from pyspark.sql import SparkSession
import os

S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")

spark = (
    SparkSession.builder
    .appName("SilverLayer_yellow")
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
yellow_b_path = f'{bronze_bucket_path}/yellow'
bronze_raw_yellow_df = spark.read.format('delta').load(yellow_b_path)

valid_ranges = {
    "VendorID": (1, 2),
    "RatecodeID": (1, 6),
    "payment_type": (1, 5),    
}
bronze_yellow_df = bronze_raw_yellow_df.distinct()

bronze_yellow_df = cf.filter_valid_range(bronze_yellow_df,valid_ranges)
bronze_yellow_df = cf.filter_time_in_range(bronze_yellow_df)

bronze_yellow_df = cf.filter_positive(bronze_yellow_df,["passenger_count","trip_distance"])

bronze_yellow_df = cf.filter_iqr_outliers(bronze_yellow_df,["trip_distance"])

bronze_yellow_df = cf.fill_na(bronze_yellow_df,["store_and_fwd_flag"],"N")
bronze_yellow_df = cf.adjust_payment_type(bronze_yellow_df)
bronze_yellow_df = cf.correct_negative_values(bronze_yellow_df,[
        "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", 
        "total_amount", "congestion_surcharge", "Airport_fee"
    ])

bronze_yellow_df = cf.rename_columns_to_snake_case(bronze_yellow_df)
cleaned_yellow_df = cf.change_timestamp_format(bronze_yellow_df)

enriched_yellow_df = cf.add_trip_duration(cleaned_yellow_df)
enriched_yellow_df = cf.add_fare_per_mile(enriched_yellow_df)

silver_bucket_path = f"{bucket_path}/silver"
yellow_s_path = f'{silver_bucket_path}/yellow'

(
    enriched_yellow_df
    .write
    .format('delta')
    .mode('append')
    .partitionBy("ingest_date")
    .save(yellow_s_path)
)