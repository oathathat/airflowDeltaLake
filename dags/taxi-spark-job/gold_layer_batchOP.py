from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")

spark = (
    SparkSession.builder
    .appName("GoldLayer")
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
    .config("spark.sql.parquet.compression.codec", "gzip")  # Set Alternative: 'lzo','gzip' for snappy(no glib error) https://medium.com/@ashwin_kumar_/gzip-snappy-and-lzo-compression-formats-in-spark-3b82a566bc7d
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") # https://docs.tecton.ai/docs/beta/tips-and-tricks/troubleshooting/conversion-from-pyspark-dataframe-to-pandas-dataframe-with-pandas-2-0
    .config("delta.autoOptimize.optimizeWrite", "true")
    .getOrCreate()
)

bucket_path = "s3a://taxi"
silver_bucket_path = f"{bucket_path}/silver"
yellow_s_path = f'{silver_bucket_path}/yellow'
green_s_path = f'{silver_bucket_path}/green'

silver_raw_yellow_df = spark.read.format("delta").load(yellow_s_path)
silver_raw_green_df = spark.read.format("delta").load(green_s_path)

daily_summary_df = (silver_raw_yellow_df  
    .groupBy(F.to_date("pickup_datetime").alias("date")) 
        .agg(
            F.count("*").alias("total_trips"),
            F.sum("fare_amount").alias("total_fare_amount"),
            F.avg("trip_distance").alias("average_trip_distance"),
            F.avg("trip_duration").alias("average_trip_duration")
        ).sort("date")
)

# Green and Yellow daily summarize
# Intersect columns in green and yellow
common_col = [_ for _ in silver_raw_yellow_df.columns if _ in silver_raw_green_df.columns]
silver_raw_yellow_df = silver_raw_yellow_df.select(common_col)
silver_raw_green_df = silver_raw_green_df.select(common_col)
combined_df = silver_raw_yellow_df.unionByName(silver_raw_green_df)
service_comparison = (combined_df
        .groupBy(F.to_date("pickup_datetime").alias("date")) 
        .pivot("source") 
        .agg(
            F.avg("fare_amount").alias("avg_fare"),  
            F.min("fare_amount").alias("min_fare"),
            F.max("fare_amount").alias("max_fare"),      
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.min("trip_distance").alias("min_trip_distance"),
            F.max("trip_distance").alias("max_trip_distance"),
            F.count("*").alias("total_trips")
        ).sort("date")
    )
# filer out invalid date(null value row)
service_comparison = service_comparison.filter(service_comparison.date != '2024-01-31')

gold_bucket_path = f"{bucket_path}/gold"
gold_daily_yellow_summary = f"{gold_bucket_path}/daily_yellow_summary"
(daily_summary_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .save(gold_daily_yellow_summary))
spark.sql(f"CREATE TABLE IF NOT EXISTS daily_yellow_summary USING DELTA LOCATION '{gold_daily_yellow_summary}'")
spark.sql(f"OPTIMIZE delta.`{gold_daily_yellow_summary}`")

gold_daily_source_metrics = f"{gold_bucket_path}/daily_source_metrics"
(service_comparison.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .save(gold_daily_source_metrics))
spark.sql(f"CREATE TABLE IF NOT EXISTS gold_daily_source_metrics USING DELTA LOCATION '{gold_daily_source_metrics}'")
spark.sql(f"OPTIMIZE delta.`{gold_daily_source_metrics}`")
spark.stop()
