from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from pyspark import SparkConf, SparkContext #,SparkFiles
import os

S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_ENDPOINT = os.environ.get("MINIO_URL")

# Get SparkContext from SparkSubmitOperator (initial new session with config may cause error)
spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

# Load source data
# data_path = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'  
# spark.sparkContext.addFile(data_path)
# yellow_df = spark.read.parquet(f"file://{SparkFiles.get("yellow_tripdata_2024-01.parquet")}") #For standalone cluster need a copy for every machine in cluster(master,worker) and local(spark driver) If not exist It will cause file path not found error
yellow_df = spark.read.parquet('/data/yellow_tripdata_2024-01.parquet') # read from mock path


# Transform data
yellow_df = (
    yellow_df.withColumn('ingest_date', current_date())
             .withColumn('source', lit('yellow'))
)

# Write to bronze layer with Delta format
yellow_df.write.format('delta') \
    .mode('append') \
    .option('mergeSchema', 'true') \
    .partitionBy('ingest_date') \
    .save("s3a://taxi/bronze/yellow")

# Stop Spark session
spark.stop()
