from pyspark.sql import SparkSession
import os

class SparkSessionManager:
    def __init__(self, session_type: str = 'bronze'):
        self.session_type = session_type
        self.S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
        self.S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
        self.S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
        self.spark = None

    def create_spark_session(self) -> SparkSession:
        common_configs = [
            ("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0"),
            ("spark.hadoop.fs.s3a.endpoint", self.S3_ENDPOINT),
            ("spark.hadoop.fs.s3a.access.key", self.S3_ACCESS_KEY),
            ("spark.hadoop.fs.s3a.secret.key", self.S3_SECRET_KEY),
            ("spark.hadoop.fs.s3a.path.style.access", "true"),
            ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
            ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            ("spark.connect.extensions.relation.classes", "org.apache.spark.sql.connect.delta.DeltaRelationPlugin"),
            ("spark.connect.extensions.command.classes", "org.apache.spark.sql.connect.delta.DeltaCommandPlugin"),
            ("spark.sql.parquet.compression.codec", "gzip"),
            ("spark.sql.execution.arrow.pyspark.enabled", "true"),         
        ]
        
        # Layer-specific configurations
        app_name = f"{self.session_type.capitalize()}Layer"
        if self.session_type == 'gold':
            common_configs.append(("delta.autoOptimize.optimizeWrite", "true"))

        # Build the session spark://jupyter:7077 , local
        builder = SparkSession.builder.appName(app_name).master("local[*]")
        for key, value in common_configs:
            builder = builder.config(key, value)

        self.spark = builder.getOrCreate()
        return self.spark

    def get_session(self) -> SparkSession:       
        return self.create_spark_session()