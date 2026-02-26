"""
Centralized Spark session factory with Delta Lake + MinIO configuration.
All Spark jobs should use get_spark_session() to ensure consistent config.
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str,
    enable_delta: bool = True,
    enable_kafka: bool = False,
    driver_memory: str = "2g",
    executor_memory: str = "2g",
) -> SparkSession:
    """
    Create a configured SparkSession for the fraud platform.

    Includes:
    - Delta Lake extensions + catalog
    - MinIO (S3A) configuration
    - Optimized Delta write settings
    - Adaptive query execution
    """
    minio_endpoint   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key       = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret_key       = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    spark_master     = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        # --------------- MinIO / S3A -------------------
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100 MB
        # --------------- Adaptive Query Execution ------
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # --------------- Shuffle & Performance ----------
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.default.parallelism", "50")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )

    if enable_delta:
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            # Delta optimizations
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        )

    if enable_kafka:
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
