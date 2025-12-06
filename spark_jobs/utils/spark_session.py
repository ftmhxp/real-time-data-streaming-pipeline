#!/usr/bin/env python3
"""
Spark Session Configuration for Sports Data Pipeline
"""
from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

def create_spark_session(app_name="SportsDataPipeline"):
    """
    Create and configure Spark session for the sports data pipeline

    Args:
        app_name (str): Name for the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    try:
        logger.info(f"Creating Spark session for: {app_name}")

        # Create base session
        spark = (SparkSession.builder
                 .appName(app_name)
                 .config("spark.sql.adaptive.enabled", "true")
                 .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                 .config("spark.sql.adaptive.skewJoin.enabled", "true")
                 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                 .config("spark.kryo.registrationRequired", "false")
                 .getOrCreate())

        # Configure for S3 access (if using AWS)
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "your-access-key")
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "your-secret-key")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Configure for Delta Lake (if using)
        spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider",
                                           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        logger.info("Spark session created successfully")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Master: {spark.sparkContext.master}")

        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def create_streaming_spark_session(app_name="SportsDataStreaming"):
    """
    Create Spark session optimized for streaming operations

    Args:
        app_name (str): Name for the Spark streaming application

    Returns:
        SparkSession: Configured Spark session for streaming
    """
    try:
        logger.info(f"Creating streaming Spark session for: {app_name}")

        spark = (SparkSession.builder
                 .appName(app_name)
                 .config("spark.streaming.backpressure.enabled", "true")
                 .config("spark.streaming.kafka.maxRatePerPartition", "1000")
                 .config("spark.streaming.receiver.maxRate", "1000")
                 .config("spark.streaming.kafka.consumer.cache.enabled", "false")
                 .config("spark.sql.shuffle.partitions", "50")
                 .config("spark.sql.streaming.schemaInference", "true")
                 .config("spark.sql.adaptive.enabled", "true")
                 .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                 .getOrCreate())

        # Configure S3 for checkpointing
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "your-access-key")
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "your-secret-key")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

        logger.info("Streaming Spark session created successfully")
        return spark

    except Exception as e:
        logger.error(f"Failed to create streaming Spark session: {e}")
        raise

def get_spark_config():
    """
    Get common Spark configuration settings

    Returns:
        dict: Configuration dictionary
    """
    return {
        # Memory settings
        "spark.driver.memory": "2g",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "2",

        # SQL optimizations
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",

        # Streaming optimizations
        "spark.streaming.backpressure.enabled": "true",
        "spark.streaming.kafka.maxRatePerPartition": "1000",

        # Delta Lake settings (if using)
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",

        # Logging
        "spark.sql.adaptive.logLevel": "WARN"
    }
