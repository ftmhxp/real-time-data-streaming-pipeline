#!/usr/bin/env python3
"""
Bronze Layer Spark Job - Raw Sports Data Ingestion
Reads from Kafka topics and stores raw data in S3 Bronze layer
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from spark_session import create_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeSportsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.kafka_bootstrap_servers = "localhost:9092"
        self.checkpoint_location = "/tmp/spark-checkpoints/bronze"
        self.bronze_s3_path = "s3a://your-bucket/bronze/sports/"

        # Kafka topics to read from
        self.topics = {
            'fixtures': 'sports.fixtures',
            'livescores': 'sports.livescores',
            'leagues': 'sports.leagues',
            'events': 'sports.events'
        }

    def read_from_kafka(self, topic_name, topic_value):
        """Read streaming data from Kafka topic"""
        try:
            logger.info(f"Reading from Kafka topic: {topic_value}")

            # Read from Kafka
            kafka_df = (self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                .option("subscribe", topic_value)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load())

            # Parse JSON value from Kafka
            parsed_df = kafka_df.select(
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("raw_json"),
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp")
            )

            # Add metadata
            enriched_df = parsed_df.withColumn(
                "bronze_ingested_at",
                current_timestamp()
            ).withColumn(
                "data_type",
                lit(topic_name)
            ).withColumn(
                "bronze_version",
                lit("1.0")
            )

            logger.info(f"Successfully created stream for topic: {topic_value}")
            return enriched_df

        except Exception as e:
            logger.error(f"Error reading from Kafka topic {topic_value}: {e}")
            return None

    def write_to_bronze(self, df, data_type):
        """Write streaming data to Bronze S3 layer"""
        try:
            output_path = f"{self.bronze_s3_path}{data_type}/"

            logger.info(f"Writing {data_type} data to Bronze layer: {output_path}")

            # Write as Delta format for Bronze layer
            query = (df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", f"{self.checkpoint_location}/{data_type}")
                .option("path", output_path)
                .partitionBy("year", "month", "day")  # Partition by date
                .start())

            logger.info(f"Started streaming query for {data_type}")
            return query

        except Exception as e:
            logger.error(f"Error writing {data_type} to Bronze layer: {e}")
            return None

    def process_fixtures(self):
        """Process fixtures data"""
        df = self.read_from_kafka("fixtures", self.topics['fixtures'])
        if df is not None:
            # Add date partitions for fixtures
            processed_df = df.withColumn("year", year(current_timestamp())) \
                            .withColumn("month", month(current_timestamp())) \
                            .withColumn("day", dayofmonth(current_timestamp()))
            return self.write_to_bronze(processed_df, "fixtures")
        return None

    def process_live_scores(self):
        """Process live scores data"""
        df = self.read_from_kafka("livescores", self.topics['livescores'])
        if df is not None:
            # Add date partitions for live scores
            processed_df = df.withColumn("year", year(current_timestamp())) \
                            .withColumn("month", month(current_timestamp())) \
                            .withColumn("day", dayofmonth(current_timestamp()))
            return self.write_to_bronze(processed_df, "livescores")
        return None

    def process_leagues(self):
        """Process leagues data"""
        df = self.read_from_kafka("leagues", self.topics['leagues'])
        if df is not None:
            # Add date partitions for leagues
            processed_df = df.withColumn("year", year(current_timestamp())) \
                            .withColumn("month", month(current_timestamp())) \
                            .withColumn("day", dayofmonth(current_timestamp()))
            return self.write_to_bronze(processed_df, "leagues")
        return None

    def process_events(self):
        """Process events data"""
        df = self.read_from_kafka("events", self.topics['events'])
        if df is not None:
            # Add date partitions for events
            processed_df = df.withColumn("year", year(current_timestamp())) \
                            .withColumn("month", month(current_timestamp())) \
                            .withColumn("day", dayofmonth(current_timestamp()))
            return self.write_to_bronze(processed_df, "events")
        return None

    def run_all_streams(self):
        """Run all streaming queries"""
        logger.info("Starting Bronze layer processing for all sports data streams")

        queries = []

        # Start all streaming queries
        fixtures_query = self.process_fixtures()
        if fixtures_query:
            queries.append(("fixtures", fixtures_query))

        livescores_query = self.process_live_scores()
        if livescores_query:
            queries.append(("livescores", livescores_query))

        leagues_query = self.process_leagues()
        if leagues_query:
            queries.append(("leagues", leagues_query))

        events_query = self.process_events()
        if events_query:
            queries.append(("events", events_query))

        if not queries:
            logger.error("No streaming queries were successfully started")
            return

        logger.info(f"Successfully started {len(queries)} streaming queries: {[name for name, _ in queries]}")

        # Keep the streams running
        try:
            # Wait for all queries to finish (they run indefinitely)
            for name, query in queries:
                logger.info(f"Waiting for {name} query to finish...")
                query.awaitTermination()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping all queries...")
            for name, query in queries:
                try:
                    query.stop()
                    logger.info(f"Stopped {name} query")
                except Exception as e:
                    logger.error(f"Error stopping {name} query: {e}")

def main():
    """Main function"""
    logger.info("Starting Bronze Layer Sports Data Processor")

    # Create Spark session
    spark = create_spark_session("BronzeSportsProcessor")

    try:
        # Create processor
        processor = BronzeSportsProcessor(spark)

        # Run all streaming jobs
        processor.run_all_streams()

    except Exception as e:
        logger.error(f"Error in Bronze processor: {e}")
        spark.stop()
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
