#!/usr/bin/env python3
"""
Silver Layer Spark Job - Sports Data Transformation & Enrichment
Reads from Bronze S3 layer, transforms data, and stores in Silver layer
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

class SilverSportsProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.bronze_s3_path = "s3a://your-bucket/bronze/sports/"
        self.silver_s3_path = "s3a://your-bucket/silver/sports/"

    def read_bronze_data(self, data_type, days_back=7):
        """Read data from Bronze layer"""
        try:
            bronze_path = f"{self.bronze_s3_path}{data_type}"

            # Read Delta format from Bronze
            df = (self.spark.read
                  .format("delta")
                  .load(bronze_path)
                  .filter(col("bronze_ingested_at") >= date_sub(current_date(), days_back)))

            logger.info(f"Read {df.count()} records from Bronze {data_type}")
            return df

        except Exception as e:
            logger.error(f"Error reading Bronze data for {data_type}: {e}")
            return None

    def transform_fixtures(self):
        """Transform fixtures data for Silver layer"""
        try:
            logger.info("Transforming fixtures data for Silver layer")

            df = self.read_bronze_data("fixtures")
            if df is None:
                return None

            # Parse JSON data
            transformed_df = df.withColumn("parsed_data", from_json(col("raw_json"), self.get_fixtures_schema()))

            # Extract key fields
            silver_df = transformed_df.select(
                col("parsed_data.id").alias("fixture_id"),
                col("parsed_data.sport_id"),
                col("parsed_data.league_id"),
                col("parsed_data.season_id"),
                col("parsed_data.stage_id"),
                col("parsed_data.state_id"),
                col("parsed_data.name").alias("fixture_name"),
                col("parsed_data.starting_at"),
                col("parsed_data.result_info"),
                col("parsed_data.venue_id"),
                col("parsed_data.length"),
                col("parsed_data.placeholder"),
                col("parsed_data.has_odds"),
                # Extract participant info
                col("parsed_data.participants")[0]["name"].alias("home_team"),
                col("parsed_data.participants")[0]["id"].alias("home_team_id"),
                col("parsed_data.participants")[1]["name"].alias("away_team"),
                col("parsed_data.participants")[1]["id"].alias("away_team_id"),
                # Metadata
                col("bronze_ingested_at"),
                col("silver_processed_at").alias(current_timestamp()),
                col("data_quality_score").alias(lit(100)),  # High quality for fixtures
                col("year"),
                col("month"),
                col("day")
            )

            # Add computed fields
            silver_df = silver_df.withColumn(
                "match_date",
                to_date(col("starting_at"))
            ).withColumn(
                "match_hour",
                hour(col("starting_at"))
            ).withColumn(
                "is_live",
                when(col("state_id") == 2, True).otherwise(False)
            ).withColumn(
                "is_finished",
                when(col("state_id") == 3, True).otherwise(False)
            )

            logger.info(f"Transformed {silver_df.count()} fixtures for Silver layer")
            return silver_df

        except Exception as e:
            logger.error(f"Error transforming fixtures: {e}")
            return None

    def transform_live_scores(self):
        """Transform live scores data for Silver layer"""
        try:
            logger.info("Transforming live scores data for Silver layer")

            df = self.read_bronze_data("livescores")
            if df is None:
                return None

            # Parse JSON data
            transformed_df = df.withColumn("parsed_data", from_json(col("raw_json"), self.get_live_scores_schema()))

            # Extract key fields
            silver_df = transformed_df.select(
                col("parsed_data.id").alias("fixture_id"),
                col("parsed_data.sport_id"),
                col("parsed_data.league_id"),
                col("parsed_data.season_id"),
                col("parsed_data.name").alias("fixture_name"),
                col("parsed_data.starting_at"),
                col("parsed_data.state_id"),
                # Extract scores
                col("parsed_data.scores")[0]["score"]["goals"].alias("home_score"),
                col("parsed_data.scores")[1]["score"]["goals"].alias("away_score"),
                # Team info
                col("parsed_data.participants")[0]["name"].alias("home_team"),
                col("parsed_data.participants")[0]["id"].alias("home_team_id"),
                col("parsed_data.participants")[1]["name"].alias("away_team"),
                col("parsed_data.participants")[1]["id"].alias("away_team_id"),
                # Events count
                size(col("parsed_data.events")).alias("events_count"),
                # Metadata
                col("bronze_ingested_at"),
                col("silver_processed_at").alias(current_timestamp()),
                col("data_quality_score").alias(lit(95)),  # High quality for live data
                col("year"),
                col("month"),
                col("day")
            )

            # Add computed fields
            silver_df = silver_df.withColumn(
                "match_date",
                to_date(col("starting_at"))
            ).withColumn(
                "total_goals",
                col("home_score") + col("away_score")
            ).withColumn(
                "score_difference",
                col("home_score") - col("away_score")
            ).withColumn(
                "is_live",
                when(col("state_id") == 2, True).otherwise(False)
            ).withColumn(
                "is_finished",
                when(col("state_id") == 3, True).otherwise(False)
            )

            logger.info(f"Transformed {silver_df.count()} live scores for Silver layer")
            return silver_df

        except Exception as e:
            logger.error(f"Error transforming live scores: {e}")
            return None

    def transform_events(self):
        """Transform events data for Silver layer"""
        try:
            logger.info("Transforming events data for Silver layer")

            df = self.read_bronze_data("events")
            if df is None:
                return None

            # Parse JSON data
            transformed_df = df.withColumn("parsed_data", from_json(col("raw_json"), self.get_events_schema()))

            # Extract key fields
            silver_df = transformed_df.select(
                col("parsed_data.id").alias("event_id"),
                col("parsed_data.fixture_id"),
                col("parsed_data.type_id"),
                col("parsed_data.section"),
                col("parsed_data.minute"),
                col("parsed_data.extra_minute"),
                col("parsed_data.result"),
                col("parsed_data.player_id"),
                col("parsed_data.player_name"),
                col("parsed_data.related_player_id"),
                col("parsed_data.related_player_name"),
                col("parsed_data.created_at"),
                # Metadata
                col("bronze_ingested_at"),
                col("silver_processed_at").alias(current_timestamp()),
                col("data_quality_score").alias(lit(90)),  # Good quality for events
                col("year"),
                col("month"),
                col("day")
            )

            # Add computed fields and event type mapping
            event_types = {
                1: "GOAL",
                2: "OWN_GOAL",
                3: "PENALTY",
                4: "RED_CARD",
                5: "YELLOW_CARD",
                6: "SUBSTITUTION",
                7: "SUBSTITUTION_OFF",
                8: "SUBSTITUTION_ON"
            }

            silver_df = silver_df.withColumn(
                "event_type",
                when(col("type_id") == 1, "GOAL")
                .when(col("type_id") == 2, "OWN_GOAL")
                .when(col("type_id") == 3, "PENALTY")
                .when(col("type_id") == 4, "RED_CARD")
                .when(col("type_id") == 5, "YELLOW_CARD")
                .when(col("type_id") == 6, "SUBSTITUTION")
                .otherwise("OTHER")
            ).withColumn(
                "event_date",
                to_date(col("created_at"))
            ).withColumn(
                "total_minutes",
                col("minute") + coalesce(col("extra_minute"), lit(0))
            )

            logger.info(f"Transformed {silver_df.count()} events for Silver layer")
            return silver_df

        except Exception as e:
            logger.error(f"Error transforming events: {e}")
            return None

    def write_to_silver(self, df, table_name):
        """Write transformed data to Silver S3 layer"""
        try:
            output_path = f"{self.silver_s3_path}{table_name}"

            logger.info(f"Writing transformed data to Silver layer: {output_path}")

            # Write as Delta format
            (df.write
             .format("delta")
             .mode("overwrite")
             .partitionBy("year", "month", "day")
             .save(output_path))

            logger.info(f"Successfully wrote {df.count()} records to Silver {table_name}")

        except Exception as e:
            logger.error(f"Error writing to Silver layer: {e}")
            raise

    def get_fixtures_schema(self):
        """Get schema for fixtures data"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("sport_id", IntegerType(), True),
            StructField("league_id", IntegerType(), True),
            StructField("season_id", IntegerType(), True),
            StructField("stage_id", IntegerType(), True),
            StructField("state_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("starting_at", StringType(), True),
            StructField("result_info", StringType(), True),
            StructField("venue_id", IntegerType(), True),
            StructField("length", IntegerType(), True),
            StructField("placeholder", BooleanType(), True),
            StructField("has_odds", BooleanType(), True),
            StructField("participants", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("_metadata", MapType(StringType(), StringType()), True)
        ])

    def get_live_scores_schema(self):
        """Get schema for live scores data"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("sport_id", IntegerType(), True),
            StructField("league_id", IntegerType(), True),
            StructField("season_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("starting_at", StringType(), True),
            StructField("state_id", IntegerType(), True),
            StructField("participants", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True)
            ])), True),
            StructField("scores", ArrayType(StructType([
                StructField("score", StructType([
                    StructField("goals", IntegerType(), True)
                ]), True)
            ])), True),
            StructField("events", ArrayType(MapType(StringType(), StringType())), True),
            StructField("_metadata", MapType(StringType(), StringType()), True)
        ])

    def get_events_schema(self):
        """Get schema for events data"""
        return StructType([
            StructField("id", IntegerType(), True),
            StructField("fixture_id", IntegerType(), True),
            StructField("type_id", IntegerType(), True),
            StructField("section", StringType(), True),
            StructField("minute", IntegerType(), True),
            StructField("extra_minute", IntegerType(), True),
            StructField("result", StringType(), True),
            StructField("player_id", IntegerType(), True),
            StructField("player_name", StringType(), True),
            StructField("related_player_id", IntegerType(), True),
            StructField("related_player_name", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("_metadata", MapType(StringType(), StringType()), True)
        ])

    def run_batch_processing(self):
        """Run batch processing for all Silver layer transformations"""
        logger.info("Starting Silver layer batch processing")

        try:
            # Process fixtures
            fixtures_df = self.transform_fixtures()
            if fixtures_df is not None:
                self.write_to_silver(fixtures_df, "fixtures")

            # Process live scores
            livescores_df = self.transform_live_scores()
            if livescores_df is not None:
                self.write_to_silver(livescores_df, "live_scores")

            # Process events
            events_df = self.transform_events()
            if events_df is not None:
                self.write_to_silver(events_df, "events")

            logger.info("Silver layer batch processing completed successfully")

        except Exception as e:
            logger.error(f"Error in Silver layer processing: {e}")
            raise

def main():
    """Main function"""
    logger.info("Starting Silver Layer Sports Data Processor")

    # Create Spark session
    spark = create_spark_session("SilverSportsProcessor")

    try:
        # Create processor
        processor = SilverSportsProcessor(spark)

        # Run batch processing
        processor.run_batch_processing()

    except Exception as e:
        logger.error(f"Error in Silver processor: {e}")
        spark.stop()
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
