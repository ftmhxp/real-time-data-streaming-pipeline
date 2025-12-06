#!/usr/bin/env python3
"""
Gold Layer Spark Job - Sports Analytics & KPIs
Creates aggregated analytics from Silver layer for dashboards and reporting
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
import sys
import os

# Add utils to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from spark_session import create_spark_session

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoldSportsAnalytics:
    def __init__(self, spark):
        self.spark = spark
        self.silver_s3_path = "s3a://your-bucket/silver/sports/"
        self.gold_s3_path = "s3a://your-bucket/gold/sports/"

    def read_silver_data(self, table_name, days_back=30):
        """Read data from Silver layer"""
        try:
            silver_path = f"{self.silver_s3_path}{table_name}"

            df = (self.spark.read
                  .format("delta")
                  .load(silver_path)
                  .filter(col("silver_processed_at") >= date_sub(current_date(), days_back)))

            logger.info(f"Read {df.count()} records from Silver {table_name}")
            return df

        except Exception as e:
            logger.error(f"Error reading Silver data for {table_name}: {e}")
            return None

    def create_match_results_analytics(self):
        """Create match results analytics"""
        try:
            logger.info("Creating match results analytics")

            # Read live scores data
            scores_df = self.read_silver_data("live_scores")
            if scores_df is None:
                return None

            # Filter for completed matches only
            completed_matches = scores_df.filter(col("is_finished") == True)

            # Create match results
            results_df = completed_matches.select(
                col("fixture_id"),
                col("fixture_name"),
                col("match_date"),
                col("home_team"),
                col("away_team"),
                col("home_score"),
                col("away_score"),
                col("total_goals"),
                col("league_id"),
                col("season_id")
            ).withColumn(
                "result",
                when(col("home_score") > col("away_score"), "HOME_WIN")
                .when(col("home_score") < col("away_score"), "AWAY_WIN")
                .otherwise("DRAW")
            ).withColumn(
                "goal_difference",
                abs(col("home_score") - col("away_score"))
            ).withColumn(
                "processed_at",
                current_timestamp()
            )

            logger.info(f"Created {results_df.count()} match results")
            return results_df

        except Exception as e:
            logger.error(f"Error creating match results analytics: {e}")
            return None

    def create_team_performance_analytics(self):
        """Create team performance analytics"""
        try:
            logger.info("Creating team performance analytics")

            results_df = self.create_match_results_analytics()
            if results_df is None:
                return None

            # Home team performance
            home_performance = results_df.select(
                col("home_team").alias("team"),
                col("league_id"),
                col("season_id"),
                when(col("result") == "HOME_WIN", 1).otherwise(0).alias("wins"),
                when(col("result") == "DRAW", 1).otherwise(0).alias("draws"),
                when(col("result") == "AWAY_WIN", 1).otherwise(0).alias("losses"),
                col("home_score").alias("goals_for"),
                col("away_score").alias("goals_against"),
                lit("home").alias("venue")
            )

            # Away team performance
            away_performance = results_df.select(
                col("away_team").alias("team"),
                col("league_id"),
                col("season_id"),
                when(col("result") == "AWAY_WIN", 1).otherwise(0).alias("wins"),
                when(col("result") == "DRAW", 1).otherwise(0).alias("draws"),
                when(col("result") == "HOME_WIN", 1).otherwise(0).alias("losses"),
                col("away_score").alias("goals_for"),
                col("home_score").alias("goals_against"),
                lit("away").alias("venue")
            )

            # Combine home and away performance
            team_performance = home_performance.union(away_performance)

            # Aggregate by team
            team_stats = team_performance.groupBy("team", "league_id", "season_id").agg(
                sum("wins").alias("total_wins"),
                sum("draws").alias("total_draws"),
                sum("losses").alias("total_losses"),
                sum("goals_for").alias("total_goals_for"),
                sum("goals_against").alias("total_goals_against"),
                count("*").alias("matches_played"),
                (sum("wins") * 3 + sum("draws")).alias("points")
            ).withColumn(
                "goal_difference",
                col("total_goals_for") - col("total_goals_against")
            ).withColumn(
                "win_percentage",
                (col("total_wins") / col("matches_played") * 100).cast("decimal(5,2)")
            ).withColumn(
                "processed_at",
                current_timestamp()
            )

            logger.info(f"Created performance analytics for {team_stats.count()} teams")
            return team_stats

        except Exception as e:
            logger.error(f"Error creating team performance analytics: {e}")
            return None

    def create_event_analytics(self):
        """Create event analytics (goals, cards, etc.)"""
        try:
            logger.info("Creating event analytics")

            events_df = self.read_silver_data("events")
            if events_df is None:
                return None

            # Event type distribution
            event_summary = events_df.groupBy("fixture_id", "event_type").agg(
                count("*").alias("event_count"),
                collect_list("player_name").alias("players_involved"),
                min("total_minutes").alias("first_occurrence"),
                max("total_minutes").alias("last_occurrence")
            )

            # Goals analytics
            goals_df = events_df.filter(col("event_type").isin(["GOAL", "OWN_GOAL", "PENALTY"]))

            goal_analytics = goals_df.select(
                col("fixture_id"),
                col("player_name"),
                col("event_type"),
                col("total_minutes"),
                col("result")
            ).withColumn(
                "scorer_type",
                when(col("event_type") == "OWN_GOAL", "OWN_GOAL")
                .when(col("event_type") == "PENALTY", "PENALTY")
                .otherwise("REGULAR")
            )

            # Card analytics
            cards_df = events_df.filter(col("event_type").isin(["YELLOW_CARD", "RED_CARD"]))

            card_analytics = cards_df.select(
                col("fixture_id"),
                col("player_name"),
                col("event_type"),
                col("total_minutes")
            )

            # Combine all event analytics
            event_analytics = event_summary.withColumn("processed_at", current_timestamp())

            logger.info(f"Created event analytics for {event_analytics.count()} fixture-event combinations")
            return event_analytics

        except Exception as e:
            logger.error(f"Error creating event analytics: {e}")
            return None

    def create_live_match_dashboard(self):
        """Create live match dashboard data"""
        try:
            logger.info("Creating live match dashboard data")

            scores_df = self.read_silver_data("live_scores")
            if scores_df is None:
                return None

            # Filter for live matches only
            live_matches = scores_df.filter(col("is_live") == True)

            # Create dashboard-ready format
            dashboard_df = live_matches.select(
                col("fixture_id"),
                col("fixture_name"),
                col("home_team"),
                col("away_team"),
                col("home_score"),
                col("away_score"),
                col("total_goals"),
                col("events_count"),
                col("league_id"),
                col("starting_at"),
                col("silver_processed_at").alias("last_updated")
            ).withColumn(
                "match_status",
                lit("LIVE")
            ).withColumn(
                "time_elapsed",
                # Calculate approximate time elapsed (this would need more complex logic in production)
                lit("45+")  # Placeholder - would calculate based on start time
            ).withColumn(
                "processed_at",
                current_timestamp()
            )

            logger.info(f"Created live dashboard data for {dashboard_df.count()} matches")
            return dashboard_df

        except Exception as e:
            logger.error(f"Error creating live match dashboard: {e}")
            return None

    def create_league_standings(self):
        """Create current league standings"""
        try:
            logger.info("Creating league standings")

            team_stats = self.create_team_performance_analytics()
            if team_stats is None:
                return None

            # Create window for ranking
            window_spec = Window.partitionBy("league_id", "season_id").orderBy(
                col("points").desc(),
                col("goal_difference").desc(),
                col("total_goals_for").desc()
            )

            standings = team_stats.withColumn(
                "position",
                rank().over(window_spec)
            ).select(
                col("league_id"),
                col("season_id"),
                col("position"),
                col("team"),
                col("matches_played"),
                col("total_wins"),
                col("total_draws"),
                col("total_losses"),
                col("total_goals_for"),
                col("total_goals_against"),
                col("goal_difference"),
                col("points"),
                col("win_percentage"),
                col("processed_at")
            ).orderBy("league_id", "season_id", "position")

            logger.info(f"Created standings for {standings.count()} team positions")
            return standings

        except Exception as e:
            logger.error(f"Error creating league standings: {e}")
            return None

    def write_to_gold(self, df, table_name):
        """Write analytics data to Gold S3 layer"""
        try:
            output_path = f"{self.gold_s3_path}{table_name}"

            logger.info(f"Writing analytics to Gold layer: {output_path}")

            # Write as Delta format for Gold layer
            (df.write
             .format("delta")
             .mode("overwrite")
             .partitionBy("year", "month", "day")
             .save(output_path))

            logger.info(f"Successfully wrote {df.count()} records to Gold {table_name}")

        except Exception as e:
            logger.error(f"Error writing to Gold layer: {e}")
            raise

    def run_gold_analytics(self):
        """Run all Gold layer analytics"""
        logger.info("Starting Gold layer analytics processing")

        try:
            # Create and save match results
            match_results = self.create_match_results_analytics()
            if match_results is not None:
                results_with_date = match_results.withColumn("year", year(col("processed_at"))) \
                                                .withColumn("month", month(col("processed_at"))) \
                                                .withColumn("day", dayofmonth(col("processed_at")))
                self.write_to_gold(results_with_date, "match_results")

            # Create and save team performance
            team_performance = self.create_team_performance_analytics()
            if team_performance is not None:
                performance_with_date = team_performance.withColumn("year", year(col("processed_at"))) \
                                                       .withColumn("month", month(col("processed_at"))) \
                                                       .withColumn("day", dayofmonth(col("processed_at")))
                self.write_to_gold(performance_with_date, "team_performance")

            # Create and save event analytics
            event_analytics = self.create_event_analytics()
            if event_analytics is not None:
                events_with_date = event_analytics.withColumn("year", year(col("processed_at"))) \
                                                 .withColumn("month", month(col("processed_at"))) \
                                                 .withColumn("day", dayofmonth(col("processed_at")))
                self.write_to_gold(events_with_date, "event_analytics")

            # Create and save live dashboard data
            live_dashboard = self.create_live_match_dashboard()
            if live_dashboard is not None:
                dashboard_with_date = live_dashboard.withColumn("year", year(col("processed_at"))) \
                                                   .withColumn("month", month(col("processed_at"))) \
                                                   .withColumn("day", dayofmonth(col("processed_at")))
                self.write_to_gold(dashboard_with_date, "live_dashboard")

            # Create and save league standings
            standings = self.create_league_standings()
            if standings is not None:
                standings_with_date = standings.withColumn("year", year(col("processed_at"))) \
                                             .withColumn("month", month(col("processed_at"))) \
                                             .withColumn("day", dayofmonth(col("processed_at")))
                self.write_to_gold(standings_with_date, "league_standings")

            logger.info("Gold layer analytics processing completed successfully")

        except Exception as e:
            logger.error(f"Error in Gold layer processing: {e}")
            raise

def main():
    """Main function"""
    logger.info("Starting Gold Layer Sports Analytics Processor")

    # Create Spark session
    spark = create_spark_session("GoldSportsAnalytics")

    try:
        # Create analytics processor
        analytics = GoldSportsAnalytics(spark)

        # Run all analytics
        analytics.run_gold_analytics()

    except Exception as e:
        logger.error(f"Error in Gold analytics processor: {e}")
        spark.stop()
        sys.exit(1)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
