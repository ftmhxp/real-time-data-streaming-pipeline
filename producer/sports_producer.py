#!/usr/bin/env python3
"""
SportMonks Sports Data Producer for Kafka Pipeline
Polls the REST API and sends live sports data to Kafka topics
"""
import requests
import json
import time
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer
import sys
import os

# Add parent directory to path for config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your API token from config
try:
    from config import SPORTMONKS_API_TOKEN
except ImportError:
    print("[-] Please create config.py with your SportMonks API token!")
    print("   Copy config_example.py to config.py and add your token.")
    exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SportsDataProducer:
    def __init__(self, api_token, kafka_bootstrap_servers='localhost:9092'):
        self.api_token = api_token
        self.base_url = "https://api.sportmonks.com/v3"

        # Kafka producer configuration
        self.kafka_config = {
            'bootstrap.servers': kafka_bootstrap_servers,
            'client.id': 'sports-producer'
        }
        self.producer = Producer(self.kafka_config)

        # Topics for different data types
        self.topics = {
            'fixtures': 'sports.fixtures',
            'livescores': 'sports.livescores',
            'leagues': 'sports.leagues',
            'events': 'sports.events'
        }

        # Track last poll times to avoid duplicate data
        self.last_poll_times = {
            'fixtures': None,
            'livescores': None,
            'leagues': None
        }

    def delivery_report(self, err, msg):
        """Callback for Kafka message delivery"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def send_to_kafka(self, topic, data, key=None):
        """Send data to Kafka topic"""
        try:
            # Convert data to JSON string
            message_value = json.dumps(data)

            # Use key for partitioning (optional)
            message_key = key or str(data.get('id', 'unknown'))

            # Send message
            self.producer.produce(
                topic,
                key=message_key,
                value=message_value,
                callback=self.delivery_report
            )

            # Flush to ensure delivery
            self.producer.flush()

            logger.info(f"Sent to {topic}: {data.get('id', 'unknown')}")

        except Exception as e:
            logger.error(f"Failed to send to Kafka: {e}")

    def fetch_leagues(self):
        """Fetch football leagues data"""
        try:
            logger.info("Fetching leagues data...")
            params = {
                'api_token': self.api_token,
                'per_page': 50  # Get more leagues
            }

            response = requests.get(f"{self.base_url}/football/leagues", params=params)

            if response.status_code == 200:
                data = response.json()
                leagues = data.get('data', [])

                for league in leagues:
                    # Add metadata
                    league['_metadata'] = {
                        'source': 'sportmonks',
                        'type': 'league',
                        'fetched_at': datetime.now().isoformat(),
                        'api_version': 'v3'
                    }

                    self.send_to_kafka(self.topics['leagues'], league, str(league['id']))

                logger.info(f"Fetched {len(leagues)} leagues")
                return leagues

            else:
                logger.error(f"Leagues API failed: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error fetching leagues: {e}")
            return []

    def fetch_fixtures(self, hours_ahead=24):
        """Fetch upcoming fixtures"""
        try:
            logger.info(f"Fetching fixtures for next {hours_ahead} hours...")

            # Calculate date range
            now = datetime.now()
            future_date = now + timedelta(hours=hours_ahead)

            params = {
                'api_token': self.api_token,
                'per_page': 100,
                'includes': 'participants;venue',
                'filter[starting_at_gte]': now.strftime('%Y-%m-%d %H:%M:%S'),
                'filter[starting_at_lte]': future_date.strftime('%Y-%m-%d %H:%M:%S')
            }

            response = requests.get(f"{self.base_url}/football/fixtures", params=params)

            if response.status_code == 200:
                data = response.json()
                fixtures = data.get('data', [])

                for fixture in fixtures:
                    # Skip if we already processed this recently
                    fixture_id = fixture['id']
                    if self._already_processed('fixtures', fixture_id):
                        continue

                    # Add metadata
                    fixture['_metadata'] = {
                        'source': 'sportmonks',
                        'type': 'fixture',
                        'fetched_at': datetime.now().isoformat(),
                        'api_version': 'v3'
                    }

                    self.send_to_kafka(self.topics['fixtures'], fixture, str(fixture_id))

                logger.info(f"Fetched {len(fixtures)} fixtures")
                return fixtures

            else:
                logger.error(f"Fixtures API failed: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error fetching fixtures: {e}")
            return []

    def fetch_live_scores(self):
        """Fetch current live scores"""
        try:
            logger.info("Fetching live scores...")

            params = {
                'api_token': self.api_token,
                'includes': 'participants;scores;events'
            }

            response = requests.get(f"{self.base_url}/football/livescores", params=params)

            if response.status_code == 200:
                data = response.json()
                live_games = data.get('data', [])

                for game in live_games:
                    # Add metadata
                    game['_metadata'] = {
                        'source': 'sportmonks',
                        'type': 'live_score',
                        'fetched_at': datetime.now().isoformat(),
                        'api_version': 'v3'
                    }

                    self.send_to_kafka(self.topics['livescores'], game, str(game['id']))

                    # Also send individual events
                    events = game.get('events', [])
                    for event in events:
                        event['_metadata'] = {
                            'source': 'sportmonks',
                            'type': 'event',
                            'fixture_id': game['id'],
                            'fetched_at': datetime.now().isoformat(),
                            'api_version': 'v3'
                        }
                        self.send_to_kafka(self.topics['events'], event, f"{game['id']}_{event['id']}")

                logger.info(f"Fetched {len(live_games)} live games with {sum(len(g.get('events', [])) for g in live_games)} events")
                return live_games

            else:
                logger.error(f"Live scores API failed: {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error fetching live scores: {e}")
            return []

    def _already_processed(self, data_type, item_id, cache_timeout_minutes=30):
        """Check if we've already processed this item recently"""
        # This is a simple in-memory check
        # In production, you'd use Redis or a database
        cache_key = f"{data_type}_{item_id}"
        now = datetime.now()

        # Simple implementation - just check if we've seen this ID recently
        # You could enhance this with a proper cache
        return False  # For now, always process

    def run_continuous(self, poll_interval_seconds=120):
        """Run continuous polling loop"""
        logger.info("Starting continuous sports data producer...")
        logger.info(f"Poll interval: {poll_interval_seconds} seconds")
        logger.info(f"Kafka bootstrap servers: {self.kafka_config['bootstrap.servers']}")

        # Initial data fetch
        logger.info("Fetching initial data...")
        self.fetch_leagues()
        self.fetch_fixtures()
        self.fetch_live_scores()

        # Continuous polling
        while True:
            try:
                start_time = time.time()

                # Fetch live data (poll more frequently)
                self.fetch_live_scores()

                # Fetch fixtures periodically (less frequent)
                if int(time.time()) % 600 == 0:  # Every 10 minutes
                    self.fetch_fixtures()

                # Calculate sleep time
                elapsed = time.time() - start_time
                sleep_time = max(0, poll_interval_seconds - elapsed)

                logger.info(f"Sleeping for {sleep_time:.1f} seconds...")
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("Shutting down producer...")
                break
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(10)  # Wait before retrying

    def run_once(self):
        """Run a single data fetch cycle"""
        logger.info("Running single data fetch cycle...")

        self.fetch_leagues()
        self.fetch_fixtures()
        self.fetch_live_scores()

        logger.info("Single cycle completed")

def main():
    """Main function"""
    import argparse

    parser = argparse.ArgumentParser(description='SportMonks Sports Data Producer')
    parser.add_argument('--kafka-servers', default='localhost:9092',
                       help='Kafka bootstrap servers (default: localhost:9092)')
    parser.add_argument('--poll-interval', type=int, default=120,
                       help='Poll interval in seconds (default: 120)')
    parser.add_argument('--once', action='store_true',
                       help='Run once instead of continuously')

    args = parser.parse_args()

    # Check API token
    if SPORTMONKS_API_TOKEN == 'your-api-token-here':
        logger.error("Please set your SPORTMONKS_API_TOKEN in config.py!")
        return

    # Create producer
    producer = SportsDataProducer(SPORTMONKS_API_TOKEN, args.kafka_servers)

    if args.once:
        producer.run_once()
    else:
        producer.run_continuous(args.poll_interval)

if __name__ == "__main__":
    main()
