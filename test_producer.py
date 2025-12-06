#!/usr/bin/env python3
"""
Test script for the Sports Data Producer
Runs a single cycle to verify everything works
"""
import sys
import os

# Add producer directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'producer'))

from producer.sports_producer import SportsDataProducer

def main():
    """Test the sports data producer"""
    print(">>> Testing Sports Data Producer")
    print("=" * 50)

    # Import API token
    try:
        from config import SPORTMONKS_API_TOKEN
    except ImportError:
        print("❌ Please create config.py with your SportMonks API token!")
        return

    if SPORTMONKS_API_TOKEN == 'your-api-token-here':
        print("❌ Please set your real API token in config.py!")
        return

    # Create producer (will connect to local Kafka)
    print("🔌 Connecting to Kafka and SportMonks API...")
    producer = SportsDataProducer(SPORTMONKS_API_TOKEN)

    try:
        # Run single cycle
        print("📊 Fetching sports data...")
        producer.run_once()

        print("✅ Producer test completed successfully!")
        print("\n📋 Check your Kafka topics for data:")
        print("   - sports.fixtures")
        print("   - sports.livescores")
        print("   - sports.leagues")
        print("   - sports.events")

    except Exception as e:
        print(f"❌ Producer test failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Make sure Kafka is running: docker-compose up -d")
        print("2. Check your API token is valid")
        print("3. Verify network connectivity")

if __name__ == "__main__":
    main()
