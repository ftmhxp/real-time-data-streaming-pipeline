#!/usr/bin/env python3
"""
Test script for SportMonks REST API connection
"""
import requests
import json
import time
from datetime import datetime

# Import your API token from config
try:
    from config import SPORTMONKS_API_TOKEN
except ImportError:
    print("[-] Please create config.py with your SportMonks API token!")
    print("   Copy config_example.py to config.py and add your token.")
    exit(1)

class SportMonksTester:
    def __init__(self, api_token):
        self.api_token = api_token
        self.base_url = "https://api.sportmonks.com/v3"

    def test_api_connection(self):
        """Test SportMonks REST API connection with comprehensive endpoints"""
        print("[+] Testing REST API connection...")

        # Test 1: Get leagues
        if not self._test_leagues():
            return False

        # Test 2: Get fixtures
        if not self._test_fixtures():
            return False

        # Test 3: Get live scores
        if not self._test_live_scores():
            return False

        print("[+] All API tests passed!")
        return True

    def _test_leagues(self):
        """Test leagues endpoint"""
        try:
            print("  [*] Testing leagues endpoint...")
            params = {'api_token': self.api_token, 'per_page': 3}
            response = requests.get(f"{self.base_url}/football/leagues", params=params)

            if response.status_code == 200:
                data = response.json()
                leagues = data.get('data', [])
                print(f"  [+] Found {len(leagues)} leagues")
                for league in leagues[:2]:
                    print(f"     - {league.get('name', 'Unknown')} (ID: {league.get('id', 'N/A')})")
                return True
            else:
                print(f"  [-] Leagues test failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"  [-] Leagues test error: {e}")
            return False

    def _test_fixtures(self):
        """Test fixtures endpoint"""
        try:
            print("  [*] Testing fixtures endpoint...")
            params = {'api_token': self.api_token, 'per_page': 3}
            response = requests.get(f"{self.base_url}/football/fixtures", params=params)

            if response.status_code == 200:
                data = response.json()
                fixtures = data.get('data', [])
                print(f"  [+] Found {len(fixtures)} recent fixtures")
                if fixtures:
                    fixture = fixtures[0]
                    participants = fixture.get('participants', [])
                    if len(participants) >= 2:
                        home = participants[0].get('name', 'Unknown')
                        away = participants[1].get('name', 'Unknown')
                        print(f"     - Latest: {home} vs {away}")
                    else:
                        print(f"     - Fixture ID {fixture.get('id', 'unknown')}: {fixture.get('name', 'Unknown')}")
                else:
                    print("     - No fixtures in recent data (this is normal)")
                return True
            else:
                print(f"  [-] Fixtures test failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"  [-] Fixtures test error: {e}")
            return False

    def _test_live_scores(self):
        """Test live scores endpoint"""
        try:
            print("  [*] Testing live scores endpoint...")
            params = {'api_token': self.api_token}
            response = requests.get(f"{self.base_url}/football/livescores", params=params)

            if response.status_code == 200:
                data = response.json()
                live_games = data.get('data', [])
                print(f"  [+] Found {len(live_games)} live games")
                if live_games:
                    game = live_games[0]
                    home = game.get('participants', [{}])[0].get('name', 'Unknown')
                    away = game.get('participants', [{}])[1].get('name', 'Unknown')
                    score = f"{game.get('scores', [{}])[0].get('score', {}).get('goals', 0)}-{game.get('scores', [{}])[1].get('score', {}).get('goals', 0)}"
                    print(f"     - Live: {home} {score} {away}")
                else:
                    print("     - No live games currently")
                return True
            else:
                print(f"  [-] Live scores test failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"  [-] Live scores test error: {e}")
            return False

def main():
    """Main test function"""
    print(">>> SportMonks REST API Tester")
    print("=" * 40)

    # Check if API token is set
    if SPORTMONKS_API_TOKEN == 'your-api-token-here':
        print("[!] Please set your SPORTMONKS_API_TOKEN in config.py!")
        print("   Get your token from: https://my.sportmonks.com/")
        return

    tester = SportMonksTester(SPORTMONKS_API_TOKEN)

    # Test REST API
    api_success = tester.test_api_connection()

    print("\n" + "=" * 40)
    print("SUMMARY:")
    print(f"REST API: {'[+] Working' if api_success else '[-] Failed'}")

    if api_success:
        print("\n[*] Perfect! Your SportMonks API is ready for the pipeline.")
        print("[*] You can now build producers to fetch live sports data.")
        print("[*] Use polling every 1-2 minutes for 'real-time' updates.")
    else:
        print("\n[!] Check your API token and account status.")

if __name__ == "__main__":
    main()
