#!/usr/bin/env python3
"""
Simple Kafka connectivity test
Tests basic connection to Kafka brokers without full client libraries
"""
import socket
import json
import time

def test_kafka_connectivity():
    """Test basic connectivity to Kafka brokers"""
    print(">>> Testing Kafka Connectivity")
    print("=" * 40)

    # Kafka broker addresses from docker-compose
    brokers = [
        ('localhost', 9092),  # kafka1
        ('localhost', 9094),  # kafka2
        ('localhost', 9096),  # kafka3
    ]

    connected_brokers = []

    for host, port in brokers:
        try:
            print(f"[*] Testing connection to {host}:{port}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                print(f"[+] Successfully connected to {host}:{port}")
                connected_brokers.append((host, port))
            else:
                print(f"[-] Failed to connect to {host}:{port}")

        except Exception as e:
            print(f"[-] Error connecting to {host}:{port}: {e}")

    if connected_brokers:
        print(f"\n[+] Kafka connectivity test PASSED!")
        print(f"[+] Connected to {len(connected_brokers)} broker(s)")
        return True
    else:
        print(f"\n[-] Kafka connectivity test FAILED!")
        print("[-] Could not connect to any brokers")
        return False

def test_api_connectivity():
    """Test SportMonks API connectivity"""
    print("\n>>> Testing SportMonks API Connectivity")
    print("=" * 40)

    try:
        # Import API token
        from config import SPORTMONKS_API_TOKEN
        if SPORTMONKS_API_TOKEN == 'your-api-token-here':
            print("[-] Please set your real API token in config.py!")
            return False

        import requests

        print("[*] Testing API connection...")
        params = {'api_token': SPORTMONKS_API_TOKEN, 'per_page': 3}
        response = requests.get("https://api.sportmonks.com/v3/football/leagues", params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            leagues = data.get('data', [])
            print(f"[+] API connection successful! Found {len(leagues)} leagues")
            return True
        else:
            print(f"[-] API request failed: {response.status_code}")
            return False

    except ImportError:
        print("[-] Please create config.py with your SportMonks API token!")
        return False
    except Exception as e:
        print(f"[-] API test failed: {e}")
        return False

def main():
    """Run all connectivity tests"""
    print(">>> Connectivity Test Suite")
    print("Testing Kafka brokers and SportMonks API")
    print()

    kafka_ok = test_kafka_connectivity()
    api_ok = test_api_connectivity()

    print("\n" + "=" * 50)
    print("FINAL RESULTS:")
    print(f"Kafka Brokers: {'[+] PASS' if kafka_ok else '[-] FAIL'}")
    print(f"SportMonks API: {'[+] PASS' if api_ok else '[-] FAIL'}")

    if kafka_ok and api_ok:
        print("\n[*] ALL SYSTEMS GO!")
        print("Your pipeline infrastructure is ready!")
        print("\nNext steps:")
        print("1. Run: python producer/sports_producer.py --once")
        print("2. Check Kafka UI: http://localhost:8080")
        print("3. Verify data in topics: sports.fixtures, sports.livescores, etc.")
    else:
        print("\n[!] Some systems need attention:")
        if not kafka_ok:
            print("  - Start Kafka: docker-compose up -d")
        if not api_ok:
            print("  - Check API token in config.py")

if __name__ == "__main__":
    main()
