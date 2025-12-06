# Sports Data Producer

This producer polls the SportMonks REST API and sends live sports data to Kafka topics for your real-time streaming pipeline.

## Features

- **Multi-topic publishing**: Sends data to separate Kafka topics by type
- **Intelligent polling**: Different intervals for different data types
- **Live scores**: Real-time match updates and events
- **Fixture data**: Upcoming matches with team information
- **League data**: Competition information and standings
- **Event streaming**: Individual match events (goals, cards, subs)

## Kafka Topics

| Topic | Description | Data Type |
|-------|-------------|-----------|
| `sports.fixtures` | Upcoming and recent matches | Fixture objects |
| `sports.livescores` | Currently live matches | Live score objects |
| `sports.leagues` | Football leagues and competitions | League objects |
| `sports.events` | Individual match events | Event objects |

## Usage

### Single Test Run
```bash
python test_producer.py
```

### Continuous Production
```bash
# Basic usage (polls every 2 minutes)
python producer/sports_producer.py

# Custom Kafka servers
python producer/sports_producer.py --kafka-servers localhost:9092

# Custom poll interval (30 seconds)
python producer/sports_producer.py --poll-interval 30

# Single run (for testing)
python producer/sports_producer.py --once
```

### Command Line Options

- `--kafka-servers`: Kafka bootstrap servers (default: `localhost:9092`)
- `--poll-interval`: Seconds between polls (default: 120)
- `--once`: Run single cycle instead of continuous

## Data Flow

```
SportMonks API → Producer → Kafka Topics → Spark Streaming → S3 → Analytics
```

### Polling Strategy

- **Live Scores**: Every 2 minutes (high priority)
- **Fixtures**: Every 10 minutes (moderate priority)
- **Leagues**: On startup only (static data)

## Data Schemas

JSON schemas are defined in `producer/schemas/`:

- `fixtures_schema.json` - Match fixture data
- `livescores_schema.json` - Live match data
- `events_schema.json` - Individual events

## API Rate Limits

- **Free Tier**: 500 requests/month
- **Current Usage**: ~60 requests/hour (continuous polling)
- **Capacity**: ~200+ hours of continuous operation

## Configuration

Set your SportMonks API token in `config.py`:

```python
SPORTMONKS_API_TOKEN = "your-token-here"
```

## Monitoring

The producer logs all activity:
- API requests and responses
- Kafka message deliveries
- Errors and retry attempts
- Data volume statistics

## Error Handling

- Automatic retry on API failures
- Kafka connection recovery
- Graceful shutdown on interrupts
- Detailed error logging

## Next Steps

1. **Test locally**: Run with `--once` to verify
2. **Scale up**: Adjust poll intervals based on needs
3. **Monitor**: Check logs and Kafka topic consumption
4. **Extend**: Add more data types (odds, statistics, etc.)
