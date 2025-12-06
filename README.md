# Real-Time Video Big Data Analytics (VBDA) Pipeline

## Project Overview
This project implements a **real-time sports data pipeline** for processing **live football data**.  
The pipeline captures live scores, fixtures, match events, and league information, transforms the data, and stores it for analytics and visualization.

**Pipeline Flow:**
SportMonks API → Kafka → Spark Structured Streaming → S3 (Bronze/Silver/Gold) → Athena → Power BI


---

## Objectives
- Build a scalable **real-time sports data pipeline**
- Implement **stream processing** for live football matches
- Store processed data in **cloud storage** (S3 / BigQuery / Snowflake)
- Enable **live sports analytics dashboards** using Power BI
- Automate recurring batch tasks with Airflow (optional)

---

## Technologies Used
| Component | Technology |
|-----------|------------|
| Data Source | SportMonks Football API |
| Data Ingestion | Apache Kafka |
| Stream Processing | PySpark / Spark Structured Streaming |
| Storage | AWS S3 (Bronze/Silver/Gold) |
| Orchestration | Apache Airflow |
| Analytics | Athena + Power BI |
| Programming | Python |
| Version Control | Git |

---

## 📂 Repository Structure
real-time-data-streaming-pipeline/
├── producer/ # Sports data ingestion from SportMonks API
│   ├── sports_producer.py # Main producer script
│   ├── schemas/ # JSON schemas for data validation
│   └── README.md # Producer documentation
├── spark_jobs/ # Streaming and batch Spark jobs
├── infrastructure/ # Kafka, Spark, Airflow configs
├── docs/ # Paper notes, architecture diagrams
├── dashboards/ # Power BI files and screenshots
├── docker-compose.yml # Local environment setup
├── requirements.txt # Python dependencies
├── test_websocket.py # API testing script (now REST-only)
├── test_producer.py # Producer testing script
├── config.py # API token configuration
└── README.md


---

## Pipeline Phases
1. **Data Ingestion**:
   Poll SportMonks API for live scores, fixtures, and events → send to Kafka topics.
2. **Stream Processing**:
   Spark Structured Streaming reads Kafka topics → transforms sports data → writes to S3 Bronze.
3. **Data Cleaning & Aggregation**:
   Bronze → Silver → Gold tables for sports analytics and KPIs.
4. **Analytics Dashboard**:
   Power BI reads from Athena/S3 → builds live sports visualizations.
5. **Automation (Optional)**:
   Airflow orchestrates batch jobs, monitoring, and notifications.

---

## Metrics & Performance
- End-to-end latency (SportMonks API → Dashboard)
- Kafka throughput (sports events/sec)
- Spark throughput (matches/sec processed)
- Live score freshness (seconds since last update)
- Match coverage (% of active games tracked)
- API quota usage (requests remaining)
- Failure rate (% messages → DLQ)  

---

## Setup Instructions
1. Clone the repository:
   ```bash
   git clone <your-repo-url>
   cd real-time-data-streaming-pipeline
2. Install dependencies:
pip install -r requirements.txt
3. Configure API token:
   - Copy `config_example.py` to `config.py`
   - Add your SportMonks API token from https://my.sportmonks.com/
   - **Note:** `config.py` is gitignored to keep your token secure
4. Start local services with Docker:
docker-compose up -d
5. Test the setup:
   ```bash
   # Test API connection
   python test_websocket.py

   # Test producer with Kafka
   python test_producer.py
6. Run continuous producer:
python producer/sports_producer.py

References

- A Survey on Video Big Data Analytics: Architecture, Challenges and - Applications (MDPI, 2025)
- Apache Kafka Documentation
- PySpark Structured Streaming Guide
- AWS S3 & Athena Docs
- Power BI Official Documentation

Notes

- This project is designed to run locally and scale to the cloud later.
- Airflow is optional in the first iteration; it will handle automation and orchestration in later phases.
=======
# real-time-data-streaming-pipeline
A real-time data engineering pipeline for processing, analyzing, and visualizing big data using Kafka, Spark Structured Streaming, S3, Airflow, and Power BI