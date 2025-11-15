# Real-Time Video Big Data Analytics (VBDA) Pipeline

## Project Overview
This project implements a **real-time data pipeline** for processing **TEDx YouTube videos**.  
The pipeline captures video metadata, statistics, and transcripts, transforms the data, and stores it for analytics and visualization.

**Pipeline Flow:**
YouTube → Kafka → Spark Structured Streaming → S3 (Bronze/Silver/Gold) → Athena → Power BI


---

## Objectives
- Build a scalable **real-time data ingestion pipeline**  
- Implement **stream processing** using PySpark  
- Store processed data in **cloud storage** (S3 / BigQuery / Snowflake)  
- Enable **analytics dashboards** using Power BI  
- Automate recurring batch tasks with Airflow (optional)

---

## Technologies Used
| Component | Technology |
|-----------|------------|
| Data Ingestion | Apache Kafka |
| Stream Processing | PySpark / Spark Structured Streaming |
| Storage | AWS S3 (Bronze/Silver/Gold) |
| Orchestration | Apache Airflow |
| Analytics | Athena + Power BI |
| Programming | Python |
| Version Control | Git |

---

## 📂 Repository Structure
vbda-pipeline/
├── producer/ # Data ingestion scripts
├── spark_jobs/ # Streaming and batch Spark jobs
├── infrastructure/ # Kafka, Spark, Airflow configs
├── docs/ # Paper notes, architecture diagrams
├── dashboards/ # Power BI files and screenshots
├── docker-compose.yml # Local environment setup
├── requirements.txt # Python dependencies
└── README.md


---

## Pipeline Phases
1. **Data Ingestion**:  
   Fetch video metadata, stats, and transcripts from YouTube → send to Kafka topics.
2. **Stream Processing**:  
   Spark Structured Streaming reads Kafka topics → transforms data → writes to S3 Bronze.
3. **Data Cleaning & Aggregation**:  
   Bronze → Silver → Gold tables for analytics.
4. **Analytics Dashboard**:  
   Power BI reads from Athena/S3 → builds visualizations.
5. **Automation (Optional)**:  
   Airflow orchestrates batch jobs, monitoring, and notifications.

---

## Metrics & Performance
- End-to-end latency (Producer → Dashboard)  
- Kafka throughput (messages/sec)  
- Spark throughput (records/sec processed)  
- S3 growth & small file optimization  
- Failure rate (% messages → DLQ)  

---

## Setup Instructions
1. Clone the repository:  
   ```bash
   git clone <your-repo-url>
   cd vbda-pipeline
2. Install dependencies:
pip install -r requirements.txt
3. Start local services with Docker:
docker-compose up -d
4. Run Producer → Spark jobs → Power BI dashboards.

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