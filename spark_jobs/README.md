# Spark Jobs - Sports Data Processing

This directory contains the Spark jobs for processing sports data through the Bronze/Silver/Gold architecture.

## Architecture Overview

```
Kafka Topics → Bronze Layer → Silver Layer → Gold Layer → Analytics
(sports.fixtures,   (Raw Data)    (Clean Data)   (Aggregated)   (Dashboards)
 sports.livescores,               Stored in S3   Stored in S3   Power BI)
 sports.events)
```

## 📁 Job Structure

### **bronze_job.py** - Raw Data Ingestion
- **Purpose**: Reads from Kafka topics and stores raw JSON data
- **Input**: Kafka streams (`sports.fixtures`, `sports.livescores`, `sports.events`, `sports.leagues`)
- **Output**: Delta tables in S3 Bronze layer
- **Schedule**: Continuous streaming job

### **silver_job.py** - Data Transformation
- **Purpose**: Parses JSON, validates schemas, enriches data
- **Input**: Bronze layer Delta tables
- **Output**: Structured Delta tables in Silver layer
- **Schedule**: Batch job (hourly/daily)

### **gold_job.py** - Analytics & KPIs
- **Purpose**: Creates aggregated metrics for dashboards
- **Input**: Silver layer Delta tables
- **Output**: Analytics-ready Delta tables in Gold layer
- **Schedule**: Batch job (hourly/daily)

## Running the Jobs

### **Prerequisites**
```bash
# Install PySpark and dependencies
pip install pyspark==3.4.1 delta-spark

# Configure AWS credentials (for S3 access)
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
```

### **Run Bronze Layer (Streaming)**
```bash
# Continuous streaming from Kafka
python bronze_job.py
```

### **Run Silver Layer (Batch)**
```bash
# Transform raw data
python silver_job.py
```

### **Run Gold Layer (Batch)**
```bash
# Create analytics
python gold_job.py
```

## 🔧 Configuration

### **S3 Paths**
Update these paths in each job file:
```python
self.bronze_s3_path = "s3a://your-bucket/bronze/sports/"
self.silver_s3_path = "s3a://your-bucket/silver/sports/"
self.gold_s3_path = "s3a://your-bucket/gold/sports/"
```

### **Kafka Settings**
```python
kafka_bootstrap_servers = "localhost:9092"
```

## Data Flow

### **Bronze Layer**
- Raw JSON from Kafka
- Basic validation
- Partitioned by date
- Minimal transformation

### **Silver Layer**
- JSON parsing with schemas
- Data type validation
- Computed fields (scores, timestamps)
- Data quality checks

### **Gold Layer**
- Aggregated metrics
- Team performance stats
- League standings
- Live dashboard data
- Event analytics

## Development Setup

### **Local Testing**
```bash
# Start Kafka first
docker-compose up -d

# Run producer to generate test data
python ../producer/sports_producer.py --once

# Run Bronze job (will read from Kafka)
python bronze_job.py

# Check results in Bronze layer
```

### **Debugging**
```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Run with limited data
python silver_job.py  # Process recent data only
```

## Monitoring

### **Job Metrics**
- **Bronze**: Messages processed/sec, lag per topic
- **Silver**: Records transformed, data quality scores
- **Gold**: Analytics generated, query performance

### **Data Quality**
- Schema validation errors
- Null value percentages
- Duplicate record detection
- Timestamp consistency

## Scheduling

### **Recommended Schedule**
- **Bronze**: Always running (streaming)
- **Silver**: Every 15-30 minutes
- **Gold**: Every hour

### **Orchestration**
Use Apache Airflow for scheduling:
```python
# Airflow DAG example
bronze_task = PythonOperator(
    task_id='bronze_job',
    python_callable=run_bronze_job,
    dag=dag
)
```

## Troubleshooting

### **Common Issues**

**1. Kafka Connection Failed**
```bash
# Check if Kafka is running
docker-compose ps

# Check Kafka logs
docker-compose logs kafka1
```

**2. S3 Access Denied**
```bash
# Verify AWS credentials
aws s3 ls s3://your-bucket/

# Check IAM permissions for S3 access
```

**3. Delta Lake Errors**
```bash
# Install Delta Spark
pip install delta-spark

# Add to Spark session
spark = configure_spark_with_delta(spark)
```

**4. Memory Issues**
```bash
# Increase Spark memory
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=8g
```

## Key Concepts

### **Delta Lake**
- ACID transactions
- Schema enforcement
- Time travel
- Optimized performance

### **Streaming vs Batch**
- **Streaming**: Real-time data ingestion (Bronze)
- **Batch**: Periodic transformations (Silver/Gold)

### **Partitioning Strategy**
- **Bronze**: By ingestion date
- **Silver/Gold**: By event date + computed fields

## Next Steps

1. **Test locally** with sample data
2. **Configure S3** credentials and paths
3. **Set up Airflow** for scheduling
4. **Add monitoring** and alerting
5. **Performance tuning** for production scale
