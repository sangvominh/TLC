# TLC Big Data Project

## 1. Overview

Big Data infrastructure using Docker Compose, Hadoop HDFS and Apache Spark.

**Architecture:**
- Hadoop NameNode
- Hadoop DataNode
- Spark Master
- Spark Worker

## 2. Requirements

- Docker Desktop
- Docker Compose

## 3. Start & Stop

Start infrastructure:

```powershell
docker compose up -d
```

Check containers:

```powershell
docker compose ps
```

Stop infrastructure:

```powershell
docker compose down
```

## 4. Data Ingestion

Upload a Parquet file to HDFS:

```powershell
docker exec namenode bash /scripts/upload_to_hdfs.sh `
  /local-data/raw/yellow_tripdata_2023-01.parquet `
  /user/tlc/raw
```

Check HDFS files:

```powershell
docker exec namenode hdfs dfs -ls /user/tlc/raw
```

## 5. Spark Test

Run Spark test:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/work-dir/test_spark.py
```

The test reads Parquet data from HDFS and displays its schema, sample records and row count.

## 6. Web UI

| Service | URL |
|---|---|
| Hadoop NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |

**HDFS path:** `/user/tlc/raw/`

**Note:** Start the infrastructure before running ingestion or Spark tests.