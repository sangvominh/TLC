# TLC

Yêu cầu: Docker Desktop đang chạy. Đặt file Parquet trong `data/raw/`.

## Chạy

```powershell
docker compose up -d
docker compose ps
```

## Nạp dữ liệu lên HDFS

Thay tên file bên dưới bằng file trong `data/raw/`:

```powershell
docker exec namenode bash /scripts/upload_to_hdfs.sh /local-data/raw/yellow_tripdata_2023-01.parquet /user/tlc/raw
```

## Kiểm tra Spark

```powershell
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/test_spark.py
```

## Chạy Phase 2

Sau khi nạp dữ liệu lên HDFS:

```powershell
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/phase2_clean.py
```

Kiểm tra dữ liệu đã làm sạch và báo cáo:

```powershell
docker exec namenode hdfs dfs -ls /user/tlc/cleaned
docker exec namenode hdfs dfs -ls /user/tlc/reports
```

## Kiểm tra và dừng

```powershell
docker exec namenode hdfs dfs -ls /user/tlc/raw
docker exec namenode hdfs dfs -ls /user/tlc/cleaned
docker compose down
```

Web UI: NameNode http://localhost:9870 · Spark http://localhost:8080

## Bash

```bash
docker compose up -d
docker exec namenode bash /scripts/upload_to_hdfs.sh /local-data/raw/yellow_tripdata_2023-01.parquet /user/tlc/raw
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/phase2_clean.py
docker exec namenode hdfs dfs -ls /user/tlc/cleaned
docker exec namenode hdfs dfs -ls /user/tlc/reports
docker compose down
```
