
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("TLC-Test")
    .getOrCreate()
)

print("Spark version:", spark.version)

hdfs_path = (
    "hdfs://namenode:8020/"
    "user/tlc/raw/yellow_tripdata_2023-01.parquet"
)

# Thử đọc dữ liệu gốc để kiểm tra kết nối Spark/HDFS.
df = spark.read.parquet(hdfs_path)

print("\n=== SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE DATA ===")
df.show(5, truncate=False)

print("\n=== ROW COUNT ===")
print(df.count())

spark.stop()
