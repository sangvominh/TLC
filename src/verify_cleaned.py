from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TLC-Verify-Cleaned").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
raw = spark.read.parquet(
    "hdfs://namenode:8020/user/tlc/raw/yellow_tripdata_2023-01.parquet"
)

cleaned = spark.read.parquet(
    "hdfs://namenode:8020/user/tlc/processed/cleaned/2023-01"
)

# 1. Số dòng thực sự được lưu
print("\n=== ROW COUNTS ===")
print("Raw:", raw.count())
print("Cleaned:", cleaned.count())

# 2. Các bản ghi trong output
print("\n=== CLEANED DATA SAMPLE ===")
cleaned.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
    "trip_distance",
    "passenger_count",
    "trip_duration",
    "pickup_hour"
).show(10, truncate=False)

# 3. Kiểm tra các điều kiện đã áp dụng
invalid = cleaned.filter(
    F.col("fare_amount").isNull()
    | (F.col("fare_amount") < 0)
    | F.col("trip_distance").isNull()
    | (F.col("trip_distance") <= 0)
    | F.col("passenger_count").isNull()
    | (F.col("passenger_count") <= 0)
    | F.col("trip_duration").isNull()
    | (F.col("trip_duration") <= 0)
    | F.col("pickup_hour").isNull()
    | (~F.col("pickup_hour").between(0, 23))
)

print("\n=== VALIDATION ===")
print("Invalid rows in output:", invalid.count())

spark.stop()