from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = (
    "hdfs://namenode:8020/user/tlc/raw/"
    "yellow_tripdata_2023-01.parquet"
)

OUTPUT = (
    "hdfs://namenode:8020/user/tlc/processed/"
    "cleaned/2023-01"
)

spark = (
    SparkSession.builder
    .appName("TLC-Data-Cleaning")
    .getOrCreate()
)

# 1. Read
df = spark.read.parquet(INPUT)

# 2. Transform
cleaned = (
    df
    .withColumn(
        "trip_duration",
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime")
    )
    .withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    )
    .filter(
        (F.col("fare_amount") >= 0)
        & (F.col("trip_distance") > 0)
        & (F.col("passenger_count") > 0)
        & (F.col("trip_duration") > 0)
    )
)

# 3. Validate
raw_count = df.count()
clean_count = cleaned.count()

print(f"Raw rows:     {raw_count}")
print(f"Clean rows:   {clean_count}")
print(f"Removed rows: {raw_count - clean_count}")

# 4. Write
cleaned.write.mode("overwrite").parquet(OUTPUT)

print(f"Saved to: {OUTPUT}")

spark.stop()