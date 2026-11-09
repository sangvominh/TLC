from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TLC-Clean").getOrCreate()

input_path = "hdfs://namenode:8020/user/tlc/raw/yellow_tripdata_2023-01.parquet"
output_path = "hdfs://namenode:8020/user/tlc/cleaned/yellow_tripdata_2023-01"

raw = spark.read.parquet(input_path)  #tạo DataFrame raw từ Parquet.

print("Schema dữ liệu thô:")
raw.printSchema()

# LỌC THỜI GIAN
with_duration = raw.withColumn( # Tạo DataFrame mới có thêm cột thời lượng tính bằng phút. 
    "trip_duration_min",
    (
        F.unix_timestamp(F.col("tpep_dropoff_datetime"))
        - F.unix_timestamp(F.col("tpep_pickup_datetime"))
    ) /60
)
#unix_timestamp(...) đổi thời điểm sang số giây; lấy thời điểm trả trừ thời điểm đón rồi chia 60 để được phút.

time_clean = with_duration.filter(
    (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    & (F.col("trip_duration_min") > 0)
    & (F.col("trip_duration_min") <= 360)
    & (F.year(F.col("tpep_pickup_datetime")) == 2023)
)
# filter (...) giữ lại các dòng thỏa điều kiện. Dấu & là phép AND giữa các điều kiện Spark

# LỌC QUÃNG ĐƯỜNG VÀ TỐC ĐỘ
distance_clean = time_clean.filter(
    (F.col("trip_distance") > 0)
    & (F.col("trip_distance") <= 100)
    & (F.col("fare_amount") > 0)
)

with_speed = distance_clean.withColumn( 
    "trip_speed_mph",
    F.col("trip_distance") / (F.col("trip_duration_min") /60)
)
# thêm tốc độ trung bình theo đơn vị dặm/giờ.

speed_clean = with_speed.filter(F.col("trip_speed_mph") <= 80)
#  loại chuyến có tốc độ trên 80 mph.

clean = (
    speed_clean.withColumn ( # cập nhật cột số khách.
        "passenger_count",
        F.when(
            (F.col("passenger_count") == 0) | (F.col("passenger_count") > 6),
            F.lit(None)
        ).otherwise(F.col("passenger_count")).cast("int")
    )
    .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
    .filter(
        F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
    )
)

clean = (
    clean
    .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
    .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
    .withColumn("pickup_month", F.month(F.col("tpep_pickup_datetime")))
    .withColumn(
        "is_weekend",
        F.dayofweek(F.col("tpep_pickup_datetime")).isin(1, 7)
    )
    .drop("trip_speed_mph")
)
print("Schema sau khi làm sạch: ")
clean.printSchema()

print("Một số dòng sau khi làm sạch: ")
clean.show(5, truncate=False)

clean.write.mode("errorifexists").parquet(output_path)

print("Đã ghi dữ liệu sạch tới: ", output_path)

spark.stop()
