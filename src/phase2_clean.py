from datetime import datetime, timezone
import json

from pyspark.sql import SparkSession, functions as F


INPUT = "hdfs://namenode:8020/user/tlc/raw/"
OUTPUT = "hdfs://namenode:8020/user/tlc/cleaned/"
REPORTS = "hdfs://namenode:8020/user/tlc/reports/"

spark = SparkSession.builder.appName("TLC-Phase-2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("Dang chay Phase 2...", flush=True)

df = spark.read.parquet(INPUT).cache()
input_rows = df.count()

# Giữ lại giá trị null; chỉ loại các dòng vi phạm ba quy tắc đã nêu.
rules = [
    ("fare_amount < 0", F.col("fare_amount").isNull() | (F.col("fare_amount") >= 0)),
    ("trip_distance == 0", F.col("trip_distance").isNull() | (F.col("trip_distance") != 0)),
    ("passenger_count == 0", F.col("passenger_count").isNull() | (F.col("passenger_count") != 0)),
]

rule_report = []
for rule, keep_rows in rules:
    rows_in = df.count()
    df = df.filter(keep_rows)
    rows_out = df.count()
    rule_report.append({
        "rule": rule,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "rows_removed": rows_in - rows_out,
    })

pickup = F.col("tpep_pickup_datetime")
dropoff = F.col("tpep_dropoff_datetime")
df = (
    df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60.0,
    )
    .withColumn("pickup_hour", F.hour(pickup))
    .withColumn("pickup_date", F.to_date(pickup))
    .withColumn("is_weekend", F.dayofweek(pickup).isin(1, 7))
)

# Chia phân vùng theo tháng nếu có từ hai tháng khác nhau; một tháng thì không chia.
month_count = (
    df.select(F.date_format("pickup_date", "yyyy-MM").alias("month"))
    .where(F.col("month").isNotNull())
    .distinct()
    .limit(2)
    .count()
)

if month_count > 1:
    df = df.withColumn("pickup_year_month", F.date_format("pickup_date", "yyyy-MM"))
    df.write.mode("overwrite").partitionBy("pickup_year_month").parquet(OUTPUT)
    partitioning = "pickup_year_month"
else:
    df.write.mode("overwrite").parquet(OUTPUT)
    partitioning = "none"

output_rows = rule_report[-1]["rows_out"]
report = {
    "input_path": INPUT,
    "output_path": OUTPUT,
    "input_rows": input_rows,
    "output_rows": output_rows,
    "rows_removed_total": input_rows - output_rows,
    "rules": rule_report,
    "partitioning": partitioning,
}

report_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
report_path = f"{REPORTS.rstrip('/')}/phase2_quality_{report_id}"
spark.sparkContext.parallelize([json.dumps(report, indent=2)], 1).saveAsTextFile(report_path)

spark.stop()

# Chỉ thông báo hoàn tất sau khi đã ghi dữ liệu sạch, báo cáo và kết thúc phiên Spark.
print("\n========== XỬ LÝ HOÀN TẤT ==========", flush=True)
print("\nQua trinh xu ly:")
for step, result in enumerate(rule_report, start=1):
    print(f"Buoc {step}: Loai dong co {result['rule']}")
    print(f"  So dong vao : {result['rows_in']:,}")
    print(f"  Da loai     : {result['rows_removed']:,}")
    print(f"  Con lai     : {result['rows_out']:,}")
print(f"So dong dau vao : {input_rows:,}")
print(f"So dong dau ra  : {output_rows:,}")
print(f"Da xu ly     : {input_rows - output_rows:,}")
print()
print("Du lieu sach:", OUTPUT)
# print("Bao cao     :", report_path)
print("=====================================", flush=True)
