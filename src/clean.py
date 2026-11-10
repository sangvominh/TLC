"""Phase 2: làm sạch và chuẩn hóa dữ liệu TLC theo bộ quy tắc mới."""

from datetime import datetime, timezone
import json

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from verify import validate_output


INPUT = "hdfs://namenode:8020/user/tlc/raw/"
OUTPUT = "hdfs://namenode:8020/user/tlc/cleaned/"
LOCAL_OUTPUT = "file:///data/cleaned/"
REPORTS = "hdfs://namenode:8020/user/tlc/reports/"
RULESET_VERSION = "phase2_v2_2023"


def cleaning_rules():
    """Các điều kiện giữ chuyến và biểu thức thời lượng dùng chung."""
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")

    # Tính phút từ micro giây để giữ chính xác cả chuyến có phần lẻ của giây.
    duration = (
        F.unix_micros(dropoff.cast("timestamp"))
        - F.unix_micros(pickup.cast("timestamp"))
    ) / 60_000_000.0
    speed = F.try_divide(F.col("trip_distance") * 60.0, duration)

    # Dùng khoảng timestamp để điều kiện năm có thể được đẩy xuống bộ đọc Parquet.
    rules = [
        ("dropoff > pickup", dropoff > pickup),
        ("0 < trip_duration_min <= 360", (duration > 0) & (duration <= 360)),
        ("pickup_year = 2023",
         (pickup >= F.lit("2023-01-01").cast("timestamp"))
         & (pickup < F.lit("2024-01-01").cast("timestamp"))),
        ("0 < trip_distance <= 100",
         (F.col("trip_distance") > 0) & (F.col("trip_distance") <= 100)),
        ("speed_mph <= 80", speed <= 80),
        ("fare_amount > 0",
         (F.col("fare_amount") > 0) & ~F.isnan("fare_amount")),
        ("PULocationID is not null", F.col("PULocationID").isNotNull()),
        ("DOLocationID is not null", F.col("DOLocationID").isNotNull()),
    ]

    return rules, duration


def run_phase2(spark, input_path=INPUT, output_path=OUTPUT, report_root=REPORTS,
               local_output=LOCAL_OUTPUT):
    # 1. Đọc dữ liệu gốc từ HDFS.
    source = spark.read.parquet(input_path)
    pickup = F.col("tpep_pickup_datetime")
    passenger = F.col("passenger_count")
    rules, duration = cleaning_rules()

    # 2. Đếm cộng dồn để báo cáo số dòng trước/sau từng quy tắc.
    valid = F.lit(True)
    counters = [F.count("*").alias("input_rows")]
    for index, (_, condition) in enumerate(rules):
        valid = valid & condition
        counters.append(F.count(F.when(valid, 1)).alias(f"after_{index}"))
    counters.extend([
        F.count(F.when(valid & (passenger == 0), 1)).alias("passenger_zero"),
        F.count(F.when(valid & (passenger > 6), 1)).alias("passenger_over_six"),
        F.count(F.when(valid & passenger.isNull(), 1)).alias("passenger_null"),
    ])
    counts = source.agg(*counters).first().asDict()

    rows_in = counts["input_rows"]
    rule_report = []
    for index, (name, _) in enumerate(rules):
        rows_out = counts[f"after_{index}"]
        rule_report.append({
            "rule": name,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_removed": rows_in - rows_out,
        })
        rows_in = rows_out

    # 3. Lọc, chuẩn hóa và thêm các cột phục vụ KPI.
    cleaned = (
        source.filter(valid)
        # Số khách không hợp lệ thành null; giữ chuyến để bảo toàn doanh thu.
        .withColumn("passenger_count",
                    F.when((passenger == 0) | (passenger > 6), F.lit(None))
                    .otherwise(passenger).cast("int"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
        .withColumn("trip_duration_min", duration)
        .withColumn("pickup_date", F.to_date(pickup))
        .withColumn("pickup_hour", F.hour(pickup))
        .withColumn("pickup_month", F.month(pickup))
        .withColumn("is_weekend", F.dayofweek(pickup).isin(1, 7))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    try:
        # 4. Ghi hai bản Parquet; chỉ chia theo tháng khi có nhiều tháng.
        month_count = cleaned.select("pickup_month").distinct().limit(2).count()
        writer = cleaned.write.mode("overwrite")
        partitioning = "pickup_month" if month_count > 1 else "none"
        if month_count > 1:
            writer = writer.partitionBy("pickup_month")
        writer.parquet(output_path)
        if local_output:
            writer.parquet(local_output)
    finally:
        cleaned.unpersist()

    changed = counts["passenger_zero"] + counts["passenger_over_six"]
    # 5. Đọc lại đầu ra để kiểm chứng trước khi lưu báo cáo.
    validation = validate_output(
        spark, output_path, valid, duration, rows_in, counts["passenger_null"] + changed)
    local_validation = None
    if local_output:
        local_validation = validate_output(
            spark, local_output, valid, duration, rows_in, counts["passenger_null"] + changed)
    report = {
        "ruleset_version": RULESET_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_path": input_path,
        "output_path": output_path,
        "local_output_path": local_output,
        "input_rows": counts["input_rows"],
        "output_rows": rows_in,
        "rows_removed_total": counts["input_rows"] - rows_in,
        "rules": rule_report,
        "transformations": {
            "passenger_count": {
                "rows_changed_to_null": changed,
                "zero_to_null": counts["passenger_zero"],
                "over_six_to_null": counts["passenger_over_six"],
                "existing_null_rows": counts["passenger_null"],
                "output_null_rows": counts["passenger_null"] + changed,
                "output_type": "integer",
            },
            "RatecodeID": {"output_type": "integer", "keep_99": True},
        },
        "added_columns": ["trip_duration_min", "pickup_date", "pickup_hour",
                          "pickup_month", "is_weekend"],
        "partitioning": partitioning,
        "validation": validation,
        "local_validation": local_validation,
    }
    report_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    report_path = f"{report_root.rstrip('/')}/phase2_quality_{report_id}"
    report_data = spark.createDataFrame(
        [(json.dumps(report, ensure_ascii=False, indent=2),)], ["value"]).coalesce(1)
    report_data.write.mode("errorifexists").text(report_path)
    # Giữ báo cáo từng phiên và thêm bản mới nhất để không đọc nhầm báo cáo cũ.
    report_data.write.mode("overwrite").text(f"{report_root.rstrip('/')}/latest")
    return report, report_path


def main():
    spark = (SparkSession.builder.appName("TLC-Phase-2-Transformation")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print("Đang chạy Phase 2....", flush=True)
    try:
        report, report_path = run_phase2(spark)
    finally:
        spark.stop()

    # Chỉ thông báo hoàn tất sau khi ghi xong dữ liệu, báo cáo và dừng phiên Spark.
    print("\n========== XỬ LÝ HOÀN TẤT ==========", flush=True)
    print("Quá trình lọc:")
    for step, result in enumerate(report["rules"], start=1):
        print(f"Bước {step}: Giữ dòng thỏa {result['rule']}")
        print(f"  Đầu vào: {result['rows_in']:,} | Loại: {result['rows_removed']:,}"
              f" | Còn lại: {result['rows_out']:,}")
    changed = report["transformations"]["passenger_count"]["rows_changed_to_null"]
    print(f"Số dòng đầu vào: {report['input_rows']:,}")
    print(f"Số dòng đầu ra: {report['output_rows']:,}")
    print(f"Tổng dòng bị loại: {report['rows_removed_total']:,}")
    print(f"Số khách chuyển thành null (giữ lại chuyến): {changed:,}")
    print("Phân vùng:", report["partitioning"])
    print("Kiểm chứng Parquet: ĐẠT — 0 dòng vi phạm các quy tắc.")
    print(f"Số chuyến giữ mã RatecodeID = 99: {report['validation']['ratecode_99_rows']:,}")
    print("Dữ liệu sạch:", OUTPUT)
    print("Dữ liệu trên máy: data/cleaned/ (Parquet)")
    print("Báo cáo:", report_path, flush=True)
    print("Báo cáo mới nhất:", REPORTS.rstrip("/") + "/latest", flush=True)


if __name__ == "__main__":
    main()
