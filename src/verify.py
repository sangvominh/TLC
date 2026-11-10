"""Đọc và kiểm chứng dữ liệu sạch đã ghi trên HDFS."""

import json

from pyspark.sql import SparkSession, functions as F


def validate_output(spark, output_path, valid, duration, expected_rows, expected_nulls):
    """Đọc lại Parquet để kiểm chứng dữ liệu thực sự đã ghi trên đĩa."""
    written = spark.read.parquet(output_path)
    # Gom các trường hợp sai quy tắc hoặc sai cột được tính thêm.
    bad = ~F.coalesce(valid, F.lit(False))
    bad = bad | (F.col("passenger_count") == 0) | (F.col("passenger_count") > 6)
    features = {
        "trip_duration_min": duration,
        "pickup_date": F.to_date("tpep_pickup_datetime"),
        "pickup_hour": F.hour("tpep_pickup_datetime"),
        "pickup_month": F.month("tpep_pickup_datetime"),
        "is_weekend": F.dayofweek("tpep_pickup_datetime").isin(1, 7),
    }
    for name, expected in features.items():
        bad = bad | ~F.col(name).eqNullSafe(expected)
    result = written.agg(
        F.count("*").alias("output_rows"),
        F.count(F.when(bad, 1)).alias("invalid_rows"),
        F.count(F.when(F.col("passenger_count").isNull(), 1)).alias("null_passengers"),
        F.count(F.when(F.col("RatecodeID") == 99, 1)).alias("ratecode_99_rows"),
    ).first().asDict()
    types_ok = all(written.schema[name].dataType.simpleString() == "int"
                   for name in ("passenger_count", "RatecodeID"))
    # Chỉ đạt khi số dòng, số khách null, kiểu cột và quy tắc đều đúng.
    if (result["output_rows"] != expected_rows or result["invalid_rows"] != 0
            or result["null_passengers"] != expected_nulls or not types_ok):
        raise RuntimeError(f"Kiểm chứng dữ liệu đầu ra không đạt: {result}; kiểu integer: {types_ok}")
    result["passed"] = True
    return result


def main():
    # Chỉ đọc dữ liệu sạch và báo cáo; không chạy lại bước làm sạch.
    from clean import REPORTS, RULESET_VERSION, cleaning_rules

    spark = (SparkSession.builder.appName("TLC-Verify-Cleaned")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = json.loads("\n".join(
            row.value for row in spark.read.text(REPORTS.rstrip("/") + "/latest").collect()))
        if report["ruleset_version"] != RULESET_VERSION:
            raise ValueError("Báo cáo không thuộc bộ quy tắc Phase 2 hiện tại.")
        rules, duration = cleaning_rules()
        valid = F.lit(True)
        for _, condition in rules:
            valid = valid & condition
        result = validate_output(
            spark, report["output_path"], valid, duration, report["output_rows"],
            report["transformations"]["passenger_count"]["output_null_rows"])
        if report.get("local_output_path"):
            local_result = validate_output(
                spark, report["local_output_path"], valid, duration, report["output_rows"],
                report["transformations"]["passenger_count"]["output_null_rows"])
            result["local_output"] = local_result
        spark.read.parquet(report["output_path"]).printSchema()
    finally:
        spark.stop()
    print("\nKIỂM CHỨNG DỮ LIỆU SẠCH: ĐẠT", flush=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

