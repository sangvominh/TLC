"""Phase 3: Spark SQL tren toan bo lo Parquet sach cua TLC/main."""

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
import re
import time
from contextlib import redirect_stdout

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

SQL_NAMES = (
    "01_revenue_by_hour", "02_top10_pickup_zones", "03_tip_by_payment",
    "04_weekday_vs_weekend", "05_avg_distance_by_zone",
)
REQUIRED = (
    "tpep_pickup_datetime", "PULocationID", "payment_type", "fare_amount",
    "total_amount", "tip_amount", "trip_distance", "trip_duration_min",
    "pickup_date", "pickup_hour", "pickup_month", "is_weekend", "passenger_count", "RatecodeID",
)
ZONE_SCHEMA = T.StructType([
    T.StructField("LocationID", T.IntegerType()), T.StructField("Borough", T.StringType()),
    T.StructField("Zone", T.StringType()), T.StructField("service_zone", T.StringType()),
])


def check_zones(zones):
    if zones.count() == 0 or zones.filter(F.col("LocationID").isNull()).limit(1).count():
        raise ValueError("Zone lookup is empty or has a null/invalid key")
    if zones.groupBy("LocationID").count().filter("count > 1").limit(1).count():
        raise ValueError("Zone lookup has duplicate LocationID keys")


def prepare_trips(source, zones):
    missing = set(REQUIRED) - set(source.columns)
    if missing:
        raise ValueError(f"Missing cleaned columns: {sorted(missing)}")
    check_zones(zones)
    data = source.select(*REQUIRED)
    # Invalid optional money is missing; signed totals are retained. Source is unchanged.
    for name in ("fare_amount", "total_amount", "tip_amount"):
        number = F.col(name).cast("double")
        finite = number.isNotNull() & ~F.isnan(number) & (F.abs(number) != float("inf"))
        data = data.withColumn(name, F.when(finite, F.expr(f"try_cast(`{name}` as decimal(28,6))")))
    invalid = (
        F.col("pickup_date").isNull() | F.col("pickup_hour").isNull()
        | F.col("pickup_month").isNull() | F.col("is_weekend").isNull()
        | F.col("PULocationID").isNull() | F.col("fare_amount").isNull()
        | (F.col("fare_amount") <= 0)
        | ~F.col("pickup_date").eqNullSafe(F.to_date("tpep_pickup_datetime"))
        | ~F.col("pickup_hour").eqNullSafe(F.hour("tpep_pickup_datetime"))
        | ~F.col("pickup_month").eqNullSafe(F.month("tpep_pickup_datetime"))
        | ~F.col("is_weekend").eqNullSafe(F.dayofweek("tpep_pickup_datetime").isin(1, 7))
    )
    profile = data.agg(
        F.count("*").alias("input_rows"),
        F.count(F.when(invalid, 1)).alias("invalid_contract_rows"),
        F.min("pickup_date").alias("min_pickup_date"), F.max("pickup_date").alias("max_pickup_date"),
        F.countDistinct("pickup_date").alias("observed_days"),
        F.count(F.when(F.col("passenger_count").isNull(), 1)).alias("null_passenger_rows"),
        F.count(F.when(F.col("RatecodeID") == 99, 1)).alias("ratecode_99_rows"),
        F.sum("total_amount").alias("recorded_revenue_usd"),
        F.count("total_amount").alias("total_observation_count"),
        F.count(F.when(F.col("total_amount") < 0, 1)).alias("negative_total_rows"),
        F.count(F.when(F.col("tip_amount").isNull(), 1)).alias("missing_or_invalid_tip_rows"),
        F.count(F.when(F.col("tip_amount") < 0, 1)).alias("negative_tip_rows"),
    ).first().asDict()
    if not profile["input_rows"] or profile["invalid_contract_rows"]:
        raise ValueError(f"Cleaned input contract failed: {profile}")
    profile["months"] = [r.asDict() for r in data.groupBy("pickup_month").count().orderBy("pickup_month").collect()]
    profile["lookup_rows"] = zones.count()
    enriched = data.join(F.broadcast(zones), data.PULocationID == zones.LocationID, "left")
    status = (F.when(F.col("LocationID").isNull(), "unmatched")
              .when(F.col("PULocationID").isin(264, 265), "unknown_or_outside")
              .otherwise("matched"))
    enriched = enriched.withColumn("zone_match_status", status)
    enriched = enriched.withColumn("Borough", F.coalesce("Borough", F.lit("Unknown")))
    enriched = enriched.withColumn("Zone", F.coalesce("Zone", F.concat(F.lit("Unmatched ID "), F.col("PULocationID"))))
    return enriched.drop("LocationID", "service_zone").persist(StorageLevel.MEMORY_AND_DISK), profile


def query_kpis(spark, trips, sql_dir):
    trips.createOrReplaceTempView("trips")
    results = {}
    for name in SQL_NAMES:
        query = (Path(sql_dir) / f"{name}.sql").read_text(encoding="utf-8")
        results[name] = spark.sql(query)
    return results


def validate_results(rows, profile):
    expected = profile["input_rows"]
    for name in (SQL_NAMES[0], SQL_NAMES[2], SQL_NAMES[3], SQL_NAMES[4]):
        if sum(row["trip_count"] for row in rows[name]) != expected:
            raise ValueError(f"Trip count conservation failed: {name}")
    all_zones = rows[SQL_NAMES[4]]
    expected_top = sorted(all_zones, key=lambda r: (-r["trip_count"], r["PULocationID"]))[:10]
    actual_top = rows[SQL_NAMES[1]]
    if [(r["PULocationID"], r["trip_count"]) for r in actual_top] != [(r["PULocationID"], r["trip_count"]) for r in expected_top]:
        raise ValueError("Top 10 ranking/tie order failed")
    for name in (SQL_NAMES[0], SQL_NAMES[3]):
        actual = sum((r["recorded_revenue_usd"] or Decimal(0)) for r in rows[name])
        if actual != (profile["recorded_revenue_usd"] or Decimal(0)):
            raise ValueError(f"Revenue conservation failed: {name}")
        if sum(r["total_observation_count"] for r in rows[name]) != profile["total_observation_count"]:
            raise ValueError("Money observation count failed")
    if sum(r["observed_days"] for r in rows[SQL_NAMES[3]]) != profile["observed_days"]:
        raise ValueError("Observed-day count failed")
    for row in rows[SQL_NAMES[2]]:
        if row["payment_type"] != 1 and (row["tip_rate_pct"] is not None or row["tipped_trip_pct"] is not None):
            raise ValueError("Unrecorded cash tips must not be interpreted as zero tip rates")
    return {"trip_conservation": True, "join_cardinality": True, "revenue_conservation": True,
            "top10_ranking": True, "observed_days": True, "tip_interpretation": True}


def write_csv_file(spark, frame, directory, name):
    """Spark writes a directory; promote its single small aggregate part to a named CSV."""
    stage = directory.rstrip("/") + "/_stage_" + name
    target = directory.rstrip("/") + "/" + name + ".csv"
    frame.coalesce(1).write.mode("errorifexists").option("header", True).csv(stage)
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path
    fs = path(stage).getFileSystem(spark._jsc.hadoopConfiguration())
    parts = [entry.getPath() for entry in fs.listStatus(path(stage)) if entry.getPath().getName().startswith("part-") and entry.getPath().getName().endswith(".csv")]
    if len(parts) != 1 or fs.exists(path(target)) or not fs.rename(parts[0], path(target)):
        raise RuntimeError(f"Could not export exactly one CSV: {target}")
    fs.delete(path(stage), True)
    return target


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="hdfs://namenode:8020/user/tlc/cleaned/")
    parser.add_argument("--phase2-report", default="hdfs://namenode:8020/user/tlc/reports/latest")
    parser.add_argument("--zones", default="file:///data/reference/taxi_zone_lookup.csv")
    parser.add_argument("--sql-dir", default=str(Path(__file__).resolve().parent / "sql/phase3"))
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.run_id):
        raise ValueError("Invalid run ID")
    output = f"file:///data/analytics/{args.run_id}"
    report_dir = Path("/data/analytics_reports") / args.run_id
    report_dir.mkdir(parents=True, exist_ok=False)
    started = time.perf_counter()
    spark = (SparkSession.builder.appName(f"TLC-Phase-3-{args.run_id}")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    trips = None
    try:
        def read_phase2():
            return json.loads("\n".join(row.value for row in spark.read.text(args.phase2_report).collect()))
        source_report = read_phase2()
        if (source_report.get("ruleset_version") != "phase2_v2_2023"
                or not source_report.get("validation", {}).get("passed")
                or source_report.get("output_path", "").rstrip("/") != args.input.rstrip("/")):
            raise ValueError("Phase 2 report does not match the cleaned source")
        source = spark.read.parquet(args.input)
        input_files = sorted(source.inputFiles())
        zones = spark.read.option("header", True).option("mode", "FAILFAST").schema(ZONE_SCHEMA).csv(args.zones)
        trips, profile = prepare_trips(source, zones)
        if profile["input_rows"] != source_report["output_rows"]:
            raise ValueError("Input row count differs from the Phase 2 report")
        matches = [r.asDict() for r in trips.groupBy("zone_match_status").count().collect()]
        if sum(row["count"] for row in matches) != profile["input_rows"]:
            raise ValueError("JOIN changed the input row count")
        profile["zone_matches"] = matches
        print("PROFILE:", json.dumps(profile, default=str), flush=True)
        frames = query_kpis(spark, trips, args.sql_dir)
        rows = {}
        timings = {}
        for name, frame in frames.items():
            step_start = time.perf_counter()
            rows[name] = [row.asDict() for row in frame.collect()]
            plan = io.StringIO()
            with redirect_stdout(plan):
                frame.explain(mode="formatted")
            (report_dir / f"{name}.explain.txt").write_text(plan.getvalue(), encoding="utf-8")
            timings[name] = round(time.perf_counter() - step_start, 3)
        checks = validate_results(rows, profile)
        outputs = []
        for name, frame in frames.items():
            path = write_csv_file(spark, frame, output, name)
            written = spark.read.option("header", True).csv(path)
            if written.columns != frame.columns or written.count() != len(rows[name]):
                raise ValueError(f"CSV readback failed: {name}")
            outputs.append({"filename": name + ".csv", "rows": len(rows[name]), "columns": frame.columns})
        if source_report != read_phase2() or input_files != sorted(spark.read.parquet(args.input).inputFiles()):
            raise ValueError("Phase 2 changed during analysis; rerun on a stable input")
        report = {
            "status": "passed", "run_id": args.run_id, "application_id": spark.sparkContext.applicationId,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "input": args.input, "input_files": input_files, "source_phase2_report": source_report,
            "scope": "all cleaned rows in the selected Phase 2 batch; no extra month or RatecodeID filter",
            "profile": profile, "checks": dict(checks, csv_readback=True), "outputs": outputs,
            "query_collect_seconds": timings, "elapsed_seconds_before_stop": round(time.perf_counter() - started, 3),
            "money_policy": "finite money -> decimal(28,6); missing stays null; signed totals retained; input unchanged",
            "tip_policy": "ratio of sums on the same nonnull nonnegative-tip subset; rates only for credit card",
            "day_policy": "observed days only; not a complete-year/month coverage claim",
        }
        (report_dir / "analysis_report.json").write_text(json.dumps(report, default=str, ensure_ascii=False, indent=2), encoding="utf-8")
        print("PHASE 3 PASSED:", json.dumps({"run_id": args.run_id, "rows": profile["input_rows"], "outputs": outputs}), flush=True)
    except Exception as error:
        (report_dir / "failure.json").write_text(json.dumps({"error": str(error)}, ensure_ascii=False, indent=2), encoding="utf-8")
        raise
    finally:
        if trips is not None:
            trips.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
