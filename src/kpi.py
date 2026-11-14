"""Phase 3: tính 5 KPI từ dữ liệu TLC đã làm sạch."""

from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession


INPUT = "file:///data/cleaned/"
ZONE_LOOKUP = "file:///data/reference/taxi_zone_lookup.csv"
OUTPUT = "file:///data/kpi/"


QUERIES = {
    "revenue_by_hour": """
        SELECT pickup_hour, SUM(CAST(total_amount AS DECIMAL(20, 2))) AS total_revenue
        FROM clean_trips GROUP BY pickup_hour ORDER BY pickup_hour
    """,
    "top_10_pickup_zones": """
        SELECT t.PULocationID AS pickup_location_id,
               COALESCE(z.Borough, 'Unknown') AS borough,
               COALESCE(z.Zone, 'Unknown') AS pickup_zone,
               COUNT(*) AS trip_count
        FROM clean_trips t
        LEFT JOIN taxi_zones z ON t.PULocationID = z.LocationID
        GROUP BY t.PULocationID, z.Borough, z.Zone
        ORDER BY trip_count DESC, pickup_location_id LIMIT 10
    """,
    "tip_rate_by_payment": """
        SELECT payment_type,
               COUNT(*) AS trip_count,
               SUM(CAST(tip_amount AS DECIMAL(20, 2))) AS total_tip,
               SUM(CAST(fare_amount AS DECIMAL(20, 2))) AS total_fare,
               ROUND(100.0 * SUM(CAST(tip_amount AS DECIMAL(20, 2)))
                     / NULLIF(SUM(CAST(fare_amount AS DECIMAL(20, 2))), 0), 4)
                     AS tip_rate_pct
        FROM clean_trips GROUP BY payment_type ORDER BY payment_type
    """,
    "weekday_vs_weekend": """
        SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
               COUNT(*) AS trip_count,
               COUNT(DISTINCT pickup_date) AS day_count,
               SUM(CAST(total_amount AS DECIMAL(20, 2))) AS total_revenue,
               ROUND(AVG(trip_distance), 4) AS avg_trip_distance,
               ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT pickup_date), 0), 2)
                     AS avg_trips_per_day,
               ROUND(SUM(CAST(total_amount AS DECIMAL(20, 2)))
                     / NULLIF(COUNT(DISTINCT pickup_date), 0), 2) AS avg_revenue_per_day
        FROM clean_trips
        GROUP BY CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END
        ORDER BY day_type
    """,
    "avg_distance_by_zone": """
        SELECT t.PULocationID AS pickup_location_id,
               COALESCE(z.Borough, 'Unknown') AS borough,
               COALESCE(z.Zone, 'Unknown') AS pickup_zone,
               COUNT(*) AS trip_count,
               ROUND(AVG(t.trip_distance), 4) AS avg_trip_distance
        FROM clean_trips t
        LEFT JOIN taxi_zones z ON t.PULocationID = z.LocationID
        GROUP BY t.PULocationID, z.Borough, z.Zone
        ORDER BY avg_trip_distance DESC, pickup_location_id
    """,
}


def run(spark, input_path=INPUT, zone_lookup=ZONE_LOOKUP, output=OUTPUT):
    lookup_file = Path(zone_lookup.removeprefix("file://"))
    if not lookup_file.is_file():
        raise FileNotFoundError(
            f"Không thấy {zone_lookup}. Đặt taxi_zone_lookup.csv vào data/reference/."
        )

    trips = spark.read.parquet(input_path)
    zones = (spark.read.option("header", True).option("inferSchema", True)
             .csv(zone_lookup))
    if zones.groupBy("LocationID").count().where("count > 1").limit(1).count():
        raise ValueError("Lookup có LocationID trùng; cần sửa trước khi join để tránh đếm dư chuyến.")
    trips.createOrReplaceTempView("clean_trips")
    zones.createOrReplaceTempView("taxi_zones")

    results = {}
    output_dir = Path(output.removeprefix("file://"))
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, query in QUERIES.items():
        destination = output_dir / f"{name}.csv"
        # Spark ghi một part-file; đổi sang tên KPI rồi dọn thư mục tạm.
        with TemporaryDirectory(prefix=".spark-", dir=output_dir) as temp_dir:
            spark_output = Path(temp_dir) / "result"
            spark.sql(query).coalesce(1).write.option("header", True).csv(
                spark_output.resolve().as_uri())
            part_file = next(spark_output.glob("part-*.csv"))
            part_file.replace(destination)
        results[name] = str(destination)
    return results


def main():
    spark = (SparkSession.builder.appName("TLC-Phase-3-KPIs")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        outputs = run(spark)
    finally:
        spark.stop()
    print("Phase 3 hoàn tất. CSV KPI:")
    for name, path in outputs.items():
        print(f"- {name}: {path}")


if __name__ == "__main__":
    main()
