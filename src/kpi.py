"""Phase 3: tính năm KPI bằng Spark SQL từ dữ liệu đã làm sạch."""

from pathlib import Path
import shutil

from pyspark.sql import SparkSession

CLEANED = "file:///data/cleaned"
ZONES = "file:///data/reference/taxi_zone_lookup.csv"
OUTPUT = Path("/data/kpi")

# Doanh thu là total_amount; tỷ lệ tip là tổng tip / tổng fare của các
# chuyến có cả hai giá trị hợp lệ (không phải trung bình tỷ lệ từng chuyến).
QUERIES = {
    "revenue_by_hour": """
        SELECT pickup_hour, COUNT(*) AS trips,
               ROUND(SUM(total_amount), 2) AS revenue_usd
        FROM trips GROUP BY pickup_hour ORDER BY pickup_hour
    """,
    "top_10_pickup_zones": """
        SELECT t.PULocationID AS location_id,
               COALESCE(z.Borough, 'Unknown') AS borough,
               COALESCE(z.Zone, 'Unknown') AS zone, COUNT(*) AS trips
        FROM trips t LEFT JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY t.PULocationID, z.Borough, z.Zone
        ORDER BY trips DESC, location_id LIMIT 10
    """,
    "tip_rate_by_payment": """
        SELECT payment_type,
               CASE payment_type WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash'
                   WHEN 3 THEN 'No charge' WHEN 4 THEN 'Dispute'
                   WHEN 5 THEN 'Unknown' WHEN 6 THEN 'Voided trip'
                   ELSE 'Other or missing' END AS payment_method,
               COUNT(*) AS trips,
               COUNT(CASE WHEN tip_amount >= 0 AND fare_amount > 0 THEN 1 END) AS trips_with_valid_tip,
               ROUND(100 * SUM(CASE WHEN tip_amount >= 0 AND fare_amount > 0 THEN tip_amount END)
                   / NULLIF(SUM(CASE WHEN tip_amount >= 0 AND fare_amount > 0 THEN fare_amount END), 0), 2) AS tip_rate_pct
        FROM trips GROUP BY payment_type ORDER BY payment_type
    """,
    "weekday_vs_weekend": """
        SELECT CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
               COUNT(*) AS trips, ROUND(SUM(total_amount), 2) AS revenue_usd,
               ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip_usd,
               ROUND(AVG(trip_distance), 2) AS avg_distance_miles
        FROM trips GROUP BY is_weekend ORDER BY is_weekend
    """,
    "avg_distance_by_zone": """
        SELECT t.PULocationID AS location_id,
               COALESCE(z.Borough, 'Unknown') AS borough,
               COALESCE(z.Zone, 'Unknown') AS zone, COUNT(*) AS trips,
               ROUND(AVG(t.trip_distance), 2) AS avg_distance_miles
        FROM trips t LEFT JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY t.PULocationID, z.Borough, z.Zone
        ORDER BY avg_distance_miles DESC, location_id
    """,
}


def main():
    spark = SparkSession.builder.appName("TLC-Phase-3-KPI").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        spark.read.parquet(CLEANED).createOrReplaceTempView("trips")
        (spark.read.option("header", True).option("inferSchema", True)
         .csv(ZONES).createOrReplaceTempView("zones"))
        OUTPUT.mkdir(parents=True, exist_ok=True)
        for name, query in QUERIES.items():
            # Spark ghi thư mục; đổi part file thành một CSV dễ mở trên máy.
            temporary = OUTPUT / ("_" + name)
            (spark.sql(query).coalesce(1).write.mode("overwrite")
             .option("header", True).csv("file://" + str(temporary)))
            part = next(temporary.glob("part-*.csv"))
            part.replace(OUTPUT / (name + ".csv"))
            shutil.rmtree(temporary)
            print(f"Đã ghi {OUTPUT / (name + '.csv')}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
