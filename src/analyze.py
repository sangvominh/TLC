from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CLEANED_PATH = "file:///data/cleaned/"
ZONE_PATH = "file:///data/raw/taxi_zone_lookup.csv"
OUTPUT_ROOT = "file:///data/phase3_output/"

def save_csv(dataframe, folder_name): # Hàm dùng chung để ghi từng kết quả KPI.
    output_path = OUTPUT_ROOT + folder_name
    (
        dataframe.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )
    print(f"Đã ghi: {output_path}", flush=True)

def main():  # Hàm chính chạy toàn bộ giai đoạn 3.
    spark = (
        SparkSession.builder
        .appName("TLC-Phase3-KPI")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")  # Giảm bớt log để dễ xem kết quả.

    try: # Đảm bảo Spark vẫn được dừng nếu có lỗi.
        trips = spark.read.parquet(CLEANED_PATH)
        zones = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(ZONE_PATH)
        )

        trips.createOrReplaceTempView("trips")  # Đăng ký dữ liệu chuyến xe để truy vấn bằng SQL.
        zones.createOrReplaceTempView("zones")  # Đăng ký bảng vùng để truy vấn bằng SQL.

        # KPI 1: tổng doanh thu theo giờ đón khách.
        revenue_by_hour = spark.sql("""
            SELECT pickup_hour, ROUND(SUM(total_amount), 2) AS revenue
            FROM trips
            GROUP BY pickup_hour
            ORDER BY pickup_hour
        """)

        # KPI 2: 10 vùng đón khách có nhiều chuyến nhất.
        top10_zones = spark.sql("""
            SELECT z.Borough, z.Zone, COUNT(*) AS trip_count
            FROM trips t
            JOIN zones z ON t.PULocationID = z.LocationID
            GROUP BY z.Borough, z.Zone
            ORDER BY trip_count DESC
            LIMIT 10
        """)

        # KPI 3: phần trăm số chuyến theo phương thức thanh toán.
        payment_share = spark.sql("""
            SELECT
                COALESCE(CAST(payment_type AS STRING), 'Unknown') AS payment_type,
                COUNT(*) AS trip_count,
                ROUND(
                    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
                    2
                ) AS percentage
            FROM trips
            GROUP BY payment_type
            ORDER BY trip_count DESC
        """)

        # KPI 4: tính doanh thu từng ngày, rồi lấy trung bình theo nhóm ngày thường/cuối tuần.
        weekday_weekend = spark.sql("""
            WITH daily_revenue AS (
                SELECT
                    pickup_date,
                    is_weekend,
                    SUM(total_amount) AS daily_revenue
                FROM trips
                GROUP BY pickup_date, is_weekend
            )
            SELECT
                CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
                COUNT(*) AS day_count,
                ROUND(AVG(daily_revenue), 2) AS avg_daily_revenue
            FROM daily_revenue
            GROUP BY is_weekend
            ORDER BY is_weekend
        """)

        # KPI 5: số chuyến và quãng đường trung bình của từng vùng đón khách.
        avg_distance_by_zone = spark.sql("""
            SELECT
                z.Borough,
                z.Zone,
                COUNT(*) AS trip_count,
                ROUND(AVG(t.trip_distance), 2) AS avg_trip_distance
            FROM trips t
            JOIN zones z ON t.PULocationID = z.LocationID
            GROUP BY z.Borough, z.Zone
            ORDER BY avg_trip_distance DESC
        """)

        save_csv(revenue_by_hour, "revenue_by_hour")  # Ghi KPI doanh thu theo giờ.
        save_csv(top10_zones, "top10_zones")  # Ghi KPI top 10 vùng đón.
        save_csv(payment_share, "payment_share")  # Ghi KPI tỷ lệ thanh toán.
        save_csv(weekday_weekend, "weekday_weekend")  # Ghi KPI ngày thường/cuối tuần.
        save_csv(avg_distance_by_zone, "avg_distance_by_zone")  # Ghi KPI quãng đường theo vùng.

        print("PHÂN TÍCH GIAI ĐOẠN 3 HOÀN TẤT", flush=True)  # Báo đã ghi xong 5 kết quả.

    finally:  # Khối này luôn chạy, kể cả khi chương trình gặp lỗi.
        spark.stop()  # Giải phóng phiên Spark.


if __name__ == "__main__":  # Chỉ chạy main khi chạy trực tiếp file này.
    main()  # Bắt đầu phân tích.
