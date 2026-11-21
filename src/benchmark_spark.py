"""Phase 4: đo Spark đọc đủ cột, giữ UI 4040 để chụp bằng chứng."""

import argparse
import json
from pathlib import Path
from time import monotonic, perf_counter, sleep
from urllib.request import urlopen

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F


def main():
    """Đo thời gian Spark đọc cùng file Parquet với Pandas.

    Nhận --input, --output, --repeats và --hold-ui từ terminal.
    Lưu thời gian, số dòng, partition và task, rồi giữ UI để chụp bằng chứng.
    Đóng SparkSession khi hoàn tất hoặc khi xảy ra lỗi.
    """
    # 1. Nhận đường dẫn dữ liệu và các thiết lập đo.
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--hold-ui", type=int, default=600)
    args = parser.parse_args()
    output = Path(args.output)
    release_file = output.parent / "release_ui"

    # 2. Tạo SparkSession, đo riêng thời gian khởi tạo.
    started = perf_counter()
    spark = (SparkSession.builder.appName("TLC-Phase4-Read-Parquet")
             .config("spark.ui.port", "4040")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    startup_seconds = perf_counter() - started
    spark.sparkContext.setLogLevel("WARN")
    cached = None
    try:
        # 3. Lần 0 chạy thử. Các lần tiếp theo dùng để so sánh với Pandas.
        records = []
        for trial in range(args.repeats + 1):
            if cached is not None:
                cached.unpersist(blocking=True)
            group = f"phase4-{'warmup' if trial == 0 else 'measured'}-{trial}"
            spark.sparkContext.setJobGroup(group, f"Read all columns - trial {trial}")
            started = perf_counter()
            cached = spark.read.parquet(Path(args.input).as_uri()).persist(
                StorageLevel.MEMORY_AND_DISK)
            # Persist + count buộc Spark đọc và cache mọi cột, tránh chỉ đếm metadata.
            rows = cached.count()
            elapsed = perf_counter() - started
            job_ids = list(spark.sparkContext.statusTracker().getJobIdsForGroup(group))
            spark.sparkContext.setJobGroup("phase4-validation", "Validate total_amount")
            partitions = cached.rdd.getNumPartitions()
            amount = cached.agg(F.sum("total_amount")).first()[0]
            records.append({
                "trial": trial, "warmup": trial == 0, "seconds": elapsed,
                "rows": rows, "columns": cached.columns, "partitions": partitions,
                "total_amount_sum": round(float(amount), 2), "job_group": group,
                "job_ids": job_ids,
            })
            print(f"Spark trial {trial}: {elapsed:.4f}s, {rows:,} rows, "
                  f"{partitions} partitions", flush=True)

        # 4. Lưu số task/stage từ Spark UI và kết quả đo vào JSON.
        ui = spark.sparkContext.uiWebUrl
        app_id = spark.sparkContext.applicationId
        snapshots = {}
        for resource in ("jobs", "stages", "storage/rdd", "executors"):
            with urlopen(f"{ui}/api/v1/applications/{app_id}/{resource}", timeout=30) as response:
                snapshots[resource] = json.load(response)
        ui_snapshot = output.parent / "spark_ui.json"
        ui_snapshot.write_text(json.dumps(snapshots, indent=2), encoding="utf-8")
        for record in records:
            record["completed_tasks"] = sum(job["numCompletedTasks"] for job in snapshots["jobs"]
                                            if job["jobId"] in record["job_ids"])
        result = {
            "engine": "Spark", "versions": {"spark": spark.version},
            "input": args.input, "file_bytes": Path(args.input).stat().st_size,
            "startup_seconds": startup_seconds, "app_id": app_id, "ui_url": ui,
            "master": spark.sparkContext.master,
            "storage_level": "MEMORY_AND_DISK", "records": records,
        }
        # Điều phối chỉ thấy file kết quả sau khi JSON đã được ghi đầy đủ.
        temporary_output = output.with_suffix(".json.tmp")
        temporary_output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        temporary_output.replace(output)
        print("Đo xong. Mở http://localhost:4040 để chụp Jobs, Stages và Storage.", flush=True)
        print(f"UI giữ tối đa {args.hold_ui}s; tạo data/phase4/release_ui để đóng sớm.", flush=True)
        # 5. Giữ Spark hoạt động để mở cổng 4040 và chụp màn hình.
        deadline = monotonic() + args.hold_ui
        while monotonic() < deadline and not release_file.exists():
            sleep(1)
    finally:
        if cached is not None:
            cached.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
