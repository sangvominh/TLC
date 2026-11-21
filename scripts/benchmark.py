"""Điều phối benchmark Docker và tạo bằng chứng Phase 4 trên máy cá nhân."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
from time import perf_counter, sleep


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "data" / "phase4"
DEPS = "/tmp/tlc-phase4-deps"


def execute(command, **kwargs):
    """Chạy một lệnh hệ thống và chờ lệnh hoàn tất.

    command là danh sách tên lệnh và tham số, ví dụ ["docker", "compose", "up", "-d"].
    kwargs chứa tùy chọn như thư mục chạy hoặc dữ liệu gửi vào tiến trình.
    Trả về kết quả của lệnh, phát sinh lỗi nếu lệnh thất bại.
    """
    return subprocess.run(command, check=True, **kwargs)


def summarize(pandas, spark, environment, input_file):
    """Nhận số liệu Pandas/Spark, cấu hình môi trường và đường dẫn file tháng.

    Kiểm tra kết quả khớp nhau, tính trung bình/trung vị rồi ghi bảng so sánh.
    Ghi thêm cấu hình và báo cáo vào data/phase4/. Hàm không trả về giá trị.
    """
    # Gom các lần chạy của cả hai công cụ vào cùng một danh sách.
    records = []
    for result in (pandas, spark):
        for record in result["records"]:
            records.append({"engine": result["engine"], **record})
    # Dùng lần chạy đầu làm mốc để đối chiếu số dòng, cột và tổng tiền.
    reference = records[0]
    for record in records:
        if (record["rows"] != reference["rows"]
                or record["columns"] != reference["columns"]
                or abs(record["total_amount_sum"] - reference["total_amount_sum"]) > 0.01):
            raise ValueError("Kết quả Pandas và Spark không khớp, không thể so sánh.")

    # Ghi số liệu từng lần, gồm cả lần chạy thử, để có thể xem lại.
    fields = ["engine", "trial", "warmup", "seconds", "rows", "column_count",
              "partitions", "completed_tasks", "total_amount_sum"]
    with (OUTPUT / "benchmark_runs.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for record in records:
            csv_row = {}
            for field in fields:
                csv_row[field] = record.get(field, "")
            csv_row["column_count"] = len(record["columns"])
            writer.writerow(csv_row)

    # Bỏ lần chạy thử khi tính các chỉ số so sánh chính thức.
    summary = []
    for result in (pandas, spark):
        times = [row["seconds"] for row in result["records"] if not row["warmup"]]
        summary.append({"engine": result["engine"], "runs": len(times),
                        "mean_seconds": statistics.mean(times),
                        "median_seconds": statistics.median(times),
                        "min_seconds": min(times), "max_seconds": max(times)})
    # Tỷ số lớn hơn 1 nghĩa là Spark nhanh hơn, nhỏ hơn 1 nghĩa là Pandas nhanh hơn.
    ratio = summary[0]["median_seconds"] / summary[1]["median_seconds"]
    with (OUTPUT / "comparison.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=[*summary[0], "pandas_over_spark_median"])
        writer.writeheader()
        for row in summary:
            writer.writerow({**row, "pandas_over_spark_median": ratio})

    # Tạo mã SHA-256 để nhận biết chính xác file dữ liệu đã dùng trong phép đo.
    digest = hashlib.sha256()
    with input_file.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    environment.update({"generated_at_utc": datetime.now(timezone.utc).isoformat(),
                        "input_sha256": digest.hexdigest(),
                        "input_bytes": input_file.stat().st_size,
                        "versions": {"pandas": pandas["versions"], "spark": spark["versions"]}})
    (OUTPUT / "environment.json").write_text(json.dumps(environment, indent=2), encoding="utf-8")
    table_rows = []
    for row in summary:
        table_rows.append(
            f"| {row['engine']} | {row['runs']} | {row['mean_seconds']:.4f} | "
            f"{row['median_seconds']:.4f} | {row['min_seconds']:.4f} | {row['max_seconds']:.4f} |")
    table = "\n".join(table_rows)
    conclusion = (f"Spark nhanh hơn Pandas {ratio:.2f} lần theo trung vị."
                  if ratio >= 1 else f"Pandas nhanh hơn Spark {1 / ratio:.2f} lần theo trung vị.")
    report = f"""# Phase 4 — Đo thời gian đọc Parquet

File `{input_file.relative_to(ROOT).as_posix()}`, {input_file.stat().st_size:,} byte.
Hai engine khớp {reference['rows']:,} dòng, {len(reference['columns'])} cột và tổng `total_amount` {reference['total_amount_sum']:.2f}.

| Engine | Lần đo | Trung bình (s) | Trung vị (s) | Nhỏ nhất (s) | Lớn nhất (s) |
|---|---|---|---|---|---|
{table}

{conclusion}
Tỷ số Pandas/Spark theo trung vị = {ratio:.4f}. Lớn hơn 1 nghĩa là Spark nhanh hơn.
Khởi tạo SparkSession mất {spark['startup_seconds']:.4f}s, được ghi riêng.

## Cách đo

1. Pandas đọc đủ cột bằng `read_parquet`, rồi đếm số dòng.
2. Spark đọc cùng file, dùng `persist` và `count` để nạp đủ cột, rồi đếm dòng.
3. Mỗi công cụ chạy thử một lần, đo {summary[0]['runs']} lần và đối chiếu kết quả.

Spark có {spark['records'][-1]['partitions']} partition và hoàn tất {spark['records'][-1]['completed_tasks']} task trong lần đo cuối.
Phiên đo là `{spark['app_id']}`.

Phép đo gồm chi phí cache của Spark. Thời gian cài thư viện, import và khởi tạo
tiến trình/SparkSession được loại khỏi khoảng đo. Chạy Pandas trước Spark,
cache hệ điều hành được giữ sau lần làm nóng. Pandas chạy trên Worker,
Spark dùng thêm Driver trên Master. Kết quả áp dụng cho file và cấu hình này.

`benchmark_runs.csv` lưu từng lần đo. `environment.json` lưu cấu hình và mã kiểm tra file.
Các file JSON còn lại lưu số liệu gốc. Ba ảnh bên dưới chụp từ Spark UI.

![Jobs — thời gian và task](screenshots/01_jobs.jpg)

![Stages — số task theo stage](screenshots/02_stages.jpg)

![Storage — partition đã cache](screenshots/03_storage.jpg)
"""
    (OUTPUT / "report.md").write_text(report, encoding="utf-8")
    print(report.split("## Cách đo")[0], flush=True)


def main():
    """Điều phối Phase 4 khi người dùng chạy python run.py benchmark.

    Đọc tùy chọn từ terminal, chuẩn bị Docker, chạy Pandas rồi chạy Spark.
    Sau khi có số liệu, gọi summarize() và chờ người dùng chụp Spark UI.
    """
    # Bước 1. Nhận tên file tháng, số lần đo và thời gian giữ Spark UI.
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw/yellow_tripdata_2023-01.parquet")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--hold-ui", type=int, default=600)
    args = parser.parse_args()
    if args.repeats < 1 or args.hold_ui < 0:
        parser.error("repeats phải >= 1 và hold-ui phải >= 0")
    input_file = (ROOT / args.input).resolve()
    data_root = (ROOT / "data").resolve()
    if not input_file.is_file() or not input_file.is_relative_to(data_root):
        parser.error("input phải là một file nằm trong data/")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / "screenshots").mkdir(exist_ok=True)
    for filename in ("pandas.json", "spark.json", "release_ui", "benchmark_runs.csv",
                     "comparison.csv", "report.md", "spark_ui.json"):
        (OUTPUT / filename).unlink(missing_ok=True)
    container_input = "/data/" + input_file.relative_to(data_root).as_posix()
    # Bước 2. Khởi động Docker và ghi lại tài nguyên để giải thích kết quả đo.
    execute(["docker", "compose", "up", "-d"], cwd=ROOT)
    info = execute(["docker", "inspect", "spark-worker", "spark-master"],
                   capture_output=True, text=True)
    containers = json.loads(info.stdout)
    environment = {"containers": [{"name": item["Name"], "image": item["Config"]["Image"],
                                  "image_id": item["Image"],
                                  "cpus": item["HostConfig"]["NanoCpus"] / 1e9,
                                  "memory_bytes": item["HostConfig"]["Memory"]}
                                 for item in containers]}
    requirements = (ROOT / "requirements-benchmark.txt").read_text(encoding="utf-8")
    # Cài thư viện vào Worker nếu chưa có đúng phiên bản.
    probe = subprocess.run(["docker", "exec", "-e", f"PYTHONPATH={DEPS}", "spark-worker",
                            "python3", "-c", "import pandas, pyarrow, numpy; "
                            "assert (pandas.__version__, pyarrow.__version__, numpy.__version__) "
                            "== ('2.2.3', '18.1.0', '2.2.6')"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if probe.returncode:
        execute(["docker", "exec", "-i", "spark-worker", "python3", "-m", "pip", "install",
                 "--disable-pip-version-check", "--upgrade", "--target", DEPS, "-r", "/dev/stdin"],
                input=requirements, text=True)
    pandas_source = (ROOT / "src" / "benchmark_pandas.py").read_text(encoding="utf-8")
    # Bước 3. Chạy script Pandas trong Worker, kết quả lưu vào pandas.json.
    execute(["docker", "exec", "-i", "-e", f"PYTHONPATH={DEPS}", "spark-worker", "python3", "-",
             "--input", container_input, "--output", "/data/phase4/pandas.json",
             "--repeats", str(args.repeats)], input=pandas_source, text=True)

    # Bước 4. Chạy script Spark và chờ file kết quả spark.json.
    command = ["docker", "exec", "spark-master", "/opt/spark/bin/spark-submit",
               "--master", "spark://spark-master:7077", "/opt/spark/work-dir/benchmark_spark.py",
               "--input", container_input, "--output", "/data/phase4/spark.json",
               "--repeats", str(args.repeats), "--hold-ui", str(args.hold_ui)]
    log_path = OUTPUT / "spark.log"
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = perf_counter() + 900
            while not (OUTPUT / "spark.json").exists():
                if process.poll() is not None:
                    process.wait()
                    print(log_path.read_text(encoding="utf-8"), flush=True)
                    raise RuntimeError("Spark kết thúc trước khi ghi kết quả, xem data/phase4/spark.log")
                if perf_counter() > deadline:
                    raise TimeoutError("Spark chưa ghi kết quả sau 15 phút, xem spark.log")
                sleep(1)
            pandas = json.loads((OUTPUT / "pandas.json").read_text(encoding="utf-8"))
            spark = json.loads((OUTPUT / "spark.json").read_text(encoding="utf-8"))
            # Bước 5. So sánh kết quả, ghi bảng CSV và chờ chụp Spark UI.
            summarize(pandas, spark, environment, input_file)
            print("Spark UI đang ở http://localhost:4040. Tạo data/phase4/release_ui để đóng sớm.", flush=True)
            if process.wait() != 0:
                raise RuntimeError(f"Spark lỗi khi kết thúc, xem {log_path}")
        finally:
            if process.poll() is None:
                (OUTPUT / "release_ui").touch()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.terminate()
            if process.poll() is not None:
                (OUTPUT / "release_ui").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
