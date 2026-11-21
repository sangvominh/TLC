"""Chạy các bước Spark trong Docker bằng lệnh Python trên máy cá nhân."""

import argparse  # Đọc tác vụ từ lệnh người dùng nhập.
from pathlib import Path
import subprocess  # Thực thi lệnh Docker từ Python.
import sys


def main():
    """Đọc tác vụ từ terminal và gọi script tương ứng.

    Với benchmark, chuyển các tùy chọn sang scripts/benchmark.py.
    Trả về mã kết thúc để terminal biết lệnh thành công hay có lỗi.
    """
    # Chọn tác vụ cần chạy, mặc định là clean.
    parser = argparse.ArgumentParser(description="Chạy Spark trong Docker.")
    parser.add_argument("task", nargs="?", default="clean",
                        choices=["clean", "test", "verify", "analyze", "benchmark"], help="Bước cần chạy (mặc định: clean).")
    args, extra = parser.parse_known_args()
    if extra and args.task != "benchmark":
        parser.error("Tham số bổ sung chỉ dùng cho benchmark.")
    # Tạo lệnh chạy file tương ứng trong container Spark.
    command = [
        "docker", "exec", "spark-master", "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        f"/opt/spark/work-dir/{args.task}.py",
    ]
    if args.task == "benchmark":
        command = [sys.executable, str(Path(__file__).parent / "scripts" / "benchmark.py"), *extra]
    try:
        # Chờ Spark chạy xong và trả về mã kết quả (0 thường là thành công).
        return subprocess.call(command)
    except FileNotFoundError:
        # Máy chưa tìm thấy lệnh docker.
        print("Không tìm thấy Docker. Hãy cài Docker Desktop và mở lại terminal.")
        return 1
    except KeyboardInterrupt:
        return 130  # Người dùng ngắt lệnh bằng Ctrl+C.


if __name__ == "__main__":
    # Chạy main khi mở file trực tiếp và chuyển mã kết quả về terminal.
    raise SystemExit(main())
