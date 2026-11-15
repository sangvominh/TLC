"""Chạy các bước Spark trong Docker bằng lệnh Python trên máy cá nhân."""

import argparse  # Đọc tác vụ từ lệnh người dùng nhập.
import subprocess  # Thực thi lệnh Docker từ Python.
import sys
from pathlib import Path


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    # Chọn clean, test hoặc verify; mặc định là clean.
    parser = argparse.ArgumentParser(description="Chạy Spark trong Docker.")
    parser.add_argument("task", nargs="?", default="clean",
                        choices=["clean", "test", "verify", "analyze"], help="Bước cần chạy (mặc định: clean).")
    parser.add_argument("--docker-context", default=None)
    parser.add_argument("--skip-tests", action="store_true", help="Phase 3: bỏ qua test khi đã kiểm thử cùng phiên bản.")
    args = parser.parse_args()  # Ví dụ: python run.py verify → task = "verify".
    if args.task == "analyze":
        command = [sys.executable, str(Path(__file__).resolve().parent / "scripts/run_phase3.py")]
        if args.docker_context:
            command += ["--docker-context", args.docker_context]
        if args.skip_tests:
            command.append("--skip-tests")
        return subprocess.call(command)
    # Tạo lệnh chạy file tương ứng trong container Spark.
    command = [
        "docker", "exec", "spark-master", "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        f"/opt/spark/work-dir/{args.task}.py",
    ]
    if args.docker_context:
        command[1:1] = ["--context", args.docker_context]
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
