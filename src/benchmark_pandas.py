"""Phase 4: đo đọc toàn bộ một file Parquet bằng Pandas trong Worker."""

import argparse
import gc
import json
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
import pyarrow as pa


def main():
    """Đo thời gian Pandas đọc toàn bộ một file Parquet.

    Nhận --input, --output và --repeats từ terminal.
    Chạy thử một lần rồi đo theo số lần yêu cầu, lưu số liệu vào file JSON.
    """
    # 1. Nhận file đầu vào, nơi lưu kết quả và số lần đo.
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    pa.set_cpu_count(3)  # Worker được cấp 3 CPU trong docker-compose.yml.

    # 2. Đọc dữ liệu và đo thời gian của từng lần.
    records = []
    # Lần 0 làm nóng, các lần sau mới được dùng cho bảng so sánh.
    for trial in range(args.repeats + 1):
        gc.collect()
        started = perf_counter()
        frame = pd.read_parquet(args.input, engine="pyarrow")
        rows = len(frame)
        elapsed = perf_counter() - started
        records.append({
            "trial": trial, "warmup": trial == 0, "seconds": elapsed,
            "rows": rows, "columns": list(frame.columns),
            # Tính ngoài khoảng đo để đối chiếu nội dung hai engine.
            "total_amount_sum": round(float(frame["total_amount"].sum()), 2),
        })
        print(f"Pandas trial {trial}: {elapsed:.4f}s, {rows:,} rows", flush=True)
        del frame

    # 3. Lưu số liệu để script điều phối so sánh với Spark.
    result = {"engine": "Pandas", "versions": {
        "pandas": pd.__version__, "pyarrow": pa.__version__, "numpy": np.__version__,
    }, "input": args.input, "file_bytes": Path(args.input).stat().st_size,
        "arrow_cpu_threads": pa.cpu_count(), "records": records}
    Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
