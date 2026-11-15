# Kiểm chứng Phase 2 — TLC/main

Ngày chạy: **14/11/2026**. Nguồn: `sangvominh/TLC`, nhánh `main`, commit `45ff25cf48f3afa7704c705ad4b1daade400ec31`.

**Kết luận: code Phase 2 chạy thành công trên dữ liệu thật, ghi Parquet và kiểm chứng đạt.** Trên máy này, chạy trực tiếp `docker compose up -d` bị chặn chia sẻ thư mục Windows. Lần kiểm chứng dùng một cấu hình Docker riêng; code, quy tắc làm sạch và Compose gốc của nhóm được giữ nguyên.

## Kết quả

| Nội dung | Kết quả |
|---|---:|
| Dữ liệu đầu vào | 3.066.766 dòng |
| Dữ liệu sạch | 2.993.943 dòng |
| Dòng bị loại | 72.823 dòng |
| Số khách chuyển thành null, vẫn giữ chuyến | 49.797 dòng |
| Tổng số khách null trong đầu ra | 113.936 dòng |
| Chuyến giữ RatecodeID = 99 | 11.195 dòng |
| Dòng vi phạm khi đọc lại Parquet | 0 |
| Bộ kiểm thử của nhóm | 5/5 bài đạt |

`3.066.766 = 2.993.943 + 72.823`. Cả bản HDFS và bản trong volume dữ liệu chung đều được `verify.py` đọc lại, kiểm tra số dòng, kiểu integer, quy tắc và các cột tính thêm.

| Lệnh / bước | Thời gian đo từ bên ngoài |
|---|---:|
| Bộ test giả lập, gồm khởi động Spark | 74,056 giây |
| `python run.py test` | 38,312 giây |
| `python run.py` | 118,619 giây |
| `python run.py verify` | 47,446 giây |

Đây là số đo của một lần chạy trên máy này, không phải kết quả benchmark. Bộ test báo riêng thời gian chạy 5 bài là 66,702 giây. Ba ứng dụng đọc dữ liệu, làm sạch và kiểm chứng trên Spark Master đều ở trạng thái `FINISHED`; job làm sạch dùng executor trên `spark-worker`.

## Dữ liệu và minh chứng

- Dữ liệu sạch trên máy: [data/cleaned/](data/cleaned/). Đã thay đầu ra cũ bằng đầu ra của lần chạy này và kiểm tra SHA-256 sau khi xuất. Có hai phân vùng `pickup_month=1` và `pickup_month=2`.
- Dữ liệu sạch trên HDFS: `hdfs://namenode:8020/user/tlc/cleaned/`.
- Báo cáo HDFS: `/user/tlc/reports/phase2_quality_20261114T113753710731Z/`, bản mới nhất tại `/user/tlc/reports/latest/`.
- [Báo cáo chất lượng JSON](.tools/runtime-main-20261114/quality_report.json).
- [Log làm sạch](.tools/runtime-main-20261114/phase2-clean.log) và [số đo của lệnh](.tools/runtime-main-20261114/phase2-clean.result.json).
- [Log kiểm chứng riêng](.tools/runtime-main-20261114/phase2-verify.log).
- [Log 5 bài test](.tools/runtime-main-20261114/synthetic-tests.log).
- [Trạng thái Spark Master sau khi chạy](.tools/runtime-main-20261114/spark-master.json).
- [Manifest đầu ra](.tools/runtime-main-20261114/cleaned-manifest.json), [hash nguồn](.tools/runtime-main-20261114/source-hashes.json), [hash code trong container](.tools/runtime-main-20261114/container-source-hashes.txt).

## Cách chạy lại trên máy này

Docker Desktop phải mở. Chạy tại `C:\Users\PC\bigdata`.

Lần kiểm chứng dùng [compose.runtime.yml](.tools/runtime-main-20261114/compose.runtime.yml) thay bind mount bằng named volume. Spark Master và Worker cùng dùng volume `bigdata_main_runtime_data`; hai volume HDFS được giữ lại. Script chuẩn bị chép đúng source/config từ thư mục dự án vào container.

Nếu các container hiện tại vẫn còn, chỉ cần khởi động lại:

```powershell
$env:DOCKER_CONTEXT = 'desktop-linux'
docker compose --project-name bigdata -f docker-compose.yml -f .tools/runtime-main-20261114/compose.runtime.yml start
```

Nếu cần tạo lại container hoặc cập nhật source/config trong container:

```powershell
$env:DOCKER_CONTEXT = 'desktop-linux'
docker compose --project-name bigdata -f docker-compose.yml -f .tools/runtime-main-20261114/compose.runtime.yml create --force-recreate
python .tools/runtime-main-20261114/prepare_runtime.py
```

`prepare_runtime.py` đợi Spark có worker và HDFS hết safe mode. Nếu chỉ dùng `start`, cũng cần đợi cụm sẵn sàng trước khi nạp hoặc xử lý dữ liệu.

Sau đó chạy từng lệnh, chờ lệnh trước hoàn tất:

```powershell
docker exec namenode bash /scripts/ingest_parquet.sh /local-data/raw/yellow_tripdata_2023-01.parquet /user/tlc/raw
python run.py test
python run.py
python run.py verify
```

Cấu hình thử ghi bản Parquet `file:///data/cleaned/` vào named volume, không tự đồng bộ với thư mục Windows. Lần kiểm chứng này đã xuất bản đó về `data/cleaned/`. Nếu chạy lại và muốn lấy kết quả mới ra máy, xuất vào một thư mục mới, ví dụ:

```powershell
docker cp spark-master:/data/cleaned .\phase2-output-new
```

Chọn tên thư mục chưa tồn tại để tránh trộn kết quả các lần chạy. Không quét đồng thời nhiều bản dữ liệu khi làm Phase 3.

Để dừng bốn container:

```powershell
docker compose --project-name bigdata -f docker-compose.yml -f .tools/runtime-main-20261114/compose.runtime.yml stop
```

Giao diện: HDFS tại `http://localhost:9870`, Spark Master tại `http://localhost:8080`.

## Giới hạn và ghi nhận

- Lỗi khởi động trực tiếp nằm ở quyền chia sẻ thư mục Docker trên Windows, xem [compose-up.log](.tools/runtime-main-20261114/compose-up.log). Không kết luận README hiện tại chạy trực tiếp được trên máy này.
- Bộ kiểm thử giả lập chạy với `local[2]` trong container; các lệnh đọc dữ liệu thật, làm sạch và verify chạy với `spark://spark-master:7077`.
- Lần gọi test đầu tiên sai đường dẫn trong container và chưa thực thi bài test; đã sửa đường dẫn gọi rồi chạy đạt 5/5. Có cảnh báo socket trong bộ test nhưng mã thoát là 0. Bộ thu log cũng đã chỉnh encoding UTF-8 sau lỗi in tiếng Việt trên console Windows; job làm sạch đã trả mã 0 và log kết quả đầy đủ.
- Không sửa code Phase 2 của nhóm, không push GitHub và chưa triển khai Phase 3. Các file mới trong `.tools/runtime-main-20261114/` chỉ phục vụ cấu hình thử, log và minh chứng.
- Có sử dụng AI để hỗ trợ chạy kiểm chứng và lập tài liệu này.
