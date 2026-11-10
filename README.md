# Dự án Big Data NYC TLC

Xây dựng quy trình xử lý dữ liệu chuyến đi Yellow Taxi bằng **Docker Compose, Hadoop HDFS và Apache Spark**. Dữ liệu đang dùng là lô tháng 01/2023; Phase 2 áp dụng chính sách làm sạch theo hướng dẫn v2.

Đã hoàn thành và kiểm chứng Phase 1–2. Phase 3 hiện có [kế hoạch phân tích](PHASE3_PLAN.md), chưa triển khai job và kết quả KPI.

## 1. Chuẩn bị

- Mở Docker Desktop và dùng chế độ Linux containers.
- Docker Compose từ **2.24.4** để sử dụng `compose.windows.yml`.
- Chạy các lệnh PowerShell trong thư mục chứa `docker-compose.yml`.
- Lần đầu cần kết nối mạng để Docker tải các image.

Các tệp đầu vào nằm trong `data/raw/`:

| Tệp | Nội dung |
|---|---|
| `yellow_tripdata_2023-01.parquet` | 3.066.766 chuyến đi, 19 cột nguồn |
| `taxi_zone_lookup.csv` | 265 mã vùng và tên vùng/quận |
| `manifest-2023-01.json` | Đường dẫn tải, kích thước và SHA-256 của dữ liệu gốc |

Nếu thiếu đầu vào, tải theo các URL trong [manifest nguồn](data/raw/manifest-2023-01.json). Bộ dữ liệu chính thức được công bố tại [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## 2. Chạy lần đầu trên Windows

**Bước 1 — Tạo và khởi động cụm:**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\start_windows.ps1
```

Script tạo các container, chép cấu hình/source/dữ liệu bằng `docker cp`, rồi chờ Spark Worker và HDFS DataNode đăng ký. Cách này hỗ trợ máy gặp lỗi `user declined directory sharing` của Docker Desktop.

`start_windows.ps1` tạo lại container khi chạy lại; các named volume HDFS được giữ. Chỉ chạy script này khi không có job đang xử lý. Khi chỉ cần bật lại cụm đã tạo, dùng lệnh `start` ở mục 3.

**Bước 2 — Đưa dữ liệu gốc lên HDFS:**

```powershell
docker --context desktop-linux exec namenode bash /scripts/upload_to_hdfs.sh /local-data/raw/yellow_tripdata_2023-01.parquet /user/tlc/raw
```

Script bỏ qua nếu file đích đã có cùng kích thước; đây không phải phép so sánh hash nội dung. Kiểm tra danh sách file:

```powershell
docker --context desktop-linux exec namenode hdfs dfs -ls /user/tlc/raw
```

**Bước 3 — Chạy Phase 2 v2:**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1
```

Script tự chép source vào Spark, chạy kiểm thử, upload bảng mã vùng, chạy job làm sạch, kiểm tra đầu ra và xuất Parquet sạch về máy. Với dữ liệu hiện tại, kết thúc thành công sẽ có:

```text
PHASE 2 PASSED: input=3066766, cleaned=2993943, rejected=72823
```

## 3. Dừng và chạy lại

Dùng cùng tên project `bigdata` mà script khởi động đã thiết lập, kể cả khi thư mục clone có tên `TLC`:

```powershell
# Xem trạng thái container
docker --context desktop-linux compose --project-name bigdata ps

# Dừng cụm
docker --context desktop-linux compose --project-name bigdata stop

# Bật lại cụm đã tạo
docker --context desktop-linux compose --project-name bigdata start
```

Sau khi bật lại, chạy `run_phase2.ps1` như mục 2. Script chờ tối đa **180 giây** để có worker ALIVE, DataNode hoạt động và HDFS ra khỏi safe mode. Trạng thái container `Started` chưa bảo đảm dịch vụ đã sẵn sàng; tiến trình chờ được ghi vào `cluster-readiness.jsonl`.

Chỉ kiểm tra cụm, không chạy làm sạch:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1 -CheckReadyOnly
```

Nếu khởi động chậm, thêm `-ReadyTimeoutSeconds 300`. Nếu code không đổi và kiểm thử đã đạt, có thể bỏ qua bộ kiểm thử khi chạy lại:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_phase2.ps1 -SkipTests
```

`stop/start` giữ dữ liệu HDFS. `down` xóa container và mạng nhưng giữ named volume nếu không thêm `-v`; dùng `down -v` sẽ xóa cả các volume đó.

Nếu Docker Desktop chia sẻ thư mục bình thường, có thể tạo cụm bằng Compose gốc thay cho helper Windows:

```powershell
docker --context desktop-linux compose --project-name bigdata up -d
```

## 4. Kiến trúc và cách xử lý

| Thành phần | Vai trò |
|---|---|
| NameNode | Quản lý thông tin hệ thống tệp HDFS |
| DataNode | Lưu các block dữ liệu |
| Spark Master | Quản lý tài nguyên cụm Spark |
| Spark Worker | Cấp tài nguyên thực thi các tác vụ Spark |

Driver của job được chạy trong container `spark-master`, còn các tác vụ dữ liệu được thực thi trên `spark-worker`.

```text
Parquet gốc trên HDFS + bảng mã vùng
  ├─ Nhánh audit: gắn lý do/cờ → thống kê → lưu rejected với giá trị nguồn
  └─ Nhánh sạch: lọc sớm → chuẩn hóa/sửa null → thêm đặc trưng
      ↓
Ghi Parquet lên HDFS → đọc lại kiểm chứng → báo cáo JSON
      ↓
Chép Parquet sạch về data/cleaned/ và xuất CSV, log, manifest hash
```

Nhánh audit đọc toàn bộ đầu vào để biết số dòng vi phạm từng quy tắc. Nhánh sạch áp dụng một điều kiện lọc tổng hợp trực tiếp trên nguồn trước khi sửa giá trị/cache. Chỉ các điều kiện được hỗ trợ mới được đẩy xuống bộ đọc Parquet; HDFS không tự thực hiện lọc SQL. Execution plan thực tế được lưu trong `cleaned-explain.txt`.

Hai nhánh có thể quét đầu vào riêng. Dữ liệu trung gian cache bằng `DISK_ONLY` để giảm áp lực RAM. Chỉ thống kê tổng hợp, bảng vùng nhỏ và tối đa 5 dòng mỗi nhóm mẫu được đưa về driver; không tải toàn bộ TLC vào Python/Pandas. Phần làm sạch dùng hàm Spark có sẵn, không dùng Python UDF.

## 5. Chính sách làm sạch v2

### Điều kiện loại dòng

| Nội dung | Điều kiện giữ lại |
|---|---|
| Giờ đón/trả | Đọc được và giờ trả sau giờ đón |
| Thời lượng | Lớn hơn 0, tối đa 360 phút |
| Năm đón | Năm 2023 |
| Quãng đường | Hữu hạn, lớn hơn 0 và tối đa 100 dặm |
| Tốc độ | Tối đa 80 mph, tính bằng `try_divide` để tránh lỗi chia cho 0 |
| Giá cước | `fare_amount` hữu hạn, lớn hơn 0 và vẫn dương sau làm tròn cent |
| Mã vùng | Cả hai LocationID chuẩn hóa được thành số nguyên khác null |

Tham số tháng nhận diện **lô file đầu vào**, không phải bộ lọc tháng đón khách. Các chuyến đủ điều kiện thuộc tháng khác trong năm 2023 vẫn được giữ; giờ trả có thể sang tháng tiếp theo.

### Sửa giá trị và gắn cảnh báo

- `passenger_count` bằng 0 hoặc >6 được đưa về null; chuyến vẫn được giữ nếu đạt các điều kiện loại dòng. Null sẵn giữ nguyên. Giá trị âm, lẻ, không hữu hạn hoặc không đọc được cũng chuyển null có cờ; các số nguyên 1–6 được giữ.
- `RatecodeID` chuyển integer; giữ mã 99 để Phase 3 quyết định cách sử dụng theo KPI. Giá trị không cast được chuyển null có cờ.
- Tổng tiền và tip âm được giữ kèm cảnh báo theo chính sách v2; tiền tùy chọn lỗi chuyển null. Không tự điền tip thiếu thành 0 hay số khách thiếu thành 1.
- Mã vùng không xác định nhưng khác null, kể cả 264/265, được giữ kèm cảnh báo. Mã thanh toán không biết cũng được gắn cảnh báo.
- Tiền dùng `decimal(18,2)` và làm tròn cent; thời gian giữ `timestamp_ntz` theo giờ nguồn. Không tự xóa dòng giống nhau vì không có khóa chuyến đi bảo đảm duy nhất.

Thêm 5 cột: `trip_duration_min`, `pickup_date`, `pickup_hour`, `pickup_month`, `is_weekend`. Giữ tên các cột nguồn và bổ sung `source_file`, `warning_flags`, `transformation_flags` để truy vết.

Đây là quy tắc chọn dữ liệu cho phân tích, không phải kết luận mọi bản ghi bị loại đều gian lận. Các phép cast phòng vệ và làm tròn tiền được mô tả chi tiết trong [PHASE2_RESULT.md](PHASE2_RESULT.md).

## 6. Kết quả và minh chứng

| Chỉ tiêu đã kiểm chứng | Số dòng |
|---|---:|
| Đầu vào | 3.066.766 |
| Dữ liệu sạch | 2.993.943 |
| Rejected | 72.823 — khoảng 2,37% |
| Dòng sạch được sửa số hành khách | 49.797 |
| Dòng sạch có số hành khách null, kể cả null sẵn | 113.936 |

Đếm chuyến bằng `COUNT(*)`, không dùng `COUNT(passenger_count)` để tránh bỏ sót chuyến có số khách null.

| Vị trí | Nội dung |
|---|---|
| HDFS `/user/tlc/cleaned/year=2023/month=01/run_id=<run_id>/` | Parquet sạch |
| HDFS `/user/tlc/rejected/year=2023/month=01/run_id=<run_id>/` | Giá trị nguồn của dòng bị loại, kèm lý do/cờ |
| HDFS `/user/tlc/quality/year=2023/month=01/run_id=<run_id>/quality_report.json` | Báo cáo chất lượng |
| `data/cleaned/year=2023/month=01/run_id=<run_id>/` | Bản Parquet sạch xuất về máy |
| `reports/phase2/<run_id>/` | JSON, CSV và log của từng lượt |

Các tệp nên mở:

- [latest.json](reports/phase2/latest.json): báo cáo của lượt thành công mới nhất.
- [latest-export.json](reports/phase2/latest-export.json): đường dẫn và hash bản Parquet cục bộ mới nhất.
- `rule-counts.csv`: số dòng trước bước / bị loại tại bước / sau bước. Mỗi dòng chỉ bị tính loại ở bước đầu tiên không đạt.
- `transformation-counts.csv`: số giá trị cần sửa trong đầu vào, tập sạch và tập rejected. Các số này không phải số dòng bị xóa và có thể chồng lặp.
- `local-output-manifest.json`: kích thước, SHA-256 và thông tin kiểm tra bản Parquet xuất về máy.
- `spark-cleaning.log`, `synthetic-tests.log`, `cluster-readiness.jsonl`: log xử lý, kiểm thử và chờ cụm sẵn sàng.

Mỗi lượt có thư mục riêng, không ghi đè kết quả cũ. `latest.json` chỉ cập nhật khi chạy thành công. Thư mục `year/month` chỉ lô nguồn; cột `pickup_month` phản ánh tháng thực của chuyến.

Phase 3 chỉ đọc **một đường dẫn `outputs.cleaned` đã chốt** hoặc bản xuất tương ứng. Quét gộp nhiều run_id sẽ tính lặp chuyến. Báo cáo/ZIP của chính sách v1 được giữ làm lịch sử, không thay thế đầu ra v2.

## 7. Kiểm thử và kiểm chứng

- Bộ kiểm thử v2 có **8 kiểm thử trên 47 dòng giả lập**, kết quả dự kiến 24 sạch và 23 rejected. Dữ liệu giả lập được xử lý riêng, không trộn vào TLC.
- Job kiểm tra `đầu vào = sạch + rejected`, đối chiếu bảng tuần tự, đọc lại Parquet và kiểm tra số dòng/schema/đặc trưng, áp dụng lại điều kiện trên tập sạch và kiểm tra số hành khách null được giữ.
- Runner kiểm tra `_SUCCESS`, header/footer Parquet và ghi hash bản xuất về máy. Nếu cụm chưa sẵn sàng thì không cố chép báo cáo Python chưa được tạo.
- Thời gian job gồm thống kê, ghi và đọc lại kiểm chứng; chưa tính kiểm thử giả lập, upload CSV và xuất dữ liệu ra Windows. Đây chưa phải benchmark Spark/Pandas.

Kiểm tra riêng kết nối Spark/HDFS của Phase 1:

```powershell
docker --context desktop-linux exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/test_spark.py
```

Lệnh này đọc Parquet thô, in schema, mẫu và số dòng. Xem [kết quả runtime Phase 1](RUNTIME_RESULT.md) và [minh chứng Phase 2](PHASE2_RESULT.md).

## 8. Các file chính

| File | Vai trò |
|---|---|
| `docker-compose.yml`, `config/*.xml` | Dịch vụ Docker và cấu hình Hadoop |
| `compose.windows.yml`, `scripts/start_windows.ps1` | Dựng cụm khi Docker Desktop không chia sẻ thư mục host |
| `scripts/upload_to_hdfs.sh` | Đưa dữ liệu gốc lên HDFS |
| `scripts/cluster_readiness.ps1` | Kiểm tra worker, DataNode và safe mode; chờ dịch vụ sẵn sàng |
| `scripts/run_phase2.ps1` | Điều phối kiểm thử, chạy job và xuất dữ liệu/báo cáo |
| `src/cleaning_policy_v2.py` | Quy tắc lọc, sửa giá trị, tạo đặc trưng và đếm thống kê |
| `src/clean_data.py` | Đọc dữ liệu, gọi xử lý, ghi Parquet và kiểm chứng |
| `src/test_cleaning_v2.py`, `src/test_cleaning_rules.py` | Bộ kiểm thử v2 và điểm khởi chạy |
| `src/test_spark.py` | Kiểm tra đọc dữ liệu HDFS của Phase 1 |
| `scripts/test_cluster_readiness.ps1` | Kiểm thử việc chờ dịch vụ, lỗi HTTP tạm thời và hết thời gian chờ |

Trong chế độ Windows không bind mount, thay đổi trên host không tự đồng bộ vào container. Runner Phase 2 tự chép source; nếu chạy thủ công, chép trước bằng:

```powershell
docker --context desktop-linux cp .\src\. spark-master:/opt/spark/work-dir
```

## 9. Giao diện theo dõi

| Giao diện | Địa chỉ |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark Master — worker và trạng thái ứng dụng | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |
| Spark UI của job — khi job đang chạy | http://localhost:4040 |

## 10. Nguồn tham khảo và giai đoạn tiếp theo

Phase 1 kế thừa [sangvominh/TLC](https://github.com/sangvominh/TLC/tree/708b15f246edea4684598bf87954aeb6c5c8542b), commit `708b15f246edea4684598bf87954aeb6c5c8542b`. Cơ chế Windows tham khảo [quy tắc gộp Compose](https://docs.docker.com/reference/compose-file/merge/) và [docker cp](https://docs.docker.com/reference/cli/docker/container/cp/).

[Kế hoạch Phase 3](PHASE3_PLAN.md) chọn dùng toàn bộ tập sạch v2, không lọc thêm tháng. Kế hoạch đã định nghĩa 5 KPI Spark SQL, JOIN bảng vùng, 5 CSV và cách kiểm chứng; chưa có job hay kết quả KPI Phase 3. Benchmark và đóng gói pipeline đầy đủ thuộc các giai đoạn tiếp theo.

**Ghi nhận AI:** Codex hỗ trợ đáng kể việc viết code, kiểm thử, kiểm chứng và tài liệu. Nhóm cần rà soát, hiểu và giải thích được phần nộp, đồng thời ghi nhận việc dùng AI trong báo cáo học phần.
