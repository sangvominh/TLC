# TLC — Spark và HDFS

## 1. Cài đặt

- Cài Docker Desktop và mở ứng dụng trước khi chạy dự án.
- Cài Python 3 và bảo đảm dùng được lệnh `python`.
- Các lệnh bên dưới dùng Git Bash trên Windows. Có thể dùng PowerShell; riêng lệnh có `MSYS_NO_PATHCONV=1` thì bỏ tiền tố này.
- Không cần cài Spark, Hadoop hoặc PySpark trên máy cá nhân; các thành phần này chạy trong Docker.

Kiểm tra các công cụ:

```bash
python --version
docker --version
docker compose version
```

Đặt file `yellow_tripdata_2023-01.parquet` vào `data/raw/`.

## 2. Hướng dẫn chạy

Mở terminal tại thư mục gốc dự án, nơi chứa `run.py` và `docker-compose.yml`.

**Bước 1: Khởi động Spark và HDFS**

```bash
docker compose up -d
docker compose ps
```

**Bước 2: Nạp dữ liệu gốc lên HDFS**

```bash
MSYS_NO_PATHCONV=1 docker exec namenode bash /scripts/ingest_parquet.sh /local-data/raw/yellow_tripdata_2023-01.parquet /user/tlc/raw
```

Nếu dùng file khác, thay tên file trong lệnh trên.

**Bước 3: Kiểm tra kết nối và chạy làm sạch**

```bash
python run.py test
python run.py
```

Chạy từng lệnh và chờ hoàn tất. Khi làm sạch thành công, terminal hiện `XỬ LÝ HOÀN TẤT`.

**Bước 4: Kiểm chứng lại kết quả**

```bash
python run.py verify
```

Xem danh sách dữ liệu sạch và báo cáo mới nhất:

```bash
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -ls /user/tlc/cleaned/
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -cat '/user/tlc/reports/latest/part-*'
```

Parquet đầu ra nằm trong `data/cleaned/` trên máy cá nhân, đồng thời có một bản
trên HDFS tại `/user/tlc/cleaned/`. Mỗi lần chạy làm sạch sẽ ghi đè cả hai bản.
Spark Master và Worker cùng gắn thư mục `data/` vào `/data` để ghi/đọc các file này.
Sau khi cập nhật cấu hình Docker, chạy lại `docker compose up -d` trước khi xử lý.

**Dừng hệ thống**

```bash
docker compose down
```

Giao diện theo dõi: [HDFS NameNode](http://localhost:9870) · [Spark Master](http://localhost:8080).

## 3. Cấu trúc thư mục

```text
TLC/
├── config/                     # Cấu hình Hadoop/HDFS
│   ├── core-site.xml           # Địa chỉ HDFS mặc định
│   └── hdfs-site.xml           # Thư mục lưu trữ và số bản sao dữ liệu
├── data/
│   ├── raw/                    # File Parquet gốc trên máy cá nhân
│   └── cleaned/                # Parquet đã làm sạch, sẵn sàng cho Phase 3
├── scripts/
│   └── ingest_parquet.sh       # Tải dữ liệu gốc lên HDFS
├── src/
│   ├── clean.py                # Làm sạch, chuẩn hóa và ghi dữ liệu cùng báo cáo
│   ├── test.py                 # Kiểm tra Spark đọc được dữ liệu gốc trên HDFS
│   └── verify.py               # Kiểm chứng dữ liệu sạch đã ghi
├── tests/
│   └── test_clean.py           # Kiểm thử code bằng dữ liệu mẫu có các trường hợp lỗi
├── run.py                      # Gọi các tác vụ clean/test/verify trong Docker
├── docker-compose.yml         # Khởi tạo NameNode, DataNode, Spark Master và Worker
└── README.md                   # Hướng dẫn sử dụng dự án
```

Dữ liệu trong HDFS được lưu riêng với thư mục trên máy cá nhân:

```text
/user/tlc/raw/                  # Dữ liệu gốc đã tải lên
/user/tlc/cleaned/              # Dữ liệu Parquet sau khi làm sạch
/user/tlc/reports/              # Báo cáo của từng lần chạy
/user/tlc/reports/latest/       # Báo cáo mới nhất
```

## 4. Bàn giao Phase 2 → Phase 3

- `src/clean.py`: làm sạch dữ liệu, tạo Parquet và báo cáo chất lượng.
- `data/cleaned/`: dữ liệu Parquet đã làm sạch; Phase 3 dùng dữ liệu này.
- `src/verify.py`: kiểm tra dữ liệu đầu ra của Phase 2.
- `run.py`: chạy các tác vụ `test`, `clean` và `verify`.

## 5. Bàn giao Phase 3 → Phase 4, 5

Phase 3 nhận dữ liệu đã làm sạch từ Phase 2 để phân tích. Kết quả bàn giao cho
Phase 4 và Phase 5 nên lấy từ cùng một bộ dữ liệu Parquet đã được kiểm chứng:

- **Nguồn dữ liệu chuẩn:** `data/cleaned/` trên máy cá nhân hoặc
  `/user/tlc/cleaned/` trên HDFS. Dùng HDFS khi xử lý bằng Spark trong Docker.
- **Điều kiện sử dụng:** chạy `python run.py verify` và chỉ tiếp tục khi kiểm
  chứng báo thành công. Mỗi lần chạy `python run.py` sẽ ghi đè dữ liệu sạch cũ.
- **Các cột hỗ trợ phân tích:** `trip_duration_min`, `pickup_date`,
  `pickup_hour`, `pickup_month`, `is_weekend`; dữ liệu chuyến đi gốc đã được
  lọc và chuẩn hóa vẫn được giữ trong Parquet.
- **Báo cáo chất lượng:** `/user/tlc/reports/latest/` trên HDFS; báo cáo gồm
  số dòng đầu vào/đầu ra, số dòng bị loại theo quy tắc, các chuyển đổi và kết
  quả kiểm chứng. Lưu hoặc trích xuất báo cáo này cùng kết quả phân tích để
  truy vết phiên dữ liệu đã dùng.
- **Nguyên tắc dùng chung:** Phase 4 và Phase 5 cần ghi rõ nguồn dữ liệu, thời
  điểm chạy và các bộ lọc/phép biến đổi bổ sung; không sửa trực tiếp dữ liệu
  trong `data/cleaned/` hoặc `/user/tlc/cleaned/`.

## 6. Phase 4 — Đo lường và bằng chứng

### Chạy benchmark

Mở Docker Desktop và terminal tại thư mục gốc dự án, rồi chạy

```bash
python run.py benchmark
```

Chương trình tự chuẩn bị thư viện, đo file tháng `data/raw/yellow_tripdata_2023-01.parquet`
và lưu kết quả vào `data/phase4/`. Lần cài thư viện cần Internet.

### Cách làm

1. **Đo Pandas** đọc file Parquet, đếm số dòng và ghi thời gian.
2. **Đo Spark** đọc cùng file, dùng `persist()` và `count()` để nạp đủ cột rồi ghi thời gian.
3. **So sánh** kiểm tra số dòng, tên cột và tổng tiền khớp nhau, sau đó tạo bảng CSV.

Mỗi công cụ chạy thử một lần rồi đo 3 lần. Bảng so sánh có thời gian trung bình
và trung vị (giá trị ở giữa khi sắp xếp các lần đo).

Có thể chọn file, số lần đo và số giây giữ UI bằng lệnh

```bash
python run.py benchmark --input data/raw/yellow_tripdata_2023-01.parquet --repeats 3 --hold-ui 600
```

### Chụp Spark UI

Khi terminal báo đo xong, mở [Spark UI](http://localhost:4040) và chụp ba tab

- **Jobs** xem thời gian chạy, lưu `screenshots/01_jobs.jpg`.
- **Stages** xem số task, lưu `screenshots/02_stages.jpg`.
- **Storage** xem số partition, lưu `screenshots/03_storage.jpg`.

Lưu ảnh trong `data/phase4/`. UI mở thêm 10 phút sau khi đo, hoặc theo `--hold-ui`.
Chụp ảnh lại khi chạy một phiên đo mới. Để đóng sớm, mở terminal khác và chạy

```bash
python -c "from pathlib import Path; Path('data/phase4/release_ui').touch()"
```

### File bàn giao

| File | Nội dung |
|---|---|
| `data/phase4/benchmark_runs.csv` | Thời gian từng lần, gồm lần làm nóng, số dòng và partition |
| `data/phase4/comparison.csv` | Trung bình, trung vị, min, max và tỷ số Pandas/Spark |
| `data/phase4/report.md` | Bảng so sánh, cách đo và nhận xét theo số liệu thực tế |
| `data/phase4/environment.json` | SHA-256 file, phiên bản thư viện và giới hạn CPU/RAM container |
| `data/phase4/pandas.json`, `spark.json` | Kết quả gốc của hai script |
| `data/phase4/spark_ui.json`, `spark.log` | API Spark UI và log để truy vết job/stage/task |
| `data/phase4/screenshots/` | Ảnh chụp Spark UI của phiên đo |

### Chức năng source

- `scripts/benchmark.py` gọi hai script đo rồi tạo bảng so sánh và báo cáo.
- `src/benchmark_pandas.py` đo thời gian Pandas đọc file trong Worker.
- `src/benchmark_spark.py` đo Spark và giữ UI để chụp bằng chứng.

Pandas chạy trong Worker với giới hạn 3 CPU và 4 GiB RAM. Spark dùng executor
trên Worker và thêm Driver trên Master, có giới hạn 0,5 CPU và 2 GiB RAM.
Thời gian đo không gồm cài thư viện, import và khởi tạo SparkSession.
Khởi tạo SparkSession được ghi riêng trong `spark.json` và báo cáo.
Chạy Pandas trước Spark và giữ cache hệ điều hành sau lần chạy thử.
Tỷ số Pandas/Spark lớn hơn 1 nghĩa là Spark nhanh hơn.
Với file nhỏ, Pandas có thể nhanh hơn do Spark tốn thời gian điều phối và cache.
