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

## 5. Phase 3: tính KPI

Sau khi có `data/cleaned/` từ Phase 2, chạy:

```bash
python3 run.py kpi
```

Kết quả được ghi đè trong `data/kpi/` dưới dạng 5 file CSV có tiêu đề:

- `revenue_by_hour.csv`: số chuyến và tổng `total_amount` theo giờ đón.
- `top_10_pickup_zones.csv`: 10 vùng đón nhiều chuyến nhất.
- `tip_rate_by_payment.csv`: tổng tip chia tổng `fare_amount` theo phương thức thanh toán, tính phần trăm. Chỉ các chuyến có tip không âm và cước dương được dùng trong phép chia; số chuyến hợp lệ được ghi riêng. Tip tiền mặt có thể không được ghi nhận.
- `weekday_vs_weekend.csv`: số chuyến, tổng doanh thu, doanh thu trung bình mỗi chuyến và quãng đường trung bình; cuối tuần gồm thứ Bảy và Chủ nhật. Tổng phụ thuộc số ngày có trong dữ liệu.
- `avg_distance_by_zone.csv`: quãng đường trung bình theo vùng đón, đơn vị dặm.

Tên vùng và borough lấy từ `data/reference/taxi_zone_lookup.csv` của TLC. Các mã không khớp được giữ với tên `Unknown`. Mọi chuyến đã làm sạch, kể cả `RatecodeID = 99`, được tính. Doanh thu là `total_amount` (bao gồm tip và phụ phí), không phải lợi nhuận. Dữ liệu sạch đã có trong repo chỉ phản ánh những tháng được xử lý; chạy Phase 2 với dữ liệu mong muốn trước khi tính KPI. Kết quả là dữ liệu sinh ra và không được đưa vào Git.
