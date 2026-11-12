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
