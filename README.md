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

## 5. Phase 3 — Phân tích bằng Spark SQL

Phase 2 phải đã chạy thành công, Spark/HDFS đang hoạt động và có báo cáo tại
`/user/tlc/reports/latest/`. Phase 3 dùng **toàn bộ lô sạch** trên HDFS, không
lọc thêm tháng hoặc bỏ các chuyến có số khách null/RatecodeID 99.

```powershell
python run.py analyze --docker-context desktop-linux
```

Nếu không dùng Docker Desktop, có thể bỏ `--docker-context desktop-linux` để
dùng context hiện tại. Runner tự lấy bảng vùng từ TLC khi chưa có, chép code/SQL
vào container, chạy kiểm thử rồi chạy job phân tích trên Spark worker.
Không cần cài PySpark trên máy cá nhân. Khi đã kiểm thử đúng phiên bản code,
có thể thêm `--skip-tests` để chạy lại nhanh hơn.

Đầu ra mỗi lần chạy được đặt riêng:

```text
results/phase3/<run_id>/
  01_revenue_by_hour.csv
  02_top10_pickup_zones.csv
  03_tip_by_payment.csv
  04_weekday_vs_weekend.csv
  05_avg_distance_by_zone.csv
reports/phase3/<run_id>/         # Báo cáo, log, số đo, query plan và hash
reports/phase3/latest.json      # Chỉ cập nhật khi hoàn tất và kiểm chứng đạt
```

Quy ước KPI:

- Doanh thu: tổng `total_amount` được ghi nhận, giữ giá trị âm; thiếu/không hữu hạn
  được bỏ qua trong phép tổng và có số đếm riêng. Đơn vị USD, không bao gồm tip tiền mặt.
- Top 10: đếm chuyến theo vùng đón; khi bằng nhau xếp LocationID tăng dần.
  JOIN trái với lookup có khóa duy nhất để giữ vùng không tìm được tên.
- Tip: `100 × SUM(tip_amount) / SUM(fare_amount)` trên **cùng tập chuyến có tip
  không null và không âm**. Chỉ nhóm thẻ tín dụng có tỷ lệ; tiền mặt không được
  diễn giải là không tip vì TLC không ghi nhận tip tiền mặt. `tipped_trip_pct`
  là phần trăm chuyến có tip dương, khác với tỷ lệ tiền tip so với cước.
- Ngày thường/cuối tuần: kèm số ngày có dữ liệu và số chuyến trung bình mỗi ngày
  có dữ liệu, thay vì chỉ so sánh hai tổng có số ngày khác nhau.
- Quãng đường: trung bình theo vùng **đón khách**, đơn vị mile.

Phạm vi ngày/tháng thực tế được ghi trong báo cáo. Có phân vùng tháng 2 không
đồng nghĩa đã có đủ dữ liệu cả tháng 2. Không sửa dữ liệu sạch đầu vào.

Các file chính: `src/analyze.py`, `src/sql/phase3/*.sql`,
`scripts/run_phase3.py`, `tests/test_analytics.py`. Bộ test dùng dữ liệu tính tay
để kiểm tra 5 KPI, công thức tip có trọng số, dữ liệu null, số tiền âm, khóa vùng
trùng/không khớp, dữ liệu nhiều tháng và việc xuất đúng 5 CSV có header.

Tài liệu nguồn: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
và [từ điển dữ liệu Yellow Taxi](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

Máy Windows này hiện dùng cấu hình chạy riêng vì Docker chặn bind mount.
Xem [RUNTIME_MAIN_RESULT.md](RUNTIME_MAIN_RESULT.md) để khởi động lại cụm hiện tại.
Phase 3 sử dụng được cả cụm với bind mount thông thường và cụm dùng volume này;
runner xuất CSV về máy qua `docker cp`.

Có sử dụng AI để hỗ trợ triển khai và kiểm chứng phần Phase 3; nhóm cần đọc và
giải thích được công thức SQL, cách JOIN và các kiểm tra trước khi bàn giao.
