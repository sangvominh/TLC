# Phase 3 — Phân tích dữ liệu sạch bằng Spark SQL

Ngày kiểm chứng: **14/11/2026**. Đã triển khai và chạy thành công trên đầu ra Phase 2 của `sangvominh/TLC/main`, nền source commit `45ff25c`.

Run: `p3-20261114T162445Z-acf6c3`. Ứng dụng Spark `app-20261114162658-0000`, trạng thái **FINISHED**, executor trên `spark-worker`. Đã xuất đúng **5 file CSV có header**, đọc lại trong Spark và trên máy cá nhân để kiểm chứng.

## 1. Đầu vào và phạm vi

- Nguồn: `hdfs://namenode:8020/user/tlc/cleaned/`, **2.993.943 chuyến**.
- Báo cáo Phase 2 nguồn: `phase2_v2_2023`, thời điểm `2026-11-14T11:37:53.611869+00:00`.
- Khoảng ngày đón thực tế: **01/01/2023–01/02/2023**, 32 ngày có dữ liệu.
- Tháng 1: **2.993.933 chuyến**; tháng 2: **10 chuyến**. Đây là lô nguồn tháng 1 có một số dòng thuộc tháng 2, không đại diện dữ liệu đầy đủ của hai tháng hoặc cả năm.
- Giữ toàn bộ lô sạch, gồm **113.936 chuyến có số khách null** và **11.195 chuyến RatecodeID=99**; không lọc thêm tháng.
- Lookup TLC: `data/reference/taxi_zone_lookup.csv`, **265 khóa duy nhất**. Nguồn và SHA-256 được lưu trong [manifest](data/reference/manifest.json).
- JOIN trái giữ toàn bộ chuyến. Có **38.316 chuyến thuộc vùng Unknown/Outside of NYC** (264/265); không làm mất các chuyến này. Dữ liệu thật không có khóa vùng không khớp lookup; trường hợp đó đã được kiểm thử bằng dữ liệu giả.

Code Phase 2, file raw và bản Parquet sạch trên máy được kiểm tra không thay đổi. Phase 3 chỉ đọc nguồn HDFS; kiểm tra lại báo cáo Phase 2 và danh sách file đầu vào sau khi xử lý để phát hiện thay đổi trong lúc chạy.

## 2. Năm KPI và kết quả

| KPI | File kết quả | Số dòng, không tính header |
|---|---|---:|
| Doanh thu theo giờ | [01_revenue_by_hour.csv](results/phase3/p3-20261114T162445Z-acf6c3/01_revenue_by_hour.csv) | 24 |
| Top 10 vùng đón | [02_top10_pickup_zones.csv](results/phase3/p3-20261114T162445Z-acf6c3/02_top10_pickup_zones.csv) | 10 |
| Tip theo thanh toán | [03_tip_by_payment.csv](results/phase3/p3-20261114T162445Z-acf6c3/03_tip_by_payment.csv) | 5 |
| Ngày thường/cuối tuần | [04_weekday_vs_weekend.csv](results/phase3/p3-20261114T162445Z-acf6c3/04_weekday_vs_weekend.csv) | 2 |
| Quãng đường trung bình theo vùng đón | [05_avg_distance_by_zone.csv](results/phase3/p3-20261114T162445Z-acf6c3/05_avg_distance_by_zone.csv) | 255 |

Các cách tính sau là lựa chọn triển khai được ghi rõ; tài liệu v2 nêu 5 KPI nhưng chưa chốt đầy đủ công thức.

**Doanh thu:** tổng `total_amount` được ghi nhận, đơn vị USD. Tổng toàn lô là **81.861.698,99 USD**. Nhóm 17 giờ cao nhất: **5.863.902,72 USD**, 204.782 chuyến. Kết quả gom mọi ngày theo giờ đón 0–23, không phải một mốc giờ duy nhất trong một ngày. Có cột tổng cước `fare_amount`, số quan sát tiền hợp lệ, số tiền thiếu và tiền âm để phân biệt.

**Top vùng đón:** đếm `COUNT(*)` theo `PULocationID`, JOIN tên vùng/quận. Đứng đầu là **JFK Airport, Queens — 153.807 chuyến**, khoảng **5,1373%** toàn lô. Khi số chuyến bằng nhau, dùng LocationID tăng dần để kết quả ổn định.

**Tip:** tỷ lệ tiền tip/cước bằng `100 × SUM(tip_amount) / SUM(fare_amount)` trên **cùng tập chuyến có tip không null, hữu hạn và không âm**. Tỷ lệ chuyến có tip dương được ghi riêng tại `tipped_trip_pct`.

- Nhóm thẻ tín dụng có **2.387.617 chuyến**, tỷ lệ tiền tip/cước **22,5136%**, tỷ lệ chuyến có tip dương **96,0379%**.
- Không dùng trung bình cộng của các tỷ lệ từng chuyến: đó là công thức khác với tỷ lệ hai tổng.
- Tip tiền mặt không được TLC ghi nhận đầy đủ. Các nhóm thanh toán vẫn xuất số chuyến, số tiền tip được ghi nhận và cờ giải thích; hai tỷ lệ chính để trống ở các nhóm ngoài thẻ tín dụng. Không kết luận khách tiền mặt không tip.
- Mã thanh toán 0 được giữ nguyên với nhãn `Unknown code: 0`; không tự gán định nghĩa mới từ schema năm khác cho lô 2023.
- Có **3 dòng tip âm** trong lô, thuộc nhóm mã 0. Giữ chuyến trong các KPI khác, loại giá trị này khỏi phép tính tip hợp lệ và ghi số đếm; không sửa Parquet nguồn.

**Ngày thường/cuối tuần:** ngày thường có **2.137.329 chuyến/23 ngày có dữ liệu**; cuối tuần có **856.614 chuyến/9 ngày**. Trung bình tương ứng **92.927,3478** và **95.179,3333 chuyến/ngày có dữ liệu**. Đây là trung bình theo ngày quan sát; ngày 01/02 chỉ có 10 chuyến nên số ngày thường bị ảnh hưởng bởi ngày có dữ liệu không đầy đủ. CSV cũng có doanh thu, tiền trung bình mỗi chuyến, khoảng cách và thời lượng trung bình. Cuối tuần là thứ Bảy/Chủ nhật, không phân loại ngày lễ.

**Khoảng cách:** `AVG(trip_distance)` theo vùng đón, đơn vị mile, kèm số chuyến và tổng khoảng cách. Không lấy trung bình của các trung bình nhóm để suy ra trung bình toàn lô. Lookup có 265 khóa nhưng dữ liệu sạch thực tế chỉ phát sinh **255 vùng đón**, nên CSV có 255 dòng.

Tiền được chuyển sang `decimal(28,6)` trong nhánh phân tích, giá trị không hữu hạn thành null; giá trị âm của tổng tiền được giữ và có số đếm. Không tự đổi tiền thiếu thành 0. Lô thật có đủ `total_amount` và không có tổng tiền âm; các trường hợp thiếu/âm/NaN/vô cực được kiểm thử riêng. Đây là số tiền ghi nhận, không bao gồm tip tiền mặt và không phải lợi nhuận.

Nguồn giải thích dữ liệu: [TLC Yellow Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf), [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## 3. Code hoạt động

1. [scripts/run_phase3.py](scripts/run_phase3.py) kiểm tra Docker/Spark/HDFS sẵn sàng, bổ sung lookup chính thức nếu chưa có, chép code/SQL vào container và chạy test.
2. [src/analyze.py](src/analyze.py) đọc báo cáo Phase 2, đọc Parquet, đối chiếu số dòng và các cột thời gian; kiểm tra lookup không có khóa null/trùng.
3. JOIN trái với lookup nhỏ bằng broadcast. Cache tập cột cần phân tích bằng `MEMORY_AND_DISK`, tạo temporary view `trips`.
4. Chạy 5 truy vấn tại [src/sql/phase3/](src/sql/phase3/). Tính toán dữ liệu thật bằng Spark SQL; chỉ đưa các kết quả tổng hợp nhỏ về driver để kiểm chứng.
5. Kiểm tra số chuyến, doanh thu, thứ hạng top 10 và số ngày. Sau tổng hợp mới `coalesce(1)`, ghi CSV tạm rồi đổi tên file part thành tên CSV rõ ràng.
6. Đọc lại CSV, xuất đúng 5 file về máy bằng `docker cp`, kiểm tra header/số dòng/SHA-256, xác nhận ứng dụng Spark `FINISHED`. Chỉ sau khi toàn bộ bước đạt mới cập nhật [latest.json](reports/phase3/latest.json).

Mỗi lần chạy có run ID riêng, không ghi đè kết quả cũ. Kết quả CSV nằm riêng dưới `results/phase3/<run_id>/`; log, query plan và báo cáo nằm dưới `reports/phase3/<run_id>/`.

## 4. Kiểm thử và minh chứng

[tests/test_analytics.py](tests/test_analytics.py): **4/4 bài đạt**, gồm bộ 6 chuyến tính tay để đối chiếu đủ 5 KPI và các tình huống:

- Tháng 2 vẫn được giữ; số khách null và RatecodeID 99 không làm mất chuyến.
- Tip dùng cùng mẫu số, tỷ lệ có trọng số; tiền mặt/null không bị diễn giải thành tỷ lệ 0.
- Khóa vùng không khớp vẫn giữ chuyến; khóa lookup trùng/null bị chặn.
- Tổng tiền âm/thiếu/NaN và tip âm/vô cực được xử lý theo quy ước.
- Thứ hạng đồng hạng có thứ tự ổn định, số ngày quan sát chính xác.
- Xuất đúng 5 CSV có header và đọc lại; số liệu làm sai chủ động bị bộ kiểm chứng phát hiện.

Kiểm tra dữ liệu thật đều đạt: bảo toàn số chuyến, bảo toàn doanh thu, JOIN không đổi số dòng, top 10 đúng thứ tự, số ngày khớp, cách diễn giải tip đúng và CSV đọc lại khớp. Tổng số chuyến theo giờ, thanh toán, nhóm ngày và vùng đều bằng **2.993.943**.

| Số đo | Thời gian |
|---|---:|
| 4 bài test, thời gian unittest báo | 75,691 giây |
| Lệnh chạy test, gồm khởi động Spark | 82,747 giây |
| Lệnh Spark phân tích, kiểm chứng và ghi CSV | 76,172 giây |
| Toàn bộ runner, gồm đợi HDFS, test và xuất file | 199,556 giây |

Đây là một lần đo trên cụm hiện tại, không phải benchmark so sánh với Pandas. Các query được cache nên số đo từng query trong báo cáo không phải phép so sánh độc lập giữa công nghệ.

Minh chứng chính:

- [analysis_report.json](reports/phase3/p3-20261114T162445Z-acf6c3/analysis_report.json): đầu vào, phạm vi ngày/tháng, số liệu chất lượng, công thức và kết quả kiểm chứng.
- [spark-analysis.log](reports/phase3/p3-20261114T162445Z-acf6c3/spark-analysis.log), [synthetic-tests.log](reports/phase3/p3-20261114T162445Z-acf6c3/synthetic-tests.log).
- [runner-status.json](reports/phase3/p3-20261114T162445Z-acf6c3/runner-status.json), [spark-master.json](reports/phase3/p3-20261114T162445Z-acf6c3/spark-master.json).
- [export-manifest.json](reports/phase3/p3-20261114T162445Z-acf6c3/export-manifest.json), [source-hashes.json](reports/phase3/p3-20261114T162445Z-acf6c3/source-hashes.json).
- Các file `*.explain.txt` trong cùng thư mục báo cáo lưu kế hoạch thực thi từng truy vấn.

Lần thử `p3-20261114T162357Z-9544a7` dừng ở bước kiểm tra Docker vì engine đang tắt; chưa chạy test/job hoặc xuất dữ liệu. Log thất bại được giữ để phân biệt với run hoàn tất nêu trên. Cảnh báo socket trong test không làm bài test thất bại.

## 5. Cách chạy lại

Mở Docker Desktop, khởi động cụm và đợi HDFS sẵn sàng theo [README](README.md) hoặc [hướng dẫn runtime trên máy này](RUNTIME_MAIN_RESULT.md). Sau đó tại thư mục dự án:

```powershell
python run.py analyze --docker-context desktop-linux
```

Không cần chạy lại Phase 2 khi đầu vào sạch và báo cáo Phase 2 vẫn còn. Runner tự đợi cụm sẵn sàng tối đa 120 giây; không tự tạo lại container hoặc xóa HDFS.

Nếu vừa kiểm thử cùng phiên bản code và chỉ muốn tạo lượt KPI mới:

```powershell
python run.py analyze --docker-context desktop-linux --skip-tests
```

Xem `reports/phase3/latest.json` để lấy thư mục kết quả mới nhất. Không trộn nhiều thư mục kết quả để cộng lại thành dữ liệu nguồn.

Đã bổ sung tác vụ `analyze` vào `run.py` và hướng dẫn README. Chưa thực hiện benchmark Phase 4 hoặc push GitHub. Có sử dụng AI để hỗ trợ triển khai, kiểm thử và viết tài liệu; nhóm cần hiểu công thức và kiểm chứng phần bàn giao.
