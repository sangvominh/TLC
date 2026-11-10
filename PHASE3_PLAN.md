# Chuẩn bị Phase 3 theo hướng dẫn v2

Ngày chuẩn bị: **09/11/2026**. Trạng thái: đã đối chiếu yêu cầu, kiểm tra đầu vào cục bộ và xác định cách tính; **chưa triển khai job/SQL, chưa có kết quả KPI Phase 3**.

## 1. Yêu cầu trong tài liệu và lựa chọn của nhóm

Theo `Hướng-dẫn-đồ-án-BIG-data-v2.pdf`, trang 5, Phase 3 cần:

1. Đọc DataFrame sạch, tạo Spark SQL temporary view.
2. Dùng các truy vấn GROUP BY, ORDER BY, SUM, AVG để tính 5 KPI: doanh thu theo giờ; top 10 vùng đón; tỷ lệ tip theo thanh toán; ngày thường/cuối tuần; khoảng cách trung bình theo vùng.
3. JOIN `taxi_zone_lookup.csv` để có tên vùng/quận.
4. Xuất **5 file CSV kết quả**.

Tài liệu không chốt công thức tỷ lệ tip, cột dùng làm doanh thu hay bộ lọc RatecodeID cho từng KPI. Những định nghĩa ở dưới là **đề xuất triển khai được ghi rõ để nhóm thống nhất**, không phải các yêu cầu bổ sung được trích từ tài liệu.

Người dùng đã chọn: **dùng toàn bộ tập sạch v2, không thêm bộ lọc tháng**. Báo cáo phải mô tả khoảng ngày thực tế và phân bố theo tháng, không gọi một lô file tháng 01 là dữ liệu đầy đủ của cả năm.

## 2. Đầu vào đã kiểm tra

- [Báo cáo Phase 2](reports/phase2/latest.json): `tlc-cleaning-v2`, report version 3, trạng thái passed, **2.993.943 dòng sạch**.
- Run nguồn: `p2v2-202301-20261108T122833705Z-9fc6e4`.
- HDFS nguồn của lượt này:

```text
hdfs://namenode:8020/user/tlc/cleaned/year=2023/month=01/run_id=p2v2-202301-20261108T122833705Z-9fc6e4
```

- Bản cục bộ: `data/cleaned/year=2023/month=01/run_id=p2v2-202301-20261108T122833705Z-9fc6e4/`. Kích thước và SHA-256 vẫn khớp [manifest xuất dữ liệu](reports/phase2/latest-export.json).
- Bảng vùng: `data/raw/taxi_zone_lookup.csv`, **265 dòng và 265 LocationID duy nhất**. Bản HDFS theo quy ước Phase 2: `/user/tlc/reference/taxi_zone_lookup.csv`.
- Mã 264 có Borough `Unknown`, Zone `N/A`; mã 265 có Zone `Outside of NYC`. Giữ nguyên ý nghĩa phân loại, không tự đổi chúng thành tên quận hợp lệ.

V2 chỉ lọc năm 2023. Đối chiếu với [đợt thử có lọc thêm tháng](reports/phase2/p2v2-202301-20261108T122201000Z-a9fdd9/TRIAL_SCOPE_NOTE.md) cho thấy **10 dòng đủ điều kiện nằm ngoài tháng 01**; kế hoạch Phase 3 giữ các dòng này như lựa chọn của người dùng. Khi chạy thật sẽ xác minh lại phân bố tháng/ngày từ Parquet và ghi vào báo cáo.

Chỉ đọc **một run đã chốt**, không quét cả thư mục chứa nhiều run_id. Đường dẫn được lấy và cố định một lần từ báo cáo nguồn lúc bắt đầu. Nếu Phase 2 được chạy lại, Phase 3 phải lưu ID/path của lượt nguồn mới vào minh chứng.

## 3. Quy ước tính chung

- Đếm chuyến bằng `COUNT(*)`. Không đếm `passenger_count`: tập sạch có **113.936 giá trị null** ở cột này, được giữ có chủ đích trong v2.
- Không chạy `dropna()` toàn bảng; không làm lại bộ lọc của Phase 2 và không xóa dữ liệu sạch gốc.
- Dùng các cột giờ/ngày/cuối tuần có sẵn theo giờ nguồn; không đổi chúng sang UTC. Cuối tuần là thứ Bảy/Chủ nhật, không phải phân loại ngày lễ.
- Đơn vị tiền USD; khoảng cách mile; thời lượng phút. Tính với độ chính xác đầy đủ, chỉ làm tròn khi xuất kết quả.
- Không loại hàng loạt `RatecodeID=99` hay RatecodeID null. Ghi số lượng các nhóm này trong báo cáo; các KPI vẫn có thể sử dụng trường tiền, thời gian, khoảng cách hợp lệ của chúng. Nếu sau này chọn lọc cho KPI riêng phải ghi rõ lý do và số dòng ảnh hưởng.
- Giữ mọi nhóm vùng, kể cả 264/265 hoặc ID không có trong lookup. Dùng LEFT JOIN và cờ `zone_match_status`; không INNER JOIN làm mất chuyến. Kiểm tra khóa lookup duy nhất trước JOIN để tránh nhân bản dòng.
- Tổng tiền null và tip null không tự điền 0. Mỗi phép tổng/trung bình ghi rõ số quan sát thực sự được dùng.

## 4. Định nghĩa 5 KPI và CSV

### KPI 1 — Doanh thu ghi nhận theo giờ

- Nhóm theo `pickup_hour` 0–23 trên toàn tập nguồn đã chọn.
- `trip_count = COUNT(*)`.
- `recorded_revenue_usd = SUM(total_amount)`, cộng theo dấu ghi nhận, bỏ qua null theo ngữ nghĩa SUM. Giá trị âm được giữ trong tổng và đếm riêng vì v2 không loại chúng trên toàn tập.
- Thêm `metered_fare_usd = SUM(fare_amount)` để phân biệt tiền cước với tổng tiền giao dịch; ghi `missing_total_count`, `negative_total_count`, `total_observation_count`.
- Nếu cả nhóm không có total_amount hợp lệ thì tổng tương ứng là null, không giả tạo thành 0. Đây là tiền ghi nhận trong dữ liệu, không khẳng định là lợi nhuận hoặc tiền tài xế thực nhận.
- CSV: `01_revenue_by_hour.csv`; dự kiến tối đa 24 dòng quan sát, sắp giờ tăng dần.

### KPI 2 — Top 10 vùng đón khách

- Nhóm theo **PULocationID**, đếm chuyến, rồi JOIN tên Borough/Zone từ lookup.
- Xếp `trip_count DESC, LocationID ASC`, lấy 10 nhóm đầu; khóa phụ giúp kết quả ổn định khi bằng số chuyến.
- Có thể thêm tỷ trọng `100 * trip_count / total_source_trips`. Tổng tỷ trọng top 10 không bắt buộc bằng 100%.
- Giữ các nhóm Unknown/Outside of NYC; gắn cờ để không diễn giải nhóm không xác định thành địa điểm cụ thể. Nếu có ID không khớp lookup, hiển thị ID và nhãn dự phòng.
- CSV: `02_top10_pickup_zones.csv`; tối đa 10 dòng.

### KPI 3 — Tỷ lệ tip theo phương thức thanh toán

- GROUP BY `payment_type`, kể cả nhóm null/mã không biết, có nhãn dự phòng theo mã gốc. Không giả định luôn có đúng một số lượng nhóm cố định.
- Phân biệt **mức tip so với cước** và **tỷ lệ chuyến có tip**:
  - `tip_rate_pct = 100 * SUM(tip_amount) / SUM(fare_amount)` trên **cùng tập chuyến có tip được ghi nhận, tip không null và >=0**.
  - `tipped_trip_pct = 100 * COUNT(tip_amount > 0) / COUNT(valid_tip_records)` trên cùng tập đủ điều kiện.
- Hai tỷ lệ chính chỉ diễn giải cho `payment_type=1` (thẻ tín dụng). Các phương thức khác vẫn có dòng và số chuyến, nhưng tỷ lệ để null với ghi chú phạm vi quan sát; không kết luận khách trả tiền mặt không tip từ số 0 trong dữ liệu.
- Ghi `trip_count`, `eligible_tip_trip_count`, `missing_tip_count`, `negative_tip_count` cho từng nhóm. Tip thiếu/âm chỉ không tham gia công thức KPI này, không làm mất chuyến ở các KPI khác.
- Mẫu số 0 trả null. Không lấy trung bình tùy tiện các phần trăm từng chuyến và không tự chặn tỷ lệ ở 100% nếu tài liệu không yêu cầu.
- CSV: `03_tip_by_payment.csv`; một dòng cho mỗi nhóm thanh toán quan sát được.

### KPI 4 — Ngày thường so với cuối tuần

- Nhóm theo `is_weekend`, xuất nhãn weekday/weekend.
- So sánh `trip_count`, tổng tiền ghi nhận, tiền trung bình **mỗi chuyến có total_amount**, quãng đường trung bình mỗi chuyến và thời lượng trung bình mỗi chuyến.
- Có `observed_days = COUNT(DISTINCT pickup_date)` và số quan sát tiền để diễn giải mẫu số.
- Không suy luận ngày thường đông hơn chỉ từ tổng chuyến, vì số ngày của hai nhóm khác nhau. Ưu tiên đối chiếu chỉ tiêu trung bình/chuyến và thông tin số ngày.
- Không mặc định lô chứa đủ 365 ngày hay đủ mọi ngày trong khoảng min/max. Nếu thêm chỉ tiêu theo ngày, phải gọi rõ là trung bình trên **ngày có dữ liệu quan sát**, kèm phân bố/độ phủ theo tháng; không coi các tháng chỉ có vài dòng ngoài lô là tháng quan sát đầy đủ.
- CSV: `04_weekday_vs_weekend.csv`; dự kiến 2 nhóm với dữ liệu hiện tại.

### KPI 5 — Quãng đường trung bình theo vùng

- “Theo vùng” được định nghĩa là **vùng đón PULocationID**, cùng quy ước với KPI 2.
- `avg_distance_miles = AVG(trip_distance)`; mỗi chuyến có trọng số bằng nhau. Xuất thêm `trip_count` để biết quy mô mẫu.
- LEFT JOIN tên vùng/quận, giữ cả nhóm không xác định. Không gộp nhiều Zone cùng Borough trước khi tính, cũng không lấy trung bình không trọng số của các trung bình vùng.
- CSV: `05_avg_distance_by_zone.csv`; một dòng mỗi PULocationID quan sát được, sắp LocationID tăng dần. Số nhóm có thể vượt số khóa lookup nếu xuất hiện ID chưa biết.

## 5. Căn cứ ngữ nghĩa dữ liệu

Theo [TLC Yellow Taxi Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf), tip tiền mặt không được thu thập trong trường tip; total_amount là tổng tiền tính cho khách và không bao gồm tip tiền mặt; mã giá cước 99 biểu thị thiếu/không xác định, không phải mã cước thỏa thuận. Vì vậy cần ghi rõ “ghi nhận” khi diễn giải tiền và không so sánh hành vi tip tiền mặt/thẻ như thể có cùng độ phủ.

Từ điển đang công bố là bản cập nhật năm 2025, còn tập đang phân tích là năm 2023. Khi đặt nhãn các mã thanh toán ít gặp phải đối chiếu metadata theo năm; mã chưa xác minh dùng nhãn Other kèm mã gốc, không áp ý nghĩa mới vào dữ liệu cũ một cách mặc định.

## 6. Kiến trúc và các file dự kiến

```text
Đọc báo cáo Phase 2 → chốt một đường dẫn Parquet sạch
    ↓
Kiểm tra schema/count/phạm vi ngày + lookup duy nhất
    ↓
Temporary views: trips_cleaned, taxi_zones
    ↓
5 truy vấn Spark SQL → dữ liệu tổng hợp nhỏ → JOIN tên vùng
    ↓
5 CSV có tên cố định + báo cáo kiểm chứng + log
```

Các file **sẽ viết khi triển khai**:

| File | Vai trò |
|---|---|
| `src/analyze_data.py` | Đọc dữ liệu, tạo views, chạy SQL, ghi kết quả, kiểm tra đầu ra. |
| `src/sql/phase3/01_revenue_by_hour.sql` … `05_avg_distance_by_zone.sql` | Tách rõ 5 công thức SQL để thành viên giải thích. |
| `src/test_analytics.py` | Bộ dữ liệu nhỏ với kết quả tính tay, kiểm tra công thức và JOIN. |
| `scripts/run_phase3.ps1` | Chép source/SQL, chạy kiểm thử và Spark job, xuất 5 CSV về máy. |
| `PHASE3_RESULT.md` | Số liệu thật, cách hiểu KPI, đường dẫn và minh chứng. |

Giữ Spark Master/Worker và HDFS hiện có. Cấu hình khởi đầu như Phase 2: driver 768 MiB, executor 1 GiB, tối đa 2 lõi, shuffle partitions 4. Chỉ bổ sung tài nguyên nếu số đo thực tế cho thấy cần. Lookup 265 dòng có thể broadcast; chỉ dữ liệu tổng hợp nhỏ mới được đưa về driver. Không thêm dịch vụ mới để tính 5 KPI.

## 7. Đầu ra và cách kiểm chứng dự kiến

- HDFS: `/user/tlc/analytics/run_id=<phase3_run>/` với 5 bộ CSV trung gian.
- Trên máy: `results/phase3/<phase3_run>/` chứa **đúng 5 file CSV nghiệp vụ** có tên ở trên.
- Minh chứng riêng: `reports/phase3/<phase3_run>/analysis_report.json`, log, hash source/input/output, kết quả kiểm tra.
- Báo cáo lưu run nguồn Phase 2, thời gian từng bước, min/max ngày, số dòng theo tháng, nhóm mã không biết, null/âm của trường tiền và mẫu số thực tế của từng KPI.

Spark `.write.csv(path)` tạo **thư mục chứa part-files**, không tạo trực tiếp một CSV tên cố định. Runner phải lấy part CSV sau khi tổng hợp và đặt đúng 5 tên cuối. Chỉ gom 1 partition khi kết quả đã nhỏ, không gom toàn bộ chuyến đi về một máy trước phân tích. Đọc lại 5 CSV để kiểm tra header, kiểu giá trị, số dòng và thứ tự.

Các kiểm tra quan trọng:

1. Số dòng đọc phải khớp báo cáo nguồn, hiện là 2.993.943; không mất dòng hành khách null và không nhân bản dòng sau JOIN.
2. Tổng trip_count theo giờ, theo thanh toán, theo loại ngày và theo toàn bộ nhóm vùng đều khớp tổng nguồn. Top 10 là tập con nên không áp điều kiện này lên riêng 10 dòng.
3. Tổng doanh thu theo giờ và theo loại ngày khớp phép SUM độc lập với cùng chính sách null/âm.
4. Trung bình khoảng cách theo vùng đối chiếu lại bằng SUM(distance)/COUNT(*) của từng vùng; có kiểm thử tránh lấy trung bình sai trọng số.
5. Tip chỉ dùng cùng một tập đủ điều kiện cho tử và mẫu; kiểm thử hai chuyến có cước khác nhau để phân biệt ratio-of-sums với average-of-ratios, cùng tình huống tip null/âm/0, tiền mặt và mẫu số 0.
6. Kiểm thử LEFT JOIN thiếu mã, duplicate key trong lookup phải báo lỗi, hòa điểm top 10, RatecodeID 99, passenger_count null, ranh giới thứ Sáu/thứ Bảy và lô chứa nhiều tháng.
7. CSV không có NaN/Infinity, không ghi phần trăm giả tạo khi thiếu dữ liệu; tên file ổn định. Không đổi null thành zero chỉ để làm CSV đẹp.

## 8. Thứ tự thực hiện

1. Viết bộ fixture và 5 công thức SQL theo định nghĩa trên.
2. Viết job + runner, chạy bộ kiểm thử nhỏ.
3. Chạy dữ liệu thật, kiểm chứng tổng chéo và xuất 5 CSV.
4. Viết PHASE3_RESULT.md để nhóm hiểu từng con số.

Benchmark Pandas/Spark và script chạy cả pipeline là giai đoạn 4–5 của tài liệu; số đo trong Phase 3 phục vụ truy vết job, không tự chứng minh Spark nhanh hơn Pandas.
