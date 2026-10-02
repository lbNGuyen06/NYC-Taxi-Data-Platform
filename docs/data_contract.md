# NYC Yellow Taxi Data Contract

## Document control

| Thuộc tính | Giá trị |
| --- | --- |
| Contract version | `0.2.0` |
| Status | Draft — đủ để triển khai bản local đầu tiên; cần đánh giá lại các rule `WARN` sau khi phân tích nhiều tháng |
| Dataset | NYC TLC Yellow Taxi Trip Records |
| Processing month | `2025-01` |
| Contract owner | Project owner |
| Last reviewed | 2026-10-02 |

Mọi thay đổi làm một record chuyển giữa `ACCEPT`, `WARN` và `QUARANTINE`, thay đổi kiểu cột, khóa hoặc định nghĩa metric phải tăng contract version và được ghi trong lịch sử thay đổi.

## 1. Source and lineage

### 1.1 Trip records

| Thuộc tính | Giá trị |
| --- | --- |
| Source | NYC Taxi and Limousine Commission (TLC) |
| Dataset | Yellow Taxi Trip Records |
| Source URL | <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page> |
| Source data dictionary | <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf> |
| File name | `yellow_tripdata_2025-01.parquet` |
| Format | Parquet |
| File size | `59,158,238` bytes |
| SHA-256 | `9AF277E4C0D3F9DEB30644DA822981E1E7DF6AF58313170FD3AA8A474485488A` |
| Observed row count | `3,475,226` |
| Download date | Unknown — file existed before lineage tracking was introduced |

TLC cho biết dữ liệu được các technology providers gửi cho TLC và không bảo đảm tuyệt đối độ chính xác. Vì vậy, contract gọi mỗi dòng là một **trip record được ghi nhận**, không khẳng định đó chắc chắn là một chuyến duy nhất ngoài thực tế.

### 1.2 Zone reference

| Thuộc tính | Giá trị |
| --- | --- |
| File name | `taxi_zone_lookup.csv` |
| Format | CSV with header |
| File size | `12,331` bytes |
| SHA-256 | `1A99E105092230F8620F301EDCCA7F80D3080642FF404D28ED957D3FA222C8ED` |
| Observed rows | `265` |
| Distinct `LocationID` | `265` |
| Download date | Unknown — file existed before lineage tracking was introduced |

## 2. Scope and assumptions

- Contract này chỉ áp dụng cho Yellow Taxi, tháng `2025-01`. Không dùng nguyên trạng cho Green Taxi, FHV hoặc HVFHV.
- Tiền tệ được hiểu là USD theo ngữ cảnh bộ dữ liệu.
- `trip_distance` có đơn vị mile theo TLC data dictionary.
- Pickup/dropoff timestamps được giữ dưới dạng local civil time của New York trong phiên bản đầu tiên. Pipeline không tự gắn UTC hoặc tự chuyển timezone. Assumption này phải được xác minh trước khi tích hợp nguồn có timezone khác.
- Ranh giới processing month là `[2025-01-01 00:00:00, 2025-02-01 00:00:00)` theo cùng cách hiểu thời gian của source.
- Null có nghĩa là không biết/không được cung cấp; không tự động thay null bằng `0`.
- `total_amount` là tổng tiền được record ghi nhận là đã tính cho hành khách và không bao gồm cash tips. Nó không được gọi là doanh thu kế toán, tiền đã đối soát hay tiền thực thu.

## 3. Grain and keys

| Dataset | Grain | Khóa/định danh logic |
| --- | --- | --- |
| Raw Yellow Taxi | Một trip record do technology provider gửi cho TLC | Source không cung cấp trip ID duy nhất |
| `fact_trips` | Một accepted Yellow Taxi trip record | `trip_id`, fingerprint do pipeline tạo |
| `dim_zone` | Một TLC taxi zone | `zone_id` (`LocationID`) |
| `dim_date` | Một ngày dương lịch | `date_key` dạng `yyyyMMdd` |
| `quarantine_trips` | Một raw record vi phạm ít nhất một rule có action `QUARANTINE` | `source_record_id` + `processing_month` |
| `quality_metrics` | Kết quả của một rule trong một processing month và rule version | (`processing_month`, `rule_id`, `rule_version`) |

### 3.1 Record fingerprints

Do source không có trip ID, pipeline sẽ tạo:

- `source_record_id`: SHA-256 trên biểu diễn canonical của **toàn bộ cột source**, dùng để nhận diện exact duplicate và truy vết quarantine.
- `trip_id`: trong phiên bản đầu tiên bằng `source_record_id` đối với accepted records.

Quy tắc canonical hash phải cố định thứ tự cột, định dạng timestamp/decimal và biểu diễn null. Thêm/bớt cột khỏi hash là breaking change và phải tăng contract version. Hai record chỉ được xem là exact duplicate khi toàn bộ source values sau canonicalization giống nhau.

## 4. Source schema observed

`Observed type` và null percentage lấy từ file tháng `2025-01`; đây là quan sát vật lý, không phải quyết định schema đích.

| Source column | Ý nghĩa nghiệp vụ | Observed type | Null % | Structural requirement |
| --- | --- | --- | ---: | --- |
| `VendorID` | Nhà cung cấp TPEP gửi record | `int32` | 0.00 | Required |
| `tpep_pickup_datetime` | Thời điểm đồng hồ tính cước được bật | `datetime64[us]` | 0.00 | Required |
| `tpep_dropoff_datetime` | Thời điểm đồng hồ tính cước được tắt | `datetime64[us]` | 0.00 | Required |
| `passenger_count` | Số hành khách do tài xế nhập | `float64` | 15.54 | Optional |
| `trip_distance` | Quãng đường taximeter ghi nhận, đơn vị mile | `float64` | 0.00 | Required |
| `RatecodeID` | Mã loại cước cuối chuyến | `float64` | 15.54 | Optional |
| `store_and_fwd_flag` | Record từng được lưu tạm trong xe hay không | `string` | 15.54 | Optional |
| `PULocationID` | TLC zone nơi đón | `int32` | 0.00 | Required |
| `DOLocationID` | TLC zone nơi trả | `int32` | 0.00 | Required |
| `payment_type` | Mã phương thức/trạng thái thanh toán | `int64` | 0.00 | Required |
| `fare_amount` | Cước tính theo thời gian và quãng đường | `float64` | 0.00 | Required |
| `extra` | Phụ phí bổ sung | `float64` | 0.00 | Required |
| `mta_tax` | Thuế MTA | `float64` | 0.00 | Required |
| `tip_amount` | Tip được ghi nhận; cash tips không được bao gồm | `float64` | 0.00 | Required |
| `tolls_amount` | Tổng phí cầu đường | `float64` | 0.00 | Required |
| `improvement_surcharge` | Improvement surcharge | `float64` | 0.00 | Required |
| `total_amount` | Tổng tiền được tính; không gồm cash tips | `float64` | 0.00 | Required |
| `congestion_surcharge` | NYS congestion surcharge | `float64` | 15.54 | Optional |
| `Airport_fee` | Phí pickup tại LaGuardia/JFK | `float64` | 15.54 | Optional |
| `cbd_congestion_fee` | Congestion Relief Zone fee từ 2025-01-05 | `float64` | 0.00 | Optional extension |

`Airport_fee` là tên vật lý có chữ `A` viết hoa trong file nguồn. Canonical schema đổi tên có chủ đích thành `airport_fee`.

## 5. Canonical input schema

| Source | Canonical name | Target type | Nullable | Transformation |
| --- | --- | --- | --- | --- |
| `VendorID` | `vendor_id` | `INT` | No | Rename + cast |
| `tpep_pickup_datetime` | `pickup_at` | `TIMESTAMP` | No | Rename |
| `tpep_dropoff_datetime` | `dropoff_at` | `TIMESTAMP` | No | Rename |
| `passenger_count` | `passenger_count` | `INT` | Yes | Safe cast; giữ null |
| `trip_distance` | `trip_distance_miles` | `DECIMAL(12,2)` | No | Cast; giữ source value trước cast trong quarantine |
| `RatecodeID` | `rate_code_id` | `INT` | Yes | Safe cast; giữ null |
| `store_and_fwd_flag` | `store_and_fwd_flag` | `STRING` | Yes | Trim + uppercase |
| `PULocationID` | `pickup_zone_id` | `INT` | No | Rename + cast |
| `DOLocationID` | `dropoff_zone_id` | `INT` | No | Rename + cast |
| `payment_type` | `payment_type_id` | `INT` | No | Rename + cast |
| `fare_amount` | `fare_amount` | `DECIMAL(12,2)` | No | Cast |
| `extra` | `extra_amount` | `DECIMAL(12,2)` | No | Rename + cast |
| `mta_tax` | `mta_tax_amount` | `DECIMAL(12,2)` | No | Rename + cast |
| `tip_amount` | `tip_amount` | `DECIMAL(12,2)` | No | Cast |
| `tolls_amount` | `tolls_amount` | `DECIMAL(12,2)` | No | Cast |
| `improvement_surcharge` | `improvement_surcharge_amount` | `DECIMAL(12,2)` | No | Rename + cast |
| `total_amount` | `total_amount` | `DECIMAL(12,2)` | No | Cast |
| `congestion_surcharge` | `congestion_surcharge_amount` | `DECIMAL(12,2)` | Yes | Rename + cast; giữ null |
| `Airport_fee` | `airport_fee_amount` | `DECIMAL(12,2)` | Yes | Explicit case-sensitive reference, rename + cast |
| `cbd_congestion_fee` | `cbd_congestion_fee_amount` | `DECIMAL(12,2)` | Yes | Rename + cast |

Cast failure trên required field tạo rule violation và quarantine; cast failure trên optional field tạo warning, giữ null và phải được đếm trong quality metrics.

## 6. Categorical domains

### 6.1 `vendor_id`

| Value | Meaning |
| ---: | --- |
| `1` | Creative Mobile Technologies, LLC |
| `2` | Curb Mobility, LLC |
| `6` | Myle Technologies Inc |
| `7` | Helix |

### 6.2 `rate_code_id`

| Value | Meaning |
| ---: | --- |
| `1` | Standard rate |
| `2` | JFK |
| `3` | Newark |
| `4` | Nassau or Westchester |
| `5` | Negotiated fare |
| `6` | Group ride |
| `99` | Null/unknown |

### 6.3 `store_and_fwd_flag`

| Value | Meaning |
| --- | --- |
| `Y` | Store-and-forward trip |
| `N` | Not a store-and-forward trip |

### 6.4 `payment_type_id`

| Value | Meaning |
| ---: | --- |
| `0` | Flex Fare trip |
| `1` | Credit card |
| `2` | Cash |
| `3` | No charge |
| `4` | Dispute |
| `5` | Unknown |
| `6` | Voided trip |

## 7. Quality-rule contract

### 7.1 Severities and actions

| Severity | Ý nghĩa | Default action |
| --- | --- | --- |
| `ERROR` | Record không đủ tin cậy cho analytical fact | `QUARANTINE` |
| `WARN` | Record vẫn dùng được nhưng cần theo dõi | `ACCEPT_WITH_WARNING` |
| `FATAL` | Dataset/schema không thể xử lý an toàn | `FAIL_JOB` |

Job phải đánh giá tất cả rules áp dụng. Một record có thể chứa nhiều `rule_id`; không dừng ở lỗi đầu tiên.

### 7.2 Record-level rules

| Rule ID | Điều kiện | Severity | Action | Rationale |
| --- | --- | --- | --- | --- |
| `DQ001` | Pickup timestamp null hoặc cast lỗi | ERROR | QUARANTINE | Không xác định được thời điểm/grain phân vùng |
| `DQ002` | Dropoff timestamp null hoặc cast lỗi | ERROR | QUARANTINE | Không tính được duration |
| `DQ003` | `dropoff_at < pickup_at` | ERROR | QUARANTINE | Thời gian đảo ngược |
| `DQ004` | `dropoff_at = pickup_at` | ERROR | QUARANTINE | Duration bằng 0 |
| `DQ005` | Pickup nằm ngoài processing month | ERROR | QUARANTINE | Sai tháng/prefix xử lý |
| `DQ006` | `trip_distance_miles <= 0` | ERROR | QUARANTINE | Không phù hợp tập fact dùng phân tích quãng đường |
| `DQ007` | `fare_amount < 0` | ERROR | QUARANTINE | Có thể là adjustment; không trộn vào fact chuyến hợp lệ |
| `DQ008` | `total_amount < 0` | ERROR | QUARANTINE | Có thể là adjustment/refund; cần phân tích riêng |
| `DQ009` | Pickup zone null/cast lỗi | ERROR | QUARANTINE | Không phân tích được nguồn chuyến |
| `DQ010` | Dropoff zone null/cast lỗi | ERROR | QUARANTINE | Không phân tích được đích chuyến |
| `DQ011` | Pickup zone không có trong zone lookup | ERROR | QUARANTINE | Vi phạm referential integrity |
| `DQ012` | Dropoff zone không có trong zone lookup | ERROR | QUARANTINE | Vi phạm referential integrity |
| `DQ013` | Exact duplicate (`source_record_id` xuất hiện nhiều lần) | ERROR | QUARANTINE duplicate thứ 2 trở đi | Tránh đếm trùng; giữ một record canonical |
| `DQ014` | `vendor_id` ngoài `{1,2,6,7}` | ERROR | QUARANTINE | Vendor code ngoài documented domain |
| `DQ015` | `payment_type_id` ngoài `{0,1,2,3,4,5,6}` | ERROR | QUARANTINE | Payment code ngoài documented domain |
| `DQ016` | `rate_code_id` non-null ngoài `{1,2,3,4,5,6,99}` | WARN | ACCEPT_WITH_WARNING | Optional category; giữ record và theo dõi drift |
| `DQ017` | `store_and_fwd_flag` non-null ngoài `{Y,N}` | WARN | ACCEPT_WITH_WARNING | Optional category; giữ record và theo dõi drift |
| `DQ018` | `passenger_count` null | WARN | ACCEPT_WITH_WARNING | Source thiếu giá trị nhưng trip vẫn phân tích được |
| `DQ019` | `passenger_count < 0` hoặc `> 8` | WARN | ACCEPT_WITH_WARNING | Giá trị đáng ngờ, chưa đủ bằng chứng để loại trip |
| `DQ020` | Optional monetary field null/cast lỗi | WARN | ACCEPT_WITH_WARNING | Không thay null bằng 0; theo dõi completeness |
| `DQ021` | `trip_distance_miles > 200` | WARN | ACCEPT_WITH_WARNING | Ngưỡng điều tra ban đầu, không phải quy tắc TLC chính thức |
| `DQ022` | Duration > 24 giờ | WARN | ACCEPT_WITH_WARNING | Ngưỡng điều tra ban đầu |
| `DQ023` | `total_amount > 1000` | WARN | ACCEPT_WITH_WARNING | Ngưỡng điều tra ban đầu |
| `DQ024` | Khi mọi component đều non-null, trị tuyệt đối của `total_amount - (fare_amount + extra_amount + mta_tax_amount + tip_amount + tolls_amount + improvement_surcharge_amount + congestion_surcharge_amount + airport_fee_amount + cbd_congestion_fee_amount)` lớn hơn `0.01` | WARN | ACCEPT_WITH_WARNING | Có thể do component/adjustment chưa được mô hình hóa đầy đủ |

Các ngưỡng `200 miles`, `24 giờ`, `$1000` được giới thiệu ở `0.1.0` và tiếp tục áp dụng trong `0.2.0`; chúng dùng để quan sát chứ không khẳng định dữ liệu sai. Sau khi profile 3–6 tháng phải đánh giá lại và tăng contract version nếu thay đổi.

### 7.3 Dataset-level rules

| Rule ID | Điều kiện | Action |
| --- | --- | --- |
| `DS001` | Thiếu required source column | FAIL_JOB |
| `DS002` | Required column có incompatible physical type | FAIL_JOB |
| `DS003` | Raw row count bằng 0 | FAIL_JOB |
| `DS004` | Zone lookup rỗng hoặc `LocationID` không unique | FAIL_JOB |
| `DS005` | `raw_count != accepted_count + distinct_quarantined_count` | FAIL_JOB |
| `DS006` | Accepted `trip_id` không unique | FAIL_JOB |

Rule counts có thể chồng nhau. Vì vậy:

```text
sum(rule_violation_count) != distinct_quarantined_record_count
```

nhưng invariant bắt buộc là:

```text
raw_count = accepted_count + distinct_quarantined_record_count
```

## 8. Output contracts

### 8.1 `fact_trips`

Mỗi dòng là một accepted Yellow Taxi trip record. Record có `WARN` vẫn được nhận vào fact; warning được ghi trong quality metrics và có thể lưu `warning_rule_ids` để audit.

| Column | Type | Nullable | Source/derivation |
| --- | --- | --- | --- |
| `trip_id` | `STRING` | No | Deterministic SHA-256 fingerprint |
| `vendor_id` | `INT` | No | Canonical input |
| `pickup_at` | `TIMESTAMP` | No | Canonical input |
| `dropoff_at` | `TIMESTAMP` | No | Canonical input |
| `pickup_date_key` | `INT` | No | `yyyyMMdd` from `pickup_at`; FK to `dim_date.date_key` |
| `dropoff_date_key` | `INT` | No | `yyyyMMdd` from `dropoff_at`; FK to `dim_date.date_key` |
| `pickup_date` | `DATE` | No | Date component of `pickup_at` |
| `dropoff_date` | `DATE` | No | Date component of `dropoff_at` |
| `pickup_hour` | `TINYINT` | No | Hour 0–23 from `pickup_at` |
| `pickup_zone_id` | `INT` | No | Canonical input |
| `dropoff_zone_id` | `INT` | No | Canonical input |
| `passenger_count` | `INT` | Yes | Canonical input |
| `trip_distance_miles` | `DECIMAL(12,2)` | No | Canonical input |
| `duration_minutes` | `DECIMAL(12,2)` | No | (`dropoff_at - pickup_at`) / 60 seconds |
| `rate_code_id` | `INT` | Yes | Canonical input |
| `payment_type_id` | `INT` | No | Canonical input |
| `store_and_fwd_flag` | `STRING` | Yes | Canonical input; `Y`, `N` or null |
| `fare_amount` | `DECIMAL(12,2)` | No | Canonical input |
| `extra_amount` | `DECIMAL(12,2)` | No | Canonical input |
| `mta_tax_amount` | `DECIMAL(12,2)` | No | Canonical input |
| `tip_amount` | `DECIMAL(12,2)` | No | Canonical input |
| `tolls_amount` | `DECIMAL(12,2)` | No | Canonical input |
| `improvement_surcharge_amount` | `DECIMAL(12,2)` | No | Canonical input |
| `congestion_surcharge_amount` | `DECIMAL(12,2)` | Yes | Canonical input |
| `airport_fee_amount` | `DECIMAL(12,2)` | Yes | Canonical input |
| `cbd_congestion_fee_amount` | `DECIMAL(12,2)` | Yes | Canonical input |
| `total_amount` | `DECIMAL(12,2)` | No | Canonical input |
| `warning_rule_ids` | `ARRAY<STRING>` | No | All matching `WARN` rule IDs; empty array nếu không có |
| `processing_month` | `STRING` | No | Pipeline argument, format `yyyy-MM` |
| `contract_version` | `STRING` | No | Contract version used by the run, currently `0.2.0` |
| `processed_at` | `TIMESTAMP` | No | Pipeline processing timestamp |

Physical partition proposal: `processing_month`, sau đó `pickup_date`. Quyết định cuối cùng chỉ được chốt sau benchmark file count/size và query patterns.

### 8.2 `dim_zone`

| Column | Type | Nullable | Source |
| --- | --- | --- | --- |
| `zone_id` | `INT` | No | `LocationID` |
| `borough` | `STRING` | No | Zone lookup |
| `zone_name` | `STRING` | No | `Zone` |
| `service_zone` | `STRING` | Yes | `service_zone` |

`zone_id` phải unique và tất cả accepted pickup/dropoff zone IDs phải tồn tại trong bảng này.

### 8.3 `dim_date`

| Column | Type | Nullable | Derivation |
| --- | --- | --- | --- |
| `date_key` | `INT` | No | `yyyyMMdd` |
| `calendar_date` | `DATE` | No | Calendar date |
| `year` | `SMALLINT` | No | Calendar year |
| `quarter` | `TINYINT` | No | 1–4 |
| `month` | `TINYINT` | No | 1–12 |
| `month_name` | `STRING` | No | English month name |
| `day` | `TINYINT` | No | Day of month |
| `day_of_week` | `TINYINT` | No | ISO convention: Monday = 1, ..., Sunday = 7 |
| `day_name` | `STRING` | No | English day name |
| `is_weekend` | `BOOLEAN` | No | `true` when `day_of_week IN (6,7)` |

Ngày dùng trong fact phải tìm thấy trong `dim_date`.

### 8.4 `quarantine_trips`

Quarantine giữ toàn bộ source columns để điều tra, cộng thêm:

| Column | Type | Nullable | Meaning |
| --- | --- | --- | --- |
| `source_record_id` | `STRING` | No | Deterministic source fingerprint |
| `error_rule_ids` | `ARRAY<STRING>` | No | Tất cả matching `ERROR` rules |
| `warning_rule_ids` | `ARRAY<STRING>` | No | Tất cả matching `WARN` rules |
| `error_count` | `INT` | No | Số error rules |
| `processing_month` | `STRING` | No | Tháng job đang xử lý |
| `source_file` | `STRING` | No | Source object/file name |
| `contract_version` | `STRING` | No | Rule version |
| `quarantined_at` | `TIMESTAMP` | No | Processing timestamp |

Không silently drop record lỗi.

### 8.5 `quality_metrics`

| Column | Type | Nullable | Meaning |
| --- | --- | --- | --- |
| `processing_month` | `STRING` | No | Tháng xử lý |
| `rule_id` | `STRING` | No | DQ/DS rule ID |
| `rule_version` | `STRING` | No | Contract version |
| `severity` | `STRING` | No | WARN/ERROR/FATAL |
| `action` | `STRING` | No | Applied action |
| `failed_record_count` | `BIGINT` | No | Record count matching rule |
| `total_record_count` | `BIGINT` | No | Raw count used as denominator |
| `failed_percent` | `DECIMAL(7,4)` | No | `failed / total * 100` |
| `measured_at` | `TIMESTAMP` | No | Measurement time |

Dataset summary metrics như `accepted_count` và `distinct_quarantined_count` được lưu bằng các metric IDs riêng hoặc một run-summary table; không giả vờ chúng là record-level rule violations.

## 9. Analytical metric definitions

| Metric | Definition | Population/notes |
| --- | --- | --- |
| `trip_count` | `COUNT(*)` | Accepted records only; record có warning vẫn được tính |
| `recorded_revenue` | `SUM(total_amount)` | Accepted records; không phải cash settlement/GAAP revenue; không gồm cash tips |
| `average_duration_minutes` | `AVG(duration_minutes)` | Accepted records; luôn đi kèm median/p95 khi phân tích outlier-sensitive workload |
| `median_duration_minutes` | p50 của `duration_minutes` | Accepted records |
| `p95_duration_minutes` | p95 của `duration_minutes` | Accepted records |
| `recorded_tip_amount` | `SUM(tip_amount)` | Cash tips không được source ghi nhận đầy đủ |

Metric theo ngày/giờ sử dụng local pickup time assumption ở contract version này. Mọi dashboard phải hiển thị kỳ dữ liệu và định nghĩa `recorded_revenue`.

## 10. Schema-evolution policy

### 10.1 Required source columns

Thiếu bất kỳ cột required nào ở Section 4 làm job fail trước transformation.

### 10.2 Optional source columns

Nếu optional column không tồn tại, pipeline tạo canonical column null với đúng target type, ghi warning cấp dataset và tiếp tục. Không áp dụng cách này cho required columns.

### 10.3 Type compatibility

- Widening an toàn, ví dụ integer width lớn hơn nhưng vẫn cast chính xác: chấp nhận và log.
- Timestamp chuyển thành string, numeric chuyển thành text không cast an toàn, hoặc decimal mất precision: fail job.
- Cast tạo null mới trên required field: quarantine record và ghi metric.
- Cột source mới: log warning, giữ raw source không đổi, không tự động đưa vào fact.
- Rename cột: xem là missing old column + new column; fail cho tới khi contract/mapping được version hóa.

### 10.4 Contract versioning

- Patch: sửa mô tả không thay đổi output hoặc classification.
- Minor: thêm optional output/metric tương thích ngược.
- Major: đổi grain, key, required field, kiểu không tương thích hoặc classification/action của quality rule.

## 11. Processing invariants and audit requirements

Mỗi run phải xuất/lưu được:

- `run_id`, processing month, source URI/file, source checksum.
- Contract version và application version/commit SHA nếu có.
- Raw, accepted, distinct quarantined counts.
- Count theo từng warning/error rule.
- Output locations và thời điểm bắt đầu/kết thúc.

Invariants bắt buộc:

```text
raw_count = accepted_count + distinct_quarantined_count
accepted_count = count(distinct trip_id)
duration_minutes > 0 for every accepted record
pickup/dropoff zone of every accepted record exists in dim_zone
pickup_date_key and dropoff_date_key of every accepted record exist in dim_date
processing_month of every accepted record matches the job month
```

## 12. Observed January 2025 baseline

Các số sau là baseline profiling, không phải thresholds cố định:

| Observation | Count | Percent |
| --- | ---: | ---: |
| Raw records | 3,475,226 | 100.0000% |
| Missing pickup/dropoff | 0 | 0.0000% |
| Dropoff before pickup | 124 | 0.0036% |
| Zero duration | 1,927 | 0.0554% |
| Pickup outside month | 22 | 0.0006% |
| Non-positive distance | 90,893 | 2.6155% |
| Negative fare | 144,118 | 4.1470% |
| Negative total | 63,037 | 1.8139% |
| Exact duplicate extra rows | 0 | 0.0000% |

Rule counts có thể overlap, vì vậy không cộng các count trên để suy ra quarantine count.

Observed extremes cần tiếp tục điều tra:

- `trip_distance` max `276,423.57` miles.
- `fare_amount` max `863,372.12`.
- `total_amount` max `863,380.37`.
- Pickup sớm nhất `2024-12-31 20:47:55`; pickup muộn nhất `2025-02-01 00:00:44`.

## 13. Open decisions for later versions

Các mục dưới đây không chặn local implementation `0.2.0`, nhưng phải được điều tra trước khi gọi pipeline production-ready:

1. Xác minh timezone chính thức của timestamps và chiến lược DST/UTC.
2. Phân tích negative fare/total theo vendor, payment type và rate code để quyết định tạo fact adjustments riêng hay tiếp tục quarantine.
3. Đánh giá non-positive-distance trips để xem có nhóm nghiệp vụ hợp lệ nào cần đưa trở lại fact.
4. Benchmark partition layout và file size trước khi chốt physical partition contract.
5. Xác minh công thức reconciliation của `total_amount` theo từng loại phí và tolerance.
6. Profile ít nhất 3–6 tháng để đánh giá schema drift và điều chỉnh warning thresholds.

## 14. Change history

| Version | Date | Change |
| --- | --- | --- |
| `0.2.0` | 2026-10-02 | Đồng bộ output contract với dimensional model: thêm pickup/dropoff date keys, dropoff date, store-and-forward flag và chốt ISO day-of-week |
| `0.1.0` | 2026-10-02 | Tạo contract triển khai đầu tiên từ profiling tháng 2025-01 |
