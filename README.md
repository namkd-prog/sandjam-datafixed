# Spark lấy raw và chỉnh mode

Sau khi lấy raw: xem [QUERY_README.md](QUERY_README.md) và chạy `query_reports.py`. Các query đã tổng hợp trong một script [levelplay_and_loss.sql](levelplay_and_loss.sql); bước này tách khỏi bước export raw.

Gói này chỉ xuất raw, không tính metric, không lọc classic, không loại user AB hay version và không đổi level ID. Đọc toàn bộ event của app `id6758755718` từ 01/04/2026 đến hết 06/10/2026 theo UTC+7. Giữ nguyên toàn bộ cột nguồn, bao gồm event_params và user_properties.

## Chạy trên máy/cluster có Spark

1. Copy config.example.json thành config.json. Điền source.path, source.format và output_path. Nguồn phải chứa đầy đủ raw lịch sử trong khoảng trên. Gói này đọc file đã có trên S3/HDFS/ADLS/thư mục; không tự tải từ StarRocks.
2. Cluster phải có connector/quyền đọc ghi tương ứng S3/ADLS và PySpark. Với nguồn Delta cần cấu hình Delta trên cluster. Các định dạng dùng Spark reader: parquet, orc, delta, json, csv.
3. Chạy:

```bash
spark-submit process.py --config config.json
```

event_ts phải là TimestampType hoặc chuỗi timestamp UTC, không phải epoch số. event_params/user_properties hỗ trợ JSON string, Map hoặc Struct; GA4 array key/value cần chuyển đổi trước. Cột bắt buộc: app_id, user_pseudo_id, event_ts, event_name, event_params, user_properties. Không dùng nguồn chỉ chứa level_start/level_end: sẽ thiếu loss, coin, booster, IAP, quảng cáo và activity cho churn.

## Rule chỉnh mode

- Tìm lần clear 650 đầu tiên theo user: level_end, success=true, event_params.level=650, user_properties.level=650, mode=classic. Điều kiện progression 650 tránh nhầm template 650 trong loop. Kiểm tra nguồn có field này; thiếu progression thì không tự suy là clear thật.
- Event có event_params.mode=classic và timestamp sau lần clear đó: mode_fixed=sl.
- Event ngay tại thời điểm clear giữ classic. Mode sl có sẵn giữ nguyên. Mode khác hoặc thiếu mode giữ nguyên; không tự gán mode cho event quảng cáo/IAP thiếu mode.
- Không thay event_params.mode, user_properties hay level ID. mode_fixed là cột phân loại phục vụ loại loop khỏi classic, không chứng minh user thực sự tham gia Super League.
- Nếu clear xảy ra trước 01/04 hoặc không được tracking, không có cutoff để sửa. Cần lịch sử sớm hơn nếu có các user này.

## Kết quả và cách dùng

- `raw_fixed/`: Parquet phân vùng analysis_date_utc7, tất cả raw trong lịch sử, tất cả mode/version/event/user.
- `user_clear650_cutoffs/`: user và first_clear_650_ts (UTC).
- Các cột thêm: mode_original, mode_fixed, is_mode_fixed, first_clear_650_ts, fix_reason, analysis_date_utc7, ab_group. Raw gốc vẫn nằm nguyên trong các cột nguồn.

Copy cả thư mục raw_fixed về nếu muốn query local; giữ các thư mục partition. Không gom thành một CSV lớn. Output path mới cho mỗi lần chạy; code từ chối ghi đè output đã tồn tại.

Sau khi lấy về mới đọc Parquet và query. query_after_download.sql có mẫu Spark SQL lọc AB 02–05/10 UTC+7 và mode_fixed=classic. Áp filter version thật/prepublish ở bước query, không ở bước lấy raw. Khi dùng công thức BI, thay điều kiện mode trong JSON bằng mode_fixed; các field level/coin/completion/... vẫn đọc JSON gốc. Event thiếu mode cần quy tắc attribution riêng ở bước metric; không bỏ chúng khỏi raw.

Vì giữ mọi event và mọi field nên không giới hạn nguồn theo danh sách metric đã gửi. Dữ liệu đến hết 06/10 chưa đủ quan sát trọn D3/D7 cho ngày 05/10: khi tính churn cần bổ sung activity đến hết 08/10 hoặc 12/10 tương ứng theo định nghĩa BI.

Kiểm tra trước giao: code được kiểm tra cú pháp; chưa chạy end-to-end bằng Spark tại máy này.
