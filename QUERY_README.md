# Query hai bảng BI sau khi lấy raw

`levelplay_and_loss.sql` là một script Spark SQL tổng hợp. `query_reports.py` nạp raw/config, chạy script và xuất Levelplay/Loss dạng Parquet và CSV. `process.py` vẫn chỉ lấy raw + chỉnh mode; không tính metric.

```bash
spark-submit query_reports.py --input s3a://YOUR_BUCKET/exports/raw_mode_run1/raw_fixed --output s3a://YOUR_BUCKET/reports/ab_run1 --observation-end 2026-10-06
```

Đặt `--observation-end` là ngày nguồn đã đủ, theo UTC+7. Không coi có một event vào ngày đó là nguồn đã hoàn chỉnh. Ngày `--as-of` mặc định là ngày chạy UTC+7. Có thể chỉ định để tái lập kết quả.

Đây là **Spark SQL, không dán vào StarRocks**. Runner cần cùng thư mục với `levelplay_and_loss.sql` và `sql_statements.py`. Nó tự tạo raw_fixed_input/report_config. Nếu chạy trên StarRocks phải chuyển dialect và thay nguồn bằng bảng/view StarRocks, kèm tính cutoff mode từ raw gốc nếu bảng chưa có mode_fixed.

Mặc định app id6758755718, ngày 02–05/10/2026, experiment firebase_exp_abt_22, groups 0/1/2, version 0.6.3–0.6.7, level 1–200. Dùng `--level-max 650` nếu cần toàn bộ classic. Script đọc thêm level tiếp theo ở biên range cho drop rate. Coin Balance mặc định resources=[coin]; thay `--balance-resources coin,ticket` nếu bộ lọc BI chọn những resource đó. Total Balance cộng mọi key hợp lệ theo tài liệu, không dùng chung công thức với Coin Balance.

## Công thức và phạm vi

- Churn Rate: user level_start X nhưng không level_start X+1 / user level_start X, trong cùng period và variant.
- D3/D7: Non Return Rate theo tài liệu mới, session_start/screen_view ở ngày +1..+N. Ngày đủ điều kiện = min(report_end, as_of_date-(N+1), observation_end-N). Mẫu số cũng cắt theo khoảng ngày này. Churn gán ở level cao nhất user chơi trong ngày trong range truy vấn. Không cộng users_active.
- Completion Rate First Attempt: AVG completion trên level_end không continue (0/null), không lọc start_count=1, không đổi sang tỷ lệ win-first.
- Coin/Total Coin Spend: SUM value_game_currency chỉ currency coin / mọi currency; mẫu số user level_start.
- Balance: số dư không âm ở level_start_turn start_count=1; mẫu số distinct user level_start_turn ở mọi attempt. Total gồm coin/ticket/key chứa booster, trừ use_booster_count.
- APS: trung bình có trọng số start_count theo số distinct winning user trong mỗi start_count trên level_end.
- Booster/Pay/Ads: booster spend value, user dùng booster trên level_end_turn, payer in_app_purchase giao với starters. Game này không có in_app_purchase_v2 trong lịch sử 01/04–06/10/2026 đã query kiểm tra; đã bỏ v2 khỏi logic report. Impression paid_ad_impression, rewarded format reward/video (inter ưu tiên). Mẫu số IMP/LAU và Rwd/LAU là user level_start đọc level/mode từ user_properties theo query BI gốc, không dùng lại mẫu số gameplay đọc event_params.
- Loss: level_end_turn, success=false, completion nonnull, start_count=1. Bucket đếm distinct user / distinct losing user; tổng % có thể >100 vì một user xuất hiện ở nhiều bucket. Giữ cách bucket của query gốc, không tự loại completion ngoài 0–100.

## Mode và raw

Giữ nguyên raw và level ID. Gameplay/currency dùng mode_fixed. Query BI gốc Ads/IAP dùng user_properties.mode/level: script đọc đúng nguồn đó và áp lại cutoff first_clear_650_ts cho property mode classic. Không tự lấy mode của user thành mode gameplay. Filter strict classic theo yêu cầu hiện tại; khác BI mặc định có gộp null/empty. Nếu thiếu mode nguồn, event không vào metric nhưng vẫn nằm trong raw export.

Giữ các system filter đã có trong nguồn SQL (China android, balance lớn, purchase outlier, earn âm). Chưa có danh sách experiment Internal của app để tái lập filter Exclude pre-publish; không tự coi debug_event=1 là playtest. Nếu BI áp filter này cần bổ sung đúng IDs. Raw không bị loại bởi các filter report.

## Churn trong bảng Loss

Người dùng đã xác nhận hai cột mang tên D3 trong Loss là drop rate. `churn_users_d3` đếm distinct user start X nhưng không start X+1; `churn_rate_d3_pct` chia số đó cho distinct user start X, nhân 100. Dùng cùng variant, period, mode và filter report. Mẫu số không phải user thua start 1. Giữ tên cột để khớp bảng BI nhưng metric này không dùng cửa sổ 3 ngày. Loss counts/buckets vẫn chỉ xét thua start_count=1. Churn trong Levelplay giữ nguyên công thức trước: Churn Rate là drop rate, D3/D7 là Non Return Rate.

Ngày 07/10/2026: D3 chỉ có thể dùng play_date tới 03/10 nếu đã đủ activity; D7 chưa có ngày đủ quan sát trong 02–05/10, trả NULL. Muốn đủ cả period cần bổ sung activity đến hết 08/10 (D3), 12/10 (D7), theo T-1 chạy từ 09/10, 13/10. Không lọc activity comeback theo mode/level/variant; vẫn giữ filter version và system theo query nguồn.

Đã kiểm tra Python và chạy 24 câu Spark SQL trên PySpark 3.5.3 với 369 dòng raw mẫu thực tế lấy từ StarRocks. Đối chiếu cùng sample giữa hai engine; chi tiết ở SQL_AUDIT.md. Đây là kiểm chứng SQL trên sample, chưa phải chạy toàn bộ export raw hay kiểm chứng connector S3/HDFS/ADLS và ghi report trên cluster đích.

Audit StarRocks ngày 07/10/2026 phát hiện user đã có progression>650 nhưng không có clear650 đúng điều kiện trong lịch sử 01/04–06/10. Giữ rule cutoff-only đã chốt, không tự suy cutoff hoặc loại user. Runner xuất `qa_missing_clear650_csv` và cảnh báo; cần kiểm tra/bổ sung lịch sử trước khi coi report đã loại hết loop. Event bị thiếu mode/level cũng giữ nguyên trong raw nhưng không vào metric strict classic. Lần start 1 bị lặp vẫn cộng theo SQL nguồn, không tự deduplicate.
