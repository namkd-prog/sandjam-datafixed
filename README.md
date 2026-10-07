# Spark lấy raw và chỉnh mode


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


Vì giữ mọi event và mọi field nên không giới hạn nguồn theo danh sách metric đã gửi. Dữ liệu đến hết 06/10 chưa đủ quan sát trọn D3/D7 cho ngày 05/10: khi tính churn cần bổ sung activity đến hết 08/10 hoặc 12/10 tương ứng theo định nghĩa BI.


## Query hai bảng BI sau khi lấy raw

`levelplay_and_loss.sql` là một script Spark SQL tổng hợp. `query_reports.py` nạp raw/config, chạy script và xuất Levelplay/Loss dạng Parquet và CSV. `process.py` vẫn chỉ lấy raw + chỉnh mode; không tính metric.

```bash
spark-submit query_reports.py --input s3a://YOUR_BUCKET/exports/raw_mode_run1/raw_fixed --output s3a://YOUR_BUCKET/reports/ab_run1 --observation-end 2026-10-06
```

Đặt `--observation-end` là ngày nguồn đã đủ, theo UTC+7. Không coi có một event vào ngày đó là nguồn đã hoàn chỉnh. Ngày `--as-of` mặc định là ngày chạy UTC+7. Có thể chỉ định để tái lập kết quả.

This is Spark SQL. Run query_reports.py with levelplay_and_loss.sql in the same directory. The Python runner creates the input/config views and splits SQL statements; no separate helper file is required. This SQL is not executable directly on StarRocks.

Mặc định app id6758755718, ngày 02–05/10/2026, experiment firebase_exp_abt_22, groups 0/1/2, version 0.6.3–0.6.7, level 1–200. Dùng `--level-max 650` nếu cần toàn bộ classic. Script đọc thêm level tiếp theo ở biên range cho drop rate. Coin Balance mặc định resources=[coin]; thay `--balance-resources coin,ticket` nếu bộ lọc BI chọn những resource đó. Total Balance cộng mọi key hợp lệ theo tài liệu, không dùng chung công thức với Coin Balance.

## Công thức và phạm vi

- IMP/LAU chỉ đếm paid_ad_impression được phân loại inter hoặc rwd (regex inter ưu tiên, rồi reward/video); loại banner, native và các format khác. Rwd/LAU chỉ đếm rwd. Đã query ad formats thực tế 02–05/10: rewarded 44.561, interstitial 32.889, native_advanced 40.662, banner 4.852 (AB production, chưa áp cutoff/mode/range nên đây không phải tử số report cuối).
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

Đã kiểm tra Python và chạy 24 câu Spark SQL trên PySpark 3.5.3 với 369 dòng raw mẫu thực tế lấy từ StarRocks. Đối chiếu cùng sample giữa hai engine; chi tiết ở the audit section below. Đây là kiểm chứng SQL trên sample, chưa phải chạy toàn bộ export raw hay kiểm chứng connector S3/HDFS/ADLS và ghi report trên cluster đích.

Audit StarRocks ngày 07/10/2026 phát hiện user đã có progression>650 nhưng không có clear650 đúng điều kiện trong lịch sử 01/04–06/10. Giữ rule cutoff-only đã chốt, không tự suy cutoff hoặc loại user. Runner xuất `qa_missing_clear650_csv` và cảnh báo; cần kiểm tra/bổ sung lịch sử trước khi coi report đã loại hết loop. Event bị thiếu mode/level cũng giữ nguyên trong raw nhưng không vào metric strict classic. Lần start 1 bị lặp vẫn cộng theo SQL nguồn, không tự deduplicate.


## Audit SQL Spark — 07/10/2026

## Sửa sau khi query dữ liệu thật

- Game id6758755718 không có in_app_purchase_v2 trong 01/04–06/10 UTC+7. Có 9.975 in_app_purchase; 38 trong 02–05/10, 25 có AB group và production version (bao gồm event mode bonus). Đã bỏ v2 khỏi logic Pay Rate/report.
- IMP/LAU, Rwd/LAU: SQL gốc dùng user_properties.level/mode cho cả quảng cáo và mẫu số level_start. Bản trước dùng mẫu số gameplay event_params, không đúng khi hai nguồn khác nhau. Đã tạo ad_start_counts riêng.
- Runner kiểm tra observation_end không vượt ngày cuối có trong nguồn; xuất QA cho progression>650 nhưng thiếu cutoff clear650.

## Kiểm tra StarRocks

- Query đầy đủ tương ứng sau sửa chạy thành công: Levelplay 600 dòng, Loss 421 dòng. Không ghi dữ liệu hay đổi cấu hình global trên server.
- Trong production version và AB period: không có user đổi giữa các variant 0/1/2.
- Không có completion ngoài 0–100 hoặc winning event thiếu/nonpositive start_count trong phạm vi metric đã kiểm tra.
- Phase>1/fractional event level không xuất hiện trong các event AB/production đã audit.
- Có 6.698 classic level_start events có event level khác property level, nên mẫu số ads phải đọc đúng nguồn.
- Có 368 user-level-variant có nhiều level_start_turn start_count=1. Giữ SUM theo SQL BI, không tự deduplicate.
- Có 3 loss start1 events không có start cùng user/level/variant trong cùng period. Giữ event loss theo SQL nguồn; không bắt buộc join với start để đếm loss.
- Có 22 AB ads events thiếu property mode/level, không vào strict-classic report; vẫn nằm trong raw.

## Rủi ro lịch sử mode chưa được tự sửa

Audit parsed AB events thấy 33.746 events có event mode classic, property progression>650 nhưng không tìm thấy clear650 hợp lệ từ 01/04. Điều kiện clear giữ như đã chốt: level_end success=true, event level650, property level650, mode classic. Chưa xác định do history thiếu, clear tracking khác hay progression khác. Không tự tạo cutoff hoặc đổi mode theo một rule mới.

Vì thế SQL chạy thành công chưa chứng minh đã loại hết loop. Runner cảnh báo và xuất qa_missing_clear650_csv (aggregate theo event_name, không xuất user IDs). Cần bổ sung/xác minh lịch sử trước khi coi báo cáo cuối cùng đã sạch loop.

## Công thức được giữ

- Churn Levelplay thường và hai cột churn mang tên D3 ở Loss: drop start X không start X+1 / start X.
- Levelplay D3/D7: Non Return Rate +1..+N; cùng eligible date range cho numerator/denominator, MAX level/user/day, session_start/screen_view không lọc mode/level/variant. Ngày 07/10 D3 chỉ tới 03/10, D7 trong report 02–05/10 chưa đủ quan sát, để NULL.
- Completion dùng AVG completion level_end continue_times=0/null, không phải win-first ratio.
- Coin vs total spend và selected vs total balance giữ riêng theo tài liệu; balance numerator attempt1, denominator mọi attempt.
- Raw và level ID không overwrite. Ads/IAP lấy property mode/level rồi áp cutoff; gameplay lấy mode_fixed.

## Giới hạn kiểm chứng

Sample 369 dòng: 24 câu Spark SQL đều phân tích/thực thi thành công. Levelplay 31 dòng, 434 ô số khớp; Loss 2 dòng, 27 ô số khớp StarRocks trên chính cùng sample (tolerance 1e-6, keys/nulls cũng đối chiếu). Đây là kiểm tra tương thích engine/formula, không xác nhận sample đủ để suy kết quả full report.

Đã chạy SQL Spark trên sample thực tế, kiểm tra output bằng cùng sample trong StarRocks. Không upload raw sample/user IDs lên GitHub. Chưa chạy full-history Spark export hoặc xác minh I/O của cluster đích. Exclude pre-publish vẫn thiếu danh sách Internal experiment IDs của app; không tự gán debug_event=1 thành test user.
