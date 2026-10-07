> Đây là định nghĩa/cấu hình của gói, chưa phải kết quả chạy trên corrected Parquet thật.
> Runner sẽ tạo bản data_readme.md riêng trong output kèm row counts và QA của lần chạy.

# Dữ liệu Sandjam iOS

App `id6758755718`. Report **2026-10-02–2026-10-05**, ngày UTC+7, bao gồm hai biên.
Input là **Parquet raw_fixed đã được process của Nam sửa**, không đọc/query StarRocks. Gameplay lọc `mode_fixed='classic'`;
ads/IAP lấy `user_properties.mode` và áp cùng `first_clear_650_ts` để loại classic sau cutoff. Không sửa JSON hay level ID.
Chia theo `firebase_exp_abt_22`, nhóm 0/1/2. Version: mọi version. **Mọi level**, không giới hạn1–200.

| File / tab Excel | Nội dung |
|---|---|
| data_metrics.csv / Data Metrics | Data Metrics của Level Play, AB × level |
| loss_first_attempt.csv / Loss 1st attempt | level_end_turn, success=false, start_count=1, completion nonnull |
| loss_all_attempts.csv / Loss All | Cùng điều kiện loss, không giới hạn start_count |

`sandjam_reports.xlsx` có đúng 3 tab, freeze AB/level, filter và heatmap. CSV UTF-8 BOM.
Giá trị phần trăm là **percentage points**:16.67 nghĩa16.67%, không phải0.1667. CSV giữ precision tính toán; Excel hiển thị2 số lẻ.
`N/A` là NULL/thiếu observation hoặc không có format ads;0 là số tính được bằng0.

## Công thức và phạm vi kiểm

- User count=distinct user; lose count=số event. Vì có lặp event/start_count, Loss1st cũng có thể lose_count>user_count.
- Distribution: distinct user trong từng bucket / distinct user thua tại level. All attempts có thể cộng bucket >100% vì user xuất hiện ở nhiều bucket. Biên `<10`, `<20`,...; bucket cuối>=90.
- Ordinary churn=startersX không startX+1 trong cùng period/AB. D3/D7 Data Metrics=Non Return, highest level theo user/ngày, không session_start/screen_view trong+1..N; denominator là starters trong cohort mature.
- Completion Rate First Attempt=AVG completion trên level_end, continue_times 0/null; không phải win-first rate và không thêm start_count=1.
- Coin spend=spend coin value_game_currency / starters; Total coin spend=mọi spend value_game_currency / starters.
- Balance numerator: resources không âm ở level_start_turn attempt1; coin hoặc mọi coin/ticket/booster hợp lệ trừ use_booster_count. Denominator=distinct start_turn user mọi attempt.
- Total IMP/LAU chỉ Inter+Rewarded. Cả ba cột ads dùng **property-level starters**; property là candidate khớp cả 3 cột ở 10 level ảnh, chưa có SQL BI xác nhận. Có thể chọn event trong config để so mẫu số event-level trong code Nam.
- Loss Churn: starters mature D3, highest level user/ngày; không level_end cùng level trong report, không session_start/screen_view sau ngày chơi tới observation_end. Churn độc lập user thua/attempts, nên cùng số ở2 bảng Loss và có thể lớn hơn user_count.
- **Loss Churn là numeric reconstruction** đã khớp count/rate ở24 level 3–26 trên raw BI 23/09–06/10. SQL phụ BI/Long chưa xác nhận business logic. Không gọi nó là công thức nguồn BI đã xác nhận.
- QA ảnh: 240 ô nhìn thấy (2 attempts × 24 level × user_count/lose_count/0–10/Churn count/rate) khớp; 144 ô Loss 1st đủ 10 bucket level 3–14 đã khớp trước đó. 130 ô Data Metrics ngoài ads và 30 ô ads dùng mẫu số property ở level 1–10 khớp. Coverage này không chứng nhận mọi level/report 02–05/10 đã sửa mode.
- Không áp Internal/pre-publish theo yêu cầu người dùng. Có system filters từ SQL nguồn, classic strict, phase<=1 cho Data Metrics; Loss distribution giữphase theo SQL show-query.

## Observation và censoring

As-of: 2026-10-07; ngày hoàn chỉnh cuối nguồn: 2026-10-06. Ngày eligible cuối:
[{"n": 3, "eligible_end": "2026-10-03"}, {"n": 7, "eligible_end": "2026-09-29"}].
Với default 07/10 và observation 06/10: D3 dùng play_date đến03/10; D7 đến29/09. Report02–05/10 chỉ có02–03/10 mature D3;
D7 không có cohort đủ tuổi nên **N/A**, không đổi thành 0. Muốn D7 của toànperiod cần input có activity tới 12/10 và as-of 13/10 trở đi.
MAX ngày nguồn chỉ là bound, không chứng minh partition của observation_end đã đầy đủ; DE phải xác nhận ngày hoàn chỉnh.
Không suy thiếu event là thiếu hành vi; không dedupe event tự ý để khớp BI.

Input QA aggregate:{"status": "chưa chạy dữ liệu thật; chỉ có trên máy DE"}.
Số dòng:{}.
Chi tiết cấu hình, schema, nguồn SQL và hash CSV/Excel ởmanifest.json; không xuất identifier.
