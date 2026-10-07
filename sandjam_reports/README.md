# Chạy report Sandjam iOS trên data đã sửa

Folder độc lập, có thể copy nguyên folder vào Git của Nam. Chỉ chạy trên **raw_fixed Parquet đã được process.py của Nam sửa**, gồm `mode_fixed` và `first_clear_650_ts`. Không tải raw, không gọi StarRocks. Python dùng DuckDB đọc Parquet local, không cần Spark/Java khi tính report.

## TLDR cho chị DE / agent

1. Dùng Python 3.10 trở lên và **output raw_fixed của Nam**, không dùng raw chưa sửa.
2. Kiểm input có activity đủ tới 06/10. Nếu ngày hoàn chỉnh cuối khác06/10, sửa `observation_end` trong config; không đoán từ MAX timestamp. Giữ report 02–05/10 và mọi version/level.
3. Chạy từfolder này, thay đúng 2 đường dẫn:

```bash
cd sandjam_reports
python -m pip install -r requirements.txt
python run_reports.py --input /path/to/raw_fixed --output /path/to/sandjam_output_20261007
```

4. Thành công phải in `COMPLETE`, có 3 CSV + Excel3 tab + `data_readme.md` + `manifest.json` với `status=complete`. Nếu thiếu cột mode_fixed/cutoff, quay lại output process của Nam; **không tự tạo mode_fixed từ raw mode để vượt kiểm tra**.
5. D7 của report 02–05/10 đang N/A vì chưa đủ tuổi; không đổi thành 0. Hai caveat công thức cần giữ khi bàn giao: Loss Churn là reconstruction khớp ảnh, mẫu số ads theo property là candidate; chi tiết ở data_readme.

JSON NULL/rỗng/sai cú pháp **không chặn job**, không cần lọc/xóa các dòng `app_loading` thiếu params. Runner đọc giá trị không parse được thành NULL, giữ dòng và Parquet gốc; không quét toàn history để kiểm JSON. Các kiểm tra schema, timestamp, coverage và corrected mode vẫn giữ.

## Input, cấu hình và output

`--input` nhận thư mục có partition/subfolder, một file Parquet hoặc glob được quote. Ví dụ `--input '/data/raw_fixed/**/*.parquet'`. Đưa **toàn bộ raw_fixed** vào; không pre-filter chỉ gameplay classic, AB hoặc02–05/10 vì cần session dùng observation và cutoff cho ads/IAP. Map/Struct JSON columns được chuyển về JSON string trong lúc đọc. GA4 key/value array cần adapter, runner báo lỗi rõ.

`config.json` đã đặt đúng yêu cầu:

- Sandjam iOS `id6758755718`, report 02–05/10/2026 inclusive, timezone UTC+7.
- Classic sau sửa; AB22 nhóm 0/1/2; **mọi version, mọi level**.
- As-of07/10, ngày nguồn hoàn chỉnh cuối06/10. DE xác nhận nguồn phủ đầy đủ ngày này, không chỉ dựa vào MAX timestamp.
- Coin balance selected resource=`coin`. Ads denominator mặc định=`property` theo candidate khớp ảnh; xem caveat trong `data_readme.md`.

Output folder mới gồm:

```text
data_metrics.csv
loss_first_attempt.csv
loss_all_attempts.csv
sandjam_reports.xlsx       # đúng 3 tab, màu heatmap/filter/freeze
data_readme.md             # định nghĩa + cấu hình thực chạy
manifest.json              # trạng thái complete, QA aggregate và hashes
sql_executed/              # SQL/config đúng lần chạy
```

Runner không ghi đè output. CSV UTF-8 BOM, NULL ghi `N/A`, giữ precision; Excel hiển thị2 số lẻ. Chỉ công bố output sau khi cả 3 CSV và Excel hoàn tất; lỗi dọn staging và exit khác0. Không xuất identifier. Có thể chỉnh RAM/thread:

```bash
python run_reports.py --input /path/to/raw_fixed --output /path/to/new_output \
  --memory-limit 4GB --threads 4 --row-cap 20000
```

SQL tách theo công việc, chạy tuần tự:

| File | Công việc |
|---|---|
| sql/00_normalize.sql | Parse JSON, timezone, fixed mode, system/report filters |
| sql/01_starts_and_ads.sql | Starters, ordinary drop, property starters cho Inter |
| sql/02_completion_resources.sql | APS, completion, spend/balance, booster, pay, ads |
| sql/03_non_return.sql | Mature windows và Data Metrics Non Return D3/D7 |
| sql/04_loss.sql | First/all distribution và auxiliary Churn Loss |
| sql/05_reports.sql | Ghép các cột thành3 output |

Đây là cú pháp **DuckDB**, không paste thẳng vào Spark/StarRocks. `run_reports.py` tạo input/config view, date macros, render placeholder và cache chỉ window report/observation, dùng disk spill khi cần.

Đã kiểm số raw BI và kiểm runner bằng Parquet giả lập; chưa chạy dữ liệu corrected thật vì nằm trên máy DE. QA chính và giới hạn nguồn công thức ở `data_readme.md`. Khi Long cung cấp SQL phụ, phần cần đối chiếu là `loss_churn_counts` trong04 và `property_starters`/ads trong01/05; không cần mò cả pipeline.

Kiểm package sau copy:

```bash
python -m unittest discover -s tests -v
```
