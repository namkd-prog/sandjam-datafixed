# SQL tối ưu — bản chạy mặc định

## TLDR cho chị DE

Dùng nguyên folder `sandjam_reports` mới; lệnh cũ tự chạy optimized. Không cần gộp SQL thủ công hay giới hạn AB/level/version ở input.

```bash
python run_reports.py --input /path/to/raw_fixed --output /path/to/new_output \
  --work-dir /path/to/local_ssd/sandjam_tmp --memory-limit 64GB --threads 8
```

`--work-dir` không bắt buộc; nên chọn SSD local, đặc biệt khi output ở network drive. Runner tự tạo database/spill trong thư mục con riêng rồi dọn khi hoàn tất hoặc báo lỗi. Cần chỗ trống cho dữ liệu trong window và intermediate/spill; không copy toàn bộ 143 GB. Không dùng `:memory:` để giữ toàn dataset.

## RAM và CPU — máy DE nên chỉnh

**8GB/4 thread là default tương thích, không phải cấu hình khuyến nghị cho máy DE khỏe.** Chị/agent kiểm RAM còn trống và số core trước khi chạy rồi truyền CLI, không cần sửa Python/config.json. Ví dụ lệnh TLDR 64GB chỉ dùng khi máy có ít nhất128GB RAM và đang đủ trống; nếu máy64GB thì đổi thành32GB.

| RAM vật lý của máy, không có job lớn khác | Gợi ý memory-limit ban đầu |
|---|---|
| 32GB | 16GB |
| 64GB | 32GB |
| 128GB | 64GB |
| 256GB | 128GB |

Dành khoảng một nửa RAM cho DuckDB lúc đầu; cân đối RAM đang dùng và các job khác, chừa RAM cho OS/filesystem cache và allocation ngoài memory_limit. RAM càng đủ càng giảm spill. CPU có từ8 core có thể thử8 threads; máy từ16 core thử8–16, không vượt core được cấp. Nếu thiếu RAM, giảm thread vì aggregate/buffer có chi phí theo thread. Không cần ép8GB khi máy chị có thể cấp nhiều hơn. Runner tắt preserve_insertion_order cho cache lớn; CSV vẫn ORDER BY AB/level và giữ nguyên duplicate/count.

Trên Linux, kiểm nhanh `free -h` và `nproc`; khi chạy ghi rõ memory/thread/work-dir vào lệnh. Runtime manifest cũng lưu các lựa chọn này để audit.

## Các bước

| SQL | Công việc |
|---|---|
| 00_stage_input.sql | Một statement duy nhất đọc Parquet: app + UTC window, chỉ 11 cột bắt buộc; lưu window vào DuckDB trên đĩa |
| 01_normalize.sql | Chỉ decode event dùng cho report/session; extract JSON theo danh sách field, chuẩn hóa mode/level/filter; lưu cohort report |
| 02_starts_and_ads.sql | Tính một lần starters, ordinary drop và property starters |
| 03_completion_resources.sql | Tính một lần completion, APS, spend/balance, booster, pay, ads |
| 04_non_return.sql | Cache session user/ngày và cohort mature; tính D3/D7 |
| 05_loss.sql | Tính một lần distribution first/all và Churn Loss, dùng chung khi export |
| 06_reports.sql | Lưu kết quả aggregate cuối, export 3 CSV/Excel không tính lại toàn join |

CTE/view không mặc nhiên cache. Bản cũ `sql/` còn các nhánh trở về Parquet; bản này materialize các đầu vào chung và aggregate bằng TABLE. Sau normalize, view nguồn Parquet và bảng stage đã bị bỏ, vì vậy các bước còn lại chỉ đọc DuckDB. Bảng trung gian lớn được bỏ khi hết caller; database/spill được dọn trước khi công bố output. Parquet gốc không bị sửa/xóa.

Window default theo giờ địa phương là **02/10–06/10/2026**, gồm report 02–05/10 và session observation tới06/10. UTC tương ứng `[2026-10-01 17:00:00, 2026-10-06 17:00:00)`. Không cần đọc history 01/04 vì `first_clear_650_ts` đã nằm trên từng dòng corrected Parquet; runner không tính lại cutoff. Stage không lọc mode/AB/phase trước khi lấy session. Không đổi metric, denominator, attempt, AB hoặc censoring.

## Điều kiện để prune Parquet

- Nếu `event_ts` là timestamp native, WHERE đặt trực tiếp lên cột theo hai biên UTC; DuckDB có thể bỏ row group ngoài window dựa trên min/max. File được sắp theo thời gian hoặc partition đúng sẽ hiệu quả hơn.
- Nếu timestamp là VARCHAR, runner vẫn đúng logic nhưng cần cast; khả năng bỏ row group theo thời gian giảm. Manifest ghi `native_timestamp_filter`; log nêu trường hợp chuỗi. File không có stats hữu ích có thể vẫn phải đọc rộng trong **một** lượt nguồn.
- `DESCRIBE`/`EXPLAIN` có thể đọc metadata/footer nhiều file; “một scan” là một statement đọc row data, không hứa chỉ một lần mở file. Không suy tốc độ trên 143GB từ test nhỏ.

## QA và log

Optimized chỉ QA window: số dòng trong `source_qa.rows` là window đã stage, **không phải toàn history**. Timestamp không cast được không vào window; không audit timestamp/mode của các dòng ngoài window. Kiểm corrected mode dùng cutoff trên stage. Giữ kiểm thiếu cột, input window rỗng và MAX ngày chưa tới observation_end; MAX không chứng minh ngày cuối đầy đủ, DE vẫn xác nhận coverage. JSON NULL/rỗng/malformed không chặn job; session thiếu params vẫn giữ. Cần audit toàn lịch sử thì dùng pipeline process/QA riêng hoặc `--sql-mode legacy` để đối chiếu; không bật lại full scan cho report mặc định.

Log có thời gian stage và từng statement. `manifest.json` ghi `sql_mode`, `source_qa.stage_seconds`, scope/window, planned source scan count và `sql_timings`. `sql_executed/source_scan_plan.json` là EXPLAIN của stage; các SQL đã render cũng nằm cùng folder. Không xuất row/identity ở log/QA.

Để đối chiếu bản cũ trên **cùng input/config**, chọn output mới:

```bash
python run_reports.py --input /path/to/raw_fixed --output /path/to/compare_old --sql-mode legacy
```

Legacy đọc rộng và chậm hơn; không cần chạy lại trên 143GB chỉ để thử. Test giả lập so 3 CSV giữa hai mode, gồm UTC boundary, NULL, JSON Struct, ads denominator và D7; còn test chạy các bước cuối sau khi file Parquet nguồn đã được gỡ khỏi fixture. Cấu hình/bất định business giữ tại `../data_readme.md`.
