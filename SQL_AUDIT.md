# Audit SQL Spark — 07/10/2026

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
