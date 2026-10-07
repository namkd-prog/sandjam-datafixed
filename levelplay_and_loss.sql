-- HUONG DAN CHAY: SQL nay tinh report SAU KHI lay raw, khong tai raw tu StarRocks.
-- Buoc 1 - Python doc file nguon S3/HDFS/ADLS/local, giu raw va them mode_fixed:
--   spark-submit process.py --config config.json
--   Sua source.path/output_path trong config.json truoc khi chay.
--   Output: <output_path>/raw_fixed (Parquet, 01/04 den het 06/10 UTC+7).
-- Buoc 2 - Python nap raw_fixed, tao view/config va chay toan bo SQL nay:
--   spark-submit query_reports.py --input /path/raw_fixed --output /path/reports_run1 --observation-end 2026-10-06
--   Output: levelplay_report, loss_report (Parquet), va *_csv trong output.
-- Khong dan truc tiep file vao StarRocks: day la cu phap Spark SQL.
-- Neu chay tay tren Spark SQL, phai tao raw_fixed_input/report_config nhu runner truoc.
-- Giu level ID goc, tach Coin Balance va Total Coin Balance.
-- Loss churn la DROP RATE, Levelplay D3/D7 la NON RETURN RATE.
-- Cac ngay chua du quan sat Dx de NULL, khong gan thanh 0.
-- Spark SQL, run via query_reports.py after process.py.
-- raw_fixed_input: original raw + mode_fixed + first_clear_650_ts.
-- report_config: one row supplied by the runner, dates in UTC+7.
-- Original level IDs and raw JSON are never overwritten.
-- 01. Doc JSON raw: success, attempt, completion, currency, booster, ad format.
CREATE OR REPLACE TEMP VIEW parsed_events AS
SELECT r.*,
 CAST(event_ts AS TIMESTAMP) AS ts_utc,
 CAST(get_json_object(event_params,'$.level') AS DOUBLE) AS raw_level,
 CAST(get_json_object(user_properties,'$.level') AS DOUBLE) AS property_level,
 CAST(get_json_object(event_params,'$.phase') AS INT) AS event_phase,
 CAST(get_json_object(user_properties,'$.phase') AS INT) AS property_phase,
 get_json_object(user_properties,'$.firebase_exp_abt_22') AS variant,
 get_json_object(user_properties,'$.mode') AS property_mode,
 LOWER(get_json_object(event_params,'$.success')) AS success,
 CAST(get_json_object(event_params,'$.start_count') AS INT) AS attempt,
 CAST(get_json_object(event_params,'$.continue_times') AS INT) AS continues,
 CAST(get_json_object(event_params,'$.completion') AS DOUBLE) AS completion,
 CAST(get_json_object(event_params,'$.use_booster_count') AS DOUBLE) AS used_boosters,
 CAST(get_json_object(event_params,'$.value') AS DOUBLE) AS currency_value,
 CAST(get_json_object(event_params,'$.value_game_currency') AS BIGINT) AS game_currency_value,
 LOWER(get_json_object(event_params,'$.virtual_currency_name')) AS currency_name,
 LOWER(get_json_object(event_params,'$.virtual_currency_type')) AS currency_type,
 LOWER(COALESCE(get_json_object(event_params,'$.ad_format'),'')) AS ad_format
FROM raw_fixed_input r WHERE app_id='id6758755718';

-- 02. Ngay UTC+7, level/phase theo BI, mode cho gameplay va Ads/IAP.
CREATE OR REPLACE TEMP VIEW normalized_events AS
SELECT *, DATE(ts_utc + INTERVAL 7 HOURS) AS local_date,
 CAST(CASE WHEN INSTR(get_json_object(user_properties,'$.level'),'.')>1
 THEN REPLACE(get_json_object(user_properties,'$.level'),'.1','')
 WHEN property_phase>1 THEN CONCAT(CAST(CAST(property_level AS INT) AS STRING),'.',CAST(property_phase AS STRING))
 ELSE get_json_object(user_properties,'$.level') END AS DOUBLE) AS property_bi_level,
 CASE WHEN property_mode='classic' AND ts_utc>first_clear_650_ts THEN 'sl' ELSE property_mode END AS property_mode_fixed,
 CASE WHEN event_name IN ('paid_ad_impression','in_app_purchase','app_remove')
 THEN CAST(CASE WHEN INSTR(get_json_object(user_properties,'$.level'),'.')>1
 THEN REPLACE(get_json_object(user_properties,'$.level'),'.1','')
 WHEN property_phase>1 THEN CONCAT(CAST(CAST(property_level AS INT) AS STRING),'.',CAST(property_phase AS STRING))
 ELSE get_json_object(user_properties,'$.level') END AS DOUBLE)
 ELSE CAST(CASE WHEN INSTR(get_json_object(event_params,'$.level'),'.')>1
 THEN REPLACE(get_json_object(event_params,'$.level'),'.1','')
 WHEN event_phase>1 THEN CONCAT(CAST(CAST(raw_level AS INT) AS STRING),'.',CAST(event_phase AS STRING))
 ELSE get_json_object(event_params,'$.level') END AS DOUBLE) END AS level,
 -- Ads/IAP use user_properties.mode in the source BI query. Apply the same cutoff.
 CASE WHEN event_name IN ('paid_ad_impression','in_app_purchase','app_remove')
 THEN CASE WHEN property_mode='classic' AND ts_utc>first_clear_650_ts THEN 'sl' ELSE property_mode END
 ELSE mode_fixed END AS metric_mode
FROM parsed_events;

-- 03. System filters theo SQL nguon. Khong thay doi raw export.
CREATE OR REPLACE TEMP VIEW system_clean AS
SELECT * FROM normalized_events
WHERE (NOT(country='China' AND platform='android') OR country IS NULL)
 AND NOT(CASE WHEN event_name='level_start_turn' THEN COALESCE(CAST(get_json_object(event_params,'$.coin') AS DOUBLE),0) ELSE 0 END>1000000)
 AND NOT(CASE WHEN event_name='in_app_purchase' AND get_json_object(event_params,'$.currency')='USD' THEN COALESCE(currency_value,0)
 WHEN event_name='in_app_purchase' AND get_json_object(event_params,'$.currency')='ILS' THEN COALESCE(currency_value,0)*0.33 ELSE 0 END>=300000000)
 AND (CASE WHEN event_name='earn_virtual_currency' THEN game_currency_value ELSE 1 END>=0);

-- 04. Filter report 02-05/10, version that, AB groups 0/1/2, classic sau sua.
CREATE OR REPLACE TEMP VIEW ab_classic_events AS
SELECT e.* FROM system_clean e CROSS JOIN report_config c
WHERE local_date BETWEEN c.report_start AND c.report_end
 AND ARRAY_CONTAINS(c.versions,app_version) AND variant IN ('0','1','2')
 -- Strict classic as requested; do not include null/empty modes implicitly.
 AND metric_mode='classic';

-- 05. User start theo level, doc them level cuoi+1 de tinh drop rate.
CREATE OR REPLACE TEMP VIEW starts AS
SELECT DISTINCT variant,level,user_pseudo_id FROM ab_classic_events CROSS JOIN report_config c
WHERE event_name='level_start' AND level BETWEEN c.level_min AND c.level_max+1
 AND (event_phase IS NULL OR event_phase<=1);

-- 06. Mau so user bat dau level (level_start).
CREATE OR REPLACE TEMP VIEW start_counts AS
SELECT variant,level,COUNT(DISTINCT user_pseudo_id) AS user_start_count
FROM starts CROSS JOIN report_config c WHERE level<=c.level_max GROUP BY variant,level;

-- 07. Drop users: start X nhung khong start X+1 trong cung period/variant.
CREATE OR REPLACE TEMP VIEW drop_counts AS
SELECT x.variant,x.level,COUNT(DISTINCT CASE WHEN y.user_pseudo_id IS NULL THEN x.user_pseudo_id END) AS dropped_users
FROM starts x LEFT JOIN starts y ON x.variant=y.variant AND x.user_pseudo_id=y.user_pseudo_id AND y.level=x.level+1
GROUP BY x.variant,x.level;

-- Original Churn query returns start/drop/remove counts separately.
-- app_remove does NOT have a mode filter in the supplied source SQL.
-- Keep remove count as a separate component, never add it to drop count.
CREATE OR REPLACE TEMP VIEW churn_components AS
WITH removes AS (
 SELECT DISTINCT e.variant,e.property_bi_level AS level,e.user_pseudo_id
 FROM system_clean e CROSS JOIN report_config c
 WHERE e.event_name='app_remove' AND e.local_date BETWEEN c.report_start AND c.report_end
 AND ARRAY_CONTAINS(c.versions,e.app_version) AND e.variant IN ('0','1','2')
 AND (INSTR(get_json_object(e.user_properties,'$.level'),'.')=0 OR e.property_phase IS NULL OR e.property_phase<=1)
), remove_counts AS (
 SELECT s.variant,s.level,COUNT(DISTINCT r.user_pseudo_id) AS user_remove_count
 FROM starts s JOIN removes r ON s.variant=r.variant AND s.level=r.level AND s.user_pseudo_id=r.user_pseudo_id
 GROUP BY s.variant,s.level
)
SELECT c.variant,c.level,c.user_start_count,
 COALESCE(d.dropped_users,0) AS user_dropped_count,
 COALESCE(r.user_remove_count,0) AS user_remove_count
FROM start_counts c
LEFT JOIN drop_counts d ON c.variant=d.variant AND c.level=d.level
LEFT JOIN remove_counts r ON c.variant=r.variant AND c.level=r.level;

-- 08. Range level va phase cho metric, theo nguon param/property tuong ung.
CREATE OR REPLACE TEMP VIEW metric_events AS
SELECT e.* FROM ab_classic_events e CROSS JOIN report_config c
WHERE level BETWEEN c.level_min AND c.level_max
 AND CASE WHEN event_name IN ('in_app_purchase','app_remove') THEN property_phase IS NULL OR property_phase<=1
 WHEN event_name='paid_ad_impression' THEN TRUE ELSE event_phase IS NULL OR event_phase<=1 END;

-- 09. Coin spend/total spend, booster, impressions, rewarded, AVG completion.
-- Completion Rate First Attempt theo BI KHONG loc attempt=1, chi continue=0/null.
CREATE OR REPLACE TEMP VIEW level_stats AS
SELECT variant,level,
 COUNT(DISTINCT CASE WHEN event_name='level_end_turn' AND used_boosters>0 THEN user_pseudo_id END) AS booster_users,
 SUM(CASE WHEN event_name='spend_virtual_currency' AND currency_name='coin' THEN game_currency_value ELSE 0 END) AS coin_spend,
 SUM(CASE WHEN event_name='spend_virtual_currency' THEN game_currency_value ELSE 0 END) AS total_coin_spend,
 SUM(CASE WHEN event_name='spend_virtual_currency' AND currency_type='booster' AND currency_name IS NOT NULL AND currency_name<>'' THEN currency_value ELSE 0 END) AS booster_count,
 -- IMP/LAU only includes inter + rwd, not banner/native/other formats.
 SUM(CASE WHEN event_name='paid_ad_impression' AND ad_format RLIKE 'inter|reward|video' THEN 1 ELSE 0 END) AS impressions,
 SUM(CASE WHEN event_name='paid_ad_impression' AND NOT(ad_format RLIKE 'inter') AND ad_format RLIKE 'reward|video' THEN 1 ELSE 0 END) AS reward_impressions,
 AVG(CASE WHEN event_name='level_end' AND (continues=0 OR continues IS NULL) THEN completion END) AS avg_completion
FROM metric_events GROUP BY variant,level;

-- 10. Payer tai level giao voi users da start level do.
CREATE OR REPLACE TEMP VIEW pay_counts AS
SELECT e.variant,e.level,COUNT(DISTINCT e.user_pseudo_id) AS payers FROM metric_events e
JOIN starts s ON e.variant=s.variant AND e.level=s.level AND e.user_pseudo_id=s.user_pseudo_id
WHERE e.event_name='in_app_purchase' GROUP BY e.variant,e.level;

-- IMP/LAU and Rwd/LAU source query uses user_properties level/mode for BOTH
-- impression numerator and level_start denominator. Do not reuse gameplay starters.
CREATE OR REPLACE TEMP VIEW ad_start_counts AS
SELECT variant,property_bi_level AS level,COUNT(DISTINCT user_pseudo_id) AS users
FROM system_clean e CROSS JOIN report_config c
WHERE event_name='level_start' AND local_date BETWEEN c.report_start AND c.report_end
 AND ARRAY_CONTAINS(c.versions,app_version) AND variant IN ('0','1','2')
 AND property_mode_fixed='classic' AND property_bi_level BETWEEN c.level_min AND c.level_max
GROUP BY variant,property_bi_level;

-- 11. APS: weighted average start_count theo distinct user win o moi attempt.
CREATE OR REPLACE TEMP VIEW aps AS
WITH wins AS (
 SELECT variant,level,attempt,COUNT(DISTINCT user_pseudo_id) AS users
 FROM metric_events WHERE event_name='level_end' AND success='true' AND attempt>0 GROUP BY variant,level,attempt
)
SELECT variant,level,SUM(attempt*users)*1.0/SUM(users) AS aps_avg FROM wins GROUP BY variant,level;

-- Balance denominator includes every attempt, not only attempt 1.
-- 12. Mau so balance: users level_start_turn o MOI attempt.
CREATE OR REPLACE TEMP VIEW balance_starters AS
SELECT variant,level,COUNT(DISTINCT user_pseudo_id) AS users
FROM metric_events WHERE event_name='level_start_turn' GROUP BY variant,level;

-- 13. Tu so balance: resources khong am o attempt=1. Total cong moi key hop le.
CREATE OR REPLACE TEMP VIEW balance_sums AS
WITH resources AS (
 SELECT variant,level,user_pseudo_id,k,CAST(v AS DOUBLE) AS amount
 FROM metric_events LATERAL VIEW EXPLODE(FROM_JSON(event_params,'MAP<STRING,STRING>')) kv AS k,v
 WHERE event_name='level_start_turn' AND attempt=1
), valid AS (
 SELECT r.* FROM resources r WHERE (k IN ('coin','ticket') OR k LIKE '%booster%')
 AND k<>'use_booster_count' AND amount>=0
)
SELECT variant,level,
 SUM(CASE WHEN ARRAY_CONTAINS(c.coin_balance_resources,k) THEN amount ELSE 0 END) AS selected_balance,
 SUM(amount) AS total_balance
FROM valid CROSS JOIN report_config c GROUP BY variant,level;

-- Dx is Non Return Rate, distinct from the ordinary drop rate above.
-- Both numerator and denominator use the same mature play-date period.
-- 14. Cua so Dx: chi lay play_date da du N ngay activity va cutoff BI T-1.
CREATE OR REPLACE TEMP VIEW churn_windows AS
SELECT n,report_start,
 LEAST(report_end,DATE_SUB(as_of_date,n+1),DATE_SUB(observation_end,n)) AS eligible_end
FROM report_config LATERAL VIEW EXPLODE(ARRAY(3,7)) ns AS n;

-- 15. Cohort level_start du quan sat, cung period cho tu so/mau so Dx.
CREATE OR REPLACE TEMP VIEW churn_play_pre AS
SELECT DISTINCT w.n,e.variant,e.level,e.user_pseudo_id,e.local_date
FROM metric_events e CROSS JOIN churn_windows w
WHERE event_name='level_start' AND e.local_date BETWEEN w.report_start AND w.eligible_end;

-- 16. Comeback activity: khong filter level/mode/variant cua session.
CREATE OR REPLACE TEMP VIEW daily_sessions AS
SELECT DISTINCT e.user_pseudo_id,e.local_date FROM system_clean e CROSS JOIN report_config c
WHERE e.event_name IN ('session_start','screen_view')
 AND e.local_date BETWEEN c.report_start AND c.observation_end
 AND ARRAY_CONTAINS(c.versions,e.app_version)
 AND (e.property_phase IS NULL OR e.property_phase<=1);
-- No mode/level/variant filter on comeback session itself.

-- 17. Non Return Dx: user khong comeback +1..+N / user start, gan MAX level/ngay.
CREATE OR REPLACE TEMP VIEW churn_counts AS
WITH denominators AS (
 SELECT n,variant,level,COUNT(DISTINCT user_pseudo_id) AS total_users
 FROM churn_play_pre GROUP BY n,variant,level
), last_level AS (
 SELECT n,variant,user_pseudo_id,local_date,MAX(level) AS level
 FROM churn_play_pre GROUP BY n,variant,user_pseudo_id,local_date
), comeback AS (
 SELECT p.n,p.variant,p.level,p.user_pseudo_id,p.local_date,MAX(CASE WHEN s.user_pseudo_id IS NOT NULL THEN 1 ELSE 0 END) AS has_comeback
 FROM last_level p LEFT JOIN daily_sessions s ON p.user_pseudo_id=s.user_pseudo_id
 AND s.local_date BETWEEN DATE_ADD(p.local_date,1) AND DATE_ADD(p.local_date,p.n)
 GROUP BY p.n,p.variant,p.level,p.user_pseudo_id,p.local_date
), churned AS (
 SELECT n,variant,level,COUNT(DISTINCT CASE WHEN has_comeback=0 THEN user_pseudo_id END) AS users_churned
 FROM comeback GROUP BY n,variant,level
)
SELECT d.*,COALESCE(ch.users_churned,0) AS users_churned,
 COALESCE(ch.users_churned,0)*100.0/d.total_users AS rate
FROM denominators d LEFT JOIN churned ch ON d.n=ch.n AND d.variant=ch.variant AND d.level=ch.level;

-- 18. Bang LEVELPLAY, dung thu tu cot tren BI va them AB group.
CREATE OR REPLACE TEMP VIEW levelplay_report AS
SELECT c.variant AS ab_group,c.level,c.user_start_count,
 COALESCE(ch.user_dropped_count,0)*100.0/c.user_start_count AS churn_rate_pct,
 d3.rate AS churn_rate_d3_pct,d7.rate AS churn_rate_d7_pct,
 COALESCE(a.aps_avg,0) AS aps_avg,
 COALESCE(s.avg_completion,0) AS completion_rate_first_attempt_pct,
 COALESCE(s.coin_spend,0)*1.0/c.user_start_count AS coin_spend_per_user,
 COALESCE(s.total_coin_spend,0)*1.0/c.user_start_count AS total_coin_spend_per_user,
 COALESCE(COALESCE(b.selected_balance,0)*1.0/NULLIF(bs.users,0),0) AS coin_balance_per_user,
 COALESCE(COALESCE(b.total_balance,0)*1.0/NULLIF(bs.users,0),0) AS total_coin_balance_per_user,
 COALESCE(s.booster_count,0)*1.0/c.user_start_count AS avg_booster_per_user,
 COALESCE(s.booster_users,0)*100.0/c.user_start_count AS booster_usage_rate_pct,
 COALESCE(p.payers,0)*100.0/c.user_start_count AS pay_rate_pct,
 COALESCE(COALESCE(s.impressions,0)*1.0/NULLIF(ad.users,0),0) AS imp_per_lau,
 COALESCE(COALESCE(s.reward_impressions,0)*1.0/NULLIF(ad.users,0),0) AS rwd_per_lau
FROM start_counts c
LEFT JOIN churn_components ch ON c.variant=ch.variant AND c.level=ch.level
LEFT JOIN level_stats s ON c.variant=s.variant AND c.level=s.level
LEFT JOIN aps a ON c.variant=a.variant AND c.level=a.level
LEFT JOIN pay_counts p ON c.variant=p.variant AND c.level=p.level
LEFT JOIN ad_start_counts ad ON c.variant=ad.variant AND c.level=ad.level
LEFT JOIN balance_starters bs ON c.variant=bs.variant AND c.level=bs.level
LEFT JOIN balance_sums b ON c.variant=b.variant AND c.level=b.level
LEFT JOIN churn_counts d3 ON c.variant=d3.variant AND c.level=d3.level AND d3.n=3
LEFT JOIN churn_counts d7 ON c.variant=d7.variant AND c.level=d7.level AND d7.n=7;

-- 19. LOSS chi level_end_turn success=false, attempt=1, completion nonnull.
CREATE OR REPLACE TEMP VIEW losses AS
SELECT *,CASE WHEN completion<10 THEN 0 WHEN completion<20 THEN 1 WHEN completion<30 THEN 2
 WHEN completion<40 THEN 3 WHEN completion<50 THEN 4 WHEN completion<60 THEN 5
 WHEN completion<70 THEN 6 WHEN completion<80 THEN 7 WHEN completion<90 THEN 8 ELSE 9 END AS bucket
FROM metric_events WHERE event_name='level_end_turn' AND success='false' AND attempt=1 AND completion IS NOT NULL;

-- 20. Bucket completion: distinct user trong bucket / distinct losing user.
-- User co nhieu loss o nhieu bucket co the lam tong % lon hon 100.
CREATE OR REPLACE TEMP VIEW loss_distribution AS
SELECT variant AS ab_group,level,COUNT(DISTINCT user_pseudo_id) AS user_count,COUNT(*) AS lose_count,
 COUNT(DISTINCT CASE WHEN bucket=0 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `0-10`,
 COUNT(DISTINCT CASE WHEN bucket=1 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `10-20`,
 COUNT(DISTINCT CASE WHEN bucket=2 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `20-30`,
 COUNT(DISTINCT CASE WHEN bucket=3 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `30-40`,
 COUNT(DISTINCT CASE WHEN bucket=4 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `40-50`,
 COUNT(DISTINCT CASE WHEN bucket=5 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `50-60`,
 COUNT(DISTINCT CASE WHEN bucket=6 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `60-70`,
 COUNT(DISTINCT CASE WHEN bucket=7 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `70-80`,
 COUNT(DISTINCT CASE WHEN bucket=8 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `80-90`,
 COUNT(DISTINCT CASE WHEN bucket=9 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS `90-100`
FROM losses GROUP BY variant,level;

-- User-confirmed Loss mapping: these legacy D3 labels represent ordinary drop rate,
-- not Non Return Rate and not a denominator limited to first-attempt losers.
-- 21. Bang LOSS: ghep DROP users/rate theo level, khong dung Non Return D3.
-- Ten cot D3 giu theo BI, mau so drop la user start, khong phai losing user.
CREATE OR REPLACE TEMP VIEW loss_report AS
SELECT l.ab_group,l.level,l.user_count,l.lose_count,
 COALESCE(d.dropped_users,0) AS churn_users_d3,
 COALESCE(d.dropped_users,0)*100.0/NULLIF(c.user_start_count,0) AS churn_rate_d3_pct,
 l.`0-10`,l.`10-20`,l.`20-30`,l.`30-40`,l.`40-50`,
 l.`50-60`,l.`60-70`,l.`70-80`,l.`80-90`,l.`90-100`
FROM loss_distribution l
LEFT JOIN start_counts c ON l.ab_group=c.variant AND l.level=c.level
LEFT JOIN drop_counts d ON l.ab_group=d.variant AND l.level=d.level;

-- 22. Xem bang ket qua. Runner Python xuat hai view nay ra Parquet/CSV.
SELECT * FROM levelplay_report ORDER BY ab_group,level;
SELECT * FROM loss_report ORDER BY ab_group,level;
