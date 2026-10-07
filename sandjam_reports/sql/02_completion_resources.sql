-- Completion continue0/null; spend và balance theo Long. Total ads chỉ Inter+Rewarded; giữ NULL khi không có format.

CREATE OR REPLACE TEMP VIEW level_stats AS
SELECT variant,level,
 COUNT(DISTINCT CASE WHEN event_name='level_end_turn' AND used_boosters>0 THEN user_pseudo_id END) AS booster_users,
 SUM(CASE WHEN event_name='spend_virtual_currency' AND currency_name='coin' THEN game_currency_value ELSE 0 END) AS coin_spend,
 SUM(CASE WHEN event_name='spend_virtual_currency' THEN game_currency_value ELSE 0 END) AS total_coin_spend,
 SUM(CASE WHEN event_name='spend_virtual_currency' AND currency_type='booster' AND currency_name IS NOT NULL AND currency_name<>'' THEN currency_value ELSE 0 END) AS booster_count,
 SUM(CASE WHEN event_name='paid_ad_impression' AND regexp_matches(ad_format,'inter|reward|video') THEN 1 ELSE 0 END) AS impressions,
 SUM(CASE WHEN event_name='paid_ad_impression' AND regexp_matches(ad_format,'inter') THEN 1 END) AS inter_impressions,
 SUM(CASE WHEN event_name='paid_ad_impression' AND NOT(regexp_matches(ad_format,'inter')) AND regexp_matches(ad_format,'reward|video') THEN 1 END) AS reward_impressions,
 AVG(CASE WHEN event_name='level_end' AND (continues=0 OR continues IS NULL) THEN completion END) AS avg_completion
FROM metric_events GROUP BY variant,level;

CREATE OR REPLACE TEMP VIEW pay_counts AS
SELECT e.variant,e.level,COUNT(DISTINCT e.user_pseudo_id) AS payers FROM metric_events e
JOIN starts s ON e.variant=s.variant AND e.level=s.level AND e.user_pseudo_id=s.user_pseudo_id
WHERE e.event_name='in_app_purchase' GROUP BY e.variant,e.level;

CREATE OR REPLACE TEMP VIEW aps AS
WITH wins AS (
 SELECT variant,level,attempt,COUNT(DISTINCT user_pseudo_id) AS users
 FROM metric_events WHERE event_name='level_end' AND success='true' AND attempt>0 GROUP BY variant,level,attempt
)
SELECT variant,level,SUM(attempt*users)*1.0/SUM(users) AS aps_avg FROM wins GROUP BY variant,level;

CREATE OR REPLACE TEMP VIEW balance_starters AS
SELECT variant,level,COUNT(DISTINCT user_pseudo_id) AS users
FROM metric_events WHERE event_name='level_start_turn' GROUP BY variant,level;

CREATE OR REPLACE TEMP VIEW balance_sums AS
WITH resources AS (
 SELECT e.variant,e.level,e.user_pseudo_id,kv.key AS k,
 TRY_CAST(json_extract_string(kv.value,'$') AS DOUBLE) AS amount
 FROM metric_events e, json_each(e.event_params) AS kv
 WHERE e.event_name='level_start_turn' AND e.attempt=1
), valid AS (
 SELECT * FROM resources WHERE (k IN ('coin','ticket') OR k LIKE '%booster%')
 AND k<>'use_booster_count' AND amount>=0
)
SELECT variant,level,
 SUM(CASE WHEN list_contains(c.coin_balance_resources,k) THEN amount ELSE 0 END) AS selected_balance,
 SUM(amount) AS total_balance
FROM valid CROSS JOIN report_config c GROUP BY variant,level;
