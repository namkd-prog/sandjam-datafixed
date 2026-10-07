-- Loss first/all tách distribution. Churn Loss dùng reconstruction khớp ảnh24 level3–26, chưa có SQL phụ BI/Long xác nhận.

CREATE OR REPLACE TABLE losses AS
SELECT variant,level,user_pseudo_id,loss_kind,CASE WHEN completion<10 THEN 0 WHEN completion<20 THEN 1 WHEN completion<30 THEN 2
 WHEN completion<40 THEN 3 WHEN completion<50 THEN 4 WHEN completion<60 THEN 5
 WHEN completion<70 THEN 6 WHEN completion<80 THEN 7 WHEN completion<90 THEN 8 ELSE 9 END AS bucket
FROM ab_classic_events CROSS JOIN (SELECT 'first' AS loss_kind UNION ALL SELECT 'all') k WHERE event_name='level_end_turn' AND success='false' AND (loss_kind='all' OR attempt=1) AND completion IS NOT NULL AND level IS NOT NULL;

DROP TABLE ab_classic_events;

CREATE OR REPLACE TABLE loss_distribution AS
SELECT loss_kind,variant AS ab_group,level,COUNT(DISTINCT user_pseudo_id) AS user_count,COUNT(*) AS lose_count,
 COUNT(DISTINCT CASE WHEN bucket=0 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "0-10",
 COUNT(DISTINCT CASE WHEN bucket=1 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "10-20",
 COUNT(DISTINCT CASE WHEN bucket=2 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "20-30",
 COUNT(DISTINCT CASE WHEN bucket=3 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "30-40",
 COUNT(DISTINCT CASE WHEN bucket=4 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "40-50",
 COUNT(DISTINCT CASE WHEN bucket=5 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "50-60",
 COUNT(DISTINCT CASE WHEN bucket=6 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "60-70",
 COUNT(DISTINCT CASE WHEN bucket=7 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "70-80",
 COUNT(DISTINCT CASE WHEN bucket=8 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "80-90",
 COUNT(DISTINCT CASE WHEN bucket=9 THEN user_pseudo_id END)*100.0/COUNT(DISTINCT user_pseudo_id) AS "90-100"
FROM losses GROUP BY loss_kind,variant,level;

CREATE OR REPLACE TABLE loss_churn_counts AS
WITH denominators AS (
 SELECT variant,level,COUNT(DISTINCT user_pseudo_id) AS mature_users
 FROM churn_play_pre WHERE n=3 GROUP BY variant,level
), last_level AS (
 SELECT variant,user_pseudo_id,local_date,MAX(level) AS level
 FROM churn_play_pre WHERE n=3 GROUP BY variant,user_pseudo_id,local_date
), ends AS (
 SELECT DISTINCT variant,level,user_pseudo_id FROM metric_events WHERE event_name='level_end'
), last_session AS (
 SELECT user_pseudo_id,MAX(local_date) AS last_date FROM daily_sessions GROUP BY user_pseudo_id
), counts AS (
 SELECT p.variant,p.level,COUNT(DISTINCT CASE WHEN e.user_pseudo_id IS NULL
 AND (s.last_date IS NULL OR s.last_date<=p.local_date) THEN p.user_pseudo_id END) AS churn_users
 FROM last_level p LEFT JOIN ends e ON p.variant=e.variant AND p.level=e.level AND p.user_pseudo_id=e.user_pseudo_id
 LEFT JOIN last_session s ON p.user_pseudo_id=s.user_pseudo_id GROUP BY p.variant,p.level
)
SELECT d.variant,d.level,d.mature_users,COALESCE(c.churn_users,0) AS churn_users,
 COALESCE(c.churn_users,0)*100.0/d.mature_users AS rate
FROM denominators d LEFT JOIN counts c ON d.variant=c.variant AND d.level=c.level;

DROP TABLE metric_events;
DROP TABLE losses;
