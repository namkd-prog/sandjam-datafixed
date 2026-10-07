-- Non Return D3/D7: cohort đủ N ngày, highest level user/ngày, không session quay lại trong +1..N. Chưa đủ ngày giữ NULL.

CREATE OR REPLACE TABLE churn_windows AS
SELECT n,report_start,
 LEAST(report_end,date_sub_days(as_of_date,n+1),date_sub_days(observation_end,n)) AS eligible_end
FROM report_config CROSS JOIN UNNEST([3,7]) AS ns(n);

CREATE OR REPLACE TABLE churn_play_pre AS
SELECT DISTINCT w.n,e.variant,e.level,e.user_pseudo_id,e.local_date
FROM metric_events e CROSS JOIN churn_windows w
WHERE event_name='level_start' AND e.local_date BETWEEN w.report_start AND w.eligible_end;

CREATE OR REPLACE TABLE daily_sessions AS
SELECT DISTINCT e.user_pseudo_id,e.local_date FROM system_clean e CROSS JOIN report_config c
WHERE e.event_name IN ('session_start','screen_view')
 AND e.local_date BETWEEN c.report_start AND c.observation_end
 AND (LEN(c.versions)=0 OR list_contains(c.versions,e.app_version))
 AND (e.property_phase IS NULL OR e.property_phase<=1);

-- Property starters and sessions are now cached; free the base window.
DROP TABLE system_clean;

CREATE OR REPLACE TABLE churn_counts AS
WITH denominators AS (
 SELECT n,variant,level,COUNT(DISTINCT user_pseudo_id) AS total_users
 FROM churn_play_pre GROUP BY n,variant,level
), last_level AS (
 SELECT n,variant,user_pseudo_id,local_date,MAX(level) AS level
 FROM churn_play_pre GROUP BY n,variant,user_pseudo_id,local_date
), comeback AS (
 SELECT p.n,p.variant,p.level,p.user_pseudo_id,p.local_date,MAX(CASE WHEN s.user_pseudo_id IS NOT NULL THEN 1 ELSE 0 END) AS has_comeback
 FROM last_level p LEFT JOIN daily_sessions s ON p.user_pseudo_id=s.user_pseudo_id
 AND s.local_date BETWEEN date_add_days(p.local_date,1) AND date_add_days(p.local_date,p.n)
 GROUP BY p.n,p.variant,p.level,p.user_pseudo_id,p.local_date
), churned AS (
 SELECT n,variant,level,COUNT(DISTINCT CASE WHEN has_comeback=0 THEN user_pseudo_id END) AS users_churned
 FROM comeback GROUP BY n,variant,level
)
SELECT d.*,COALESCE(ch.users_churned,0) AS users_churned,
 COALESCE(ch.users_churned,0)*100.0/d.total_users AS rate
FROM denominators d LEFT JOIN churned ch ON d.n=ch.n AND d.variant=ch.variant AND d.level=ch.level;
