-- Mọi level; ordinary drop so startX và startX+1. Ads property denominator là candidate đã khớp cả3 cột ở10 level, chưa có SQL BI xác nhận.

CREATE OR REPLACE TEMP VIEW starts AS
SELECT DISTINCT variant,level,user_pseudo_id FROM ab_classic_events CROSS JOIN report_config c
WHERE event_name='level_start' AND level IS NOT NULL
 AND (event_phase IS NULL OR event_phase<=1);

CREATE OR REPLACE TEMP VIEW start_counts AS
SELECT variant,level,COUNT(DISTINCT user_pseudo_id) AS user_start_count
FROM starts CROSS JOIN report_config c GROUP BY variant,level;

CREATE OR REPLACE TEMP VIEW drop_counts AS
SELECT x.variant,x.level,COUNT(DISTINCT CASE WHEN y.user_pseudo_id IS NULL THEN x.user_pseudo_id END) AS dropped_users
FROM starts x LEFT JOIN starts y ON x.variant=y.variant AND x.user_pseudo_id=y.user_pseudo_id AND y.level=x.level+1
GROUP BY x.variant,x.level;

CREATE OR REPLACE TEMP VIEW property_starters AS
SELECT e.variant,e.property_normalized_level AS level,COUNT(DISTINCT e.user_pseudo_id) AS users
FROM system_clean e CROSS JOIN report_config c
WHERE e.event_name='level_start' AND e.local_date BETWEEN c.report_start AND c.report_end
 AND (LEN(c.versions)=0 OR list_contains(c.versions,e.app_version)) AND e.variant IN ({{variants}})
 AND e.property_corrected_mode='classic' AND (e.event_phase IS NULL OR e.event_phase<=1)
GROUP BY e.variant,e.property_normalized_level;
