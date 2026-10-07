-- Spark SQL. Replace path with your exported raw_fixed directory.
CREATE OR REPLACE TEMP VIEW raw_fixed AS
SELECT * FROM parquet.`s3a://YOUR_BUCKET/exports/raw_mode_run1/raw_fixed`;

-- The view is for subsequent BI metric queries, not an export filter.
CREATE OR REPLACE TEMP VIEW ab_classic AS
SELECT * FROM raw_fixed
WHERE analysis_date_utc7 >= DATE '2026-10-02'
  AND analysis_date_utc7 < DATE '2026-10-06'
  AND mode_fixed = 'classic'
  AND ab_group IN ('0', '1', '2');

-- Inspect before applying production-version/prepublish filters.
SELECT ab_group, app_version, event_name, COUNT(*) AS event_count
FROM ab_classic GROUP BY ab_group, app_version, event_name;
