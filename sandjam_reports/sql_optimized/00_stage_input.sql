-- The only Parquet data scan in optimized mode. Native timestamps allow row-group pruning.
-- Bounds are UTC, converted from local report_start/observation_end (UTC+7).
-- Keep every event in this bounded window; do not prefilter session AB/mode.
CREATE OR REPLACE TABLE raw_fixed_input AS
SELECT {{input_columns}}
FROM parquet_source
WHERE app_id={{app_id}} AND {{input_timestamp_filter}};
