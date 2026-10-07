-- Parquet đã sửa; gameplay dùng mode_fixed, ads/IAP dùng property mode sau cùng cutoff. Không sửa raw JSON.

CREATE OR REPLACE TEMP VIEW parsed_events AS
SELECT r.*,
 TRY_CAST(event_ts AS TIMESTAMP) AS ts_utc,
 TRY_CAST(json_extract_string(event_params,'$.level') AS DOUBLE) AS raw_level,
 TRY_CAST(json_extract_string(user_properties,'$.level') AS DOUBLE) AS property_level,
 TRY_CAST(json_extract_string(event_params,'$.phase') AS INT) AS event_phase,
 TRY_CAST(json_extract_string(user_properties,'$.phase') AS INT) AS property_phase,
 json_extract_string(user_properties,'$.firebase_exp_abt_22') AS variant,
 json_extract_string(user_properties,'$.mode') AS property_mode,
 LOWER(json_extract_string(event_params,'$.success')) AS success,
 TRY_CAST(json_extract_string(event_params,'$.start_count') AS INT) AS attempt,
 TRY_CAST(json_extract_string(event_params,'$.continue_times') AS INT) AS continues,
 TRY_CAST(json_extract_string(event_params,'$.completion') AS DOUBLE) AS completion,
 TRY_CAST(json_extract_string(event_params,'$.use_booster_count') AS DOUBLE) AS used_boosters,
 TRY_CAST(json_extract_string(event_params,'$.value') AS DOUBLE) AS currency_value,
 TRY_CAST(json_extract_string(event_params,'$.value_game_currency') AS BIGINT) AS game_currency_value,
 LOWER(json_extract_string(event_params,'$.virtual_currency_name')) AS currency_name,
 LOWER(json_extract_string(event_params,'$.virtual_currency_type')) AS currency_type,
 LOWER(COALESCE(json_extract_string(event_params,'$.ad_format'),'')) AS ad_format
FROM raw_fixed_input r WHERE app_id={{app_id}};

CREATE OR REPLACE TEMP VIEW normalized_events AS
SELECT *, DATE(ts_utc + INTERVAL 7 HOURS) AS local_date,
 TRY_CAST(CASE WHEN INSTR(json_extract_string(user_properties,'$.level'),'.')>1
 THEN REPLACE(json_extract_string(user_properties,'$.level'),'.1','')
 WHEN property_phase>1 THEN CONCAT(TRY_CAST(TRY_CAST(property_level AS INT) AS VARCHAR),'.',TRY_CAST(property_phase AS VARCHAR))
 ELSE json_extract_string(user_properties,'$.level') END AS DOUBLE) AS property_normalized_level,
 CASE WHEN property_mode='classic' AND ts_utc>first_clear_650_ts THEN 'sl' ELSE property_mode END AS property_corrected_mode,
 CASE WHEN event_name IN ('paid_ad_impression','in_app_purchase','in_app_purchase_v2','app_remove')
 THEN TRY_CAST(CASE WHEN INSTR(json_extract_string(user_properties,'$.level'),'.')>1
 THEN REPLACE(json_extract_string(user_properties,'$.level'),'.1','')
 WHEN property_phase>1 THEN CONCAT(TRY_CAST(TRY_CAST(property_level AS INT) AS VARCHAR),'.',TRY_CAST(property_phase AS VARCHAR))
 ELSE json_extract_string(user_properties,'$.level') END AS DOUBLE)
 ELSE TRY_CAST(CASE WHEN INSTR(json_extract_string(event_params,'$.level'),'.')>1
 THEN REPLACE(json_extract_string(event_params,'$.level'),'.1','')
 WHEN event_phase>1 THEN CONCAT(TRY_CAST(TRY_CAST(raw_level AS INT) AS VARCHAR),'.',TRY_CAST(event_phase AS VARCHAR))
 ELSE json_extract_string(event_params,'$.level') END AS DOUBLE) END AS level,
 CASE WHEN event_name IN ('paid_ad_impression','in_app_purchase','in_app_purchase_v2','app_remove')
 THEN CASE WHEN property_mode='classic' AND ts_utc>first_clear_650_ts THEN 'sl' ELSE property_mode END
 ELSE mode_fixed END AS metric_mode
FROM parsed_events;

CREATE OR REPLACE TEMP VIEW system_clean AS
SELECT * FROM normalized_events
WHERE (NOT(country='China' AND platform='android') OR country IS NULL)
 AND NOT(CASE WHEN event_name='level_start_turn' THEN COALESCE(TRY_CAST(json_extract_string(event_params,'$.coin') AS DOUBLE),0) ELSE 0 END>1000000)
 AND NOT(CASE WHEN event_name='in_app_purchase' AND json_extract_string(event_params,'$.currency')='USD' THEN COALESCE(currency_value,0)
 WHEN event_name='in_app_purchase' AND json_extract_string(event_params,'$.currency')='ILS' THEN COALESCE(currency_value,0)*0.33 ELSE 0 END>=300000000)
 AND (CASE WHEN event_name='earn_virtual_currency' THEN game_currency_value ELSE 1 END>=0);

CREATE OR REPLACE TEMP VIEW ab_classic_events AS
SELECT e.* FROM system_clean e CROSS JOIN report_config c
WHERE local_date BETWEEN c.report_start AND c.report_end
 AND (LEN(c.versions)=0 OR list_contains(c.versions,app_version)) AND variant IN ({{variants}})
 AND metric_mode='classic';

CREATE OR REPLACE TEMP VIEW metric_events AS
SELECT e.* FROM ab_classic_events e CROSS JOIN report_config c
WHERE level IS NOT NULL
 AND CASE WHEN event_name IN ('in_app_purchase','in_app_purchase_v2','app_remove') THEN property_phase IS NULL OR property_phase<=1
 WHEN event_name='paid_ad_impression' THEN TRUE ELSE event_phase IS NULL OR event_phase<=1 END;
