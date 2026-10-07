-- Parquet đã sửa; gameplay dùng mode_fixed, ads/IAP dùng property mode sau cùng cutoff. Không sửa raw JSON.

CREATE OR REPLACE TABLE parsed_events AS
WITH decoded AS (
 SELECT r.* REPLACE(TRY_CAST(event_params AS JSON) AS event_params,
                    TRY_CAST(user_properties AS JSON) AS user_properties)
 FROM raw_fixed_input r WHERE app_id={{app_id}}
 AND event_name IN ('level_start','level_start_turn','level_end','level_end_turn',
 'spend_virtual_currency','paid_ad_impression','in_app_purchase','in_app_purchase_v2',
 'app_remove','session_start','screen_view')
), extracted AS (
 SELECT *,json_extract_string(event_params,
 ['$.level','$.phase','$.success','$.start_count','$.continue_times','$.completion',
  '$.use_booster_count','$.value','$.value_game_currency','$.virtual_currency_name',
  '$.virtual_currency_type','$.ad_format','$.coin','$.currency']) AS ep,
 json_extract_string(user_properties,
 ['$.level','$.phase','$.firebase_exp_abt_22','$.mode']) AS up
 FROM decoded
)
SELECT * EXCLUDE(ep,up),event_ts AS ts_utc,
 ep[1] AS event_level_text,up[1] AS property_level_text,
 TRY_CAST(ep[1] AS DOUBLE) AS raw_level,
 TRY_CAST(up[1] AS DOUBLE) AS property_level,
 TRY_CAST(ep[2] AS INT) AS event_phase,
 TRY_CAST(up[2] AS INT) AS property_phase,
 up[3] AS variant,up[4] AS property_mode,
 LOWER(ep[3]) AS success,
 TRY_CAST(ep[4] AS INT) AS attempt,
 TRY_CAST(ep[5] AS INT) AS continues,
 TRY_CAST(ep[6] AS DOUBLE) AS completion,
 TRY_CAST(ep[7] AS DOUBLE) AS used_boosters,
 TRY_CAST(ep[8] AS DOUBLE) AS currency_value,
 TRY_CAST(ep[9] AS BIGINT) AS game_currency_value,
 LOWER(ep[10]) AS currency_name,
 LOWER(ep[11]) AS currency_type,
 LOWER(COALESCE(ep[12],'')) AS ad_format,
 TRY_CAST(ep[13] AS DOUBLE) AS start_coin,ep[14] AS purchase_currency
FROM extracted;

-- The Parquet source is no longer reachable after this point.
DROP TABLE raw_fixed_input;
DROP VIEW parquet_source;

CREATE OR REPLACE TEMP VIEW normalized_events AS
SELECT *, DATE(ts_utc + INTERVAL 7 HOURS) AS local_date,
 TRY_CAST(CASE WHEN INSTR(property_level_text,'.')>1
 THEN REPLACE(property_level_text,'.1','')
 WHEN property_phase>1 THEN CONCAT(TRY_CAST(TRY_CAST(property_level AS INT) AS VARCHAR),'.',TRY_CAST(property_phase AS VARCHAR))
 ELSE property_level_text END AS DOUBLE) AS property_normalized_level,
 CASE WHEN property_mode='classic' AND ts_utc>first_clear_650_ts THEN 'sl' ELSE property_mode END AS property_corrected_mode,
 CASE WHEN event_name IN ('paid_ad_impression','in_app_purchase','in_app_purchase_v2','app_remove')
 THEN TRY_CAST(CASE WHEN INSTR(property_level_text,'.')>1
 THEN REPLACE(property_level_text,'.1','')
 WHEN property_phase>1 THEN CONCAT(TRY_CAST(TRY_CAST(property_level AS INT) AS VARCHAR),'.',TRY_CAST(property_phase AS VARCHAR))
 ELSE property_level_text END AS DOUBLE)
 ELSE TRY_CAST(CASE WHEN INSTR(event_level_text,'.')>1
 THEN REPLACE(event_level_text,'.1','')
 WHEN event_phase>1 THEN CONCAT(TRY_CAST(TRY_CAST(raw_level AS INT) AS VARCHAR),'.',TRY_CAST(event_phase AS VARCHAR))
 ELSE event_level_text END AS DOUBLE) END AS level,
 CASE WHEN event_name IN ('paid_ad_impression','in_app_purchase','in_app_purchase_v2','app_remove')
 THEN CASE WHEN property_mode='classic' AND ts_utc>first_clear_650_ts THEN 'sl' ELSE property_mode END
 ELSE mode_fixed END AS metric_mode
FROM parsed_events;

CREATE OR REPLACE TABLE system_clean AS
SELECT * FROM normalized_events
WHERE (NOT(country='China' AND platform='android') OR country IS NULL)
 AND NOT(CASE WHEN event_name='level_start_turn' THEN COALESCE(start_coin,0) ELSE 0 END>1000000)
 AND NOT(CASE WHEN event_name='in_app_purchase' AND purchase_currency='USD' THEN COALESCE(currency_value,0)
 WHEN event_name='in_app_purchase' AND purchase_currency='ILS' THEN COALESCE(currency_value,0)*0.33 ELSE 0 END>=300000000)
 AND (CASE WHEN event_name='earn_virtual_currency' THEN game_currency_value ELSE 1 END>=0);

-- system_clean owns the materialized normalized rows.
DROP VIEW normalized_events;
DROP TABLE parsed_events;

CREATE OR REPLACE TABLE ab_classic_events AS
SELECT e.variant,e.level,e.user_pseudo_id,e.event_name,e.event_phase,e.property_phase,
 e.local_date,e.success,e.attempt,e.completion,e.used_boosters,e.currency_name,e.currency_type,
 e.game_currency_value,e.currency_value,e.ad_format,e.continues,e.event_params
FROM system_clean e CROSS JOIN report_config c
WHERE local_date BETWEEN c.report_start AND c.report_end
 AND (LEN(c.versions)=0 OR list_contains(c.versions,app_version)) AND variant IN ({{variants}})
 AND metric_mode='classic';

CREATE OR REPLACE TABLE metric_events AS
SELECT e.* FROM ab_classic_events e CROSS JOIN report_config c
WHERE level IS NOT NULL
 AND CASE WHEN event_name IN ('in_app_purchase','in_app_purchase_v2','app_remove') THEN property_phase IS NULL OR property_phase<=1
 WHEN event_name='paid_ad_impression' THEN TRUE ELSE event_phase IS NULL OR event_phase<=1 END;
