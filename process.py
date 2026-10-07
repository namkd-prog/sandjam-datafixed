"""Export full raw history with analytical mode correction; no metrics or event filters."""
import argparse
import json
from datetime import datetime, timedelta, timezone


def utc_bound(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone(timedelta(hours=7))).astimezone(timezone.utc).replace(tzinfo=None).isoformat(' ')


def run(spark, cfg):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, MapType, StructType
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.conf.set('spark.sql.ansi.enabled', 'false')
    source = cfg['source']
    options = dict(source.get('options', {}))
    if source['format'] == 'csv':
        options.update(header='true', inferSchema='false', multiLine='true', quote='"', escape='"', mode='FAILFAST')
    raw = spark.read.format(source['format']).options(**options).load(source['path'])
    required = {'app_id','user_pseudo_id','event_ts','event_name','event_params','user_properties'}
    if not required.issubset(raw.columns):
        raise ValueError('Missing columns: ' + str(required - set(raw.columns)))
    added = {'mode_original','mode_fixed','is_mode_fixed','first_clear_650_ts','fix_reason','analysis_date_utc7','ab_group','_event_ts_utc'}
    if added.intersection(raw.columns):
        raise ValueError('Input must be original raw, not a previous corrected export')
    originals = raw.columns

    def j(column, key):
        typ = raw.schema[column].dataType
        if isinstance(typ, StringType):
            return F.get_json_object(F.col(column), '$.' + key)
        if isinstance(typ, MapType):
            return F.col(column)[key].cast('string')
        if isinstance(typ, StructType):
            return F.col(column + '.' + key).cast('string') if key in typ.fieldNames() else F.lit(None).cast('string')
        raise ValueError(column + ' must be JSON string, Map or Struct; GA4 key/value arrays need an adapter')

    start = utc_bound(cfg['history_start_utc7'])
    end = utc_bound(cfg['history_end_exclusive_utc7'])
    if start >= end:
        raise ValueError('Invalid date range')
    d = raw.withColumn('_event_ts_utc', F.col('event_ts').cast('timestamp'))
    # Invalid timestamps must not silently disappear from the requested raw.
    app = d.where(F.col('app_id') == cfg['app_id'])
    if app.where(F.col('_event_ts_utc').isNull()).limit(1).count():
        raise ValueError('Invalid/null event_ts; supply UTC ISO timestamp strings or TimestampType, not epoch integers')
    d = app.where((F.col('_event_ts_utc') >= F.lit(start).cast('timestamp')) &
                  (F.col('_event_ts_utc') < F.lit(end).cast('timestamp')))
    d = d.withColumn('mode_original', j('event_params','mode'))
    clear = (F.col('event_name') == 'level_end') & (F.lower(j('event_params','success')) == 'true') & \
            (j('event_params','level').cast('double') == 650) & \
            (j('user_properties','level').cast('double') == 650) & (F.col('mode_original') == 'classic')
    cutoffs = d.where(clear & F.col('user_pseudo_id').isNotNull()).groupBy('user_pseudo_id').agg(
        F.min('_event_ts_utc').alias('first_clear_650_ts'))
    fixed = d.join(cutoffs,'user_pseudo_id','left')
    changed = F.coalesce((F.col('mode_original') == 'classic') &
                         (F.col('_event_ts_utc') > F.col('first_clear_650_ts')), F.lit(False))
    fixed = fixed.withColumn('is_mode_fixed', changed).withColumn('mode_fixed',
        F.when(F.col('is_mode_fixed'), F.lit('sl')).otherwise(F.col('mode_original')))
    fixed = fixed.withColumn('fix_reason', F.when(F.col('is_mode_fixed'),'classic_after_clear650')
        .when(F.col('mode_original') == 'sl','existing_sl')
        .when(F.col('first_clear_650_ts').isNull(),'no_clear650_in_history').otherwise('unchanged'))
    fixed = fixed.withColumn('analysis_date_utc7', F.to_date(F.from_utc_timestamp('_event_ts_utc','Asia/Bangkok')))
    fixed = fixed.withColumn('ab_group',j('user_properties',cfg['experiment_key']))
    output = fixed.select(*originals,'mode_original','mode_fixed','is_mode_fixed','first_clear_650_ts',
                          'fix_reason','analysis_date_utc7','ab_group')
    root = cfg['output_path'].rstrip('/')
    output.write.mode('errorifexists').partitionBy('analysis_date_utc7').parquet(root + '/raw_fixed')
    cutoffs.write.mode('errorifexists').parquet(root + '/user_clear650_cutoffs')
    print('COMPLETE: ' + root + '/raw_fixed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    with open(args.config, encoding='utf-8') as f:
        cfg = json.load(f)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('id6758755718-raw-mode-fix').getOrCreate()
    try:
        run(spark,cfg)
    finally:
        spark.stop()
