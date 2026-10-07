"""Run the single consolidated Spark SQL script against the corrected Parquet export."""
import argparse
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


def run(spark,args):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType,MapType
    spark.conf.set('spark.sql.session.timeZone','UTC')
    spark.conf.set('spark.sql.ansi.enabled','false')
    raw=spark.read.parquet(args.input)
    required={'mode_fixed','first_clear_650_ts','event_params','user_properties','app_id',
              'user_pseudo_id','event_ts','event_name','app_version','country','platform'}
    if not required.issubset(raw.columns):
        raise ValueError('Missing columns: '+str(required-set(raw.columns)))
    for name in ('event_params','user_properties'):
        if isinstance(raw.schema[name].dataType,(StructType,MapType)):
            raw=raw.withColumn(name,F.to_json(F.col(name)))
    raw.createOrReplaceTempView('raw_fixed_input')
    observation_end=raw.select(F.max(F.to_date(F.col('event_ts').cast('timestamp')+F.expr('INTERVAL 7 HOURS')))).first()[0]
    if observation_end is None:
        raise ValueError('Empty input')
    # Maximum date is only a bound, not proof of complete source coverage.
    if args.observation_end:
        observation_end=datetime.strptime(args.observation_end,'%Y-%m-%d').date()
    config=[(datetime.strptime(args.start,'%Y-%m-%d').date(),datetime.strptime(args.end,'%Y-%m-%d').date(),
             datetime.strptime(args.as_of,'%Y-%m-%d').date(),observation_end,
             args.level_min,args.level_max,args.versions.split(','),args.balance_resources.split(','))]
    spark.createDataFrame(config,'report_start date, report_end date, as_of_date date, observation_end date, level_min int, level_max int, versions array<string>, coin_balance_resources array<string>').createOrReplaceTempView('report_config')
    sql=Path(__file__).with_name('levelplay_and_loss.sql').read_text(encoding='utf-8')
    # This maintained script has no semicolons inside SQL string literals.
    for statement in sql.split(';'):
        if statement.strip():
            spark.sql(statement)
    for name in ('levelplay_report','loss_report'):
        result=spark.table(name).orderBy('ab_group','level')
        result.write.mode('errorifexists').parquet(args.output.rstrip('/')+'/'+name)
        result.coalesce(1).write.mode('errorifexists').option('header','true').csv(args.output.rstrip('/')+'/'+name+'_csv')
    spark.table('churn_windows').show(truncate=False)
    print('COMPLETE reports='+args.output)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--input',required=True,help='raw_fixed Parquet directory')
    p.add_argument('--output',required=True)
    p.add_argument('--start',default='2026-10-02')
    p.add_argument('--end',default='2026-10-05')
    p.add_argument('--as-of',default=datetime.now(ZoneInfo('Asia/Bangkok')).strftime('%Y-%m-%d'))
    p.add_argument('--observation-end',help='Last COMPLETE source date in UTC+7')
    p.add_argument('--level-min',type=int,default=1)
    p.add_argument('--level-max',type=int,default=200)
    p.add_argument('--versions',default='0.6.3,0.6.4,0.6.5,0.6.6,0.6.7')
    p.add_argument('--balance-resources',default='coin')
    args=p.parse_args()
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName('sandjam-ab-bi-reports').getOrCreate()
    try: run(spark,args)
    finally: spark.stop()
