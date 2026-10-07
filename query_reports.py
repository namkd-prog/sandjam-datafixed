"""Run the single consolidated Spark SQL script against the corrected Parquet export."""
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
def split_sql(sql):
    result,buf=[],[]
    i=0
    quote=None
    while i<len(sql):
        c=sql[i]
        if quote:
            buf.append(c)
            if c=='\\' and i+1<len(sql):
                i+=1
                buf.append(sql[i])
            elif c==quote:
                if i+1<len(sql) and sql[i+1]==quote:
                    i+=1
                    buf.append(sql[i])
                else:
                    quote=None
        elif sql.startswith('--',i):
            end=sql.find('\n',i)
            i=len(sql) if end<0 else end
            buf.append('\n')
            continue
        elif sql.startswith('/*',i):
            end=sql.find('*/',i+2)
            if end<0:
                raise ValueError('Unclosed SQL block comment')
            i=end+2
            buf.append(' ')
            continue
        elif c in ("'",'"','`'):
            quote=c
            buf.append(c)
        elif c==';':
            statement=''.join(buf).strip()
            if statement:
                result.append(statement)
            buf=[]
        else:
            buf.append(c)
        i+=1
    if quote:
        raise ValueError('Unclosed SQL quote')
    statement=''.join(buf).strip()
    if statement:
        result.append(statement)
    return result



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
        declared_end=datetime.strptime(args.observation_end,'%Y-%m-%d').date()
        if declared_end>observation_end:
            raise ValueError('--observation-end exceeds the latest date present in raw input')
        observation_end=declared_end
    config=[(datetime.strptime(args.start,'%Y-%m-%d').date(),datetime.strptime(args.end,'%Y-%m-%d').date(),
             datetime.strptime(args.as_of,'%Y-%m-%d').date(),observation_end,
             args.level_min,args.level_max,args.versions.split(','),args.balance_resources.split(','))]
    spark.createDataFrame(config,'report_start date, report_end date, as_of_date date, observation_end date, level_min int, level_max int, versions array<string>, coin_balance_resources array<string>').createOrReplaceTempView('report_config')
    sql=Path(__file__).with_name('levelplay_and_loss.sql').read_text(encoding='utf-8')
    for number,statement in enumerate(split_sql(sql),1):
        print(f'Executing SQL statement {number}: {statement.splitlines()[0]}',flush=True)
        try:
            spark.sql(statement)
        except Exception as exc:
            raise RuntimeError(f'SQL statement {number} failed:\n{statement}\n') from exc
    qa=spark.sql('''SELECT event_name,
        COUNT(*) AS classic_events_with_missing_clear650,
        COUNT(DISTINCT user_pseudo_id) AS affected_users
        FROM ab_classic_events
        WHERE property_level>650 AND first_clear_650_ts IS NULL
        GROUP BY event_name''')
    qa.coalesce(1).write.mode('errorifexists').option('header','true').csv(args.output.rstrip('/')+'/qa_missing_clear650_csv')
    if qa.limit(1).count():
        print('WARNING: post650 classic events lack historical clear650. See qa_missing_clear650_csv. Reports retain them under the agreed cutoff-only rule.',flush=True)
    for name in ('levelplay_report','loss_report','loss_all_attempts_report'):
        result=spark.table(name).orderBy('ab_group','level')
        result.coalesce(1).write.mode('errorifexists').option('header','true').csv(args.output.rstrip('/')+'/'+name+'_csv')
    spark.table('churn_windows').show(truncate=False)
    print('COMPLETE reports='+args.output)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--input',required=True,help='raw_fixed Parquet directory')
    p.add_argument('--output',required=True)
    p.add_argument('--start',default='2026-10-02')
    p.add_argument('--end',default='2026-10-05')
    p.add_argument('--as-of',default=datetime.now(timezone(timedelta(hours=7))).strftime('%Y-%m-%d'))
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
