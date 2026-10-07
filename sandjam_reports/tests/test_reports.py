"""QA local bằng identity giả; không chứa raw user thật."""
import csv
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import run_reports as reports


def fixture(path, bad_fix=False, missing_mode=False, nested=False):
    import duckdb
    con = duckdb.connect()
    con.execute('''CREATE TABLE fixture(app_id VARCHAR,user_pseudo_id VARCHAR,event_ts TIMESTAMP,
        event_name VARCHAR,event_params VARCHAR,user_properties VARCHAR,mode_fixed VARCHAR,
        first_clear_650_ts TIMESTAMP,app_version VARCHAR,country VARCHAR,platform VARCHAR)''')
    rows = []
    def add(user, name, day, level=3, ab='0', params=None, property_level=None, mode='classic', cutoff=None):
        ep = dict(level=level, mode='classic', phase=1)
        ep.update(params or {})
        up = dict(level=level if property_level is None else property_level, mode='classic', phase=1, firebase_exp_abt_22=ab)
        rows.append(('id6758755718', 'fake_' + user, day + ' 01:00:00', name, json.dumps(ep), json.dumps(up),
                     mode, cutoff, '9.9.9', 'Vietnam', 'ios'))
    add('a', 'level_start', '2026-10-02')
    add('b', 'level_start', '2026-10-02', property_level=4)
    add('c', 'level_start', '2026-10-02')
    add('d', 'level_start', '2026-10-04')
    for completion in (5,70):
        add('a', 'level_end_turn', '2026-10-02', params=dict(start_count=1,success=False,completion=completion))
    add('b', 'level_end_turn', '2026-10-02', params=dict(start_count=2,success=False,completion=5))
    add('c', 'level_end_turn', '2026-10-02', params=dict(start_count=1,success=False,completion=20))
    add('b', 'level_end', '2026-10-02', params=dict(start_count=2,success=False,completion=50,continue_times=0))
    add('c', 'session_start', '2026-10-06')
    add('a', 'level_start_turn', '2026-10-02', params=dict(start_count=1,coin=100,ticket=50,booster_x=2,use_booster_count=17))
    add('b', 'level_start_turn', '2026-10-02', params=dict(start_count=2,coin=10))
    add('a', 'spend_virtual_currency', '2026-10-02', params=dict(virtual_currency_name='coin',value_game_currency=10))
    add('a', 'spend_virtual_currency', '2026-10-02', params=dict(virtual_currency_name='booster_x',virtual_currency_type='booster',value=1,value_game_currency=35))
    add('a', 'paid_ad_impression', '2026-10-02', params=dict(ad_format='interstitial'))
    add('a', 'paid_ad_impression', '2026-10-02', params=dict(ad_format='banner'))
    add('wide', 'level_start', '2026-10-02', level=300, ab='2')
    add('phase', 'level_end_turn', '2026-10-02', level=4,
        params=dict(phase=2,start_count=1,success=False,completion=25))
    cutoff = '2026-04-01 01:00:00'
    add('old', 'level_end', '2026-04-01', level=650, ab='1', params=dict(success=True), cutoff=cutoff)
    add('old', 'level_start', '2026-10-02', ab='1', mode='classic' if bad_fix else 'sl', cutoff=cutoff)
    add('old', 'paid_ad_impression', '2026-10-02', ab='1', mode=None, cutoff=cutoff, params=dict(ad_format='interstitial',mode=None))
    con.executemany('INSERT INTO fixture VALUES (?,?,?,?,?,?,?,?,?,?,?)', rows)
    columns = '* EXCLUDE(mode_fixed)' if missing_mode else '*'
    if nested:
        schemas=con.execute('SELECT json_group_structure(event_params::JSON), json_group_structure(user_properties::JSON) FROM fixture').fetchone()
        columns='* EXCLUDE(event_params,user_properties),from_json(event_params,'+reports.literal(schemas[0])+') AS event_params,from_json(user_properties,'+reports.literal(schemas[1])+') AS user_properties'
    con.execute('COPY (SELECT ' + columns + ' FROM fixture) TO ' + reports.literal(path) + ' (FORMAT PARQUET)')
    con.close()


class ReportsTest(unittest.TestCase):
    def execute(self, tmp, **fixture_args):
        src = Path(tmp) / 'raw_fixed.parquet'
        fixture(src, **fixture_args)
        dest = Path(tmp) / 'out'
        args = SimpleNamespace(input=str(src),output=str(dest),config=str(ROOT/'config.json'),
                               memory_limit='256MB',threads=1,row_cap=1000)
        reports.run(args)
        return dest

    def test_outputs_formulas_censoring_and_modes(self):
        from openpyxl import load_workbook
        with tempfile.TemporaryDirectory() as tmp:
            dest = self.execute(tmp)
            def read(name):
                with (dest/name).open(encoding='utf-8-sig') as f:
                    return list(csv.DictReader(f))
            dm = read('data_metrics.csv')
            self.assertEqual(len(dm),2)  # corrected SL excluded; all levels includes300
            self.assertEqual({(r['ab_group'],float(r['level'])) for r in dm},{('0',3),('2',300)})
            d = dm[0]
            self.assertEqual(d['ab_group'],'0')
            self.assertEqual(int(d['user_start_count']),4)
            self.assertEqual(float(d['completion_rate_first_attempt_pct']),50)
            self.assertEqual(float(d['coin_balance_per_user']),50)
            self.assertEqual(float(d['total_coin_balance_per_user']),76)
            self.assertEqual(float(d['coin_spend_per_user']),2.5)
            self.assertEqual(float(d['total_coin_spend_per_user']),11.25)
            self.assertAlmostEqual(float(d['imp_per_lau']),1/3)  # excludes banner
            self.assertAlmostEqual(float(d['inter_imp_per_lau']),1/3)
            self.assertEqual(d['rwd_per_lau'],'N/A')
            self.assertEqual(d['churn_rate_d7_pct'],'N/A')
            first=read('loss_first_attempt.csv')[0]
            alls=read('loss_all_attempts.csv')[0]
            self.assertEqual((int(first['user_count']),int(first['lose_count'])),(2,3))
            self.assertEqual((int(alls['user_count']),int(alls['lose_count'])),(3,4))
            self.assertEqual(first['churn_users_d3'], '1')
            self.assertEqual(first['churn_users_d3'],alls['churn_users_d3'])
            self.assertAlmostEqual(float(first['churn_rate_d3_pct']),100/3)
            self.assertEqual(float(first['0-10']),50)
            self.assertEqual(float(read('loss_first_attempt.csv')[1]['level']),4.2)  # Loss keepsphase from sourceBI
            wb=load_workbook(dest/'sandjam_reports.xlsx')
            self.assertEqual(wb.sheetnames,['Data Metrics','Loss 1st attempt','Loss All'])
            self.assertTrue(wb['Loss All'].conditional_formatting)
            self.assertEqual(wb['Data Metrics'].freeze_panes,'C2')
            manifest=json.loads((dest/'manifest.json').read_text())
            for path,expected in manifest['files'].items():
                self.assertEqual(reports.sha(dest/path),expected)
            self.assertEqual(manifest['status'],'complete')
            self.assertEqual(len(list(dest.glob('*.csv'))),3)
            self.assertNotIn('fake_', (dest/'data_readme.md').read_text())

    def test_input_must_be_corrected(self):
        for kwargs in (dict(missing_mode=True),dict(bad_fix=True)):
            with self.subTest(kwargs=kwargs),tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(ValueError):
                    self.execute(tmp,**kwargs)
                self.assertFalse((Path(tmp)/'out').exists())

    def test_existing_output_not_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest=self.execute(tmp)
            marker=dest/'manifest.json'
            digest=reports.sha(marker)
            with self.assertRaises(ValueError):
                self.execute(tmp)
            self.assertEqual(reports.sha(marker),digest)

    def test_struct_json_same_outputs(self):
        with tempfile.TemporaryDirectory() as tmp1,tempfile.TemporaryDirectory() as tmp2:
            string_output=self.execute(tmp1)
            struct_output=self.execute(tmp2,nested=True)
            for filename,_,_ in reports.OUTPUTS.values():
                self.assertEqual((string_output/filename).read_bytes(),(struct_output/filename).read_bytes())


if __name__=='__main__':
    unittest.main()
