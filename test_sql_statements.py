import unittest
from pathlib import Path
from sql_statements import split_sql

class SqlStatementsTest(unittest.TestCase):
    def test_comment_separator(self):
        self.assertEqual(split_sql('-- note; do not run\nSELECT 1;'),['SELECT 1'])

    def test_quoted_and_block_separator(self):
        self.assertEqual(split_sql("SELECT ';', 'it''s;'; /* x; */ SELECT 2;"),["SELECT ';', 'it''s;'",'SELECT 2'])

    def test_actual_script(self):
        statements=split_sql(Path(__file__).with_name('levelplay_and_loss.sql').read_text(encoding='utf-8'))
        self.assertEqual(len(statements),23)
        self.assertTrue(all(s.startswith(('CREATE OR REPLACE TEMP VIEW','SELECT')) for s in statements))

if __name__=='__main__':
    unittest.main()
