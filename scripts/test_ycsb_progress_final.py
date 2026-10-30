"""Retained producer logs and fail-closed output controls; no product runs."""
import pathlib
import tempfile
import unittest
import ycsb_compare_validate as v

DATA = pathlib.Path(__file__).resolve().parent / 'testdata/ycsb_compare'
LOGS = [DATA/'retained-progress'/name for name in ('attempt-01-load.out','successor-01-load.out')]

class ProgressFinalTests(unittest.TestCase):
    def parse(self, value, phase='load', expected=100000):
        with tempfile.TemporaryDirectory(prefix='ycsb-progress-') as d:
            p=pathlib.Path(d)/'load.out'; p.write_text(value)
            rows=v.parse_log(p); v.check_counts(rows,expected,phase,p); return {r['operation']:r for r in rows}
    def test_actual_both_retained_loads(self):
        for p in LOGS:
            with self.subTest(path=p):
                text=p.read_text(); self.assertGreater(text.count('INSERT - Takes(s):'),1)
                rows=self.parse(text); self.assertEqual(rows['TOTAL']['count'],100000)
        self.assertEqual(self.parse(LOGS[0].read_text())['TOTAL']['ops_per_sec'],841.9)
        self.assertEqual(self.parse(LOGS[1].read_text())['TOTAL']['ops_per_sec'],853.0)
    def test_retained_run(self):
        self.assertEqual(self.parse((DATA/'run_1.out').read_text(),'run',10000)['TOTAL']['count'],10000)
        p=DATA/'retained-progress/worker-run.out'
        self.assertEqual(self.parse(p.read_text(),'run',128)['TOTAL']['count'],128)
    def test_missing_marker(self):
        s=LOGS[1].read_text(); s=s.replace('Run finished, takes 1m57.257858368s\n','')
        with self.assertRaisesRegex(ValueError,'missing final marker'): self.parse(s)
    def test_duplicate_marker(self):
        s=LOGS[1].read_text(); s=s.replace('Run finished, takes 1m57.257858368s\n','Run finished, takes 1m57.257858368s\nRun finished, takes 1m57.257858368s\n')
        with self.assertRaisesRegex(ValueError,'duplicate final marker'): self.parse(s)
    def test_malformed_marker(self):
        s=LOGS[1].read_text().replace('Run finished, takes 1m57.257858368s','Run finished, takes bad')
        with self.assertRaisesRegex(ValueError,'malformed final marker'): self.parse(s)
    def test_duplicate_final(self):
        s=LOGS[1].read_text(); last=s.splitlines()[-1]; s+=last+'\n'
        with self.assertRaisesRegex(ValueError,'duplicate TOTAL'): self.parse(s)
    def test_truncated_and_missing_final(self):
        s=LOGS[1].read_text()
        for bad in (s[:-1],s.rsplit('\n',2)[0]+'\n'):
            with self.subTest(bad=bad[-30:]):
                with self.assertRaises(ValueError): self.parse(bad)
    def test_bad_progress_row(self):
        s=LOGS[1].read_text().replace('INSERT - Takes(s): 10.0, Count: 8652','INSERT - Takes(s): 10.0, Count: bad',1)
        with self.assertRaisesRegex(ValueError,'malformed measurement'): self.parse(s)
        s=LOGS[1].read_text().replace('INSERT - Takes(s): 10.0','INSE-RT - Takes(s): 10.0',1)
        with self.assertRaisesRegex(ValueError,'malformed measurement'): self.parse(s)
    def test_bad_final_row(self):
        s=LOGS[1].read_text().replace('Count: 100000','Count: bad',1)
        with self.assertRaisesRegex(ValueError,'malformed measurement'): self.parse(s)
    def test_wrong_count(self):
        s=LOGS[1].read_text().replace('Count: 100000','Count: 99999')
        with self.assertRaisesRegex(ValueError,'count mismatch'): self.parse(s)
    def test_progress_error_cannot_disappear(self):
        s=LOGS[1].read_text(); row=[line for line in s.splitlines() if line.startswith('INSERT - Takes')][0]
        s=s.replace(row,row+'\n'+row.replace('INSERT -','INSERT_ERROR -').replace('Count: 8652','Count: 1'),1)
        with self.assertRaisesRegex(ValueError,'errors in progress'): self.parse(s)
    def test_decreasing_progress_and_progress_greater_than_final(self):
        s=LOGS[1].read_text()
        bad1=s.replace('Count: 8652','Count: 20000',1)
        bad2=s.replace('Count: 8652','Count: 200000')
        with self.assertRaisesRegex(ValueError,'decreasing progress count'): self.parse(bad1)
        with self.assertRaises(ValueError): self.parse(bad2)
    def test_unexpected_progress_cannot_disappear(self):
        s=LOGS[1].read_text(); row=[line for line in s.splitlines() if line.startswith('INSERT - Takes')][0]
        s=s.replace(row,row+'\n'+row.replace('INSERT -','DELETE -'),1)
        with self.assertRaisesRegex(ValueError,'final rows do not cover progress'): self.parse(s)
    def test_final_error_and_unexpected_ops_rejected(self):
        s=LOGS[1].read_text(); marker='Run finished, takes 1m57.257858368s\n'; final=s.split(marker)[1]; row=final.splitlines()[0]
        for bad in (row.replace('INSERT -','INSERT_ERROR -'),row.replace('INSERT -','DELETE -')):
            with self.subTest(op=bad[:12]):
                with self.assertRaises(ValueError): self.parse(s+bad+'\n')

if __name__=='__main__': unittest.main()
