"""Offline validation controls; never launches benchmark workloads or services."""
import copy
from pathlib import Path
import tempfile
import unittest

import aggregate
import run


class RecipeControls(unittest.TestCase):
    def test_retained_ycsb_progress_and_final(self):
        data = Path(__file__).resolve().parents[1]/'testdata/ycsb_compare'
        text = (data/'retained-progress/successor-01-load.out').read_text()
        self.assertEqual(run.parse_ycsb(text, 100000, 'load')['TOTAL']['ops_per_sec'], 853.0)
        row = next(line for line in text.splitlines() if line.startswith('INSERT - Takes'))
        invalid = [text[:-1], text.replace('Run finished, takes ', 'Run done, takes '),
                   text+text.splitlines()[-1]+'\n', text.replace('Count: 100000', 'Count: 99999'),
                   text.replace(row, row+'\n'+row.replace('INSERT -', 'INSERT_ERROR -'), 1),
                   text.replace('Count: 8652', 'Count: bad', 1)]
        for value in invalid:
            with self.subTest(tail=value[-100:]), self.assertRaises(ValueError):
                run.parse_ycsb(value, 100000, 'load')

    def test_recorded_linux_and_macos_paths(self):
        for value in ('/tmp/task/runs', '/private/tmp/task/runs', '/mnt/fast4tb/task/runs'):
            self.assertEqual(aggregate.recorded_path(value), value)
        for value in ('relative/task', '/mnt/task/../other'):
            with self.assertRaises(ValueError):
                aggregate.recorded_path(value)

    def test_strict_json(self):
        for value in ('{"a":1,"a":2}', '{"a":NaN}', '{"a":1e999}'):
            with self.assertRaises(ValueError):
                run.strict_json(value)

    def test_close_identity_and_errors(self):
        env = dict(YCSB_PHASE_ID='phase', YCSB_CLIENT_BUILD_ID='build')
        receipt = dict(schema_version=1, phase_id='phase', client_build_id='build',
                       declared_profile='profile', phase='run', db='mongodb', total_excludes_close=True)
        for name in ('command', 'db_close', 'workload_close'):
            receipt[name] = dict(attempted=True, success=True, error='')
        run.validate_receipt(receipt, env, 'profile', 'run')
        for name in ('command', 'db_close', 'workload_close'):
            bad = copy.deepcopy(receipt)
            bad[name]['success'] = False
            with self.assertRaises(ValueError):
                run.validate_receipt(bad, env, 'profile', 'run')
        bad = dict(receipt, phase_id='stale')
        with self.assertRaises(ValueError):
            run.validate_receipt(bad, env, 'profile', 'run')

    def test_changed_frozen_recipe_fails_before_launch(self):
        old = {name: getattr(run, name, None) for name in ('ROOT', 'FREEZE', 'FREEZE_HASH')}
        try:
            with tempfile.TemporaryDirectory() as directory:
                run.ROOT = Path(directory)
                (run.ROOT/'recipe').mkdir()
                (run.ROOT/'recipe/run.py').write_text('changed')
                (run.ROOT/'freeze.json').write_text('{}')
                run.FREEZE_HASH = run.sha(run.ROOT/'freeze.json')
                run.FREEZE = {'recipe_sources': {'run.py': '0'*64}}
                with self.assertRaisesRegex(ValueError, 'frozen recipe changed'):
                    run.verify_freeze()
                (run.ROOT/'freeze.json').write_text('changed')
                with self.assertRaisesRegex(ValueError, 'frozen manifest changed'):
                    run.verify_freeze()
        finally:
            for name, value in old.items():
                setattr(run, name, value)


if __name__ == '__main__':
    unittest.main()
