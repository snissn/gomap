import importlib.util
import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

spec = importlib.util.spec_from_file_location('cow_json_load_regression', Path(__file__).with_name('cow_json_load_regression.py'))
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def summary(insert=100000, total=100000, errors=None):
    fail = '' if errors is None else f'INSERT_ERROR - Takes(s): 1.0, Count: {errors}, OPS: 1.0\n'
    return ('INSERT - Takes(s): 10.0, Count: 12, OPS: 1.0\n'
            'Run finished, takes 20s\n'
            f'INSERT - Takes(s): 20.0, Count: {insert}, OPS: 1.0\n'
            + fail + f'TOTAL  - Takes(s): 20.0, Count: {total}, OPS: 1.0\n')


class LoadCountsTest(unittest.TestCase):
    def test_success_ignores_intermediate_summary(self):
        self.assertEqual(runner.load_counts(summary()), {'INSERT': 100000, 'TOTAL': 100000})

    def test_rejects_partial_counts_and_errors(self):
        for value in (summary(insert=1692, total=1692, errors=98308),
                      summary(total=99999), summary(errors=1), summary(insert=100001)):
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.load_counts(value)

    def test_rejects_missing_or_duplicate_terminal_data(self):
        for value in ('INSERT - Takes(s): 1.0, Count: 100000\n',
                      summary() + 'TOTAL - Takes(s): 1, Count: 100000\n',
                      summary() + 'Run finished, takes 2s\n',
                      summary().replace('INSERT - Takes(s): 20.0', 'UPDATE - Takes(s): 20.0'),
                      summary(errors=0).replace('Count: 0,', 'Count: -1,'),
                      summary(errors=0).replace('Count: 0,', 'Count: broken,'),
                      summary().replace('Count: 100000,', 'Count: 100000garbage,')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                runner.load_counts(value)

    def test_rejects_unpinned_client_overlay(self):
        with self.assertRaises(ValueError):
            runner.client_overlay(b'unexpected client source')

    def test_observation_failure_reaps_owned_child(self):
        started = []
        popen = subprocess.Popen

        def capture(*args, **kwargs):
            proc = popen(*args, **kwargs)
            started.append(proc)
            return proc

        with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
            with mock.patch.object(runner.subprocess, 'Popen', side_effect=capture), \
                 mock.patch.object(runner, 'observe_child', side_effect=OSError('unreadable environment')):
                with self.assertRaises(OSError):
                    runner.run_observed([sys.executable, '-c', 'import time; time.sleep(30)'],
                                        env=dict(os.environ), stdout=out, stderr=err, timeout=2)
        self.assertEqual(len(started), 1)
        self.assertIsNotNone(started[0].returncode)

    def test_signal_interrupt_reaps_owned_child(self):
        started = []
        popen = subprocess.Popen

        def capture(*args, **kwargs):
            proc = popen(*args, **kwargs)
            started.append(proc)
            return proc

        previous = signal.signal(signal.SIGTERM, runner.interrupt)
        timer = threading.Timer(.2, os.kill, args=(os.getpid(), signal.SIGTERM))
        try:
            with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
                with mock.patch.object(runner.subprocess, 'Popen', side_effect=capture):
                    timer.start()
                    with self.assertRaises(runner.RunnerInterrupted):
                        runner.run_observed([sys.executable, '-c', 'import time; time.sleep(30)'],
                                            env=runner.env_without_override(), stdout=out, stderr=err, timeout=2)
        finally:
            timer.cancel()
            timer.join()
            signal.signal(signal.SIGTERM, previous)
        self.assertEqual(len(started), 1)
        self.assertIsNotNone(started[0].returncode)


if __name__ == '__main__':
    unittest.main()
