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
    def test_classification_requires_actual_matching_failure(self):
        red = {'signature_observation_complete': True, 'client_exit': 0,
               'workload_completed': True, 'load_failed': True, 'other_failure': False,
               'server_exit': 1, 'observed_counts': runner.parse_load_summary(
                   summary(insert=1692, total=1692, errors=98308)),
               'reducer_signature_present': True, 'errors': ['failed load', 'close exit 1']}
        self.assertEqual(runner.classify(red), 'baseline_reproduced')
        for changes in ({'reducer_signature_present': False}, {'other_failure': True},
                        {'workload_completed': False}, {'client_exit': None},
                        {'signature_observation_complete': False},
                        {'observed_counts': {'INSERT_ERROR': 0}},
                        {'observed_counts': {'INSERT_ERROR': 1}},
                        {'observed_counts': {'INSERT': 1692, 'TOTAL': 1692, 'INSERT_ERROR': 1}},
                        {'observed_counts': {'INSERT': 1692, 'TOTAL': 1691, 'INSERT_ERROR': 98308}}):
            with self.subTest(changes=changes):
                self.assertEqual(runner.classify(red | changes), 'inconclusive')

    def test_green_with_signature_is_rejected(self):
        green = {'signature_observation_complete': True, 'client_exit': 0,
                 'workload_completed': True, 'load_failed': False, 'other_failure': False,
                 'server_exit': 0, 'verifier_exit': 0,
                 'observed_counts': runner.parse_load_summary(summary()),
                 'reducer_signature_present': False, 'errors': []}
        self.assertEqual(runner.classify(green), 'green')
        for changes in ({'reducer_signature_present': True}, {'verifier_exit': None},
                        {'signature_observation_complete': False}, {'workload_completed': False},
                        {'other_failure': True}):
            with self.subTest(changes=changes):
                self.assertEqual(runner.classify(green | changes), 'inconclusive')

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

    def test_signal_during_server_close_is_deferred_and_reaped(self):
        code = ('import signal,sys,time\n'
                'def close(sig, frame):\n time.sleep(.5); sys.exit(0)\n'
                'signal.signal(signal.SIGTERM, close)\n'
                'print("ready", flush=True)\n'
                'while True: time.sleep(1)\n')
        proc = subprocess.Popen([sys.executable, '-c', code], stdout=subprocess.PIPE, text=True,
                                start_new_session=True)
        self.assertEqual(proc.stdout.readline().strip(), 'ready')
        timer = threading.Timer(.2, os.kill, args=(os.getpid(), signal.SIGTERM))
        timer.start()
        try:
            exit_code, errors, pending = runner.close_server(proc)
        finally:
            timer.cancel()
            timer.join()
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)
            proc.stdout.close()
        self.assertEqual(exit_code, 0)
        self.assertEqual(errors, [])
        self.assertEqual(pending, [signal.SIGTERM])
        self.assertIsNotNone(proc.returncode)


if __name__ == '__main__':
    unittest.main()
