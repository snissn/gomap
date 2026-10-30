import importlib.util
import json
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
    def test_run_profile_requires_verifier_and_exact_readback(self):
        profile = 'command_wal_durable'
        valid = dict(rows=100000, profile=profile, actual_mode='cow_btree', native=True,
                     expected_hashed_keys_verified=True, verification_max_scan_documents=100001,
                     verification_max_cursor_retained_bytes=256 << 20)
        cases = [('success', 0, valid), ('verifier_exit', 1, valid), ('invalid_json', 0, '{')]
        cases += [(key, 0, valid | {key: value}) for key, value in dict(
            rows=99999, profile='wrong', actual_mode='skiplist', native=False,
            expected_hashed_keys_verified=False, verification_max_scan_documents=100000,
            verification_max_cursor_retained_bytes=1).items()]
        for name, code, readback in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                server, client, verifier = (root / name for name in ('server', 'client', 'verifier'))
                proc = mock.Mock(pid=123)
                proc.poll.return_value = None

                def start(*args, **kwargs):
                    kwargs['stdout'].write(b'TreeDB Native Server listening on 127.0.0.1:12345\n'
                                           b'TreeDB storage: shared_cow\n')
                    kwargs['stdout'].flush()
                    return proc

                def observed(argv, **kwargs):
                    if argv[0] == str(client):
                        kwargs['stdout'].write(summary().encode())
                        return 0, {'exe': str(client)}
                    self.assertEqual(argv, [str(verifier), '-dir', str(root / profile / 'db'), '-profile', profile])
                    kwargs['stdout'].write((json.dumps(readback) if isinstance(readback, dict) else readback).encode())
                    return code, {'exe': str(verifier)}

                with mock.patch.object(runner.subprocess, 'Popen', side_effect=start), \
                     mock.patch.object(runner, 'observe_child', return_value={'exe': str(server)}), \
                     mock.patch.object(runner, 'close_server', return_value=(0, [], [])), \
                     mock.patch.object(runner, 'run_observed', side_effect=observed) as run:
                    self.assertEqual(runner.run_profile(root, profile, server, client, verifier, {}), name == 'success')
                self.assertEqual(run.call_count, 2)
                receipt = json.loads((root / profile / 'terminal.json').read_text())
                self.assertEqual(receipt['verifier_exit'], code)
                self.assertEqual(receipt['classification'], 'green' if name == 'success' else 'inconclusive')
                if name not in ('success', 'verifier_exit', 'invalid_json'):
                    self.assertTrue(any('invalid reopen verifier result' in error for error in receipt['errors']))

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
        code = ('import os,signal,sys,time\n'
                'def close(sig, frame):\n os.write(1, b"closing\\n"); os.read(0, 1); sys.exit(0)\n'
                'signal.signal(signal.SIGTERM, close)\n'
                'os.write(1, b"ready\\n")\n'
                'while True: time.sleep(1)\n')
        proc = subprocess.Popen([sys.executable, '-c', code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                                start_new_session=True)
        self.assertEqual(proc.stdout.readline().strip(), 'ready')
        wait = proc.wait

        def signal_after_close_started(*args, **kwargs):
            self.assertEqual(proc.stdout.readline().strip(), 'closing')
            # close_server has installed its deferred handlers and the child
            # cannot exit until this main-thread callback releases it.
            os.kill(os.getpid(), signal.SIGTERM)
            proc.stdin.write('release\n')
            proc.stdin.flush()
            return wait(*args, **kwargs)

        try:
            with mock.patch.object(proc, 'wait', side_effect=signal_after_close_started):
                exit_code, errors, pending = runner.close_server(proc)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)
            proc.stdin.close()
            proc.stdout.close()
        self.assertEqual(exit_code, 0)
        self.assertEqual(errors, [])
        self.assertEqual(pending, [signal.SIGTERM])
        self.assertIsNotNone(proc.returncode)


if __name__ == '__main__':
    unittest.main()
