"""Synthetic controls only: no client or database benchmark is run."""
import errno
import os
import select
import subprocess
import sys
import time
import unittest
from types import SimpleNamespace
from unittest import mock

import ycsb_runtime_observer as observer


class FakeObserver:
    def __init__(self, samples, identity, error):
        self.samples = samples
        self.identity = identity
        self.error = error
        self.observation_error = None
        self.terminal_exit_race = None
    def sample(self, child):
        self.observation_error = {'type': type(self.error).__name__, 'message': str(self.error)}
        raise self.error


@unittest.skipUnless(sys.platform=='linux' and hasattr(os,'pidfd_open'), 'Linux /proc and pidfd controls')
class ShortChildControls(unittest.TestCase):
    def test_pending_uses_fast_cadence_only_until_witness(self):
        self.assertEqual(observer.sample_interval(SimpleNamespace(samples=[]), .25), .001)
        self.assertEqual(observer.sample_interval(SimpleNamespace(samples=[{}]), .25), .25)
        self.assertEqual(observer.sample_interval(SimpleNamespace(samples=[]), .0005), .0005)

    def test_exit_race_preserves_prior_witness_and_diagnostic(self):
        child = SimpleNamespace(pid=1234)
        failure = FileNotFoundError(errno.ENOENT, 'gone', '/proc/1234/exe')
        bound = FakeObserver([{'runtime_gomaxprocs': 12}], (1234, (10, 20)), failure)
        self.assertIsNone(observer.sample_owned_or_exited(bound, child, lambda timeout: True))
        self.assertIsNone(bound.observation_error)
        self.assertEqual(bound.terminal_exit_race['type'], 'FileNotFoundError')
        self.assertEqual(bound.samples, [{'runtime_gomaxprocs': 12}])

    def test_no_witness_live_child_wrong_pid_or_unrelated_failure_fail_closed(self):
        child = SimpleNamespace(pid=1234)
        proc_error = FileNotFoundError(errno.ENOENT, 'gone', '/proc/1234/exe')
        other_error = FileNotFoundError(errno.ENOENT, 'gone', '/sys/fs/cgroup/cpu.max')
        permission = PermissionError(errno.EACCES, 'denied', '/proc/1234/exe')
        cases = [([], (1234, (1, 2)), proc_error, True),
                 ([{}], (1234, (1, 2)), proc_error, False),
                 ([{}], (9999, (1, 2)), proc_error, True),
                 ([{}], (1234, (1, 2)), other_error, True),
                 ([{}], (1234, (1, 2)), permission, True)]
        for samples, ident, err, exited in cases:
            with self.subTest(samples=samples, ident=ident, err=err, exited=exited):
                bound=FakeObserver(samples, ident, err)
                with self.assertRaises(OSError):
                    observer.sample_owned_or_exited(bound, child, lambda timeout: exited)
                self.assertIsNotNone(bound.observation_error)

    def test_bounded_pidfd_recheck_records_every_predicate(self):
        child=SimpleNamespace(pid=1234)
        failure=FileNotFoundError(errno.ENOENT, 'gone', '/proc/1234/exe')
        bound=FakeObserver([{'synthetic': True}], (1234, (1, 2)), failure)
        readiness=mock.Mock(side_effect=[False,True])
        self.assertIsNone(observer.sample_owned_or_exited(bound, child, readiness, .04))
        self.assertEqual(readiness.call_args_list,[mock.call(0),mock.call(.04)])
        check=bound.exit_race_check
        self.assertEqual((check['errno'],check['filename'],check['expected_pid']),
                         (errno.ENOENT,'/proc/1234/exe',1234))
        self.assertTrue(check['eligible'])
        self.assertFalse(check['pidfd_ready_initial'])
        self.assertTrue(check['pidfd_ready_after_wait'])
        self.assertGreaterEqual(check['wait_elapsed_seconds'],0)

    def test_bounded_no_exit_and_pidfd_errors_remain_fail_closed(self):
        child=SimpleNamespace(pid=1234)
        failure=FileNotFoundError(errno.ENOENT, 'gone', '/proc/1234/exe')
        for effect in ([False,False], [OSError(errno.EBADF,'bad pidfd')]):
            bound=FakeObserver([{'synthetic': True}], (1234, (1, 2)), failure)
            readiness=mock.Mock(side_effect=effect)
            with self.assertRaises(OSError):
                observer.sample_owned_or_exited(bound,child,readiness,.02)
            self.assertIsNotNone(bound.observation_error)
            if len(effect)==2:
                self.assertFalse(bound.exit_race_check['pidfd_ready_after_wait'])
            else:
                self.assertEqual(bound.exit_race_check['pidfd_check_error']['errno'],errno.EBADF)

    def test_fast_direct_child_with_pending_then_synthetic_witness(self):
        # The synthetic witness is NOT a runtime or product qualification.
        child=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(.20)'])
        fd=os.pidfd_open(child.pid)
        class PendingThenSample:
            samples=[]
            def sample(self, c):
                if not hasattr(self, 'started'):
                    self.started=time.monotonic()
                    return None
                if time.monotonic()-self.started < .02:
                    return None
                self.samples.append({'synthetic': True})
                return self.samples[0]
        sample=PendingThenSample()
        try:
            while not select.select([fd], [], [], 0)[0] and not sample.samples:
                observer.sample_owned_or_exited(sample, child, lambda timeout: bool(select.select([fd],[],[],timeout)[0]))
                select.select([fd], [], [], observer.sample_interval(sample, .25))
            self.assertTrue(sample.samples, 'synthetic startup witness missed')
        finally:
            child.wait(timeout=2)
            os.close(fd)

    def test_real_observer_exit_race_uses_child_pidfd_and_requires_witness(self):
        # Artificially force a /proc disappearance only after the real owned
        # synthetic child exits, so no scheduling race is needed in the test.
        for has_witness in (True, False):
            child=subprocess.Popen([sys.executable, '-c', 'pass'])
            fd=os.pidfd_open(child.pid)
            bound=observer.RuntimeObserver.__new__(observer.RuntimeObserver)
            bound.expected_gomaxprocs=None
            bound.samples=[{'synthetic': True}] if has_witness else []
            bound.identity=(child.pid, (os.getpid(), 123))
            bound.observation_error=None
            bound.terminal_exit_race=None
            try:
                self.assertTrue(select.select([fd], [], [], 2)[0])
                failure=FileNotFoundError(errno.ENOENT, 'gone', f'/proc/{child.pid}/exe')
                with mock.patch.object(bound, '_sample', side_effect=failure):
                    pinned_exit=lambda timeout: bool(select.select([fd], [], [], timeout)[0])
                    if has_witness:
                        self.assertIsNone(observer.sample_owned_or_exited(bound, child, pinned_exit))
                        self.assertEqual(bound.terminal_exit_race['type'], 'FileNotFoundError')
                    else:
                        with self.assertRaises(FileNotFoundError):
                            observer.sample_owned_or_exited(bound, child, pinned_exit)
                        self.assertIsNotNone(bound.observation_error)
            finally:
                child.wait(timeout=2)
                os.close(fd)
            self.assertFalse(os.path.exists(f'/proc/{child.pid}'))

if __name__ == '__main__':
    unittest.main()
