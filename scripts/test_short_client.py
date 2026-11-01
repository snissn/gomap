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

class MappingExitControls(unittest.TestCase):
    def mapping_observer(self, has_witness=True):
        bound = observer.RuntimeObserver.__new__(observer.RuntimeObserver)
        bound.expected_gomaxprocs = None
        # Synthetic prior records test routing only, never runtime qualification.
        bound.samples = [{'synthetic': True}] if has_witness else []
        bound.identity = (1234, (10, 20))
        bound.elf_identity = (1, 2, 3, 4, 5)
        bound.observation_error = None
        bound.terminal_exit_race = None
        bound.exit_race_check = None
        return bound

    def invoke_mapping_loss(self, bound, ready, *, attempt=None, missing=(),
                            error=None, wait=.04, child=None):
        child = child or SimpleNamespace(pid=1234)
        def absent(owned):
            bound.last_attempt.update(parent_pid=10, start_ticks=20,
                                      exe_identity=list(bound.elf_identity))
            bound.last_attempt.update(attempt or {})
            for key in missing:
                bound.last_attempt.pop(key, None)
            if error is not None:
                raise error
            return observer.mapping_pending([], 0, True)
        with mock.patch.object(bound, '_sample', side_effect=absent):
            return observer.sample_owned_or_exited(bound, child, ready, wait)

    def test_absent_witnessed_mapping_with_pinned_exit_keeps_only_prior_samples(self):
        bound = self.mapping_observer()
        prior = list(bound.samples)
        ready = mock.Mock(return_value=True)
        self.assertIsNone(self.invoke_mapping_loss(bound, ready))
        ready.assert_called_once_with(0)
        self.assertEqual(bound.samples, prior)
        self.assertIsNone(bound.observation_error)
        self.assertEqual(bound.terminal_exit_race,
                         {'type': 'RuntimeMappingDisappeared',
                          'message': 'runtime mapping disappeared after observation'})
        self.assertEqual(bound.last_attempt['state'], 'failed')
        self.assertTrue(bound.exit_race_check['mapping_identity_matches'])
        self.assertTrue(bound.exit_race_check['pidfd_ready_initial'])

    def test_bounded_exit_recheck_uses_existing_deadline(self):
        bound = self.mapping_observer()
        ready = mock.Mock(side_effect=[False, True])
        self.assertIsNone(self.invoke_mapping_loss(bound, ready))
        self.assertEqual(ready.call_args_list, [mock.call(0), mock.call(.04)])
        self.assertTrue(bound.exit_race_check['pidfd_ready_after_wait'])
        self.assertEqual(bound.exit_race_check['wait_bound_seconds'], .04)
        self.assertEqual(len(bound.samples), 1)

    def test_live_mapping_loss_stays_error(self):
        bound = self.mapping_observer()
        ready = mock.Mock(side_effect=[False, False])
        with self.assertRaisesRegex(observer.RuntimeMappingDisappeared,
                                    'runtime mapping disappeared'):
            self.invoke_mapping_loss(bound, ready)
        self.assertEqual(ready.call_args_list, [mock.call(0), mock.call(.04)])
        self.assertIsNotNone(bound.observation_error)
        self.assertIsNone(bound.terminal_exit_race)
        self.assertEqual(bound.last_attempt['state'], 'failed')

    def test_mapping_loss_requires_prior_complete_witness(self):
        bound = self.mapping_observer(has_witness=False)
        ready = mock.Mock(return_value=True)
        with self.assertRaisesRegex(observer.RuntimeMappingDisappeared,
                                    'runtime mapping disappeared'):
            self.invoke_mapping_loss(bound, ready)
        ready.assert_not_called()
        self.assertFalse(bound.exit_race_check['eligible'])
        self.assertEqual(bound.samples, [])
        self.assertIsNotNone(bound.observation_error)
        self.assertIsNone(bound.terminal_exit_race)

    def test_mapping_failure_attempt_identity_must_match(self):
        cases = [('failed PID', {}, {'pid': 1235}, ()),
                 ('parent', {}, {'parent_pid': 11}, ()),
                 ('start ticks', {}, {'start_ticks': 21}, ()),
                 ('ELF', {}, {'exe_identity': [1, 2, 3, 4, 6]}, ()),
                 ('bound PID', {'identity': (9999, (10, 20))}, {}, ()),
                 ('unbound identity', {'identity': None}, {}, ())]
        cases += [('missing '+key, {}, {}, (key,))
                  for key in ('pid', 'parent_pid', 'start_ticks', 'exe_identity')]
        for label, changed, attempt, missing in cases:
            with self.subTest(case=label):
                bound = self.mapping_observer()
                for key, value in changed.items():
                    setattr(bound, key, value)
                ready = mock.Mock(return_value=True)
                with self.assertRaisesRegex(observer.RuntimeMappingDisappeared,
                                            'runtime mapping disappeared'):
                    self.invoke_mapping_loss(bound, ready, attempt=attempt,
                                             missing=missing)
                ready.assert_not_called()
                self.assertFalse(bound.exit_race_check['eligible'])
                self.assertIsNotNone(bound.observation_error)
                self.assertIsNone(bound.terminal_exit_race)

    def test_unrelated_errors_are_never_mapping_exit(self):
        errors = [RuntimeError('runtime/cpu policy changed'),
                  RuntimeError('runtime mapping disappeared after observation'),
                  PermissionError(errno.EACCES, 'denied', '/proc/1234/mem'),
                  OSError(errno.EIO, 'read failed', '/proc/1234/maps'),
                  FileNotFoundError(errno.ENOENT, 'gone', '/sys/fs/cgroup/cpu.max'),
                  FileNotFoundError(errno.ENOENT, 'gone', '/proc/9999/exe')]
        for error in errors:
            with self.subTest(error=error):
                bound = self.mapping_observer()
                ready = mock.Mock(return_value=True)
                with self.assertRaises(type(error)) as caught:
                    self.invoke_mapping_loss(bound, ready, error=error)
                self.assertIs(caught.exception, error)
                ready.assert_not_called()
                self.assertIsNotNone(bound.observation_error)
                self.assertIsNone(bound.terminal_exit_race)

    def test_mapping_exit_rejects_bad_pidfd_and_invalid_wait_bounds(self):
        bound = self.mapping_observer()
        failure = OSError(errno.EBADF, 'bad pidfd')
        ready = mock.Mock(side_effect=failure)
        with self.assertRaises(OSError) as caught:
            self.invoke_mapping_loss(bound, ready)
        self.assertIs(caught.exception, failure)
        self.assertEqual(bound.exit_race_check['pidfd_check_error']['errno'], errno.EBADF)
        self.assertIsNotNone(bound.observation_error)
        self.assertIsNone(bound.terminal_exit_race)
        for wait in (-.001, .051):
            with self.subTest(wait=wait):
                bound = self.mapping_observer()
                ready = mock.Mock(return_value=True)
                with self.assertRaisesRegex(RuntimeError, 'invalid exit recheck bound'):
                    self.invoke_mapping_loss(bound, ready, wait=wait)
                ready.assert_not_called()
                self.assertIsNotNone(bound.observation_error)
                self.assertIsNone(bound.terminal_exit_race)

    def test_mapping_startup_pending_and_deadline_unchanged(self):
        with mock.patch.object(observer.time, 'monotonic', return_value=11):
            self.assertTrue(observer.mapping_pending([], 10, False))
            self.assertFalse(observer.mapping_pending([['mapped']], 10, True))
            with self.assertRaisesRegex(RuntimeError, 'startup deadline exceeded'):
                observer.mapping_pending([], 8, False)

    @unittest.skipUnless(sys.platform == 'linux' and hasattr(os, 'pidfd_open'),
                         'Linux /proc and pidfd controls')
    def test_mapping_loss_uses_real_pinned_direct_child_exit(self):
        # The process, parent/start/ELF metadata and pidfd are real. The prior
        # sample and missing mapping are synthetic, not a runtime witness.
        for exited in (False, True):
            for has_witness in (False, True):
                with self.subTest(exited=exited, has_witness=has_witness):
                    child = subprocess.Popen(
                        [sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'],
                        stdin=subprocess.PIPE)
                    fd = None
                    try:
                        fd = os.pidfd_open(child.pid)
                        identity = observer.start_identity(child.pid)
                        self.assertEqual(identity[0], os.getpid())
                        stat = os.stat('/proc/'+str(child.pid)+'/exe')
                        elf = (stat.st_dev, stat.st_ino, stat.st_size,
                               stat.st_mtime_ns, stat.st_ctime_ns)
                        bound = self.mapping_observer(has_witness=has_witness)
                        bound.identity = (child.pid, identity)
                        bound.elf_identity = elf
                        attempt = dict(parent_pid=identity[0], start_ticks=identity[1])
                        if exited:
                            child.stdin.close()
                            self.assertTrue(select.select([fd], [], [], 2)[0],
                                            'owned child did not exit before mapping-loss check')
                        ready = lambda timeout: bool(select.select([fd], [], [], timeout)[0])
                        if exited and has_witness:
                            self.assertIsNone(self.invoke_mapping_loss(
                                bound, ready, attempt=attempt, child=child, wait=.01))
                            self.assertTrue(bound.exit_race_check['pidfd_ready_initial'])
                            self.assertTrue(bound.exit_race_check['mapping_identity_matches'])
                        else:
                            with self.assertRaises(observer.RuntimeMappingDisappeared):
                                self.invoke_mapping_loss(bound, ready, attempt=attempt,
                                                         child=child, wait=.01)
                            self.assertIsNotNone(bound.observation_error)
                            self.assertIsNone(bound.terminal_exit_race)
                        self.assertEqual(len(bound.samples), int(has_witness))
                        if not child.stdin.closed:
                            child.stdin.close()
                        self.assertEqual(child.wait(timeout=2), 0)
                    finally:
                        if not child.stdin.closed:
                            child.stdin.close()
                        try:
                            child.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            child.kill()
                            child.wait(timeout=2)
                            self.fail('owned mapping-loss control required forced cleanup')
                        finally:
                            if fd is not None:
                                os.close(fd)
                    self.assertFalse(os.path.exists('/proc/'+str(child.pid)))


if __name__ == '__main__':
    unittest.main()
