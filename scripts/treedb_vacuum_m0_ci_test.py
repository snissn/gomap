#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.dont_write_bytecode = True
SCRIPT = Path(__file__).with_name('treedb_vacuum_m0_ci.py')
spec = importlib.util.spec_from_file_location('m0ci', SCRIPT)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class SupervisorTest(unittest.TestCase):
    def test_exit_and_existing_packet(self):
        for status in (0, 1):
            with tempfile.TemporaryDirectory() as directory:
                packet = Path(directory) / 'packet'
                self.assertEqual(m.supervise([sys.executable, '-c', f'raise SystemExit({status})'],
                                             packet, 'hosted-semantic', 720, grace=0.1), status)
                receipt = (packet / 'ci-budget.json').read_text()
                self.assertEqual(json.loads(receipt)['completed'], status == 0)
                with self.assertRaises(ValueError):
                    m.supervise(['false'], packet, 'hosted-semantic', 720)
                self.assertEqual((packet / 'ci-budget.json').read_text(), receipt)

    def test_timeout_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            packet = Path(directory) / 'packet'
            self.assertEqual(m.supervise([sys.executable, '-c', 'import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(60)'],
                                         packet, 'hosted-semantic', 0.05, grace=0.1), 124)
            receipt = json.loads((packet / 'ci-budget.json').read_text())
            self.assertFalse(receipt['completed'])
            self.assertEqual(receipt['termination_reason'], 'timeout')

    def test_external_interrupt_kills_owned_descendants_and_records_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            packet = root / 'packet'
            pid_file = root / 'descendant'
            child_code = ("import signal,time,os,pathlib; signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                          f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid())); time.sleep(60)")
            parent_code = ("import subprocess,sys,time,pathlib; "
                           f"p=subprocess.Popen([sys.executable,'-c',{child_code!r}]); "
                           "time.sleep(60)")
            # Exercise the public shell wrapper too: its exec must preserve the
            # PID/signals, while a fake git root points at harmless child work.
            (root / 'scripts').mkdir()
            capture = root / 'scripts/treedb_vacuum_m0_capture.sh'
            import shlex
            capture.write_text('exec ' + shlex.join([sys.executable, '-c', parent_code]) + '\n')
            fake_bin = root / 'bin'
            fake_bin.mkdir()
            git = fake_bin / 'git'
            git.write_text('#!/bin/sh\nprintf %s ' + shlex.quote(str(root)) + '\n')
            git.chmod(0o755)
            env = dict(os.environ, PATH=str(fake_bin) + os.pathsep + os.environ['PATH'],
                       RUN_DIR=str(packet), M0_QUALIFICATION_PROFILE='hosted-semantic')
            supervisor = subprocess.Popen(['bash', str(SCRIPT.with_suffix('.sh'))], env=env)
            descendant = None
            try:
                deadline = time.monotonic() + 5
                while not pid_file.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(pid_file.exists())
                descendant = int(pid_file.read_text())
                supervisor.send_signal(signal.SIGTERM)
                self.assertEqual(supervisor.wait(timeout=5), 143)
                receipt = json.loads((packet / 'ci-budget.json').read_text())
                self.assertFalse(receipt['completed'])
                self.assertEqual(receipt['termination_reason'], 'interrupted')
                self.assertEqual(receipt['interrupted_signal'], signal.SIGTERM)
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    stat = Path(f'/proc/{descendant}/stat')
                    if not stat.exists() or stat.read_text().split()[2] == 'Z':
                        break
                    time.sleep(0.01)
                else:
                    self.fail('owned descendant remains running')
            finally:
                if supervisor.poll() is None:
                    supervisor.kill()
                    supervisor.wait()
                if descendant is not None:
                    try:
                        os.kill(descendant, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_success_drains_descendant_before_receipt_and_reap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_file = root / 'pid'
            code = ("import os,signal,time,pathlib; "
                    "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
                    f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid())); time.sleep(60)")
            parent = ("import subprocess,sys,time,pathlib; "
                      f"subprocess.Popen([sys.executable,'-c',{code!r}]); "
                      f"p=pathlib.Path({str(pid_file)!r}); "
                      "exec('while not p.exists(): time.sleep(0.01)')")
            real_kill = os.killpg
            def checked_kill(pid, sig):
                # Every signal must occur while the real child is unreaped.
                self.assertIsNotNone(os.waitid(os.P_PID, pid, os.WEXITED | os.WNOWAIT))
                return real_kill(pid, sig)
            with mock.patch.object(m.os, 'killpg', side_effect=checked_kill):
                self.assertEqual(m.supervise([sys.executable, '-c', parent], root / 'packet',
                                             'hosted-semantic', 5, grace=0.1), 0)
            pid = int(pid_file.read_text())
            stat = Path(f'/proc/{pid}/stat')
            self.assertTrue(not stat.exists() or stat.read_text().split()[2] == 'Z')
            self.assertTrue(json.loads((root / 'packet/ci-budget.json').read_text())['completed'])

    def test_lost_reaped_custody_never_signals_reused_id(self):
        real_spawn = subprocess.Popen
        def reaped(*args, **kwargs):
            child = real_spawn(*args, **kwargs)
            child.wait()
            return child
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(m.subprocess, 'Popen', side_effect=reaped), \
             mock.patch.object(m.os, 'killpg') as kill:
            packet = Path(directory) / 'packet'
            self.assertEqual(m.supervise([sys.executable, '-c', 'pass'], packet,
                                         'hosted-semantic', 5, grace=0.1), 125)
            kill.assert_not_called()
            self.assertFalse(json.loads((packet / 'ci-budget.json').read_text())['completed'])

    def test_failed_drain_is_incomplete(self):
        real_read = Path.read_text
        def fail_proc(path, *args, **kwargs):
            if str(path).startswith('/proc/'):
                raise PermissionError('injected failed drain observation')
            return real_read(path, *args, **kwargs)
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(Path, 'read_text', fail_proc):
            packet = Path(directory) / 'packet'
            self.assertEqual(m.supervise([sys.executable, '-c', 'pass'], packet,
                                         'hosted-semantic', 5, grace=0.1), 125)
            self.assertFalse(json.loads(real_read(packet / 'ci-budget.json'))['completed'])

    def test_failed_termination_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(m.os, 'killpg', side_effect=PermissionError('injected termination failure')):
            packet = Path(directory) / 'packet'
            self.assertEqual(m.supervise([sys.executable, '-c', 'pass'], packet,
                                         'hosted-semantic', 5, grace=0.1), 125)
            self.assertFalse(json.loads((packet / 'ci-budget.json').read_text())['completed'])
            # Reap only after the failed supervisor has finished issuing signals.
            while True:
                try:
                    if os.waitpid(-1, os.WNOHANG) == (0, 0):
                        break
                except ChildProcessError:
                    break

    def test_receipt_write_failure_cannot_return_success(self):
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(Path, 'write_text', side_effect=OSError('injected receipt failure')):
            packet = Path(directory) / 'packet'
            self.assertEqual(m.supervise([sys.executable, '-c', 'pass'], packet,
                                         'hosted-semantic', 5, grace=0.1), 125)
            self.assertFalse((packet / 'ci-budget.json').exists())

    def test_workflow_does_not_infer_controlled_custody(self):
        workflow = SCRIPT.parent.parent / '.github/workflows/treedb-vacuum-m0.yml'
        text = workflow.read_text()
        self.assertIn('M0_QUALIFICATION_PROFILE: hosted-semantic', text)
        self.assertNotIn("runner.environment == 'self-hosted'", text)


if __name__ == '__main__':
    unittest.main()
