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
    def test_main_binds_checkout_identity_and_actions_invocation_before_launch(self):
        env = {'M0_QUALIFICATION_PROFILE': 'controlled-performance', 'M0_CONTROLLED': '1',
               'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2', 'M0_SOURCE_SHA': 'b' * 40,
               'RUN_DIR': '/unused/new-packet'}
        with mock.patch.dict(os.environ, env), \
             mock.patch.object(m.subprocess, 'check_output', side_effect=['/bound/root\n', 'a' * 40 + '\n']) as git, \
             mock.patch.object(m, 'supervise', return_value=0) as launch:
            self.assertEqual(m.main(), 0)
        self.assertEqual(git.call_args_list[1].args[0], ['git', '-C', '/bound/root', 'rev-parse', 'HEAD'])
        self.assertEqual(launch.call_args.kwargs['identity'], {
            'source_sha': 'a' * 40, 'run': {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2'}})

    def test_controlled_invocation_requires_bound_run_and_attempt(self):
        for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT'):
            with self.subTest(key=key), mock.patch.dict(os.environ, {
                    'M0_QUALIFICATION_PROFILE': 'controlled-performance', 'M0_CONTROLLED': '1',
                    'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2', key: ''}), \
                 mock.patch.object(m.subprocess, 'check_output', side_effect=['/bound/root', 'a' * 40]), \
                 mock.patch.object(m, 'supervise') as launch:
                with self.assertRaisesRegex(ValueError, 'exact source/run/attempt'):
                    m.main()
                launch.assert_not_called()

    def test_supervisor_freezes_identity_before_child_launch(self):
        identity = {'source_sha': 'a' * 40,
                    'run': {'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '2'}}
        real_popen = subprocess.Popen
        def launch(*args, **kwargs):
            identity['source_sha'] = 'b' * 40
            identity['run']['GITHUB_RUN_ATTEMPT'] = '3'
            return real_popen(*args, **kwargs)
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(m.subprocess, 'Popen', side_effect=launch):
            packet = Path(directory) / 'packet'
            self.assertEqual(m.supervise([sys.executable, '-c', 'pass'], packet,
                'controlled-performance', 900, grace=0.1, identity=identity), 0)
            receipt = json.loads((packet / 'ci-budget.json').read_text())
            self.assertEqual(receipt['source_sha'], 'a' * 40)
            self.assertEqual(receipt['run']['GITHUB_RUN_ATTEMPT'], '2')

    def test_controlled_profile_cannot_bypass_explicit_custody_mode(self):
        for profile, mode in [('controlled-performance', '0'), ('controlled-performance', ''),
                              ('hosted-semantic', '1'), ('hosted-semantic', 'invalid')]:
            with self.subTest(profile=profile, mode=mode), mock.patch.dict(os.environ,
                    M0_QUALIFICATION_PROFILE=profile, M0_CONTROLLED=mode), \
                    mock.patch.object(m.subprocess, 'check_output') as command:
                with self.assertRaisesRegex(ValueError, 'custody mode disagree'):
                    m.main()
                command.assert_not_called()

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

    def test_hosted_interrupt_fixture_is_isolated_from_controlled_parent(self):
        with mock.patch.dict(os.environ, M0_CONTROLLED='1',
                             M0_QUALIFICATION_PROFILE='controlled-performance'):
            self.test_external_interrupt_kills_owned_descendants_and_records_failure()

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
                       RUN_DIR=str(packet), M0_QUALIFICATION_PROFILE='hosted-semantic', M0_CONTROLLED='0')
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

    def test_repeated_real_signals_at_cleanup_entry_cannot_escape_ownership(self):
        # Trace the first executable finally line, before cleanup has started.
        # A raising handler used to escape here on the second SIGTERM.
        source = SCRIPT.read_text().splitlines()
        boundary = next(i + 1 for i, line in enumerate(source)
                        if line == '        if child is not None:')
        real_spawn = subprocess.Popen
        owned = []
        def spawn(*args, **kwargs):
            child = real_spawn(*args, **kwargs)
            owned.append(child)
            os.kill(os.getpid(), signal.SIGTERM)
            return child
        reached = []
        def trace(frame, event, arg):
            if (event == 'line' and frame.f_code.co_filename == str(SCRIPT)
                    and frame.f_lineno == boundary):
                reached.append(True)
                for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGTERM):
                    os.kill(os.getpid(), sig)
            return trace
        with tempfile.TemporaryDirectory() as directory:
            packet = Path(directory) / 'packet'
            try:
                sys.settrace(trace)
                with mock.patch.object(m.subprocess, 'Popen', side_effect=spawn):
                    status = m.supervise([sys.executable, '-c', 'import time; time.sleep(60)'],
                                         packet, 'hosted-semantic', 5, grace=0.1)
                self.assertIsNotNone(owned[0].returncode)
                with self.assertRaises(ProcessLookupError):
                    os.killpg(owned[0].pid, 0)
            finally:
                sys.settrace(None)
                for child in owned:
                    if child.returncode is None:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait()
            self.assertEqual(reached, [True])
            self.assertEqual(status, 143)
            self.assertIsNotNone(owned[0].returncode)
            receipt = json.loads((packet / 'ci-budget.json').read_text())
            self.assertEqual(receipt['termination_reason'], 'interrupted')
            self.assertEqual(receipt['interrupted_signal'], signal.SIGTERM)
            self.assertFalse(receipt['completed'])

    def test_real_signal_receipt_publication_boundary(self):
        signals = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
        phases = ('pending-write', 'before-boundary', 'after-boundary',
                  'before-replace', 'after-replace')
        for sig in signals:
            for phase in phases:
                with self.subTest(signal=sig, phase=phase), tempfile.TemporaryDirectory() as directory:
                    packet = Path(directory) / 'packet'
                    real_write, real_replace = Path.write_text, Path.replace
                    real_mask, real_spawn = signal.pthread_sigmask, subprocess.Popen
                    writes, children = [], []
                    def send():
                        # Multiple real deliveries must remain bounded. The first
                        # observed signal wins when cancellation is accepted.
                        for _ in range(3):
                            os.kill(os.getpid(), sig)
                    def write(path, *args, **kwargs):
                        if path.name == 'ci-budget.json.pending':
                            writes.append(True)
                            if phase == 'pending-write':
                                send()
                        return real_write(path, *args, **kwargs)
                    def mask(how, values):
                        if how == signal.SIG_BLOCK and phase == 'before-boundary':
                            send()
                        result = real_mask(how, values)
                        if how == signal.SIG_BLOCK and phase == 'after-boundary':
                            send()
                        return result
                    def replace(path, target):
                        if phase == 'before-replace':
                            send()
                        result = real_replace(path, target)
                        if phase == 'after-replace':
                            send()
                        return result
                    def spawn(*args, **kwargs):
                        child = real_spawn(*args, **kwargs)
                        children.append(child)
                        return child
                    started = time.monotonic()
                    with mock.patch.object(Path, 'write_text', write), \
                         mock.patch.object(Path, 'replace', replace), \
                         mock.patch.object(m.signal, 'pthread_sigmask', mask), \
                         mock.patch.object(m.subprocess, 'Popen', side_effect=spawn):
                        status = m.supervise([sys.executable, '-c', 'pass'], packet,
                                             'hosted-semantic', 5, grace=0.1)
                    accepted = phase in ('pending-write', 'before-boundary')
                    self.assertLess(time.monotonic() - started, 5)
                    self.assertEqual(len(writes), 2 if accepted else 1)
                    self.assertEqual(status, 128 + sig if accepted else 0)
                    receipt = json.loads((packet / 'ci-budget.json').read_text())
                    self.assertEqual(receipt['completed'], not accepted)
                    self.assertEqual(receipt['exit_status'], status)
                    self.assertEqual(receipt['interrupted_signal'], sig if accepted else None)
                    self.assertEqual(receipt['termination_reason'],
                                     'interrupted' if accepted else 'capture-exit')
                    self.assertIsNotNone(children[0].returncode)
                    with self.assertRaises(ProcessLookupError):
                        os.killpg(children[0].pid, 0)

    def test_spawn_failure_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            packet = Path(directory) / 'packet'
            self.assertEqual(m.supervise(['/nonexistent/m0-capture'], packet,
                                         'hosted-semantic', 5, grace=0.1), 127)
            receipt = json.loads((packet / 'ci-budget.json').read_text())
            self.assertEqual(receipt['termination_reason'], 'launch-failure')
            self.assertFalse(receipt['completed'])

    def test_enospc_receipt_cannot_return_success(self):
        import errno
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.object(Path, 'write_text', side_effect=OSError(errno.ENOSPC, 'disk full')):
            self.assertEqual(m.supervise([sys.executable, '-c', 'pass'], Path(directory) / 'packet',
                                         'hosted-semantic', 5, grace=0.1), 125)

    def test_inherited_pipe_descendant_is_drained(self):
        with tempfile.TemporaryDirectory() as directory:
            packet = Path(directory) / 'packet'
            code = ("import importlib.util; "
                    f"s=importlib.util.spec_from_file_location('m',{str(SCRIPT)!r}); "
                    "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
                    "import sys; "
                    f"sys.exit(m.supervise([sys.executable,'-c',"
                    "'import subprocess,sys; subprocess.Popen([sys.executable, \"-c\", \"import time; time.sleep(60)\"])'],"
                    f"{str(packet)!r},'hosted-semantic',5,grace=0.1))")
            run = subprocess.run([sys.executable, '-c', code], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, timeout=5)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertTrue(json.loads((packet / 'ci-budget.json').read_text())['completed'])

    def test_workflow_does_not_infer_controlled_custody(self):
        workflow = SCRIPT.parent.parent / '.github/workflows/treedb-vacuum-m0.yml'
        text = workflow.read_text()
        self.assertIn("M0_QUALIFICATION_PROFILE: ${{ env.M0_CONTROLLED == '1' && 'controlled-performance' || 'hosted-semantic' }}", text)
        self.assertIn("vars.TREEDB_M0_CONTROLLED_ADMISSION_SHA == github.event.pull_request.head.sha", text)
        self.assertNotIn("runner.environment == 'self-hosted'", text)


if __name__ == '__main__':
    unittest.main()
