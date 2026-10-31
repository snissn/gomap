"""Bounded synthetic safety controls; retained results are authoritative.

Run only in a coordinator-assigned lane. No database workloads.
"""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import select
import signal
import shlex
import sys
import tempfile
import time
import unittest
from unittest import mock

GUARD = Path(__file__).with_name('ycsb_campaign_guard.py')
SCRIPTS = Path(os.environ.get('GUARD_TEST_SCRIPTS_DIR') or Path(__file__).resolve().parent)
OUTPUT_ROOT = Path(os.environ.get('GUARD_TEST_OUTPUT_ROOT') or
                   os.environ.get('TMPDIR') or tempfile.gettempdir())
PIDFD_PLATFORM = sys.platform.startswith('linux') and all((
    hasattr(os, 'pidfd_open'), hasattr(signal, 'pidfd_send_signal'),
    hasattr(os, 'waitid'), hasattr(os, 'P_PIDFD')))
spec = importlib.util.spec_from_file_location('guard_under_test', GUARD)
g = importlib.util.module_from_spec(spec)
spec.loader.exec_module(g)


def fake_row(pid, tick, parent, rss=100):
    return dict(pid=pid, start_ticks=tick, ppid=parent, pgrp=pid, session=pid, state='S', rss_bytes=rss)


def register_fixture(registry, identity):
    fd = os.pidfd_open(identity['pid'])
    try:
        if g.stat(identity['pid'])['start_ticks'] != identity['start_ticks'] or g.exited(fd):
            raise AssertionError('fixture identity/liveness mismatch')
    except BaseException:
        os.close(fd)  # No signaling authority is obtained on verification failure.
        raise
    registry.append(fd)


def stop_test_owned(fd, process=None):
    if fd is None:
        return
    for signum in (signal.SIGTERM, signal.SIGKILL):
        if not g.exited(fd):
            try:
                signal.pidfd_send_signal(fd, signum)
            except ProcessLookupError:
                if not g.exited(fd):
                    raise
        if select.select([fd], [], [], 2)[0]:
            if process is not None:
                process.wait(timeout=2)
            return
    raise AssertionError('exact test-owned process survived bounded cleanup')


@unittest.skipUnless(PIDFD_PLATFORM, 'Linux pidfd and P_PIDFD waitid support required')
class IdentityRaces(unittest.TestCase):
    def test_unverified_fixture_never_registered_or_signaled(self):
        registry=[]
        with mock.patch.object(os,'pidfd_open',return_value=330), mock.patch.object(g,'stat',return_value={'start_ticks':999}), mock.patch.object(os,'close') as close, mock.patch.object(signal,'pidfd_send_signal') as send:
            with self.assertRaises(AssertionError):register_fixture(registry,{'pid':33,'start_ticks':332})
            for fd in registry:stop_test_owned(fd)
        self.assertEqual(registry,[]);close.assert_called_once_with(330);send.assert_not_called()

    def test_fixture_fd_a_proc_b_liveness_failure_never_signaled(self):
        registry=[]
        with mock.patch.object(os,'pidfd_open',return_value=330), mock.patch.object(g,'stat',return_value={'start_ticks':332}), mock.patch.object(g,'exited',return_value=True), mock.patch.object(os,'close') as close, mock.patch.object(signal,'pidfd_send_signal') as send:
            with self.assertRaises(AssertionError):register_fixture(registry,{'pid':33,'start_ticks':332})
        self.assertEqual(registry,[]);close.assert_called_once_with(330);send.assert_not_called()
    def test_fd_a_proc_b_rejected_then_b_repin_accounts_rss(self):
        owned = g.Owned()
        parent, replacement = fake_row(22, 220, owned.guard_pid), fake_row(33, 332, 22, 4096)
        owned.rows[22] = dict(parent, fd=220)
        with mock.patch.object(g.os, 'pidfd_open', side_effect=[330, 331]), \
             mock.patch.object(g, 'stat', side_effect=lambda pid: {22: parent, 33: replacement}[pid]), \
             mock.patch.object(g, 'exited', side_effect=lambda fd: fd == 330), \
             mock.patch.object(g.os, 'close') as close:
            with self.assertRaisesRegex(RuntimeError, 'child pidfd exited'):
                owned.pin(33, 22)
            close.assert_called_once_with(330)
            self.assertNotIn(33, owned.rows)
            owned.pin(33, 22)
            self.assertEqual(owned.rows[33]['fd'], 331)
            entries = [type('Entry', (), {'name': str(pid)})() for pid in (22, 33)]
            with mock.patch.object(g.Path, 'iterdir', return_value=entries):
                rss, live = owned.sample()
            self.assertEqual(rss, 4196)
            self.assertIn(33, [item['pid'] for item in live])

    def test_direct_unreaped_quick_exit_zombie_is_supported(self):
        owned = g.Owned()
        zombie = dict(fake_row(33, 332, owned.guard_pid), state='Z', rss_bytes=0)
        with mock.patch.object(g.os, 'pidfd_open', return_value=330), \
             mock.patch.object(g, 'stat', return_value=zombie), \
             mock.patch.object(g, 'exited', return_value=True):
            owned.pin(33, owned.guard_pid)
        self.assertEqual(owned.rows[33]['start_ticks'], 332)

    def test_reaping_old_a_never_numeric_reaps_replacement_b(self):
        owned = g.Owned()
        owned.rows[33] = dict(fake_row(33, 331, 22), fd=330)
        with mock.patch.object(g, 'exited', return_value=True), \
             mock.patch.object(g.os, 'waitpid', side_effect=AssertionError('numeric wait forbidden')), \
             mock.patch.object(g.os, 'waitid', side_effect=ChildProcessError) as wait, \
             mock.patch.object(g.os, 'close') as close:
            owned.reap_adopted(None)
        wait.assert_called_once_with(os.P_PIDFD, 330, os.WEXITED | os.WNOHANG)
        close.assert_called_once_with(330)
        self.assertNotIn(33,owned.rows);self.assertNotIn('fd',owned.retired[0])

    def test_exact_adopted_zombie_reaps_by_pidfd(self):
        owned = g.Owned()
        owned.rows[33] = dict(fake_row(33, 331, owned.guard_pid), fd=330)
        with mock.patch.object(g, 'exited', return_value=True), \
             mock.patch.object(g.os, 'waitid', return_value=object()) as wait, \
             mock.patch.object(g.os, 'close') as close:
            owned.reap_adopted(None)
        wait.assert_called_once_with(os.P_PIDFD, 330, os.WEXITED | os.WNOHANG)
        close.assert_called_once_with(330)
        self.assertNotIn(33,owned.rows)

    def test_retirement_preserves_root_live_and_not_yet_waitable_pins(self):
        owned=g.Owned();owned.root_pid=33
        for pid in (33,34,35):owned.rows[pid]=dict(fake_row(pid,pid*10,owned.guard_pid),fd=pid*10)
        with mock.patch.object(g,'exited',side_effect=lambda fd:fd!=340), \
             mock.patch.object(g.os,'waitid',return_value=None) as wait, \
             mock.patch.object(g.os,'close') as close:
            owned.reap_adopted(None)
        wait.assert_called_once_with(os.P_PIDFD,350,os.WEXITED|os.WNOHANG)
        close.assert_not_called();self.assertEqual(set(owned.rows),{33,34,35})

    def test_kernel_without_pidfd_waitid_fails_closed(self):
        with mock.patch.object(g.os, 'pidfd_open', return_value=330), \
             mock.patch.object(g.os, 'waitid', side_effect=OSError(22, 'unsupported')), \
             mock.patch.object(g.os, 'close') as close:
            with self.assertRaises(OSError):
                g.check_pidfd_waitid()
        close.assert_called_once_with(330)

    def test_closed_high_descriptor_fails_closed(self):
        import fcntl
        fd = os.pidfd_open(os.getpid())
        try:
            high = fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 2048)
            os.close(high)
            with self.assertRaisesRegex(RuntimeError, 'invalid pidfd'):
                g.exited(high)
        finally:
            os.close(fd)


FIXTURE = """
import json,os,subprocess,sys,time
from pathlib import Path
children=[subprocess.Popen([sys.executable,'-c','import time; time.sleep(12)'],start_new_session=True) for _ in range(2)]
identities=[]
for p in children:
 f=Path('/proc/%d/stat'%p.pid).read_text().rsplit(')',1)[1].split()
 identities.append({'pid':p.pid,'start_ticks':int(f[19])})
Path('fixture.tmp').write_text(json.dumps(identities));os.replace('fixture.tmp','fixture.json')
while not Path('release').exists():time.sleep(.01)
ACTION
"""

# Include the actual root in test-owner authority for failure paths where the
# broken guard can itself exit before cleaning its still-running root.
ROOT_FIXTURE = FIXTURE.replace('identities=[]', "f=Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()\nidentities=[{'pid':os.getpid(),'start_ticks':int(f[19])}]")


@unittest.skipUnless(PIDFD_PLATFORM, 'Linux pidfd and P_PIDFD waitid support required')
class InjectedSafety(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            g.check_pidfd_waitid()
        except OSError as error:
            if error.errno not in (g.errno.ENOSYS, g.errno.EINVAL, g.errno.ENODEV,
                                   g.errno.EOPNOTSUPP):
                raise
            raise unittest.SkipTest(f'kernel pidfd/waitid support unavailable: {error}')

    def invoke(self, source, inject='', *, validation=False, fixture=False, flags=(), shell=False, missing_result=False, driver_timeout=20):
        root = Path(tempfile.mkdtemp(dir=OUTPUT_ROOT, prefix=self._testMethodName+'-'))
        unrelated = subprocess.Popen([sys.executable, '-c', 'import time;time.sleep(60)'])
        unrelated_fd = os.pidfd_open(unrelated.pid)
        tick = g.stat(unrelated.pid)['start_ticks']; fixture_fds = [];proc=None;supervisor_fd=None;cleanup_errors=[]
        try:
            wrapper = "import importlib.util,sys\ns=importlib.util.spec_from_file_location('guard',sys.argv[1]);g=importlib.util.module_from_spec(s);s.loader.exec_module(g)\nsys.argv=sys.argv[1:]\n" + inject + "\nraise SystemExit(g.main())"
            argv = [sys.executable, '-B', '-c', wrapper, str(GUARD), '--scripts-dir', str(SCRIPTS), '--out', str(root/'guard'), '--cwd', str(root), '--filesystem', str(root), '--free-floor-bytes', '1', '--deadline', '8', '--cleanup-seconds', '.3', *flags]
            if validation: argv += ['--validation-json', str(root/'validation.json')]
            argv += ['--', '/bin/bash' if shell else sys.executable, '-c', source]
            (root/'command.json').write_text(json.dumps(argv,indent=2)+'\n')
            env = os.environ.copy();env.pop('GOMAXPROCS', None)
            with (root/'driver.stdout').open('w') as stdout, (root/'driver.stderr').open('w') as stderr:
                proc = subprocess.Popen(argv, cwd=root, env=env, stdout=stdout, stderr=stderr)
                supervisor_fd=os.pidfd_open(proc.pid)  # Exact direct unreaped child.
                if fixture:
                    deadline = time.monotonic()+5
                    while not (root/'fixture.json').exists() and proc.poll() is None and time.monotonic()<deadline:time.sleep(.01)
                    for identity in json.loads((root/'fixture.json').read_text()):
                        register_fixture(fixture_fds,identity)
                    (root/'release').touch()
                code=proc.wait(timeout=driver_timeout)
            result_path=root/'guard/result.json'
            result=json.loads(result_path.read_text()) if result_path.exists() else {'status':'missing-result','cleanup_signals':[]}
            if not missing_result:self.assertTrue(result_path.exists())
            self.assertFalse(g.exited(unrelated_fd));self.assertEqual(g.stat(unrelated.pid)['start_ticks'],tick)
            self.assertNotIn(unrelated.pid,[item['pid'] for item in result['cleanup_signals']])
            for fd in fixture_fds:self.assertTrue(select.select([fd],[],[],1)[0], 'owned fixture survived cleanup')
            if 'root_identity' in result:
                identity=json.loads((root/'guard/root.identity.json').read_text())['process']
                self.assertEqual(identity,result['root_identity'])
                for item in result['cleanup_signals']:
                    if item.get('role')=='original-shell':self.assertEqual((item['pid'],item['start_ticks']),(identity['pid'],identity['start_ticks']))
            return code,result,root
        finally:
            original_failure=repr(sys.exc_info()[1]) if sys.exc_info()[1] else None
            for name,fd,process in [('supervisor',supervisor_fd,proc), *[('verified-fixture',fd,None) for fd in fixture_fds], ('unrelated-test-owned',unrelated_fd,unrelated)]:
                if fd is None:continue
                try:stop_test_owned(fd,process)
                except BaseException as error:cleanup_errors.append(f'{name}: {error}')
                finally:os.close(fd)
            (root/'test-owner-cleanup.json').write_text(json.dumps({'original_failure':original_failure,'cleanup_errors':cleanup_errors},indent=2)+'\n')
            if cleanup_errors and original_failure is None:self.fail(str(cleanup_errors))

    def test_quick_zero_exit_has_durable_identity(self):
        code,result,_=self.invoke('pass')
        self.assertEqual(code,0);self.assertGreater(result['root_identity']['start_ticks'],0)
        self.assertFalse(result['whole_interval_verified'])

    def test_real_high_descriptor_pidfd_readiness(self):
        import fcntl
        root = Path(tempfile.mkdtemp(dir=OUTPUT_ROOT, prefix=self._testMethodName+'-'))
        proc = subprocess.Popen(['/bin/cat'], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL)
        fd = os.pidfd_open(proc.pid)
        high = None
        try:
            high = fcntl.fcntl(fd, fcntl.F_DUPFD_CLOEXEC, 2048)
            (root/'descriptor.json').write_text(json.dumps({'pid':proc.pid,'fd':high,'start_ticks':g.stat(proc.pid)['start_ticks']})+'\n')
            self.assertGreaterEqual(high,2048)
            self.assertFalse(g.exited(high))
            proc.stdin.close()
            proc.wait(timeout=2)
            self.assertTrue(g.exited(high))
        finally:
            stop_test_owned(fd,proc)
            if proc.stdin and not proc.stdin.closed:proc.stdin.close()
            if high is not None:os.close(high)
            os.close(fd)

    def test_high_descriptor_root_wait_and_owned_cleanup(self):
        # The second cell replaces only readiness to expose the independent
        # root wait call on the original guard, which also used select().
        duplicate="import fcntl,time\nreal_open=g.os.pidfd_open\ndef opened(pid,*a):\n fd=real_open(pid,*a)\n try:return fcntl.fcntl(fd,fcntl.F_DUPFD_CLOEXEC,2048)\n finally:g.os.close(fd)\ng.os.pidfd_open=opened\nreal_pin=g.Owned.pin\ndef pin(self,pid,parent):\n value=real_pin(self,pid,parent)\n if parent==self.guard_pid:\n  deadline=time.monotonic()+4\n  while not g.Path('fixture.json').exists() and time.monotonic()<deadline:time.sleep(.001)\n return value\ng.Owned.pin=pin\n"
        ready="def ready(fd,timeout=0):\n p=g.select.poll();p.register(fd,g.select.POLLIN);events=p.poll(int(timeout*1000))\n if any(mask & g.select.POLLNVAL for _,mask in events):raise RuntimeError('invalid pidfd')\n return bool(events)\ng.exited=ready\n"
        for override in ('',ready):
            with self.subTest(root_wait_override=bool(override)):
                code,result,_=self.invoke(ROOT_FIXTURE.replace('ACTION','time.sleep(.3);raise SystemExit(0)'),duplicate+override,fixture=True)
                self.assertNotEqual(code,0)  # Deliberate live setsid children.
                self.assertTrue(any('child exited with live owned descendants' in e for e in result['errors']))
                self.assertFalse(any('out of range' in e or 'invalid pidfd' in e for e in result['errors']))
                self.assertTrue(result['cleanup_signals'])

    def test_sequential_churn_retires_fds_preserves_root_and_cleans_orphans(self):
        # Passive pin acknowledgements ensure all 1050 actual children are
        # observed during ordinary sampled iterations, rather than missed peaks.
        inject="real_pin=g.Owned.pin\nroot_identity=None\ndef pin(self,pid,parent):\n global root_identity\n value=real_pin(self,pid,parent)\n if parent==self.guard_pid and root_identity is None:root_identity=(pid,self.rows[pid]['fd'],self.rows[pid]['start_ticks'])\n elif parent==root_identity[0]:g.Path('pin-%d'%pid).touch()\n return value\ng.Owned.pin=pin\nreal_sample=g.Owned.sample\nmax_rows=0\nmax_fd=0\ndef sample(self):\n global max_rows,max_fd\n value=real_sample(self)\n if root_identity is not None:\n  pid,fd,tick=root_identity\n  assert self.rows[pid]['fd']==fd and self.rows[pid]['start_ticks']==tick,'root pin retired or changed'\n max_rows=max(max_rows,len(self.rows));max_fd=max(max_fd,max((r['fd'] for r in self.rows.values()),default=0))\n g.Path('churn.guard.json').write_text(__import__('json').dumps({'max_rows':max_rows,'max_fd':max_fd,'retired_count':len(self.retired),'root_identity':root_identity}))\n return value\ng.Owned.sample=sample\n"
        action="""for i in range(1050):
 p=subprocess.Popen(['/bin/cat'],stdin=subprocess.PIPE,stdout=subprocess.DEVNULL)
 try:
  deadline=time.monotonic()+2
  while not Path('pin-%d'%p.pid).exists() and time.monotonic()<deadline:time.sleep(.001)
  if not Path('pin-%d'%p.pid).exists():raise RuntimeError('guard failed to pin sequential child')
 finally:
  p.stdin.close();p.wait(timeout=2)
Path('churn.complete.json').write_text(json.dumps({'children':1050}))
raise SystemExit(0)"""
        code,result,root=self.invoke(ROOT_FIXTURE.replace('time.sleep(12)','time.sleep(150)').replace('ACTION',action),inject,fixture=True,flags=('--interval','.001','--deadline','120'),driver_timeout=140)
        self.assertNotEqual(code,0)  # Original root deliberately leaves setsid orphans.
        self.assertEqual(json.loads((root/'churn.complete.json').read_text())['children'],1050)
        audit=json.loads((root/'churn.guard.json').read_text())
        self.assertLessEqual(audit['max_rows'],8);self.assertLess(audit['max_fd'],1024)
        retired=result['retired_processes']
        self.assertGreaterEqual(len(retired),1050)
        self.assertTrue(all('fd' not in row for row in retired))
        self.assertNotIn(result['root_identity']['pid'],[row['pid'] for row in retired])
        self.assertTrue(any('child exited with live owned descendants' in e for e in result['errors']))
        self.assertFalse(any('out of range' in e or 'invalid pidfd' in e for e in result['errors']))

    def test_nonzero_and_deadline_cannot_be_partial_acceptance(self):
        for source,flags in [('raise SystemExit(7)',()),('import time;time.sleep(12)',('--deadline','.1'))]:
            code,result,_=self.invoke(source,flags=flags)
            self.assertNotEqual(code,0);self.assertEqual(result['status'],'failed')

    def test_existing_namespace_refuses_before_launch(self):
        code,result,root=self.invoke("from pathlib import Path;Path('launched').touch()", "g.Path('guard').mkdir()",missing_result=True)
        self.assertNotEqual(code,0);self.assertFalse((root/'launched').exists())

    def test_test_owner_timeout_cleans_exact_supervisor_and_fixtures(self):
        with self.assertRaises(subprocess.TimeoutExpired):
            self.invoke(FIXTURE.replace('ACTION','time.sleep(12)'),fixture=True,driver_timeout=.05)
        roots=sorted(OUTPUT_ROOT.glob(self._testMethodName+'-*'))
        receipt=json.loads((roots[-1]/'test-owner-cleanup.json').read_text())
        self.assertEqual(receipt['cleanup_errors'],[]);self.assertIn('TimeoutExpired',receipt['original_failure'])

    def test_aggregate_rss_multiple_individually_fitting_children(self):
        source=FIXTURE.replace('import time; time.sleep(12)','import time; data=bytearray(24*1024*1024); time.sleep(12)').replace('ACTION','time.sleep(12)')
        cap=50*1024**2
        code,result,root=self.invoke(source,fixture=True,flags=('--rss-cap-bytes',str(cap)))
        self.assertNotEqual(code,0);self.assertGreater(result['max_rss_bytes'],cap)
        samples=[json.loads(line) for line in (root/'guard/samples.jsonl').read_text().splitlines()]
        self.assertTrue(any(s['rss_bytes']>cap and len(s['live'])>=3 and all(row['rss_bytes']<cap for row in s['live']) for s in samples))

    def test_initial_free_floor_prevents_launch(self):
        inject="from types import SimpleNamespace\ng.os.statvfs=lambda _:SimpleNamespace(f_bavail=0,f_frsize=1)"
        code,result,root=self.invoke("from pathlib import Path;Path('launched').touch()",inject)
        self.assertNotEqual(code,0);self.assertNotIn('root_identity',result);self.assertFalse((root/'launched').exists())
        self.assertTrue(any('free floor' in e for e in result['errors']))

    def test_midrun_free_floor_and_resource_io_clean_owned(self):
        for action in ("return SimpleNamespace(f_bavail=0,f_frsize=1)","raise OSError('injected resource IO')"):
            inject="from types import SimpleNamespace\nreal=g.os.statvfs\ndef fs(path):\n if g.Path('release').exists():"+action+"\n return real(path)\ng.os.statvfs=fs"
            code,result,_=self.invoke(FIXTURE.replace('ACTION','time.sleep(12)'),inject,fixture=True)
            self.assertNotEqual(code,0);self.assertEqual(result['status'],'failed');self.assertTrue(result['cleanup_signals'])

    def test_root_pin_failure_cleans_direct_child_and_adoptees(self):
        inject="import time\nreal=g.Owned.pin\nfailed=False\ndef pin(self,pid,parent):\n global failed\n if not failed and parent==self.guard_pid:\n  failed=True\n  while not g.Path('release').exists():time.sleep(.01)\n  raise OSError('injected root pin failure')\n return real(self,pid,parent)\ng.Owned.pin=pin"
        code,result,_=self.invoke(FIXTURE.replace('ACTION','time.sleep(12)'),inject,fixture=True)
        self.assertNotEqual(code,0);self.assertTrue(any('root pin failure' in e for e in result['errors']))

    def test_cancellation_in_cleanup_shell_descendant_and_kill_stages(self):
        source=FIXTURE.replace('import time; time.sleep(12)','import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(12)').replace('ACTION','time.sleep(12)')
        for stage in ('shell','descendant','kill'):
            inject="import os,signal\nreal=g.Owned.send\nsent=False\ndef send(self,pid,sig):\n global sent\n value=real(self,pid,sig)\n root=next(iter(self.rows))\n stage="+repr(stage)+"\n if value and not sent and ((stage=='shell' and pid==root) or (stage=='descendant' and pid!=root and sig==signal.SIGTERM) or (stage=='kill' and sig==signal.SIGKILL)):\n  sent=True;os.kill(os.getpid(),signal.SIGTERM)\n return value\ng.Owned.send=send"
            code,result,_=self.invoke(source,inject,fixture=True,flags=('--deadline','.7'))
            self.assertNotEqual(code,0);self.assertEqual(result['status'],'failed')
            self.assertIn(signal.SIGTERM,result['pending_cleanup_signals'])

    def test_final_receipt_io_and_finalization_cancellation_veto_exit(self):
        base="import os,signal,sys\nsys.path.insert(0,"+repr(str(SCRIPTS))+ ")\nh=g.importlib.import_module('ycsb_compare_validate')\nreal=h.write_new\ndef write(path,value):\n if path.name=='result.json':\n  ACTION\n return real(path,value)\nh.write_new=write"
        code,result,_=self.invoke('pass',base.replace('ACTION',"raise OSError('injected final receipt IO')"),missing_result=True)
        self.assertNotEqual(code,0);self.assertEqual(result['status'],'missing-result')
        inject=base.replace('ACTION',"real(path,value);os.kill(os.getpid(),signal.SIGTERM)")
        code,result,_=self.invoke('pass',inject)
        self.assertNotEqual(code,0)
        # A completed-looking receipt never overrides failed supervisor exit.
        self.assertEqual(result['status'],'guard-completed-pending-independent-artifact-review')

    def test_shell_foreground_trap_order_and_failed_original_cleanup(self):
        foreground="import os,json,time;from pathlib import Path;f=Path('/proc/self/stat').read_text().rsplit(')',1)[1].split();Path('fixture.tmp').write_text(json.dumps([{'pid':os.getpid(),'start_ticks':int(f[19])}]));os.replace('fixture.tmp','fixture.json')\nwhile not Path('release').exists():time.sleep(.01)\ntime.sleep(.2)"
        source="trap \"printf '%s\\n' cleanup >> events\" EXIT\ntrap \"printf '%s\\n' term >> events; exit 0\" TERM\n"+shlex.quote(sys.executable)+' -c '+shlex.quote(foreground)+"\nprintf '%s\\n' operations-passed >> events\n"
        inject="from types import SimpleNamespace\nreal=g.os.statvfs\ndef fs(path):\n if g.Path('release').exists():return SimpleNamespace(f_bavail=0,f_frsize=1)\n return real(path)\ng.os.statvfs=fs"
        code,result,root=self.invoke(source,inject,shell=True,fixture=True,flags=('--cleanup-seconds','1'))
        self.assertNotEqual(code,0);self.assertEqual(result['status'],'failed')
        self.assertEqual((root/'events').read_text(),'term\ncleanup\n')
        self.assertEqual(result['cleanup_signals'][0]['role'],'original-shell')
        code,result,root=self.invoke("trap 'printf cleanup-failed >> events; exit 9' EXIT\nprintf operations-passed >> events\nexit 0",shell=True)
        self.assertNotEqual(code,0);self.assertEqual(result['exit_code'],9)
        self.assertEqual((root/'events').read_text(),'operations-passedcleanup-failed')

    def test_missing_malformed_duplicate_and_invalid_validation(self):
        for contents in (None,'{','{"status":"invalid","errors":[]}','{"status":"validated-consumer-evidence","errors":["x"]}','{"status":"invalid","status":"validated-consumer-evidence","errors":[]}'):
            source='pass' if contents is None else "from pathlib import Path;Path('validation.json').write_text("+repr(contents)+")"
            code,result,_=self.invoke(source,validation=True)
            self.assertNotEqual(code,0);self.assertEqual(result['status'],'failed')

    def test_valid_validation(self):
        code,_,_=self.invoke("from pathlib import Path;Path('validation.json').write_text('{\"status\":\"validated-consumer-evidence\",\"errors\":[]}')",validation=True)
        self.assertEqual(code,0)

    def test_live_setsid_owned_cleanup_and_unrelated_survival(self):
        code,result,_=self.invoke(FIXTURE.replace('ACTION','raise SystemExit(0)'),fixture=True)
        self.assertNotEqual(code,0);self.assertEqual(result['status'],'failed')

    def test_rapid_reap_and_adopted_children(self):
        action="for _ in range(100):\n p=os.fork()\n if p==0:os._exit(0)\n os.waitpid(p,0)\nraise SystemExit(0)"
        code,_,_=self.invoke(FIXTURE.replace('ACTION',action),fixture=True);self.assertNotEqual(code,0)

    def test_cancellation_during_popen_retains_exact_root(self):
        inject="import os,signal\nreal=g.subprocess.Popen\ndef spawn(*a,**k):\n p=real(*a,**k);os.kill(os.getpid(),signal.SIGTERM);return p\ng.subprocess.Popen=spawn\n"
        code,result,_=self.invoke('import time;time.sleep(12)',inject)
        self.assertNotEqual(code,0);self.assertIn('root_identity',result);self.assertTrue(result['cleanup_signals'])

    def test_discovery_failure_cleans_known_owned(self):
        inject="real=g.Owned.sample\ndef broken(self):\n value=real(self)\n if g.Path('release').exists() and len(self.rows)>1:raise OSError('injected discovery failure')\n return value\ng.Owned.sample=broken\n"
        code,result,_=self.invoke(FIXTURE.replace('ACTION','time.sleep(12)'),inject,fixture=True)
        self.assertNotEqual(code,0);self.assertTrue(any('discovery failure' in e for e in result['errors']))

    def test_sample_log_failure_cleans_known_owned(self):
        inject="real=g.Path.open\nclass Log:\n def __init__(self,f):self.f=f\n def write(self,data):\n  if g.Path('release').exists():raise OSError('injected sample log failure')\n  return self.f.write(data)\n def close(self):return self.f.close()\ndef opened(path,*a,**k):\n f=real(path,*a,**k);return Log(f) if path.name=='samples.jsonl' else f\ng.Path.open=opened\n"
        code,result,_=self.invoke(FIXTURE.replace('ACTION','time.sleep(12)'),inject,fixture=True)
        self.assertNotEqual(code,0);self.assertTrue(any('sample log failure' in e for e in result['errors']))

    def test_fork_during_cleanup(self):
        source="import signal,subprocess,sys,time,os,json\nfrom pathlib import Path\ndef term(*_):\n p=subprocess.Popen([sys.executable,'-c','import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(12)'],start_new_session=True)\n f=Path('/proc/%d/stat'%p.pid).read_text().rsplit(')',1)[1].split()\n Path('fixture.tmp').write_text(json.dumps([{'pid':p.pid,'start_ticks':int(f[19])}]));os.replace('fixture.tmp','fixture.json')\n while not Path('release').exists():time.sleep(.01)\n raise SystemExit(0)\nsignal.signal(signal.SIGTERM,term)\nwhile True:time.sleep(.01)"
        code,result,_=self.invoke(source,fixture=True,flags=('--deadline','.5','--cleanup-seconds','1'))
        self.assertNotEqual(code,0);self.assertEqual(result['status'],'failed')


if __name__ == '__main__':
    unittest.main()
