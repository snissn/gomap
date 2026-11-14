"""Offline controls for diagnostic source binding and evidence boundaries."""
import fcntl
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import idle_checkpoint_diagnostic as diagnostic


class IdleCheckpointDiagnosticControls(unittest.TestCase):
    def test_source_anchor_changes_fail_closed(self):
        self.assertEqual(diagnostic.replace_once('abc', 'b', 'X'), 'aXc')
        for source in ('ac', 'abbc'):
            with self.assertRaisesRegex(ValueError, 'anchor changed'):
                diagnostic.replace_once(source, 'b', 'X')

    def test_process_allocations_use_total_operations_and_actual_writes(self):
        keys = ('idle5415.total_alloc', 'idle5415.mallocs',
                'ycsb.gateway.write_concern.LogicalWrites',
                'ycsb.gateway.write_concern.VisibleAcknowledgements',
                'treedb.cache.cow.txn.logical_seq', 'treedb.cache.auto_checkpoint.count')
        before = dict.fromkeys(keys, '100')
        after = dict(zip(keys, ('2100', '500', '105', '105', '105', '101')))
        result = diagnostic.allocation_delta(before, after, 100, 5)
        self.assertEqual(result['process_bytes_per_operation'], 20)
        self.assertEqual(result['process_objects_per_operation'], 4)
        self.assertIn('not update-only', result['basis'])
        with self.assertRaisesRegex(ValueError, 'actual logical writes'):
            diagnostic.allocation_delta(before, after, 100, 6)
        after['idle5415.mallocs'] = '99'
        with self.assertRaisesRegex(ValueError, 'counter decreased'):
            diagnostic.allocation_delta(before, after, 100, 5)

    def test_maintenance_pairing_preserves_reason_write_activity_and_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'events.jsonl'
            begin = dict(id=1, pid=123, ordinal=1, stage='begin', reason='idle', unix_nano=1000000,
                         elapsed_nano=0, last_write_unix_nano=950000, failed=False)
            end = dict(begin, stage='end', unix_nano=1200000, elapsed_nano=200000,
                       last_write_unix_nano=1100000)
            def write(events):
                path.write_text(''.join(json.dumps(e)+'\n' for e in events))
            write([begin, end])
            result = diagnostic.maintenance_events(path, 123)
            self.assertEqual(result[0]['begin_quiet_ms'], .05)
            self.assertTrue(result[0]['writes_published_during_pass'])
            for events in ([begin], [end], [begin, dict(end, failed=True)],
                           [dict(begin, pid=999), end], [begin, dict(end, reason='size')]):
                write(events)
                with self.assertRaises(ValueError):
                    diagnostic.maintenance_events(path, 123)

    def test_missing_duplicate_trace_cannot_hide_observed_completions(self):
        allocation = dict(auto_checkpoint_before=0, auto_checkpoint_after=2)
        passes = [dict(end=dict(ordinal=i)) for i in (1, 2)]
        diagnostic.require_event_coverage(passes, allocation)
        for events in ([], passes[:1], passes+passes[:1]):
            with self.assertRaisesRegex(ValueError, 'does not cover'):
                diagnostic.require_event_coverage(events, allocation)
        diagnostic.require_event_coverage([], dict(auto_checkpoint_before=2, auto_checkpoint_after=2))

    def test_fresh_attempt_does_not_reuse_failed_database_label(self):
        with tempfile.TemporaryDirectory() as directory:
            labels = []
            arm = SimpleNamespace(gateway=lambda: None, ycsb_phase=lambda: None)
            def fail(*args):
                labels.append(arm.OUT.name)
                raise ValueError('preserved failed attempt')
            arm.run_case = fail
            for attempt in ('one', 'two'):
                out = Path(directory)/attempt
                out.mkdir()
                with self.assertRaisesRegex(ValueError, 'preserved failed'):
                    diagnostic.collect(arm, Path('/unused'), {}, out/'arm-A', 10000)
            self.assertNotEqual(*labels)


    def fake_toolchain(self, root, mismatch=False, configured_flags=''):
        root = root.resolve()
        goroot = root/'effective'
        (goroot/'bin').mkdir(parents=True)
        tools = goroot/'pkg/tool/linux_amd64'
        tools.mkdir(parents=True)
        environment = dict(GOROOT=str(goroot),GOTOOLDIR=str(tools),GOVERSION='go1.26.10',
                           GOOS='linux',GOARCH='amd64',CGO_ENABLED='1',GOFLAGS=configured_flags)
        source = '#!'+sys.executable+'\n'+r"""
import json,os,sys
from pathlib import Path
settings = SETTINGS
if sys.argv[1]=='env':
    assert os.environ['GOTOOLCHAIN'] in ('go1.26.10','local')
    assert os.environ.get('GOROOT') in (None,settings['GOROOT'])
    settings['GOFLAGS']=os.environ.get('GOFLAGS',settings['GOFLAGS'])
    if Path(sys.argv[0]).name=='launcher' and MISMATCH:
        settings['GOVERSION']='go1.25.0'
    print(json.dumps(settings))
elif sys.argv[1]=='version': print('go version go1.26.10 linux/amd64')
elif sys.argv[1:]==['tool','compile','-V']: print('compile version go1.26.10')
elif sys.argv[1]=='build':
    print('intentional build failure',file=sys.stderr)
    sys.exit(17)
else: raise AssertionError(sys.argv)
"""
        source = source.replace('SETTINGS',repr(environment)).replace('MISMATCH',repr(mismatch))
        launcher = root/'launcher'
        driver = goroot/'bin/go'
        for path in (launcher,driver):
            path.write_text(source)
            path.chmod(0o755)
        return SimpleNamespace(REPO=root,FREEZE={'product_commit':'a'*40,'build_environment':{'go_env':environment}}),launcher,driver

    def test_launcher_auto_switch_and_failed_build_receipt_use_effective_driver(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arm, launcher, driver = self.fake_toolchain(root)
            binding = {}
            with patch.dict(os.environ, {'GOROOT':'/wrong/inherited/root'}):
                effective, env = diagnostic.resolve_toolchain(arm,launcher,binding)
            self.assertEqual(effective,driver)
            self.assertEqual(env['GOTOOLCHAIN'],'local')
            self.assertEqual(binding['toolchain']['compiler_version'],'compile version go1.26.10')
            self.assertEqual(binding['toolchain']['go_env']['GOFLAGS'], '')
            out = root/'overlay'
            with patch.object(diagnostic,'overlay_sources',return_value={}):
                with self.assertRaisesRegex(ValueError,'overlay build failed'):
                    diagnostic.build_overlay(arm,out,launcher)
            receipt = json.loads((out/'build.json').read_text())
            self.assertEqual(receipt['argv'][0],str(driver))
            self.assertEqual(receipt['exit_code'],17)
            self.assertEqual(receipt['toolchain']['go_env']['GOVERSION'],'go1.26.10')
            self.assertIn('intentional build failure',(out/'build.err').read_text())

    def test_toolchain_mismatch_fails_closed_with_build_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arm,launcher,_ = self.fake_toolchain(root,mismatch=True)
            out = root/'overlay'
            with patch.object(diagnostic,'overlay_sources') as sources:
                with self.assertRaisesRegex(ValueError,'toolchain mismatch: GOVERSION'):
                    diagnostic.build_overlay(arm,out,launcher)
                sources.assert_not_called()
            receipt = json.loads((out/'build.json').read_text())
            self.assertEqual(receipt['error_type'],'ValueError')
            self.assertNotIn('argv',receipt)


    def test_effective_compiler_mismatch_fails_before_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arm, launcher, driver = self.fake_toolchain(root)
            driver.write_text(driver.read_text().replace('compile version go1.26.10','compile version go1.25.0'))
            out = root/'overlay'
            with patch.object(diagnostic,'overlay_sources') as sources:
                with self.assertRaisesRegex(ValueError,'effective Go compiler version mismatch'):
                    diagnostic.build_overlay(arm,out,launcher)
                sources.assert_not_called()
            receipt = json.loads((out/'build.json').read_text())
            self.assertEqual(receipt['error_type'],'ValueError')
            self.assertNotIn('argv',receipt)

    def test_inherited_build_flags_fail_before_overlay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arm, launcher, _ = self.fake_toolchain(root)
            with patch.dict(os.environ, {'GOFLAGS': '-race=true'}), patch.object(diagnostic, 'overlay_sources') as sources:
                with self.assertRaisesRegex(ValueError, 'unsupported effective GOFLAGS'):
                    diagnostic.build_overlay(arm, root/'overlay', launcher)
                sources.assert_not_called()
            receipt = json.loads((root/'overlay/build.json').read_text())
            self.assertEqual(json.loads(receipt['toolchain_probes'][0]['stdout'])['GOFLAGS'], '-race=true')
            self.assertNotIn('argv', receipt)

    def test_configured_build_flags_fail_before_overlay(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arm, launcher, _ = self.fake_toolchain(root, configured_flags='-gcflags=all=-N')
            environment = dict(os.environ)
            environment.pop('GOFLAGS', None)
            with patch.dict(os.environ, environment, clear=True), patch.object(diagnostic, 'overlay_sources') as sources:
                with self.assertRaisesRegex(ValueError, 'unsupported effective GOFLAGS'):
                    diagnostic.build_overlay(arm, root/'overlay', launcher)
                sources.assert_not_called()
            receipt = json.loads((root/'overlay/build.json').read_text())
            self.assertEqual(json.loads(receipt['toolchain_probes'][0]['stdout'])['GOFLAGS'], '-gcflags=all=-N')
            self.assertNotIn('argv', receipt)

    def test_sigterm_overlay_build_reaps_child_before_lane_unlock_and_retains_failure(self):
        actor = r"""
import os,signal,sys
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import idle_checkpoint_ab as ab
import idle_checkpoint_diagnostic as diagnostic
root=Path(sys.argv[1])
previous=signal.getsignal(signal.SIGTERM)
arm=SimpleNamespace(REPO=root,FREEZE={'product_commit':'a'*40})
try:
    with ab.termination_scope(), ExitStack() as stack:
        ab.lock_lane(stack,[root/'lane.lock'])
        with patch.object(diagnostic,'resolve_toolchain',return_value=(root/'driver',os.environ.copy())),patch.object(diagnostic,'overlay_sources',return_value={}):
            diagnostic.build_overlay(arm,root/'overlay',root/'driver')
except InterruptedError:
    assert signal.getsignal(signal.SIGTERM)==previous
    (root/'handler-restored').write_text('yes')
else: raise AssertionError('build did not cancel')
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            driver = root/'driver'
            driver.write_text('#!'+sys.executable+'\n'+
                "import os,signal,time\nfrom pathlib import Path\n"+
                "def end(*args):\n time.sleep(.25)\n raise SystemExit(0)\n"+
                "signal.signal(signal.SIGTERM,end)\npid=Path("+repr(str(root/'build.pid'))+")\n"+
                "temporary=pid.with_suffix('.tmp')\ntemporary.write_text(str(os.getpid()))\ntemporary.replace(pid)\ntime.sleep(60)\n")
            driver.chmod(0o755)
            process = subprocess.Popen([sys.executable,'-c',actor,str(root)],cwd=Path(diagnostic.__file__).parent,
                                       stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            child = None
            try:
                deadline = time.monotonic()+10
                while not (root/'build.pid').exists():
                    self.assertIsNone(process.poll())
                    self.assertLess(time.monotonic(),deadline,'overlay child did not start')
                    time.sleep(.01)
                child = int((root/'build.pid').read_text())
                os.kill(process.pid,signal.SIGTERM)
                time.sleep(.05)
                os.kill(process.pid,signal.SIGTERM)
                with (root/'lane.lock').open('a') as lane:
                    while True:
                        try:
                            fcntl.flock(lane,fcntl.LOCK_EX|fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            self.assertLess(time.monotonic(),deadline,'lane not released')
                            time.sleep(.01)
                    with self.assertRaises(ProcessLookupError): os.kill(child,0)
                _,stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode,0,stderr)
                self.assertTrue((root/'handler-restored').exists())
                receipt = json.loads((root/'overlay/build.json').read_text())
                self.assertEqual(receipt['error_type'],'InterruptedError')
                self.assertEqual(receipt['pid'],child)
                self.assertEqual(receipt['interrupted_child_cleanup']['exit_code'],0)
                self.assertNotIn('binary_sha256',receipt)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
                if child is not None:
                    try: os.killpg(child,signal.SIGKILL)
                    except ProcessLookupError: pass

    def test_owned_command_preserves_handler_and_masks_on_exit_and_spawn_failure(self):
        previous_handler = signal.getsignal(signal.SIGTERM)
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGUSR1})
        expected_mask = signal.pthread_sigmask(signal.SIG_BLOCK,set())
        try:
            binding = {}
            command = [sys.executable,'-c',
                       'import json,signal; print(json.dumps(sorted(signal.pthread_sigmask(signal.SIG_BLOCK,set()))))']
            result = diagnostic.owned_command(command,binding,stdout=subprocess.PIPE,text=True)
            self.assertEqual(result.returncode,0)
            self.assertEqual(json.loads(result.stdout),sorted(expected_mask))
            self.assertEqual(binding['exit_code'],0)
            with patch.object(diagnostic.subprocess,'Popen',side_effect=OSError('spawn failed')):
                with self.assertRaisesRegex(OSError,'spawn failed'):
                    diagnostic.owned_command(command,{})
            self.assertEqual(signal.getsignal(signal.SIGTERM),previous_handler)
            self.assertEqual(signal.pthread_sigmask(signal.SIG_BLOCK,set()),expected_mask)
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK,previous_mask)

    def test_sigterm_after_popen_reaps_before_registration_and_lane_unlock(self):
        actor = r"""
import json,os,signal,subprocess,sys
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch
import idle_checkpoint_ab as ab
import idle_checkpoint_diagnostic as diagnostic
root=Path(sys.argv[1])
previous_handler=signal.getsignal(signal.SIGTERM)
previous_mask=signal.pthread_sigmask(signal.SIG_BLOCK,set())
binding={}
original_popen=subprocess.Popen
def cancelled_acquisition(*args,**kwargs):
    child=original_popen(*args,**kwargs)
    assert 'pid' not in binding
    (root/'acquired.tmp').write_text(json.dumps(dict(pid=child.pid,registered=False)))
    (root/'acquired.tmp').replace(root/'acquired.json')
    # The actual child exists, but owned_command has not received it yet.
    os.kill(os.getpid(),signal.SIGTERM)
    return child
try:
    with ab.termination_scope(), ExitStack() as stack:
        ab.lock_lane(stack,[root/'lane.lock'])
        try:
            with patch.object(subprocess,'Popen',side_effect=cancelled_acquisition):
                diagnostic.owned_command([sys.executable,'-c','import time; time.sleep(60)'],binding,
                                         stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        finally:
            (root/'binding.json').write_text(json.dumps(binding))
except InterruptedError:
    assert signal.getsignal(signal.SIGTERM)==previous_handler
    assert signal.pthread_sigmask(signal.SIG_BLOCK,set())==previous_mask
    (root/'restored').write_text('handler and mask restored')
else: raise AssertionError('acquisition did not cancel')
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = subprocess.Popen([sys.executable,'-c',actor,str(root)],cwd=Path(diagnostic.__file__).parent,
                                       stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            child = None
            try:
                deadline = time.monotonic()+10
                while not (root/'acquired.json').exists():
                    self.assertIsNone(process.poll())
                    self.assertLess(time.monotonic(),deadline,'real Popen did not return')
                    time.sleep(.01)
                acquisition = json.loads((root/'acquired.json').read_text())
                child = acquisition['pid']
                with (root/'lane.lock').open('a') as lane:
                    while True:
                        try:
                            fcntl.flock(lane,fcntl.LOCK_EX|fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            self.assertLess(time.monotonic(),deadline,'lane not released')
                            time.sleep(.01)
                    try: os.kill(child,0)
                    except ProcessLookupError: alive = False
                    else: alive = True
                _,stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode,0,stderr)
                self.assertTrue((root/'restored').exists())
                binding = json.loads((root/'binding.json').read_text())
                self.assertFalse(alive,dict(acquisition=acquisition,binding=binding,child_alive_after_unlock=alive))
                self.assertEqual(binding['pid'],child)
                self.assertIn('interrupted_child_cleanup',binding)
                self.assertIsNotNone(binding['exit_code'])
            finally:
                if child is not None:
                    try: os.killpg(child,signal.SIGKILL)
                    except ProcessLookupError: pass
                if process.poll() is None:
                    process.kill()
                process.communicate()

    def test_first_sigterm_during_timeout_cleanup_reaps_before_unlock(self):
        actor = r"""
import json,os,signal,subprocess,sys,time
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import idle_checkpoint_ab as ab
import idle_checkpoint_diagnostic as diagnostic
root,mode=Path(sys.argv[1]),sys.argv[2]
previous=signal.getsignal(signal.SIGTERM)
if mode!='overlay':
    frozen=root/'frozen';(frozen/'recipe').mkdir(parents=True);(frozen/'bin').mkdir()
    sources={}
    for name in ('run.py','ycsb_log.py','census.go'):
        target=frozen/'recipe'/name
        target.write_bytes(Path(ab.recipe.__file__).with_name(name).read_bytes())
        sources[name]=ab.recipe.sha(target)
    binaries={}
    for name in ('go-ycsb','treedb-mongo-gateway','mongo_gateway_bench','ycsb-correctness-fixture','mongo_gateway_compare_report','census'):
        target=frozen/'bin'/name;target.write_text('');binaries[name]=ab.recipe.sha(target)
    ab.recipe.save(frozen/'freeze.json',dict(recipe_sources=sources,binaries=binaries,mongodb_image_id='unused',sources=[dict(path=str(root),commit='a'*40,blobs={})]))
    def git(argv,**kwargs): return '' if 'status' in argv else 'a'*40+'\n'
    with patch.object(ab.recipe.subprocess,'check_output',side_effect=git):
        arm=ab.load_arm(frozen,'A',ab.recipe.sha(frozen/'freeze.json'))
else:
    arm=SimpleNamespace(REPO=root,FREEZE={'product_commit':'a'*40})
original_popen=subprocess.Popen
def ready_popen(*args,**kwargs):
    child=original_popen(*args,**kwargs)
    try:
        deadline=time.monotonic()+5
        while not (root/'child.pid').exists():
            assert child.poll() is None, 'driver exited before readiness'
            assert time.monotonic()<deadline, 'driver readiness deadline'
            time.sleep(.005)
        return child
    except BaseException:
        ab.terminate_group(child)
        raise
try:
    with ab.termination_scope(), ExitStack() as stack:
        ab.lock_lane(stack,[root/'lane.lock'])
        stack.enter_context(patch.object(subprocess,'Popen',side_effect=ready_popen))
        if mode=='overlay':
            original=diagnostic.owned_command
            def bounded(argv,binding,**kwargs): return original(argv,binding,timeout=.15,**kwargs)
            with patch.object(diagnostic,'resolve_toolchain',return_value=(root/'driver',os.environ.copy())),patch.object(diagnostic,'overlay_sources',return_value={}),patch.object(diagnostic,'owned_command',side_effect=bounded):
                diagnostic.build_overlay(arm,root/'overlay',root/'driver')
        elif mode=='command':
            arm.command(root,'client',[root/'driver'],timeout=.15)
        else:
            child=subprocess.Popen([root/'driver'],start_new_session=True)
            while not (root/'child.pid').exists(): time.sleep(.005)
            service=dict(kind='gomap',process=child,stopped=False,stop_attempts=0,stem='server',handles=[])
            # Enter the same forced cleanup used after a normal Close timeout.
            original_wait=child.wait
            def timeout_once(*args,**kwargs):
                if kwargs.get('timeout')==180: raise subprocess.TimeoutExpired([root/'driver'],180)
                return original_wait(*args,**kwargs)
            with patch.object(child,'wait',side_effect=timeout_once): arm.stop(root,service)
except (InterruptedError,subprocess.TimeoutExpired,ValueError):
    assert signal.getsignal(signal.SIGTERM)==previous
    (root/'handler-restored').write_text('yes')
else: raise AssertionError('timeout/cancellation did not fail')
"""
        for mode in ('overlay','command','stop'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                driver = root/'driver'
                driver.write_text('#!'+sys.executable+'\n'+
                    "import os,signal,time\nfrom pathlib import Path\n"+
                    "def end(*args):\n signal.signal(signal.SIGTERM,signal.SIG_IGN)\n Path("+repr(str(root/'term-seen'))+").write_text('yes')\n time.sleep(.4)\n raise SystemExit(0)\n"+
                    "signal.signal(signal.SIGTERM,end)\nPath("+repr(str(root/'child.pid'))+").write_text(str(os.getpid()))\ntime.sleep(60)\n")
                driver.chmod(0o755)
                process = subprocess.Popen([sys.executable,'-c',actor,str(root),mode],cwd=Path(diagnostic.__file__).parent,
                                           stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                child = None
                try:
                    deadline = time.monotonic()+10
                    while not (root/'term-seen').exists():
                        self.assertIsNone(process.poll())
                        self.assertLess(time.monotonic(),deadline,'timeout cleanup did not start')
                        time.sleep(.01)
                    child = int((root/'child.pid').read_text())
                    os.kill(process.pid,signal.SIGTERM)
                    with (root/'lane.lock').open('a') as lane:
                        while True:
                            try:
                                fcntl.flock(lane,fcntl.LOCK_EX|fcntl.LOCK_NB)
                                break
                            except BlockingIOError:
                                self.assertLess(time.monotonic(),deadline,'lane not released')
                                time.sleep(.01)
                        with self.assertRaises(ProcessLookupError): os.kill(child,0)
                    _,stderr = process.communicate(timeout=5)
                    self.assertEqual(process.returncode,0,stderr)
                    self.assertTrue((root/'handler-restored').exists())
                    receipt_path = {'overlay':'overlay/build.json','command':'client.command.json','stop':'server-cleanup-1.stop.json'}[mode]
                    receipt = json.loads((root/receipt_path).read_text())
                    if mode=='overlay': self.assertEqual(receipt['error_type'],'InterruptedError')
                    elif mode=='command':
                        self.assertTrue(receipt['timed_out'])
                        self.assertEqual(receipt['exit_code'],0)
                    else: self.assertTrue(receipt['absent'])
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate()
                    if child is not None:
                        try: os.killpg(child,signal.SIGKILL)
                        except ProcessLookupError: pass

    def test_rpc_sigterm_preserves_enclosing_repeat_signal_cleanup_state(self):
        actor = r"""
import os,signal,sys
from pathlib import Path
from unittest.mock import patch
import idle_checkpoint_ab as ab
import idle_checkpoint_diagnostic as diagnostic
root=Path(sys.argv[1])/'treedb_mongo';root.mkdir()
manifest=dict(targets=['treedb_mongo'],run_repeats=1,campaign_id='control',server_builds={'treedb_mongo':{}},phase_stats=dict(schema_version=1,enabled=True,max_observations=64,rpc_timeout_seconds=5))
previous=signal.getsignal(signal.SIGTERM)
def cancelled_read(*args): os.kill(os.getpid(),signal.SIGTERM)
with patch.object(diagnostic.live.c,'read',side_effect=cancelled_read):
    try: diagnostic.live.phase_stats_rpc(root,'default','before','phase',manifest)
    except InterruptedError: pass
    else: raise AssertionError('default RPC handler did not cancel')
assert signal.getsignal(signal.SIGTERM)==previous
assert (root/'default.phase-stats-before.failure.json').exists()
with ab.termination_scope():
    with patch.object(diagnostic.live.c,'read',side_effect=cancelled_read):
        try: diagnostic.live.phase_stats_rpc(root,'load','before','phase',manifest)
        except InterruptedError: pass
        else: raise AssertionError('RPC did not cancel')
    os.kill(os.getpid(),signal.SIGTERM)
assert signal.getsignal(signal.SIGTERM)==previous
assert (root/'load.phase-stats-before.failure.json').exists()
"""
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run([sys.executable,'-c',actor,directory],cwd=Path(diagnostic.__file__).parent,
                                    capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)


if __name__ == '__main__':
    unittest.main()
