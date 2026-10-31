"""Synthetic controls for the owned-child runtime observer integration."""
import argparse
import copy
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

import ycsb_compare_validate as compare


class RuntimePreflightControls(unittest.TestCase):
    def test_exact_elf_and_host_requirements(self):
        with mock.patch.object(compare.sys,'platform','linux'), mock.patch.object(compare.platform,'machine',return_value='x86_64'), mock.patch.object(pathlib.Path,'is_file',return_value=True), mock.patch.object(compare.os,'pidfd_open',create=True):
            for sha in compare.OBSERVED_CLIENT_SYMBOLS:compare.runtime_client_preflight(dict(exe_sha256=sha))
            with self.assertRaisesRegex(ValueError,'exact ELF/symbol'):compare.runtime_client_preflight(dict(exe_sha256='0'*64))
            with mock.patch.object(compare.sys,'platform','darwin'),self.assertRaisesRegex(ValueError,'Linux x86_64'):compare.runtime_client_preflight(dict(exe_sha256=compare.CONTROLLED_CLIENT_SHA))
            with mock.patch.object(compare.platform,'machine',return_value='aarch64'),self.assertRaisesRegex(ValueError,'Linux x86_64'):compare.runtime_client_preflight(dict(exe_sha256=compare.CONTROLLED_CLIENT_SHA))


@unittest.skipUnless(sys.platform=='linux' and hasattr(os,'pidfd_open'), 'Linux /proc and pidfd controls')
class RuntimeControls(unittest.TestCase):
    def test_recheck_refuses_changed_executable_or_pid_and_read_failures(self):
        stat=SimpleNamespace(st_dev=1,st_ino=2,st_size=3,st_mtime_ns=4,st_ctime_ns=5)
        proc=mock.MagicMock();proc.__truediv__.return_value.stat.return_value=stat
        helper=compare.runtime_observer
        with mock.patch.object(helper,'start_identity',return_value=(10,11)):
            helper.recheck_identity(20,proc,(10,11),(1,2,3,4,5))
            with self.assertRaisesRegex(RuntimeError,'ELF identity changed'):
                helper.recheck_identity(20,proc,(10,11),(1,2,3,4,6))
            proc.__truediv__.return_value.stat.side_effect=PermissionError('unreadable')
            with self.assertRaises(PermissionError):helper.recheck_identity(20,proc,(10,11),(1,2,3,4,5))
        with mock.patch.object(helper,'start_identity',return_value=(10,12)):
            with self.assertRaisesRegex(RuntimeError,'child identity changed'):
                helper.recheck_identity(20,proc,(10,11),(1,2,3,4,5))

    def test_startup_mapping_pending_is_bounded_and_not_allowed_after_observation(self):
        with mock.patch.object(compare.runtime_observer.time,'monotonic',return_value=11):
            self.assertTrue(compare.runtime_observer.mapping_pending([],10,False))
            self.assertFalse(compare.runtime_observer.mapping_pending([['mapped']],10,False))
            with self.assertRaisesRegex(RuntimeError,'disappeared'):
                compare.runtime_observer.mapping_pending([],10,True)
            with self.assertRaisesRegex(RuntimeError,'deadline'):
                compare.runtime_observer.mapping_pending([],8,False)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.cell = self.root / 'cell'
        self.cell.mkdir()
        (self.root / 'manifest.json').write_text(json.dumps(dict(
            campaign_id='synthetic', profile='command_wal_relaxed',
            client=dict(build_id='synthetic', exe_sha256=compare.CONTROLLED_CLIENT_SHA))))

    @staticmethod
    def sample(pid, start):
        return dict(pid=pid, start_ticks=start, GOMAXPROCS_present=False,
                    runtime_gomaxprocs=12, address=hex(compare.CONTROLLED_GOMAX_SYMBOL),
                    leader_affinity=[0], cgroup=dict(membership='/', ancestors=[{'cpu.max':'max 100000'}]))

    def test_actual_owned_process_cpu_observation_and_identity_guards(self):
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(10)'],
                                 env={k:v for k,v in os.environ.items() if k != 'GOMAXPROCS'})
        try:
            identity=compare.observe(child.pid)
            policy=compare.observe_cpu_policy(child.pid,identity)
            compare.validate_cpu_policy(policy,identity)
            wrong=dict(identity,start_ticks=identity['start_ticks']+1)
            with self.assertRaisesRegex(ValueError,'process changed'):
                compare.observe_cpu_policy(child.pid,wrong)
            with mock.patch.object(compare.runtime_observer,'quota_snapshot',side_effect=PermissionError('unreadable quota')):
                with self.assertRaises(PermissionError): compare.observe_cpu_policy(child.pid,identity)
            with mock.patch.object(compare.os,'sched_getaffinity',side_effect=[{0},{1}]):
                with self.assertRaisesRegex(ValueError,'affinity changed'):
                    compare.observe_cpu_policy(child.pid,identity)
            with mock.patch.object(compare,'observe',side_effect=[identity,wrong]):
                with self.assertRaisesRegex(ValueError,'changed during'):
                    compare.observe_cpu_policy(child.pid,identity)
            for invalid in (None,dict(policy,leader_affinity=[]),dict(policy,cgroup={}),dict(policy,process=wrong)):
                with self.assertRaises(ValueError):compare.validate_cpu_policy(invalid,identity)
        finally:
            child.terminate();child.wait(timeout=3)

    def test_controlled_child_sampling_and_missing_observation_fail_closed(self):
        class FakeObserver:
            produce=True
            def __init__(self,*args): self.samples=[]
            def sample(self,child):
                if self.produce:
                    self.samples.append(RuntimeControls.sample(child.pid,compare.observe_core(child.pid)['start_ticks']))
            def validate(self):
                if not self.samples: raise RuntimeError('no validated nonzero actual-child runtime observation')
                return dict(status='actual-child-runtime-observed',sample_count=len(self.samples),
                            first=self.samples[0],last=self.samples[-1],whole_interval_verified=False,
                            unperturbed_timing_verified=False)
            def diagnostics(self): return dict(error='synthetic test')
        for produce,stem in [(True,'success'),(False,'missing')]:
            FakeObserver.produce=produce
            args=argparse.Namespace(directory=str(self.cell),stem=stem,timeout=3,cleanup_timeout=.2,
                                    command=[sys.executable,'-c','import time; time.sleep(.6)'])
            with mock.patch.object(compare,'exe_digest',return_value=compare.CONTROLLED_CLIENT_SHA), \
                 mock.patch.object(compare.runtime_observer,'RuntimeObserver',FakeObserver):
                if produce:
                    self.assertEqual(compare.launch(args),0)
                    launch=compare.read(self.cell/(stem+'.launch.json'))
                    compare.validate_client_runtime(launch['runtime_observation'],launch['process'])
                    self.assertGreaterEqual(launch['runtime_observation']['sample_count'],2)
                    corrupt=copy.deepcopy(launch['runtime_observation']);corrupt['samples'][0]['start_ticks']+=1
                    with self.assertRaises(ValueError):compare.validate_client_runtime(corrupt,launch['process'])
                    for key,changed in [('runtime_gomaxprocs',13),('leader_affinity',[1]),('cgroup',dict(membership='/other',ancestors=[{}]))]:
                        corrupt=copy.deepcopy(launch['runtime_observation'])
                        corrupt['samples'][-1][key]=changed;corrupt['last']=copy.deepcopy(corrupt['samples'][-1])
                        with self.assertRaisesRegex(ValueError,'changed across samples'):
                            compare.validate_client_runtime(corrupt,launch['process'])
                else:
                    with self.assertRaisesRegex(RuntimeError,'no validated'):
                        compare.launch(args)
                    failure=compare.read(self.cell/(stem+'.failure.json'))
                    self.assertTrue(failure['cleanup']['reaped'])
                    self.assertFalse(pathlib.Path('/proc',str(failure['pid'])).exists())
                    self.assertFalse((self.cell/(stem+'.launch.json')).exists())

    def test_wrong_bound_client_refused_before_spawn(self):
        args=argparse.Namespace(directory=str(self.cell),stem='wrong',command=[sys.executable,'-c','raise SystemExit(0)'])
        with mock.patch.object(compare,'exe_digest',return_value='f'*64), mock.patch.object(compare.subprocess,'Popen') as spawn:
            with self.assertRaisesRegex(ValueError,'client executable mismatch'):compare.launch(args)
            spawn.assert_not_called()

    def test_controlled_role_bindings_and_unknown_refused(self):
        controlled=compare.CONTROLLED_CLIENT_SHA
        manifest=dict(client=dict(exe_sha256=controlled),fixture=dict(binary='/fixture',exe_sha256='f'*64),control=dict(binary='/control',exe_sha256='c'*64),docker_binary='/docker',server_builds={'mongodb':{'docker_cli_sha256':'d'*64}})
        self.assertTrue(compare.resolve_launch_role(manifest,['/client'],controlled,'client'))
        for role,path,digest in [('fixture','/fixture','f'*64),('control','/control','c'*64),('docker','/docker','d'*64)]:
            self.assertFalse(compare.resolve_launch_role(manifest,[path],digest,role))
            with self.assertRaises(ValueError):compare.resolve_launch_role(manifest,[path],'0'*64,role)
            with self.assertRaises(ValueError):compare.resolve_launch_role(manifest,['/wrong'],digest,role)
            with self.assertRaises(ValueError):compare.resolve_launch_role(manifest,[path],controlled,role)
        with self.assertRaises(ValueError):compare.resolve_launch_role(manifest,['/fixture'],'f'*64,'client')
        with self.assertRaises(ValueError):compare.resolve_launch_role(manifest,['/client'],controlled,'unknown')

    def test_public_helper_callers_supply_expected_role(self):
        import ycsb_compare_live as live
        import ycsb_mongodb_reference as reference
        import contextlib
        manifest=dict(campaign_id='synthetic',profile='p',fixture=dict(binary='/fixture',exe_sha256='f'*64),control=dict(binary='/control',exe_sha256='c'*64),formats={'treedb_native':'bson'},server_builds={'treedb_native':{'source_commit':'a'*40}},deadlines={'phase_seconds':1,'helper_seconds':1,'cleanup_seconds':1,'phase_outer_seconds':1,'helper_outer_seconds':1})
        with mock.patch.object(live.c,'read',return_value=manifest), mock.patch.object(live.c,'launch',return_value=0) as launch:
            live.run_helper(self.cell,'treedb_native','127.0.0.1:1234','snapshot','loaded')
            self.assertEqual(launch.call_args.args[0].role,'fixture')
            self.assertEqual(launch.call_args.args[0].command[0],'/fixture')
        driver=reference.Docker.__new__(reference.Docker)
        driver.m=manifest;driver.directory=self.cell;driver.deadline=float('inf')
        with mock.patch.object(reference.c,'write_new'), mock.patch.object(reference.c,'outer_deadline',return_value=contextlib.nullcontext()), mock.patch.object(reference.c,'launch',return_value=0) as launch:
            for command,phase,expected in [(['/fixture'],False,'fixture'),(['/control'],False,'control'),(['/client'],True,'client')]:
                driver.helper('synthetic',command,phase)
                self.assertEqual(launch.call_args.args[0].role,expected)

    def test_live_fixture_to_real_launcher_with_bound_elf(self):
        import ycsb_compare_live as live
        manifest=dict(campaign_id='synthetic',profile='p',client=dict(build_id='synthetic',exe_sha256=compare.CONTROLLED_CLIENT_SHA),fixture=dict(binary='/fixture',exe_sha256='f'*64),formats={'treedb_native':'bson'},server_builds={'treedb_native':{'source_commit':'a'*40}})
        (self.root/'manifest.json').write_text(json.dumps(manifest))
        identity=dict(pid=123,start_ticks=456,exe_sha256='f'*64,gomaxprocs_unset=True)
        child=mock.Mock(pid=123);child.wait.return_value=0
        patches=[mock.patch.object(compare,'exe_digest',return_value='f'*64),mock.patch.object(compare.subprocess,'Popen',return_value=child),mock.patch.object(compare.os,'pidfd_open',return_value=999),mock.patch.object(compare.os,'close'),mock.patch.object(compare,'observe_core',return_value={k:identity[k] for k in ('pid','start_ticks','exe_sha256')}),mock.patch.object(compare,'observe',return_value=identity)]
        import contextlib
        with contextlib.ExitStack() as stack:
            mocks=[stack.enter_context(p) for p in patches]
            live.run_helper(self.cell,'treedb_native','127.0.0.1:1234','snapshot','loaded')
            self.assertEqual(mocks[1].call_count,1)
            self.assertEqual(compare.read(self.cell/'snapshot_loaded.launch.json')['process'],identity)
            manifest['fixture']['exe_sha256']='e'*64
            (self.root/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError,'fixture executable mismatch'):
                live.run_helper(self.cell,'treedb_native','127.0.0.1:1234','snapshot','preclose')
            self.assertEqual(mocks[1].call_count,1)

    def test_all_server_lifetimes_require_bound_policy(self):
        identity=dict(pid=123,start_ticks=456,exe_sha256='a'*64,gomaxprocs_unset=True)
        policy=dict(process=identity,leader_affinity=[0],cgroup=dict(membership='/',ancestors=[{'cpu.max':'max 100000'}]))
        for prefix in ('server','reopened','fixture-write','fixture-reopened'):
            (self.cell/(prefix+'.launch.json')).write_text(json.dumps(dict(process=identity,cpu_policy=policy)))
        compare.validate_server_policies(self.cell)
        path=self.cell/'fixture-reopened.launch.json'
        path.write_text(json.dumps(dict(process=identity)))
        with self.assertRaisesRegex(ValueError,'CPU policy'):compare.validate_server_policies(self.cell)


if __name__ == '__main__': unittest.main()
