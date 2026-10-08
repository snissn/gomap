"""Pure synthetic readiness/custody controls; no Go or benchmark children."""
import collect
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from collect import LoadReadiness, host_gate, source_preflight, cancel_collector
from prepare_config import draft
from protocol import LOAD_READINESS, digest, label, schedule, sha, write, validate_load_readiness, validate_readiness

INIT = "1 0 100 0.0 1 Ss 1 init\n"

class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="c3-load-ready-")
        self.out = Path(self.temp.name)
        self.c = draft()
        # A finite one-cell synthetic ledger exercises all14 ordered positions.
        # The real draft's full54/756 coverage is checked separately below.
        self.c['cases'] = self.c['cases'][:1]
        self.host = {'system': 'Linux', 'node': 'synthetic', 'machine': 'x86_64', 'release': 'synthetic',
                     'cpu_count': 12, 'cpu_affinity': list(range(12)), 'max_load1': 5., 'max_load5': 5.,
                     'min_free_bytes': 1, 'tmpdir': '/synthetic/tmp', 'tmpdir_device': 7}
        self.c['host'] = self.host
        self.state = {}
        for name, v in self.c['variants'].items():
            source = self.out / name; source.mkdir()
            (source / 'input.go').write_text('synthetic source; not a Go program\n')
            binary = self.out / (name + '.binary'); binary.write_text('synthetic nonexecutable\n')
            v.update(source=str(source), binary=str(binary), source_tree_sha256=digest(name), binary_sha256=sha(binary))
            self.state[name] = {'source': source, 'binary': binary,
                'identity': {'tree_sha256': v['source_tree_sha256'], 'files': [{'path': 'input.go', 'sha256': sha(source / 'input.go'), 'git_mode': '100644'}]}}
        self.clock = 1_000_000_000
        self.loads = [[1., 1., 1.]]
        self.raw = INIT
        self.calls = 0
        self.mutate_host = lambda h: None
        self.cancel_wait = False
        self.patches = [patch('collect.time.monotonic_ns', side_effect=self.tick),
                        patch('collect.time.sleep', side_effect=self.sleep),
                        patch('collect.host_snapshot', side_effect=self.snapshot),
                        patch('collect.subprocess.Popen')]
        for p in self.patches: p.start()
        self.ledger = LoadReadiness(self.out, LOAD_READINESS)
        self.receipts = []

    def tearDown(self):
        import collect
        collect.subprocess.Popen.assert_not_called()
        for p in reversed(self.patches): p.stop()
        self.temp.cleanup()

    def tick(self):
        self.clock += 1_000_000
        return self.clock

    def sleep(self, seconds):
        self.clock += int(seconds * 1_000_000_000 * (0.5 if self.cancel_wait else 1))
        if self.cancel_wait:
            cancel_collector(15, None)

    def snapshot(self, out, name, storage, source, env):
        self.calls += 1
        h = {'at': 'synthetic', 'uname': {k: self.host[k] for k in ('system','node','machine','release')},
             'cpu_count': 12, 'cpu_affinity': list(range(12)), 'load': self.loads[min(self.calls-1,len(self.loads)-1)],
             'free_bytes': 100, 'storage_path': '/synthetic/tmp', 'storage_device': 7, 'source_path': str(source)}
        self.mutate_host(h)
        for suffix in ['meminfo', 'cpuinfo', 'mounts', 'processes']:
            p = out / (name + '-' + suffix + '.txt');p.write_text(self.raw if suffix == 'processes' else 'synthetic\n')
            h[suffix + '_sha256'] = sha(p)
        write(out / (name + '-host.json'), h)
        return h

    def admit(self, item=None):
        item = list(schedule(self.c))[len(self.receipts)] if item is None else item
        name = label(item)
        return self.ledger.admit(name, None, self.state[item['variant']]['source'], {}, self.host, [],
                                 lambda: collect.source_preflight(self.state, self.c, name))

    def leaf(self):
        item = list(schedule(self.c))[len(self.receipts)]; name = label(item)
        before, index, proof = self.admit(item)
        self.assertFalse((self.out / (name + '.stdout')).exists())
        spawn = self.tick(); end = self.tick()
        write(self.out / (name + '-monitor.json'), {'started_monotonic_ns': spawn, 'reaped_monotonic_ns': end})
        self.receipts.append({'label': name,'before': before,'readiness_probe_index': index,
                              'readiness_probe_sha256': proof,'spawn_monotonic_ns': spawn})

    def finish(self, error=None):
        self.ledger.finish(error)
        self.terminal = ({'type': type(error).__name__, 'error': str(error), 'retained_runs':len(self.receipts)}
                         if error else {'runs':len(self.receipts)})
        self.reseal()

    def reseal(self):
        self.ledger.save(); self.terminal['readiness_sha256'] = sha(self.out / 'readiness.json')

    def check(self):
        return validate_readiness(self.out, self.c, self.receipts, self.terminal)

    def complete(self):
        for _ in schedule(self.c): self.leaf()
        self.finish();self.check()

    def test_uniform_order_global_budget_and_high_then_ready(self):
        self.loads = [[5.0001,4.,0.],[4.5,4.5,999.]]
        self.complete()
        self.assertEqual(len(self.ledger.value['probes']),29)
        self.assertEqual(len(self.ledger.value['waits']),1)
        self.assertGreaterEqual(self.ledger.value['total_wait_ns'],5_000_000_000)
        self.assertEqual(len(self.receipts),14)
        self.assertEqual(self.receipts[0]['readiness_probe_index'],2)
        self.assertEqual(len(list(self.out.glob('*-readiness-*-host.json'))),29)

    def test_readiness_headroom_and_original_host_boundaries(self):
        self.loads=[[4.5001,1.,0.],[4.5,4.5,0.]];self.complete()
        self.assertEqual(self.ledger.value['probes'][0]['wait_reason'],'readiness-headroom')
        h=copy.deepcopy(self.receipts[0]['before']);h['load']=[5.,5.,0.]
        host_gate(h,self.host)
        h['load']=[5.0001,1.,0.]
        with self.assertRaisesRegex(ValueError,'host contention'):host_gate(h,self.host)
        for component in [0,1]:
            h['load']=[4.5,4.5,0.];h['load'][component]=4.5001
            from protocol import load_wait_reason
            self.assertEqual(load_wait_reason(h,self.host,LOAD_READINESS),'readiness-headroom')
        self.assertEqual(self.ledger.value['probes'][2]['decision'],'admitted')

    def test_high_retries_skip_hashes_and_final_probe_follows_refresh(self):
        self.loads=[[6.,1.,0.],[6.,1.,0.],[1.,1.,0.],[1.,1.,0.]]
        import collect
        real=collect.source_preflight
        with patch('collect.source_preflight',wraps=real) as refresh:
            self.leaf()
        self.assertEqual(refresh.call_count,1)
        probes=self.ledger.value['probes']
        self.assertEqual([p['decision'] for p in probes],['wait','wait','refresh','admitted'])
        self.assertTrue(all(p['preflight'] is None for p in probes[:-1]))
        self.assertIsNotNone(probes[-1]['preflight'])
        self.assertEqual(self.receipts[0]['readiness_probe_index'],3)

    def test_load_rebound_during_refresh_reenters_same_block(self):
        self.loads=[[6.,1.,0.],[1.,1.,0.],[4.5001,1.,0.],[1.,1.,0.],[1.,1.,0.]]
        self.leaf()
        self.assertEqual([p['decision'] for p in self.ledger.value['probes']],['wait','refresh','wait','refresh','admitted'])
        self.assertEqual(len(self.ledger.value['blocked_intervals']),1)
        self.assertEqual(len(self.ledger.value['waits']),2)
        self.assertEqual(self.ledger.value['probes'][2]['wait_reason'],'readiness-headroom')
        for _ in list(schedule(self.c))[1:]:self.leaf()
        self.finish();self.check()

    def test_cancel_during_incremental_probe_or_wait_save(self):
        for wait_save in [False, True]:
            with self.subTest(wait_save=wait_save):
                self.ledger=LoadReadiness(self.out,LOAD_READINESS);self.calls=0
                self.loads=[[6.,1.,0.]]
                real=self.ledger.save;cancelled=False
                def interrupted():
                    nonlocal cancelled
                    target=self.ledger.value['waits'] if wait_save else self.ledger.value['probes']
                    if target and not cancelled:
                        cancelled=True
                        raise KeyboardInterrupt('synthetic ledger save cancellation')
                    real()
                with patch.object(self.ledger,'save',side_effect=interrupted),self.assertRaises(KeyboardInterrupt) as caught:
                    self.admit()
                self.finish(caught.exception);self.check()
                self.assertEqual(self.receipts,[])

    def test_retry_probe_work_consumes_total_budget_without_extra_sleep(self):
        self.loads=[[6.,1.,0.],[6.,1.,0.]]
        original=self.snapshot
        def slow_probe(*args):
            if self.calls:self.clock+=600_000_000_000
            return original(*args)
        with patch('collect.host_snapshot',side_effect=slow_probe),self.assertRaisesRegex(ValueError,'budget exhausted') as caught:
            self.admit()
        self.finish(caught.exception);self.check()
        self.assertEqual(len(self.ledger.value['waits']),1)
        self.assertEqual(self.ledger.value['probes'][-1]['decision'],'refused')
        self.assertEqual(self.receipts,[])

    def test_refreshed_hash_work_consumes_budget_before_ready_spawn(self):
        self.loads=[[6.,1.,0.],[1.,1.,0.],[1.,1.,0.]]
        import collect
        original=collect.source_preflight
        def slow_refresh(*args):
            self.clock+=600_000_000_000
            return original(*args)
        with patch('collect.source_preflight',side_effect=slow_refresh),self.assertRaisesRegex(ValueError,'budget exhausted') as caught:
            self.admit()
        self.finish(caught.exception);self.check()
        self.assertEqual(len(self.ledger.value['waits']),1)
        self.assertGreater(self.ledger.value['total_wait_ns'],600_000_000_000)
        self.assertEqual(self.receipts,[])

    def test_every_nonload_guard_immediate_even_with_high_load(self):
        for field, value in [('cpu_count',11),('cpu_affinity',[0,1,2,3]),('free_bytes',0),('storage_path','/wrong'),('storage_device',8),('load',[float('nan'),1,1])]:
            self.loads = [[6.,6.,0.]];self.calls=0
            self.mutate_host = lambda h,k=field,v=value: h.update({k:v})
            with self.subTest(field=field),self.assertRaises(ValueError): self.admit()
            self.assertEqual(self.ledger.value['waits'],[])
            self.assertEqual(self.receipts,[])
        for field in ['system','node','machine','release']:
            self.mutate_host=lambda h,k=field:h['uname'].update({k:'wrong'})
            with self.subTest(field=field),self.assertRaises(ValueError):self.admit()
        self.assertEqual(self.ledger.value['waits'],[])

    def test_foreign_and_malformed_census_immediate(self):
        self.loads=[[6.,6.,0.]]
        for raw in ['',INIT+INIT,INIT+'901 1 1 0.0 1 S 901 compile\n',INIT+'901 1 1 NaN 1 S 901 python3\n']:
            self.raw=raw
            with self.subTest(raw=raw),self.assertRaises(ValueError):self.admit()
        self.assertEqual(self.ledger.value['waits'],[])

    def test_source_and_binary_change_during_wait(self):
        original_sleep=self.sleep
        for field in ['source','binary']:
            self.loads=[[6.,1.,0.],[1.,1.,0.]];self.calls=0
            target=self.state['candidate']['source']/'input.go' if field=='source' else self.state['candidate']['binary']
            original=target.read_bytes()
            def mutate(seconds):original_sleep(seconds);target.write_bytes(b'changed during wait\n')
            with patch('collect.time.sleep',side_effect=mutate),self.subTest(field=field),self.assertRaisesRegex(ValueError,field+' drift'):
                self.admit()
            target.write_bytes(original)
        self.assertEqual(self.receipts,[])

    def test_total_budget_not_renewed_between_leaves(self):
        self.loads=[[6.,1.,0.],[1.,1.,0.]];self.leaf()
        first_total=self.ledger.value['total_wait_ns'];self.assertGreater(first_total,0)
        self.loads=[[6.,1.,0.]];self.calls=0
        with self.assertRaisesRegex(ValueError,'budget exhausted') as caught:self.admit()
        self.finish(caught.exception);result=self.check()
        self.assertGreaterEqual(result['total_wait_ns'],600_000_000_000)
        self.assertEqual(len(self.receipts),1)
        self.assertLess(self.ledger.value['waits'][-1]['requested_ns'],5_000_000_000)
        slept=sum(w['completed_monotonic_ns']-w['started_monotonic_ns'] for w in self.ledger.value['waits'])
        self.assertGreater(result['total_wait_ns'],slept)

    def test_cancel_wait_keeps_incremental_trace_and_no_child(self):
        self.loads=[[6.,1.,0.]];self.cancel_wait=True
        with self.assertRaises(KeyboardInterrupt) as caught:self.admit()
        self.finish(caught.exception);self.check()
        self.assertEqual(len(self.ledger.value['probes']),1)
        self.assertGreater(self.ledger.value['total_wait_ns'],0)
        self.assertLess(self.ledger.value['total_wait_ns'],5_000_000_000)
        self.assertEqual(self.receipts,[])

    def test_refused_census_failure_trace_audits_without_accepting_timings(self):
        self.raw=INIT+'901 1 1 0.0 1 S 901 compile\n'
        with self.assertRaises(ValueError) as caught:self.admit()
        self.finish(caught.exception);self.assertEqual(self.check()['status'],'failed')

    def test_cancel_during_preflight_retains_unadmitted_probe(self):
        with patch('collect.drift',side_effect=KeyboardInterrupt('synthetic preflight cancellation')):
            with self.assertRaises(KeyboardInterrupt) as caught:self.admit()
        self.finish(caught.exception);self.check()
        self.assertEqual(self.ledger.value['probes'][1]['decision'],'refused')
        self.assertIsNone(self.ledger.value['probes'][1]['host_sha256'])
        self.assertEqual(self.receipts,[])

    def test_scheduler_oversleep_never_admits_after_budget(self):
        self.loads=[[6.,1.,0.],[1.,1.,0.]]
        def oversleep(seconds):self.clock+=601_000_000_000
        with patch('collect.time.sleep',side_effect=oversleep),self.assertRaisesRegex(ValueError,'budget exhausted') as caught:
            self.admit()
        self.finish(caught.exception);self.check()
        self.assertEqual(self.calls,1)
        self.assertEqual(self.receipts,[])

    def test_long_delayed_cancellation_remains_truthful_failure(self):
        self.loads=[[6.,1.,0.]]
        def cancel(seconds):self.clock+=601_000_000_000;cancel_collector(15,None)
        with patch('collect.time.sleep',side_effect=cancel),self.assertRaises(KeyboardInterrupt) as caught:self.admit()
        self.finish(caught.exception);self.check()
        self.assertEqual(self.receipts,[])

    def test_source_mode_change_during_wait_refuses(self):
        target=self.state['candidate']['source']/'input.go';mode=target.stat().st_mode
        def mutate(seconds):self.sleep(seconds);target.chmod(0o755)
        self.loads=[[6.,1.,0.],[1.,1.,0.]]
        with patch('collect.time.sleep',side_effect=mutate),self.assertRaisesRegex(ValueError,'source drift') as caught:
            self.admit()
        target.chmod(mode);self.finish(caught.exception);self.check()

    def test_resealed_rejected_census_cannot_hide_foreign_work(self):
        self.loads=[[6.,1.,0.],[1.,1.,0.]];self.complete()
        p=self.ledger.value['probes'][0];raw=self.out/(p['prefix']+'-processes.txt')
        raw.write_text(INIT+'999 1 1 0.0 1 S 999 compile\n')
        host=self.out/(p['prefix']+'-host.json');h=json.loads(host.read_text());h['processes_sha256']=sha(raw)
        write(host,h);p['host_sha256']=sha(host);self.reseal()
        with self.assertRaisesRegex(ValueError,'bypasses original gates'):self.check()

    def test_failed_packet_analyzer_still_refuses_before_reading_rows(self):
        import analyze
        write(self.out/'failure.json',{'type':'ValueError','error':'synthetic failure'})
        with patch('sys.argv',['analyze.py',str(self.out)]),self.assertRaisesRegex(ValueError,'failed capture retained'):
            analyze.main()

    def test_postleaf_load_and_census_stay_fatal(self):
        self.leaf()
        h=copy.deepcopy(self.receipts[0]['before']);h['load']=[5.0001,1,0]
        with self.assertRaisesRegex(ValueError,'host contention'):host_gate(h,self.host)
        from protocol import process_census
        with self.assertRaisesRegex(ValueError,'foreign active'):process_census(INIT+'901 1 1 0.0 1 S 901 go\n')
        self.assertEqual(self.ledger.value['waits'],[])

    def test_missing_tampered_reordered_resealed_probes_and_admissions(self):
        self.loads=[[6.,1.,0.],[1.,1.,0.]];self.complete()
        original=copy.deepcopy(self.ledger.value);receipts=copy.deepcopy(self.receipts)
        mutations=[lambda v:v['probes'].pop(0),lambda v:v['probes'].reverse(),
                   lambda v:v['waits'].clear(),lambda v:v.update(total_wait_ns=0),
                   lambda v:v['probes'][0].update(decision='admitted'),
                   lambda v:v['waits'][0].update(requested_ns=1),
                   lambda v:v['probes'][2].update(preflight={}),
                   lambda v:v['probes'][2].update(preflight=None),
                   lambda v:v['blocked_intervals'].clear(),
                   lambda v:v['blocked_intervals'][0].update(started_monotonic_ns=v['blocked_intervals'][0]['started_monotonic_ns']+1),
                   lambda v:v['blocked_intervals'][0].update(completed_monotonic_ns=v['blocked_intervals'][0]['completed_monotonic_ns']-1),
                   lambda v:v['probes'][1].update(prefix='../escape'),
                   lambda v:v['probes'][1].update(started_monotonic_ns=True)]
        for mutate in mutations:
            self.ledger.value=copy.deepcopy(original);mutate(self.ledger.value);self.reseal()
            with self.assertRaises(ValueError):self.check()
        self.ledger.value=original;self.reseal()
        for field,value in [('readiness_probe_index',0),('readiness_probe_sha256','0'*64),('spawn_monotonic_ns',1)]:
            self.receipts=copy.deepcopy(receipts);self.receipts[0][field]=value
            with self.assertRaises(ValueError):self.check()
        self.receipts=receipts
        p=self.out/(original['probes'][0]['prefix']+'-processes.txt');p.write_text(INIT+'999 1 1 0.0 1 S 999 compile\n')
        with self.assertRaisesRegex(ValueError,'snapshot drift'):self.check()
        p.unlink()
        with self.assertRaises(ValueError):self.check()

    def test_extra_readiness_raw_and_ledger_tamper(self):
        self.complete()
        extra=self.out/'extra-readiness-000999-processes.txt';extra.write_text(INIT)
        with self.assertRaisesRegex(ValueError,'extra readiness'):self.check()
        extra.unlink();(self.out/'readiness.json').write_text('{}')
        with self.assertRaisesRegex(ValueError,'tampered readiness'):self.check()

class PolicyTests(unittest.TestCase):
    def test_fixed_typed_finite_policy_and_unchanged_matrix(self):
        validate_load_readiness(dict(LOAD_READINESS))
        for field in ['poll_seconds','total_wait_seconds']:
            for x in [True,False,0,-1,1.5,float('inf'),float('nan'),'5',None,[],6000]:
                with self.subTest(field=field,value=x),self.assertRaises(ValueError):
                    validate_load_readiness(dict(LOAD_READINESS,**{field:x}))
        for field in ['max_load1','max_load5']:
            for x in [True,4,5.,4.5001,float('inf'),float('nan'),'4.5',None]:
                with self.subTest(field=field,value=x),self.assertRaises(ValueError):
                    validate_load_readiness(dict(LOAD_READINESS,**{field:x}))
        for x in [None,{},dict(LOAD_READINESS,extra=True),dict(LOAD_READINESS,schema='old')]:
            with self.assertRaises(ValueError):validate_load_readiness(x)
        c=draft();s=list(schedule(c));self.assertEqual(len(c['cases']),54);self.assertEqual(len(s),756)
        self.assertEqual(sum(x['phase']=='warmup' for x in s),108)
        self.assertEqual(sum(x['phase']=='measured' for x in s),648)
        self.assertEqual(c['environment']['GOMAXPROCS'],'4')
        self.assertTrue(all(x['iterations']==1024 and x['warmup_iterations']==128 for x in c['cases']))

if __name__=='__main__':unittest.main()
