"""Small fake-process rehearsal; no databases, Go builds or retained timings."""
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import unified_bench_quicksilver_capture as capture


def report(reads=10000):
    stats = {'treedb.profile.resolved': 'command_wal_durable', 'treedb.durability_mode': 'wal_on_sync',
             'treedb.profile.ordinary_ack_class': 'durable_wal_prefix', 'treedb.profile.production': 'true',
             'treedb.profile.bench_unsafe': 'false', 'treedb.vlog.read_integrity': 'verify',
             'treedb.negative_lookup_filter.active_bytes': '0'}
    phases = []
    mixed_present = 4 if reads == 16 else reads//10
    for name, present in zip(capture.PHASES, (reads, 0, mixed_present, mixed_present)):
        absent = reads-present
        kinds = [absent//3 + (i < absent%3) for i in range(3)]
        phases.append(dict(name=name, ops=reads, seconds=1., composition_seconds=1., ops_per_sec=float(reads),
                           requested_present=present, requested_absent=absent, observed_hits=present,
                           distinct_present_requests=present, distinct_absent_requests=absent, distinct_accesses=reads,
                           miss_kind_requests_arbitrary_common_prefix_deleted=kinds,
                           p50_us=1., p95_us=2., p99_us=3., p999_us=4., max_us=5.,
                           process_bytes_per_op=0., process_allocated_bytes=0, process_allocs_per_op=0., process_mallocs=0,
                           stats_before=stats, stats_after=stats))
    group = dict(records=4096, raw_bytes=131072, compressed_bytes=65536, compressed_to_raw_ratio=.5)
    return dict(engine='treedb', gomaxprocs=12, profiled=False,
                config=dict(case='realistic', keys=40000, aggregate_reads=reads, workers=4, updates=40000,
                            reads_per_snapshot=64, concurrent_duration_ns=8000000000, commit_mode='ordinary',
                            barrier_policy='initial/final Checkpoint; up to four separately timed concurrent Checkpoints at mutation-batch quarters',
                            generation='generic-v1', seed=24, mixture='primary', working_set='uniform', miss_percent=90),
                updated_keys=40000, update_batch_ms=[1.]*40, checkpoint_ms=[1.]*4, mutation_commit_batches=160,
                mutations=dict(updates=10000, deletes=10000, inserts=10000, overwrite_targets=10000, overwrite_sets=40000),
                initial_verified_keys=40000, initial_verified_misses=80400, verified_keys=40000, verified_misses=90400,
                trace_bytes=0, oracle_state_bytes=40000, distinct_tracking_bytes=125000,
                loaded_distribution=dict(small_medium_large_values=[40000, 0, 0], namespace_hostname_opaque_keys=[13334, 13333, 13333],
                                         structured_values=40000, opaque_values=0, min_key_bytes=32, max_key_bytes=32, key_bytes=1280000,
                                         min_value_bytes=32, max_value_bytes=32, value_bytes=1280000),
                compressibility=dict(codec='stdlib DEFLATE BestSpeed',
                                     basis='up to 4096 distinct loaded records, seeded coprime-stride sample; each record compressed independently including identity/generation header; setup only, not an engine codec claim',
                                     total=group, structured=group, opaque=dict(records=0, raw_bytes=0, compressed_bytes=0, compressed_to_raw_ratio=0.),
                                     sample_small_medium_large_records=[4096, 0, 0]),
                registered_cli_flags={'profile': 'durable', 'keep': 'false', 'quicksilver-duration': '8s', 'treedb-disable-read-checksum': 'false'},
                initial_stats=stats, final_stats=stats, phases=phases, load_seconds=1., initial_checkpoint_ms=1., reopen_ms=1.,
                final_checkpoint_ms=1., final_reopen_ms=1., initial_files={}, final_files={})


class CaptureRehearsal(unittest.TestCase):
    def test_churn_contract(self):
        self.assertEqual(capture.churn_settings(dict(engine='treedb', churn_rounds=2)), (2, 6000000000))
        for cell in (dict(churn_rounds=-1), dict(churn_rounds=33), dict(churn_rounds=True),
                     dict(churn_pause='6s'), dict(engine='lmdb', churn_rounds=2),
                     dict(engine='treedb', case='random4k', churn_rounds=2),
                     dict(engine='treedb', churn_rounds=2, churn_pause_ns=-1),
                     dict(engine='treedb', churn_rounds=2, churn_pause_ns=60000000001)):
            with self.assertRaises(AssertionError):
                capture.churn_settings(cell)
        packet = report()
        cell = dict(engine='treedb', keys=40000, reads=10000, churn_rounds=2)
        packet['config'].update(churn_rounds=2, churn_pause_ns=6000000000,churn_shape="full-refresh")
        packet['registered_cli_flags'].update({'quicksilver-churn-rounds': '2', 'quicksilver-churn-pause': '6s', 'max-wall': '30m12s', 'quicksilver-churn-shape':'full-refresh'})
        snapshot = dict(captured_at_unix_nano=1,process_heap_alloc_bytes=1, process_rss_bytes=2, process_rss_supported=True,
                        engine_stats=packet['final_stats'])
        round = dict(round=1, restored_keys=40000,restore_commit_batches=40, mutation_targets=40000, mutation_commit_batches=160,
                     mutations=packet['mutations'], verified_keys=40000, verified_misses=90400,
                     wall_seconds=10., write_seconds=1., checkpoint_seconds=1., verification_seconds=1.,
                     pause_seconds=6., pause_started_unix_nano=1000000001, pause_finished_unix_nano=7000000001,
                     before=snapshot, after=dict(snapshot, captured_at_unix_nano=8000000001))
        packet['maintenance_churn'] = dict(shape='full-refresh',writer_semantics='insert identities already exist',label='bounded write/automatic-maintenance characterization; not warmed read throughput or a steady-state bound',
                                          leaf_generation_pack_maintenance_env='1', rounds=[round, dict(round, round=2, pause_started_unix_nano=11000000001, pause_finished_unix_nano=17000000001,
                                                              before=dict(snapshot, captured_at_unix_nano=10000000001),
                                                              after=dict(snapshot, captured_at_unix_nano=18000000001))],
                                          final_files_after_close={'maindb/index.db': 4096})
        metadata = dict(env={'TREEDB_ENABLE_LEAF_GENERATION_PACK_MAINTENANCE': '1'})
        with tempfile.TemporaryDirectory() as temp:
            capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)
            for section, field, value in ((round, 'verified_keys', 1), (round, 'round', 2),
                                           (round, 'pause_seconds', 5.), (round, 'mutation_commit_batches', 1),
                                           (round, 'pause_started_unix_nano', True), (round, 'pause_started_unix_nano', 0),
                                           (round, 'pause_finished_unix_nano', float('inf')),
                                           (round, 'pause_finished_unix_nano', 2**63),
                                           (round, 'pause_finished_unix_nano', 999999999),
                                           (round, 'pause_finished_unix_nano', 7002000001),
                                           (round, 'pause_started_unix_nano', -1),
                                           (snapshot, 'captured_at_unix_nano', 1000000002),
                                           (snapshot, 'process_rss_supported', False),
                                           (packet['maintenance_churn'], 'leaf_generation_pack_maintenance_env', '')):
                original = section[field]
                section[field] = value
                with self.assertRaises(AssertionError):
                    capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)
                section[field] = original
            end = round['pause_finished_unix_nano']
            round['pause_finished_unix_nano'] = end+500000
            capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)
            round['pause_finished_unix_nano'] = end
            after = round['after']['captured_at_unix_nano']
            round['after']['captured_at_unix_nano'] = round['pause_finished_unix_nano']-1
            with self.assertRaises(AssertionError):
                capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)
            round['after']['captured_at_unix_nano'] = after
            second = packet['maintenance_churn']['rounds'][1]
            second_start = second['pause_started_unix_nano']
            second['pause_started_unix_nano'] = round['pause_started_unix_nano']
            with self.assertRaises(AssertionError):
                capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)
            second['pause_started_unix_nano'] = second_start
            marker = round.pop('pause_started_unix_nano')
            with self.assertRaises(KeyError):
                capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)
            round['pause_started_unix_nano'] = marker
            original = packet['registered_cli_flags']['max-wall']
            packet['registered_cli_flags']['max-wall'] = '30m0s'
            with self.assertRaises(AssertionError):
                capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)
            packet['registered_cli_flags']['max-wall'] = original
            packet['maintenance_churn']['rounds'].pop()
            with self.assertRaises(AssertionError):
                capture.validate([packet], cell, pathlib.Path(temp), {}, metadata)

    def test_sparse_and_retained_settings(self):
        self.assertEqual(capture.churn_settings(dict(engine='treedb',churn_rounds=2,churn_shape='sparse')), (2,6000000000))
        for cell in (dict(engine='treedb',churn_shape='sparse'), dict(engine='treedb',churn_rounds=2,churn_shape='wrong')):
            with self.assertRaises(AssertionError):
                capture.churn_settings(cell)
        for interval in (0,True,60001,'100'):
            with self.assertRaises(AssertionError):
                capture.sampling_interval(dict(rss_sample_interval_ms=interval))
        with tempfile.TemporaryDirectory() as temp:
            retained=pathlib.Path(temp)/'fixture'
            retained.mkdir()
            cell=dict(engine='treedb',keys=40000,reads=10000,measure_dir=str(retained))
            with self.assertRaises(AssertionError):
                capture.retained_settings(cell)
            (retained/'maindb').mkdir()
            (retained/'maindb/index.db').write_bytes(b'marker')
            packet=report()
            packet['config'].update(final_fixture=True,barrier_policy='retained pre-oracle; final Checkpoint; up to four separately timed concurrent Checkpoints at mutation-batch quarters')
            packet.update(data_dir=str(retained),measurement_state='retained final fixture; pre/post full oracle; no population reload or restore',
                          writer_semantics='idempotent repeated mutation schedule; insert identities already exist and are SET again',
                          load_seconds=0,deleted_preparation_seconds=0,initial_checkpoint_ms=0,reopen_ms=0,initial_verified_misses=90400)
            packet['registered_cli_flags']['quicksilver-measure-dir']=str(retained)
            metadata=dict(env={})
            capture.validate([packet],cell,pathlib.Path(temp),{},metadata)
            self.assertEqual(metadata['retained_fixture']['directory'],str(retained.resolve()))
            packet['initial_verified_misses']=80400
            with self.assertRaises(AssertionError):
                capture.validate([packet],cell,pathlib.Path(temp),{},metadata)
            for bad in (dict(cell,churn_rounds=1),dict(cell,case='structured256'),dict(cell,measure_dir='relative')):
                with self.assertRaises(AssertionError):
                    capture.retained_settings(bad)

    def test_churn_pause_pair_preflight(self):
        for text, ns in (('6s', 6000000000), ('1m', 60000000000), ('1m0s', 60000000000),
                         ('1ms', 1000000), ('2ns', 2), ('1.5µs', 1500), ('1.000000001s', 1000000001)):
            cell = dict(engine='treedb', churn_rounds=2, churn_pause=text, churn_pause_ns=ns)
            self.assertEqual(capture.churn_settings(cell), (2, ns))
        for text, ns in (('1m', 1), ('6s', 1), ('1ns', 60000000000), ('0.5s', 500000001),
                         ('30s30s', 60000000000), ('-1s', 1), ('', 1), ('9'*1000+'s', 1),
                         (6000000000, 6000000000)):
            with self.assertRaisesRegex(AssertionError, 'text must match nanoseconds'):
                capture.churn_settings(dict(engine='treedb', churn_rounds=2, churn_pause=text, churn_pause_ns=ns))
        with self.assertRaisesRegex(AssertionError, 'text must match nanoseconds'):
            capture.churn_settings(dict(engine='treedb', churn_rounds=2, churn_pause_ns=1))
        # Exercise the shared main preflight: hostile pairs stop before any
        # benchmark or native-library subprocess, without constructing a DB.
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            receipt = root/'receipt.json'
            receipt.write_text('{}')
            manifest = dict(build_env={'GOWORK': 'off'}, libraries={}, sources={},
                            receipts={role: dict(path='receipt.json', sha256=capture.sha256(receipt))
                                      for role in ('source', 'build', 'native', 'runner')})
            plan = dict(output='invalid', cells=[dict(label='invalid', engine='treedb', churn_rounds=32,
                                                      churn_pause='1m', churn_pause_ns=1)])
            manifest_path, plan_path = root/'manifest.json', root/'plan.json'
            manifest_path.write_text(json.dumps(manifest))
            plan_path.write_text(json.dumps(plan))
            with mock.patch.object(sys, 'argv', ['capture', str(manifest_path), str(plan_path)]), mock.patch.object(capture.subprocess, 'run') as run:
                with self.assertRaisesRegex(AssertionError, 'text must match nanoseconds'):
                    capture.main()
                run.assert_not_called()

    def test_churn_wall_allowance_maximum_and_precision(self):
        self.assertEqual(capture.capture_wall_limit(dict(engine='treedb')), '30m')
        self.assertEqual(capture.capture_wall_limit(dict(engine='treedb', churn_rounds=2)), '30m12s')
        self.assertEqual(capture.capture_wall_limit(dict(engine='treedb', churn_rounds=32,
                                                       churn_pause='2ns', churn_pause_ns=2)), '30m0.000000064s')
        cell = dict(engine='treedb', keys=40000, reads=10000, churn_rounds=32,
                    churn_pause='1m0s', churn_pause_ns=60000000000)
        self.assertEqual(capture.capture_wall_limit(cell), '1h2m0s')
        packet = report()
        packet['config'].update(churn_rounds=32, churn_pause_ns=60000000000,churn_shape="full-refresh")
        packet['registered_cli_flags'].update({'quicksilver-churn-rounds': '32',
                                               'quicksilver-churn-pause': '1m0s', 'max-wall': '1h2m0s', 'quicksilver-churn-shape':'full-refresh'})
        snapshot = dict(captured_at_unix_nano=1,process_heap_alloc_bytes=1, process_rss_bytes=2, process_rss_supported=True,
                        engine_stats=packet['final_stats'])
        rounds = [dict(round=i, restored_keys=40000,restore_commit_batches=40, mutation_targets=40000, mutation_commit_batches=160,
                       mutations=packet['mutations'], verified_keys=40000, verified_misses=90400,
                       wall_seconds=64., write_seconds=1., checkpoint_seconds=1., verification_seconds=1.,
                       pause_seconds=60., pause_started_unix_nano=(i-1)*70000000000+1000000001,
                       pause_finished_unix_nano=(i-1)*70000000000+61000000001,
                       before=dict(snapshot, captured_at_unix_nano=(i-1)*70000000000+1),
                       after=dict(snapshot, captured_at_unix_nano=(i-1)*70000000000+62000000001)) for i in range(1, 33)]
        packet['maintenance_churn'] = dict(shape='full-refresh',writer_semantics='insert identities already exist',label='bounded write/automatic-maintenance characterization; not warmed read throughput or a steady-state bound',
                                          leaf_generation_pack_maintenance_env='', rounds=rounds,
                                          final_files_after_close={'maindb/index.db': 4096})
        with tempfile.TemporaryDirectory() as temp:
            capture.validate([packet], cell, pathlib.Path(temp), {}, dict(env={}))
            cell['churn_pause'] = '1m'
            capture.validate([packet], cell, pathlib.Path(temp), {}, dict(env={}))
            packet['registered_cli_flags']['max-wall'] = '30m0s'
            with self.assertRaises(AssertionError):
                capture.validate([packet], cell, pathlib.Path(temp), {}, dict(env={}))

    def test_retained_database_contract(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = pathlib.Path(temp)/'evidence'/'capture'/'1-treedb'
            directory.mkdir(parents=True)
            retained = pathlib.Path(temp)/'working-dbs'/'bench-quicksilver-treedb-test'
            retained.mkdir(parents=True)
            packet = report()
            packet['registered_cli_flags']['keep'] = 'true'
            packet['data_dir'] = str(retained)
            cell = dict(engine='treedb', keys=40000, reads=10000, keep=True)
            metadata = {'env': {'TMPDIR': str(retained.parent)}}
            capture.validate([packet], cell, directory, {}, metadata)
            packet['data_dir'] = str(directory)
            with self.assertRaises(AssertionError):
                capture.validate([packet], cell, directory, {}, metadata)

    def test_process_capture_rejects_wrong_contract_and_native_identity_keeps_raw(self):
        real_run = subprocess.run
        controls = dict(GOMEMLIMIT='off', GOGC='100', GOMAXPROCS='12',
                        TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES='1073741824')

        def timed_run(command, **kwargs):
            if command[0] == '/usr/bin/ldd':
                self.assertEqual(command, ['/usr/bin/ldd', str(binary)])
                self.assertEqual(kwargs['env']['LD_LIBRARY_PATH'], str(root))
                if scenario in ('static', 'script'):
                    kwargs['stderr'].write('not a dynamic executable\n')
                    return subprocess.CompletedProcess(command, 1)
                resolved = other if scenario == 'wrong-path' else library
                if scenario == 'wrong-hash':
                    library.write_bytes(b'changed after manifest preflight')
                kwargs['stdout'].write(f'linux-vdso.so.1 (0x1234)\nlibc.so.6 => {resolved} (0x2345)\n{loader} (0x3456)\n')
                return subprocess.CompletedProcess(command, 0)
            self.assertEqual({k: kwargs['env'][k] for k in controls}, controls)
            self.assertEqual(command[:3], ['/usr/bin/time', '-v', str(root/'bin'/'fake-bench')])
            # Controlled ELF/ldd fixture avoids a native build. Execute its output
            # producer in a fresh real Python process; no claim of native timing.
            kwargs['env'] = {k: v for k, v in kwargs['env'].items() if not k.startswith('LD_')}
            return real_run([sys.executable, str(root/'fake-engine.py'), *command[3:]], **kwargs)

        def sampled_run(command,expected_executable,sample_path,interval_ms,**kwargs):
            self.assertEqual(expected_executable,binary)
            self.assertEqual(interval_ms,100)
            result=timed_run(command,**kwargs)
            sample_path.write_text('{"fake_rehearsal_only":true}\n')
            summary=dict(complete=scenario!='sampling-error',samples=1,interval_ms=interval_ms)
            pathlib.Path(str(sample_path)+'.summary.json').write_text(json.dumps(summary))
            return result,summary

        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            (root/'bin').mkdir()
            binary = root/'bin'/'fake-bench'
            binary.write_bytes(b'\x7fELFcontrolled test placeholder')
            (root/'fake-engine.py').write_text('import pathlib, sys\nsys.stderr.write("fake raw stderr\\n")\nprint(pathlib.Path("payload.json").read_text())\n')
            binary.chmod(0o755)
            library, loader, other = root/'libc.so.6', root/'ld-linux.so.2', root/'other-libc.so.6'
            loader.write_bytes(b'approved loader')
            other.write_bytes(b'unapproved runtime')
            receipt = root/'receipt.json'
            receipt.write_text('{"scope":"fake-process rehearsal only"}')
            manifest = dict(build_env={'GOWORK': 'off', 'LD_LIBRARY_PATH': str(root), 'GOMEMLIMIT': '2GiB',
                                       'GOGC': '25', 'TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES': '77'}, libraries={},
                            sources={'fake': dict(head='0'*40, binary='fake-bench', binary_sha256=capture.sha256(binary))},
                            receipts={role: dict(path='receipt.json', sha256=capture.sha256(receipt)) for role in ('source', 'build', 'native', 'runner')})
            manifest_path, plan_path = root/'manifest.json', root/'plan.json'
            cell = dict(label='fake', source='fake', engine='treedb', keys=40000)
            ratio_scenarios = ('ratio-mixed', 'ratio-concurrent', 'zero', 'full')
            for scenario in ('dynamic', 'static', 'tiny', 'sampling', 'sampling-error', 'commits', *ratio_scenarios, 'wrong-path', 'wrong-hash', 'ambient-preload', 'manifest-audit', 'script'):
                commits = 40 if scenario == 'commits' else 160
                accepted = scenario in ('dynamic', 'static', 'tiny', 'sampling')
                cell.pop('rss_sample_interval_ms',None)
                if scenario.startswith('sampling'):
                    cell['rss_sample_interval_ms']=100
                binary.write_bytes(b'#!/bin/sh\n' if scenario == 'script' else b'\x7fELFcontrolled test placeholder')
                manifest['sources']['fake'].update(linkage='static' if scenario in ('static', 'script') else 'dynamic', binary_sha256=capture.sha256(binary))
                library.write_bytes(b'approved runtime')
                manifest['libraries'] = {str(path): dict(sha256=capture.sha256(path)) for path in (library, loader)}
                manifest['build_env']['LD_AUDIT'] = '/injected.so' if scenario == 'manifest-audit' else ''
                manifest_path.write_text(json.dumps(manifest))
                cell['reads'] = 16 if scenario == 'tiny' else 10000
                cell['miss_percent'] = 0 if scenario == 'zero' else 100 if scenario == 'full' else 90
                packet = report(cell['reads'])
                packet['mutation_commit_batches'] = commits
                packet['config']['miss_percent'] = cell['miss_percent']
                if scenario in ('ratio-mixed', 'ratio-concurrent'):
                    phase = packet['phases'][2 if scenario == 'ratio-mixed' else 3]
                    phase.update(requested_present=2500, requested_absent=7500, observed_hits=2500,
                                 distinct_present_requests=2500, distinct_absent_requests=7500,
                                 miss_kind_requests_arbitrary_common_prefix_deleted=[2500]*3)
                (root/'payload.json').write_text(json.dumps([packet]))
                plan_path.write_text(json.dumps(dict(output=scenario, cells=[cell])))
                clean_loader = {k: '' for k in os.environ if k.startswith('LD_')}
                clean_loader.update(GOMEMLIMIT='512MiB', GOGC='50')
                clean_loader['LD_PRELOAD'] = '/injected.so' if scenario == 'ambient-preload' else ''
                with mock.patch.dict(os.environ, clean_loader), mock.patch.object(sys, 'argv', ['capture', str(manifest_path), str(plan_path)]), mock.patch.object(capture.subprocess, 'run', timed_run), mock.patch.object(capture, 'run_with_rss', sampled_run):
                    if accepted:
                        capture.main()
                    else:
                        with self.assertRaises(AssertionError):
                            capture.main()
                directory = root/scenario/'1-fake'
                metadata = json.loads((directory/'run.json').read_text())
                self.assertEqual(metadata.get('validated', False), accepted)
                self.assertEqual({k: metadata['env'][k] for k in controls}, controls)
                self.assertEqual(metadata['plan_sha256'], hashlib.sha256(plan_path.read_bytes()).hexdigest())
                if accepted or scenario in ('commits','sampling-error') or scenario in ratio_scenarios:
                    self.assertEqual(metadata['rc'], 0)
                    self.assertEqual(json.loads((directory/'stdout.json').read_text())[0]['mutation_commit_batches'], commits)
                    self.assertIn('fake raw stderr', (directory/'stderr.log').read_text())
                    if scenario in ratio_scenarios:
                        self.assertIn('configured miss ratio rejected', metadata['error'])
                        self.assertEqual(metadata['miss_ratio_validation'][-1]['requested_absent'],
                                         7500 if scenario.startswith('ratio-') else 9000)
                    if scenario == 'tiny':
                        self.assertEqual(metadata['miss_ratio_validation'][0]['requested_absent'], 12)
                        self.assertGreater(metadata['miss_ratio_validation'][0]['tolerance_requests'], 2.4)
                else:
                    self.assertNotIn('rc', metadata)
                    self.assertEqual((directory/'stdout.json').read_text(), '')
                    self.assertIn('error', metadata)
                self.assertEqual(metadata['rss_observer_sha256'],capture.sha256(pathlib.Path(capture.owned_process_rss.__file__)))
                if scenario.startswith('sampling'):
                    self.assertEqual(metadata['rss_sampling']['complete'],accepted)
                    self.assertEqual(metadata['rss_sampling']['samples'],1)
                    self.assertTrue((directory/'rss_samples.jsonl').is_file())
                    self.assertTrue((directory/'rss_samples.jsonl.summary.json').is_file())
                    if not accepted:
                        self.assertIn('incomplete owned-process RSS sampling',metadata['error'])
                resolution = metadata['native_resolution']
                self.assertEqual(resolution['loader_env']['LD_LIBRARY_PATH'], str(root))
                if scenario in ('ambient-preload', 'manifest-audit', 'script'):
                    self.assertNotIn('rc', resolution)
                    self.assertEqual((directory/'ldd.stdout.txt').read_text(), '')
                    if scenario == 'script':
                        self.assertIn('benchmark executable must be ELF', metadata['error'])
                    else:
                        variable = 'LD_PRELOAD' if scenario == 'ambient-preload' else 'LD_AUDIT'
                        self.assertEqual(resolution['loader_env'][variable], '/injected.so')
                elif scenario == 'static':
                    self.assertEqual(resolution['rc'], 1)
                    self.assertIn('not a dynamic executable', (directory/'ldd.stderr.txt').read_text())
                else:
                    self.assertEqual(resolution['rc'], 0)
                    self.assertIn('libc.so.6 =>', (directory/'ldd.stdout.txt').read_text())
                if scenario == 'dynamic':
                    self.assertEqual(resolution['libraries'], {str(path): capture.sha256(path) for path in (library, loader)})
                if scenario == 'wrong-path':
                    self.assertIn(str(other), resolution['libraries'])
                if scenario == 'wrong-hash':
                    self.assertNotEqual(resolution['libraries'][str(library)], manifest['libraries'][str(library)]['sha256'])
                self.assertIn('finished', metadata)
            optimized = real_run([sys.executable, '-O', str(pathlib.Path(capture.__file__)), str(manifest_path), str(plan_path)], capture_output=True, text=True)
            self.assertNotEqual(optimized.returncode, 0)
            self.assertIn('do not use -O', optimized.stderr)


if __name__ == '__main__':
    unittest.main()
