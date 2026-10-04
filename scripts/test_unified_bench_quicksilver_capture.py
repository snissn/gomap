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
    def test_process_capture_rejects_wrong_contract_and_native_identity_keeps_raw(self):
        real_run = subprocess.run

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
            self.assertEqual(command[:3], ['/usr/bin/time', '-v', str(root/'bin'/'fake-bench')])
            # Controlled ELF/ldd fixture avoids a native build. Execute its output
            # producer in a fresh real Python process; no claim of native timing.
            kwargs['env'] = {k: v for k, v in kwargs['env'].items() if not k.startswith('LD_')}
            return real_run([sys.executable, str(root/'fake-engine.py'), *command[3:]], **kwargs)

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
            manifest = dict(build_env={'GOWORK': 'off', 'LD_LIBRARY_PATH': str(root)}, libraries={},
                            sources={'fake': dict(head='0'*40, binary='fake-bench', binary_sha256=capture.sha256(binary))},
                            receipts={role: dict(path='receipt.json', sha256=capture.sha256(receipt)) for role in ('source', 'build', 'native', 'runner')})
            manifest_path, plan_path = root/'manifest.json', root/'plan.json'
            cell = dict(label='fake', source='fake', engine='treedb', keys=40000)
            ratio_scenarios = ('ratio-mixed', 'ratio-concurrent', 'zero', 'full')
            for scenario in ('dynamic', 'static', 'tiny', 'commits', *ratio_scenarios, 'wrong-path', 'wrong-hash', 'ambient-preload', 'manifest-audit', 'script'):
                commits = 40 if scenario == 'commits' else 160
                accepted = scenario in ('dynamic', 'static', 'tiny')
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
                clean_loader['LD_PRELOAD'] = '/injected.so' if scenario == 'ambient-preload' else ''
                with mock.patch.dict(os.environ, clean_loader), mock.patch.object(sys, 'argv', ['capture', str(manifest_path), str(plan_path)]), mock.patch.object(capture.subprocess, 'run', timed_run):
                    if accepted:
                        capture.main()
                    else:
                        with self.assertRaises(AssertionError):
                            capture.main()
                directory = root/scenario/'1-fake'
                metadata = json.loads((directory/'run.json').read_text())
                self.assertEqual(metadata.get('validated', False), accepted)
                self.assertEqual(metadata['plan_sha256'], hashlib.sha256(plan_path.read_bytes()).hexdigest())
                if accepted or scenario == 'commits' or scenario in ratio_scenarios:
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
