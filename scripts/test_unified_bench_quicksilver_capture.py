"""Small fake-process rehearsal; no databases, Go builds or retained timings."""
import hashlib
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import unified_bench_quicksilver_capture as capture


def report():
    stats = {'treedb.profile.resolved': 'command_wal_durable', 'treedb.durability_mode': 'wal_on_sync',
             'treedb.profile.ordinary_ack_class': 'durable_wal_prefix', 'treedb.profile.production': 'true',
             'treedb.profile.bench_unsafe': 'false', 'treedb.vlog.read_integrity': 'verify',
             'treedb.negative_lookup_filter.active_bytes': '0'}
    phases = []
    for name, present in zip(capture.PHASES, (16, 0, 4, 4)):
        phases.append(dict(name=name, ops=16, seconds=1., composition_seconds=1., ops_per_sec=16.,
                           requested_present=present, requested_absent=16-present, observed_hits=present,
                           distinct_present_requests=present, distinct_absent_requests=16-present, distinct_accesses=16,
                           miss_kind_requests_arbitrary_common_prefix_deleted=([6, 5, 5] if present == 0 else [4, 4, 4] if present == 4 else [0, 0, 0]),
                           p50_us=1., p95_us=2., p99_us=3., p999_us=4., max_us=5.,
                           process_bytes_per_op=0., process_allocated_bytes=0, process_allocs_per_op=0., process_mallocs=0,
                           stats_before=stats, stats_after=stats))
    group = dict(records=4096, raw_bytes=131072, compressed_bytes=65536, compressed_to_raw_ratio=.5)
    return dict(engine='treedb', gomaxprocs=12, profiled=False,
                config=dict(case='realistic', keys=40000, aggregate_reads=16, workers=4, updates=40000,
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
    def test_process_capture_rejects_collapsed_overwrite_commits_and_keeps_raw(self):
        real_run = subprocess.run

        def timed_run(command, **kwargs):
            self.assertEqual(command[:3], ['/usr/bin/time', '-v', str(root/'bin'/'fake-bench')])
            # Darwin time lacks -v. Only replace this measurement prefix; execute
            # the fake binary in a fresh OS process using the real capture files.
            return real_run(command[2:], **kwargs)

        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            (root/'bin').mkdir()
            binary = root/'bin'/'fake-bench'
            binary.write_text('#!'+sys.executable+'\nimport pathlib, sys\nsys.stderr.write("fake raw stderr\\n")\nprint(pathlib.Path("payload.json").read_text())\n')
            binary.chmod(0o755)
            receipt = root/'receipt.json'
            receipt.write_text('{"scope":"fake-process rehearsal only"}')
            manifest = dict(build_env={'GOWORK': 'off'}, libraries={},
                            sources={'fake': dict(head='0'*40, binary='fake-bench', binary_sha256=capture.sha256(binary))},
                            receipts={role: dict(path='receipt.json', sha256=capture.sha256(receipt)) for role in ('source', 'build', 'native', 'runner')})
            manifest_path, plan_path = root/'manifest.json', root/'plan.json'
            manifest_path.write_text(json.dumps(manifest))
            cell = dict(label='fake', source='fake', engine='treedb', keys=40000, reads=16)
            for output, commits in [('accepted', 160), ('rejected', 40)]:
                packet = report()
                packet['mutation_commit_batches'] = commits
                (root/'payload.json').write_text(json.dumps([packet]))
                plan_path.write_text(json.dumps(dict(output=output, cells=[cell])))
                with mock.patch.object(sys, 'argv', ['capture', str(manifest_path), str(plan_path)]), mock.patch.object(capture.subprocess, 'run', timed_run):
                    if commits == 160:
                        capture.main()
                    else:
                        with self.assertRaises(AssertionError):
                            capture.main()
                directory = root/output/'1-fake'
                metadata = json.loads((directory/'run.json').read_text())
                self.assertEqual(metadata['rc'], 0)
                self.assertEqual(metadata.get('validated', False), commits == 160)
                self.assertEqual(metadata['plan_sha256'], hashlib.sha256(plan_path.read_bytes()).hexdigest())
                self.assertEqual(json.loads((directory/'stdout.json').read_text())[0]['mutation_commit_batches'], commits)
                self.assertIn('fake raw stderr', (directory/'stderr.log').read_text())
                self.assertIn('finished', metadata)
            optimized = real_run([sys.executable, '-O', str(pathlib.Path(capture.__file__)), str(manifest_path), str(plan_path)], capture_output=True, text=True)
            self.assertNotEqual(optimized.returncode, 0)
            self.assertIn('do not use -O', optimized.stderr)


if __name__ == '__main__':
    unittest.main()
