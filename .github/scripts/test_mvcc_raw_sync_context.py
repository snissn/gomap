#!/usr/bin/env python3
"""Fixture-only tests. No benchmarks or host tracing."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('context', ROOT / 'scripts/mvcc_raw_sync_context.py')
context = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(context)


class ContextTests(unittest.TestCase):
    def test_missing_and_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'counter'
            self.assertEqual(context.read_counter(path)['status'], 'unavailable')
            path.write_bytes(b'x' * (context.LIMIT + 2))
            result = context.read_counter(path)
            self.assertEqual(result['status'], 'truncated')
            self.assertEqual(len(result['text']), context.LIMIT)
            link = Path(directory) / 'link'
            link.symlink_to(path)
            self.assertEqual(context.read_counter(link)['status'], 'unavailable')

    def test_mount_identity_and_cgroup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proc = root / 'proc'
            (proc / 'self').mkdir(parents=True)
            group = root / 'cg' / 'sample'
            group.mkdir(parents=True)
            (group / 'io.stat').write_text('8:0 wbytes=10\n')
            (proc / 'self/cgroup').write_text('0::/sample\n')
            (proc / 'self/mountinfo').write_text(
                f'1 0 8:0 / / rw - ext4 /dev/fake rw\n2 1 0:1 / {root}/cg rw - cgroup2 cgroup rw\n')
            fixture = context.fixture_identity(proc, root)
            self.assertEqual(fixture['parent_mount']['source'], '/dev/fake')
            self.assertEqual(fixture['actual_fixture'], 'unavailable')
            self.assertEqual(context.snapshot(proc)['cgroup_counters']['io.stat']['text'], '8:0 wbytes=10\n')

    def test_child_output_failure_and_exclusive_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'context.json'
            code = 'import sys; print("original sample"); sys.exit(7)'
            command = [sys.executable, str(ROOT / 'scripts/mvcc_raw_sync_context.py'),
                       '--output', str(output), '--binary', sys.executable,
                       '--source-sha', 'a' * 40, '--source-tree', 'b' * 40,
                       '--', sys.executable, '-c', code]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 7)
            self.assertEqual(result.stdout, 'original sample\n')
            record = json.loads(output.read_text())
            self.assertEqual(record['child_status'], 7)
            self.assertLessEqual(record['start_monotonic_ns'], record['end_monotonic_ns'])
            original = output.read_bytes()
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(output.read_bytes(), original)
            output.unlink()
            target = root / 'preserved'
            target.write_text('preserved')
            output.symlink_to(target)
            self.assertNotEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(target.read_text(), 'preserved')

    def test_incomplete_mountinfo_withholds_mount_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proc = root / 'proc'
            (proc / 'self').mkdir(parents=True)
            mountinfo = proc / 'self/mountinfo'
            mountinfo.write_text('1 0 8:0 / / rw - ext4 /dev/fake rw\n' +
                                 ' ' * context.LIMIT +
                                 f'2 1 8:1 / {root} rw - ext4 /dev/actual rw\n')
            fixture = context.fixture_identity(proc, root)
            self.assertEqual(fixture['mountinfo_status'], 'truncated')
            self.assertEqual(fixture['parent_mount']['status'], 'unavailable')
            self.assertNotIn('source', fixture['parent_mount'])
            mountinfo.unlink()
            fixture = context.fixture_identity(proc, root)
            self.assertEqual(fixture['mountinfo_status'], 'unavailable')
            self.assertEqual(fixture['parent_mount']['status'], 'unavailable')

    def test_full_driver_refuses_occupied_output_before_any_work(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            occupied = root / 'occupied'
            occupied.mkdir()
            marker = occupied / 'original.json'
            marker.write_text('retained original evidence')
            for output in (occupied, root / 'dangling'):
                if output != occupied:
                    output.symlink_to(root / 'absent')
                environment = dict(os.environ, RAW_SYNC_CONTEXT='1', OUT_DIR=str(output),
                                   BASELINE_HASH='invalid-no-fetch-allowed', CANDIDATE_HASH='HEAD')
                result = subprocess.run(['bash', str(ROOT / 'scripts/mvcc_raw_path_gate.sh')],
                                        env=environment, capture_output=True, text=True)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn('refusing preexisting OUT_DIR', result.stderr)
                self.assertEqual(marker.read_text(), 'retained original evidence')
                self.assertNotIn('fetch', result.stderr)
            self.assertTrue((root / 'dangling').is_symlink())

    def test_missing_proc_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory:
            result = context.snapshot(Path(directory))
            self.assertTrue(all(result[name]['status'] == 'unavailable' for name in context.COUNTERS))
            self.assertEqual(result['cgroup_counters']['status'], 'unavailable')

    def test_off_and_on_command_preservation(self):
        script = (ROOT / 'scripts/mvcc_raw_path_gate.sh').read_text()
        function = script[script.index('run_sample() {'):script.index('\nrun_pair() {')]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shim = root / 'shim'
            shim.mkdir()
            (shim / 'taskset').write_text('#!/bin/sh\nprintf "%s\\n" "$@"\n')
            (shim / 'taskset').chmod(0o755)
            for mode, group in (('0', 'durable_sync'), ('1', 'get_versioned')):
                output = root / f'{mode}-{group}.txt'
                environment = dict(os.environ, PATH=f'{shim}:' + os.environ['PATH'])
                command = f'''set -euo pipefail
RAW_SYNC_CONTEXT={mode}
CPUSET=0
PROCESSES=/dev/null
{function}
run_sample baseline 1 {group} binary regex '{output}' 2s
'''
                subprocess.run(['bash', '-c', command], env=environment, check=True)
                self.assertEqual(output.read_text().splitlines(), ['-c', '0', 'env', 'GOMAXPROCS=1', 'binary', '-test.run', '^$', '-test.bench', 'regex', '-test.benchmem', '-test.benchtime=2s', '-test.count=1'])
            self.assertFalse(list(root.glob('raw-sync*')))


if __name__ == '__main__':
    unittest.main()
