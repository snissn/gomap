#!/usr/bin/env python3
"""Discriminating source-binding rejection cases for the R1 capture."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('r1_source', Path(__file__).with_name('r1_collection_source.py'))
SOURCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SOURCE)


class SourceBindingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.original = Path.cwd()
        os.chdir(self.temporary.name)
        self.addCleanup(os.chdir, self.original)
        subprocess.run(['git', 'init', '-q'], check=True)
        self.inputs = ['go.mod', 'go.sum', 'TreeDB/db/db.go',
                       'cmd/collection_workload_bench/main.go',
                       'cmd/collection_workload_bench/native_replace.go',
                       'scripts/r1_collection_capture.sh', 'scripts/r1_collection_summary.py',
                       'scripts/r1_collection_source.py', 'TreeDB/collections/schema.bin']
        for path in self.inputs:
            p = Path(path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(('frozen ' + path + '\n').encode())
        subprocess.run(['git', 'add', '.'], check=True)
        subprocess.run(['git', '-c', 'user.name=R1 test', '-c', 'user.email=r1-test@example.invalid',
                        'commit', '-qm', 'frozen fixture'], check=True)

    def identity(self, extra=()):
        actual = {'cmd/collection_workload_bench/main.go',
                  'cmd/collection_workload_bench/native_replace.go',
                  'TreeDB/db/db.go', 'TreeDB/collections/schema.bin', *extra}
        with patch.object(SOURCE, 'compiled_local_files', return_value=actual):
            return SOURCE.source_identity()

    def test_clean_inputs_and_embedded_asset_are_bound(self):
        value = self.identity()
        self.assertTrue(value['clean'])
        self.assertIn('TreeDB/collections/schema.bin', value['runtime_blobs'])
        before = value['harness_sha256']
        # Previously omitted command helper must affect harness identity.
        Path('cmd/collection_workload_bench/native_replace.go').write_bytes(b'revised\n')
        subprocess.run(['git', 'add', '.'], check=True)
        subprocess.run(['git', '-c', 'user.name=R1 test', '-c', 'user.email=r1-test@example.invalid',
                        'commit', '-qm', 'new helper'], check=True)
        self.assertNotEqual(before, self.identity()['harness_sha256'])

    def test_dirty_runtime_cannot_claim_head_bytes(self):
        Path('TreeDB/db/db.go').write_bytes(b'changed actual code\n')
        with self.assertRaisesRegex(ValueError, 'working build input differs'):
            self.identity()

    def test_dirty_command_helper_and_embed_are_rejected(self):
        for path in ('cmd/collection_workload_bench/native_replace.go',
                     'TreeDB/collections/schema.bin'):
            original = Path(path).read_bytes()
            Path(path).write_bytes(b'changed\n')
            with self.assertRaisesRegex(ValueError, 'working build input differs'):
                self.identity()
            Path(path).write_bytes(original)

    def test_untracked_selected_build_input_is_rejected(self):
        Path('TreeDB/db/generated.go').write_bytes(b'not committed\n')
        with self.assertRaisesRegex(ValueError, 'not a committed regular file'):
            self.identity(extra=('TreeDB/db/generated.go',))

    def test_missing_selected_input_is_rejected(self):
        Path('TreeDB/db/db.go').unlink()
        with self.assertRaises(FileNotFoundError):
            self.identity()

    def test_untracked_symlink_to_committed_input_is_rejected(self):
        root = str(Path.cwd())
        Path('cmd/collection_workload_bench/generated.go').symlink_to('main.go')
        package = {'Dir': root + '/cmd/collection_workload_bench',
                   'GoFiles': ['main.go', 'generated.go']}
        with patch.object(SOURCE.subprocess, 'check_output', side_effect=[SOURCE.json.dumps({'GOWORK': 'off', 'GOFLAGS': ''}), SOURCE.json.dumps(package)]):
            with patch.object(SOURCE, 'git', return_value=root):
                with self.assertRaisesRegex(ValueError, 'symlink'):
                    SOURCE.compiled_local_files('frozen-go')

    def test_overlay_modfile_and_active_workspace_cannot_claim_head(self):
        environments = [
            ({'GOWORK': 'off', 'GOFLAGS': '-overlay=/outside/overlay.json'}, 'input substitution'),
            ({'GOWORK': 'off', 'GOFLAGS': '-modfile outside.mod'}, 'input substitution'),
            ({'GOWORK': '/outside/go.work', 'GOFLAGS': ''}, 'active Go workspace'),
        ]
        for environment, message in environments:
            with self.subTest(environment=environment):
                with patch.object(SOURCE.subprocess, 'check_output', return_value=SOURCE.json.dumps(environment)):
                    with patch.object(SOURCE, 'git', return_value=str(Path.cwd())):
                        with self.assertRaisesRegex(ValueError, message):
                            SOURCE.compiled_local_files('frozen-go')

    def test_external_local_replace_and_escaping_input_fail_closed(self):
        root = str(Path.cwd())
        packages = [
            {'Dir': root + '/cmd/collection_workload_bench', 'GoFiles': ['main.go']},
            {'Dir': '/outside/replaced', 'Module': {'Replace': {'Dir': '/outside/replaced'}}},
        ]
        with patch.object(SOURCE.subprocess, 'check_output', side_effect=[SOURCE.json.dumps({'GOWORK': 'off', 'GOFLAGS': ''}), SOURCE.json.dumps(packages[0]) + SOURCE.json.dumps(packages[1])]):
            with patch.object(SOURCE, 'git', return_value=root):
                with self.assertRaisesRegex(ValueError, 'unbound external local'):
                    SOURCE.compiled_local_files('frozen-go')
        packages = [{'Dir': root + '/cmd/collection_workload_bench', 'GoFiles': ['../../../../outside.go']}]
        with patch.object(SOURCE.subprocess, 'check_output', side_effect=[SOURCE.json.dumps({'GOWORK': 'off', 'GOFLAGS': ''}), SOURCE.json.dumps(packages[0])]):
            with patch.object(SOURCE, 'git', return_value=root):
                with self.assertRaisesRegex(ValueError, 'escapes checkout'):
                    SOURCE.compiled_local_files('frozen-go')


if __name__ == '__main__':
    unittest.main()
