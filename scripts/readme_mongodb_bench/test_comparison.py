"""Offline failure gates. No Go build, benchmark or server is launched."""
import copy
import json
from pathlib import Path
import signal
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import comparison
import run


class ComparisonControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.original = self.root/'original'
        self.side = self.root/'side'
        for path in (self.original/'bin', self.original/'recipe', self.side/'bin'):
            path.mkdir(parents=True)
        binaries = {}
        for name in comparison.HELPERS:
            for parent in (self.original, self.side):
                (parent/'bin'/name).write_text(name)
            binaries[name] = run.sha(self.original/'bin'/name)
        recipes = {'census.go': run.sha(Path(comparison.__file__).with_name('census.go'))}
        (self.original/'recipe/census.go').write_bytes(Path(comparison.__file__).with_name('census.go').read_bytes())
        run.save(self.original/'freeze.json', dict(binaries=binaries, recipe_sources=recipes,
                 sources=[{}, dict(path='/client', commit='client-sha', root_tree='client-tree')]))
        (self.side/'receipt.json').write_text('{}')
        self.product = dict(path='/product', commit='product-sha', tree='product-tree')
        self.harness = dict(path='/harness', commit='harness-sha', tree='harness-tree')
        self.manifest = dict(schema_version=1, kind='gomap-ycsb-side-freeze', product=self.product,
            harness=self.harness, client=dict(path='/client', commit='client-sha', tree='client-tree'),
            host=comparison.host(),
            original_freeze=dict(path=str(self.original/'freeze.json'), sha256=run.sha(self.original/'freeze.json')),
            recipe_sources={n: run.sha(Path(comparison.__file__).with_name(n)) for n in comparison.RECIPES},
            build_receipt=dict(path=str(self.side/'receipt.json'), sha256=run.sha(self.side/'receipt.json')),
            case=dict(count=100000, operations=10000, profile=comparison.PROFILE, ack='relaxed', target='gomap', workers=16))
        self.source_patch = patch.object(comparison, 'source', side_effect=lambda p: {
            '/product':self.product, '/harness':self.harness,
            '/client':dict(path='/client', commit='client-sha', tree='client-tree')}[str(p)])
        self.source_patch.start()
        self.addCleanup(self.source_patch.stop)
        self.build_patch = patch.object(comparison, 'build_identity')
        self.build_patch.start()
        self.addCleanup(self.build_patch.stop)
        self.env_patch = patch.dict(comparison.os.environ, {}, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def verify(self, manifest=None):
        comparison.verify(manifest or self.manifest, self.side, Path('/product'))

    def test_manifest_and_changed_helper_recipe_receipt_hashes(self):
        self.verify()
        for path in (self.side/'bin/go-ycsb', self.original/'bin/census',
                     self.original/'bin/ycsb-correctness-fixture', self.original/'recipe/census.go',
                     self.side/'receipt.json', self.original/'freeze.json'):
            original = path.read_bytes()
            with self.subTest(path=path):
                path.write_bytes(original+b'changed')
                with self.assertRaises(ValueError):
                    self.verify()
            path.write_bytes(original)
        changed = copy.deepcopy(self.manifest)
        changed['recipe_sources']['run.py'] = '0'*64
        with self.assertRaisesRegex(ValueError, 'comparison recipe changed'):
            self.verify(changed)

    def test_invalid_workload_host_and_observer_environment(self):
        for key, value in (('operations', 123), ('count', 1), ('profile', 'durable'),
                           ('ack', 'durable'), ('workers', 1), ('target', 'mongodb')):
            changed = copy.deepcopy(self.manifest)
            changed['case'][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'selected case'):
                self.verify(changed)
        changed = copy.deepcopy(self.manifest)
        changed['host']['hostname'] = 'other-host'
        with self.assertRaisesRegex(ValueError, 'host changed'):
            self.verify(changed)
        for key, value in (('GOMAP_READ_PROFILE', '1'), ('GOGC', 'off'),
                           ('TREEDB_HOT_PATH_STATS', '1'), ('TREEDB_LEAF_PAGE_READ_CACHE_ENTRIES', '0'),
                           ('GODEBUG', 'gctrace=1'), ('GOMEMLIMIT', '64MiB'), ('GOEXPERIMENT', 'arenas')):
            for environment in (comparison.os.environ, run.ENV):
                with self.subTest(key=key, inherited=environment is comparison.os.environ), \
                     patch.dict(environment, {key:value}), \
                     self.assertRaisesRegex(ValueError, 'observer/runtime'):
                    self.verify()

    def test_explicit_client_path_keeps_original_source_identity(self):
        self.source_patch.stop()
        def identity(path):
            if str(path) == '/copied-client':
                return dict(path='/copied-client', commit='client-sha', tree='client-tree')
            return {'/product':self.product, '/harness':self.harness}[str(path)]
        moved = copy.deepcopy(self.manifest)
        moved['client']['path'] = '/copied-client'
        with patch.object(comparison, 'source', side_effect=identity):
            self.verify(moved)
            moved['client']['commit'] = 'wrong-sha'
            with self.assertRaisesRegex(ValueError, 'external client source'):
                self.verify(moved)
        self.source_patch.start()

    def test_build_binary_hash_and_source_fail_before_tool_invocation(self):
        self.build_patch.stop()
        receipt = dict(schema_version=1, source_commit='product-sha', source_tree='product-tree', binary_sha256='0'*64)
        binary = self.side/'bin/go-ycsb'
        with self.assertRaisesRegex(ValueError, 'binary hash'):
            comparison.build_identity(receipt, self.product, binary)
        receipt['source_commit'] = 'wrong'
        with self.assertRaisesRegex(ValueError, 'source identity'):
            comparison.build_identity(receipt, self.product, binary)
        self.build_patch.start()

    def test_toolchain_receipt_vcs_and_build_argv(self):
        self.build_patch.stop()
        binary = self.side/'bin/go-ycsb'
        go_env = dict(GOVERSION='go1.26.10', GOROOT='/go', GOTOOLDIR='/go/pkg/tool/linux_amd64')
        version, compiler = 'go version go1.26.10 linux/amd64', 'compile version go1.26.10'
        info = str(binary)+': go1.26.10\n\tbuild\tvcs.revision=product-sha\n\tbuild\tvcs.modified=false'
        receipt = dict(schema_version=1, source_commit='product-sha', source_tree='product-tree',
            binary_sha256=run.sha(binary), argv=['/go/bin/go', 'build', '-buildvcs=true', '-o', str(binary),
                                              './TreeDB/mongo_gateway/server.go'],
            environment=dict(GOROOT='/go', GOTOOLCHAIN='local', GOWORK='off', GOFLAGS=''),
            go_version=version, go_env=go_env, compiler_version=compiler, binary_build_info=info)
        def check(value):
            with patch.object(comparison, 'output', side_effect=[version, json.dumps(go_env), compiler, info]):
                comparison.build_identity(value, self.product, binary)
        check(receipt)
        relocated = copy.deepcopy(receipt)
        relocated['argv'][4] = '/original-build/gateway'
        relocated['binary_build_info'] = info.replace(str(binary)+': ', '/original-build/gateway: ', 1)
        check(relocated)  # Keep the honest original output path after copying the exact binary.
        with self.assertRaisesRegex(ValueError, 'binary hash'):
            check(dict(relocated, binary_sha256='0'*64))
        for field, value in (('compiler_version', 'compile version go1.26.9'),
                             ('argv', ['/go/bin/go', 'build', '-overlay=/tmp/x']),
                             ('environment', dict(receipt['environment'], GOROOT='/wrong'))):
            bad = copy.deepcopy(receipt)
            bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                check(bad)
        info = info.replace('vcs.modified=false', 'vcs.modified=true')
        bad = dict(receipt, binary_build_info=info)
        with self.assertRaisesRegex(ValueError, 'VCS build identity'):
            check(bad)
        info = str(binary)+': go1.26.10'  # Explicit receipt binds file-list builds without VCS settings.
        check(dict(receipt, binary_build_info=info))
        self.build_patch.start()

    def test_selector_failure_retained_and_attempt_not_reused(self):
        (self.side/'bin/treedb-mongo-gateway').write_text('gateway')
        run.save(self.side/'comparison-freeze.json', self.manifest)
        args = SimpleNamespace(root=self.side, repo=Path('/product'), operations=10000,
            expected_comparison_sha256=run.sha(self.side/'comparison-freeze.json'), attempt_label='pair01-base')
        old = {key:getattr(run, key, None) for key in ('ROOT','REPO','BIN','OUT','FREEZE','FREEZE_HASH','COMPARISON')}
        for key, value in old.items():
            self.addCleanup(setattr, run, key, value)
        with patch.object(comparison, 'HOST_LOCK', self.root/'lock'), \
             patch.object(comparison, 'output', return_value='snapshot'), \
             patch.object(run, 'run_case', side_effect=ValueError('failed sample')) as case:
            with self.assertRaisesRegex(ValueError, 'failed sample'):
                comparison.execute(args)
            self.assertTrue((run.OUT/'selector-failure.json').exists())
            with self.assertRaises(FileExistsError):
                comparison.execute(args)
            case.assert_called_once_with('ycsb', 'relaxed', 'gomap', 1, 100000, 10000)

    def case_context(self):
        old = {key:getattr(run, key, None) for key in ('ROOT', 'REPO', 'OUT', 'FREEZE_HASH')}
        for key in old:
            self.addCleanup(setattr, run, key, old[key])
        run.ROOT = self.root
        run.REPO = self.root
        run.OUT = self.root/'runs'
        run.OUT.mkdir()
        run.FREEZE_HASH = 'side-hash'

    def test_partial_startup_and_timeout_cleanup_registered_service(self):
        self.case_context()
        for timeout in (False, True):
            with self.subTest(timeout=timeout):
                def startup(*args):
                    run.SERVICES.append({'stem':'partial', 'stop_attempts':0})
                    if timeout:
                        signal.raise_signal(signal.SIGALRM)
                    raise ValueError('startup failed')
                with patch.object(run, 'verify_freeze'), patch.object(run, 'gateway', side_effect=startup), \
                     patch.object(run, 'stop') as stop:
                    label = 'timeout' if timeout else 'startup'
                    with self.assertRaises((ValueError, TimeoutError)):
                        comparison.bounded_case(lambda: run.run_case('ycsb', label, 'gomap', 1, 100000, 10000),
                                                seconds=1)
                    stop.assert_called_once()
                    self.assertTrue(stop.call_args.kwargs['abort'])
                    failure = run.strict_json((run.OUT/('ycsb-'+label+'-p1-gomap')/'failure.json').read_text())
                    self.assertEqual(failure['state'], 'invalid')
        self.assertEqual(signal.getitimer(signal.ITIMER_REAL), (0.0, 0.0))

    def test_real_gateway_registration_survives_popen_failure(self):
        self.case_context()
        old_bin = getattr(run, 'BIN', None)
        self.addCleanup(setattr, run, 'BIN', old_bin)
        run.BIN = self.side/'bin'
        with patch.object(run, 'verify_freeze'), patch.object(run.subprocess, 'Popen', side_effect=OSError('spawn failed')):
            with self.assertRaisesRegex(OSError, 'spawn failed'):
                run.run_case('ycsb', 'partial', 'gomap', 1, 100000, 10000)
        self.assertEqual(len(run.SERVICES), 1)
        self.assertTrue(run.SERVICES[0]['stopped'])
        self.assertTrue(all(handle.closed for handle in run.SERVICES[0]['handles']))
        receipt = run.strict_json((run.OUT/'ycsb-partial-p1-gomap/server-cleanup-1.stop.json').read_text())
        self.assertTrue(receipt['not_spawned'] and receipt['absent'])

    def test_reopened_typed_mismatch_is_invalid_and_cleanup_runs(self):
        self.case_context()
        def gateway(*args):
            service = dict(uri='mongodb://localhost:1', stem=args[-1])
            run.SERVICES.append(service)
            return service
        with patch.object(run, 'verify_freeze'), patch.object(run, 'gateway', side_effect=gateway), \
             patch.object(run, 'ready'), patch.object(run, 'ycsb_phase', return_value={}), \
             patch.object(run, 'fixture'), patch.object(run, 'physical', return_value={}), \
             patch.object(run, 'census', side_effect=[{}, {'canonical_typed_state_sha256':'before'},
                                                       {'canonical_typed_state_sha256':'after'}]), \
             patch.object(run, 'stop') as stop:
            with self.assertRaisesRegex(ValueError, 'state changed on reopen'):
                run.run_case('ycsb', 'relaxed', 'gomap', 1, 100000, 10000)
            self.assertTrue(any(call.kwargs.get('abort') for call in stop.call_args_list))
            self.assertTrue((run.OUT/'ycsb-relaxed-p1-gomap/failure.json').exists())

    def test_missing_close_receipt_fails_ycsb_phase(self):
        old = {key:getattr(run, key, None) for key in ('BIN', 'FREEZE')}
        for key in old:
            self.addCleanup(setattr, run, key, old[key])
        run.BIN = self.side/'bin'
        run.FREEZE = dict(binaries={'go-ycsb':'client-hash'})
        with patch.object(run, 'command', return_value=''), patch.object(run, 'parse_ycsb', return_value={}):
            with self.assertRaises(FileNotFoundError):
                run.ycsb_phase(self.side, 'run', 'mongodb://localhost/ycsb?w=1', 100000, 10000, comparison.PROFILE)


if __name__ == '__main__':
    unittest.main()
