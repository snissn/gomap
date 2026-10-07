import copy
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

import quicksilver_maintenance as m


def report(mode='exhaustive'):
    domains = [dict(name=n, path=n, bytes=b, files=1, zero_byte_files=0)
               for n, b in [('index', 4096), ('wal', 32), ('value_vlog', 1000), ('leaf_vlog', 2000)]]
    domains.append(dict(name='total', path='db', bytes=7128, files=4, zero_byte_files=0))
    return dict(mode=mode, dry_run=False, before=domains, after=copy.deepcopy(domains),
                phases=[dict(name='checkpoint', wall_time_nanos=1000),
                        dict(name='index-vacuum', status='not_required', wall_time_nanos=0)],
                remaining_debt={k: False if k == 'index_vacuum_required' else 0 for k in
                    ['value_log_rewrite_segments', 'value_log_rewrite_bytes', 'value_log_gc_segments', 'value_log_gc_bytes',
                     'leaf_pack_generations', 'leaf_pack_bytes', 'leaf_gc_generations', 'leaf_gc_bytes', 'zero_byte_value_log_files', 'index_vacuum_required']},
                fully_compacted=True, policy_fully_compacted=True, byte_minimized=mode == 'exhaustive')


def settle_report(mode='exhaustive'):
    initial = report(mode)
    initial['remaining_debt']['leaf_gc_generations'] = 9
    initial['remaining_debt']['leaf_gc_bytes'] = 9774643
    initial.update(fully_compacted=False, policy_fully_compacted=False, byte_minimized=False)
    basis = dict(CommitSeq=25, RootPageID=100, SystemRootPageID=200, AppliedCommandLSN=36,
                 MaxEntryRevision=36, LeafGenerationStateVersion=4)
    result = dict(basis, CommitSeq=26)

    def root(token, durable, visible):
        return dict(CommitSeq=token['CommitSeq'], UserRootPageID=token['RootPageID'],
                    SystemRootPageID=token['SystemRootPageID'], AppliedCommandLSN=token['AppliedCommandLSN'],
                    MaxEntryRevision=token['MaxEntryRevision'], Durable=durable, Visible=visible)
    audit = report(mode)
    audit['dry_run'] = True
    audit['phases'] = [dict(name='index-vacuum', status='not_required', wall_time_nanos=0)]
    return dict(schema=1, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT, status='completed',
                reports=[initial, audit], initial_status='succeeded', audit_status='succeeded', cleanup_status='succeeded',
                leaf_gc=dict(status='succeeded', stats=dict(GenerationsTotal=9, GenerationsWritable=0, GenerationsLive=0,
                    GenerationsRetiring=0, GenerationsEligible=9, GenerationsDeleted=9, FilesDeleted=9,
                    BytesEligible=9774643, BytesDeleted=9774643, ManifestRevisionGCUnsupported=False,
                    ManifestRevisionsTotal=5, ManifestRevisionsProtected=2, ManifestRevisionsEligible=3,
                    ManifestRevisionsDeleted=3, ManifestRevisionBytesEligible=5554, ManifestRevisionBytesDeleted=5554)), refresh=dict(basis=basis, result=result, status='succeeded',
                    checkpoint_status='succeeded', summary_before_status='succeeded', summary_after_status='succeeded',
                    next_lsn_before=37, next_lsn_after=37, roots_before=[root(basis, True, True)],
                    roots_after=[root(basis, True, False), root(result, True, True)]))


class MaintenanceTests(unittest.TestCase):
    def test_all_cli_subcommands_reject_optimized_python_before_dispatch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            commands = [
                ['capture', str(root/'manifest.json'), str(root/'plan.json')],
                ['calibrate', '--metric', 'elapsed_seconds', '--out', str(root/'noise.json'),
                 *[str(root/f'C{i}.json') for i in (1, 2, 3)]],
                ['analyze', str(root/'bundle.json'), '--out', str(root/'result.json')],
            ]
            for flags, setting in [(['-O'], None), (['-OO'], None), ([], '1'), ([], '2')]:
                env = dict(os.environ)
                env.pop('PYTHONOPTIMIZE', None)
                if setting is not None:
                    env['PYTHONOPTIMIZE'] = setting
                for command in commands:
                    with self.subTest(flags=flags, setting=setting, command=command[0]):
                        result = subprocess.run([sys.executable, '-B', *flags, m.__file__, *command],
                                                env=env, capture_output=True, text=True, timeout=10)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn('validation requires Python assertions', result.stderr)
                        self.assertEqual(result.stdout, '')
                        self.assertEqual(list(root.iterdir()), [])

    def test_physical_copy_and_fingerprint(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); fixture = root/'fixture'; fixture.mkdir()
            (fixture/'value').write_bytes(b'real pointer value')
            (fixture/'index.db').write_bytes(b'physical identity metadata')
            expected = m.fingerprint(fixture)
            m.shutil.copytree(fixture, root/'copy')
            self.assertEqual(expected, m.fingerprint(root/'copy'))
            self.assertNotEqual((fixture/'value').stat().st_ino, (root/'copy'/'value').stat().st_ino)
            payload = m.fingerprint(root/'copy', payload_only=True)
            (root/'copy'/'index.db').write_bytes(b'restored identity metadata')
            self.assertEqual(payload, m.fingerprint(root/'copy', payload_only=True))
            (root/'copy'/'value').write_bytes(b'different pointer value')
            self.assertNotEqual(expected, m.fingerprint(root/'copy'))
            (fixture/'alias').symlink_to('value')
            with self.assertRaisesRegex(ValueError, 'symlink'):
                m.fingerprint(fixture)

    def test_mode_flags_accounting_and_terminal_phases(self):
        for mode in ('full', 'exhaustive'):
            cell = dict(mode=mode, batch_size=8192)
            self.assertIs(m.validate_report(report(mode), cell)['byte_minimized'], mode == 'exhaustive')
            command = m.compact_command('/bin/treemap', '/db', cell)
            self.assertIn('-sync-each-phase', command)
            self.assertEqual(command[-4:], ['-leaf-pack-max-passes', '64', '-rewrite-batch-size', '8192'])
            for mutation in [lambda r: r.update(dry_run=True), lambda r: r['after'][0].update(bytes=1),
                             lambda r: r['phases'][0].update(status='planned'),
                             lambda r: r.update(byte_minimized=not r['byte_minimized']),
                             lambda r: r['remaining_debt'].update(value_log_gc_bytes=1)]:
                broken = report(mode); mutation(broken)
                with self.assertRaises(ValueError):
                    m.validate_report(broken, cell)
        with self.assertRaises(ValueError):
            m.compact_command('/bin/treemap', '/db', dict(mode='full', batch_size=True))

    def test_overlap_and_output_alias_rejected_before_any_write(self):
        with tempfile.TemporaryDirectory() as temp:
            base = pathlib.Path(temp).resolve(); root = base/'root'; root.mkdir()
            fixture = root/'fixture'; fixture.mkdir(); (fixture/'value').write_bytes(b'frozen')
            outside = base/'outside'; outside.mkdir(); (root/'alias').symlink_to(outside, target_is_directory=True)
            m.save(root/'manifest.json', {})
            for path, output in [(str(root), 'out'), (str(fixture/'..'), 'out'), (str(fixture), 'fixture/out'),
                                 (str(fixture), 'alias/out')]:
                m.save(root/'plan.json', dict(output=output, cells=[dict(label='test', fixture=path)]))
                before = sorted(str(p) for p in base.rglob('*')); frozen = m.fingerprint(fixture)
                with self.assertRaises(ValueError):
                    m.capture(root/'manifest.json', root/'plan.json')
                self.assertEqual(before, sorted(str(p) for p in base.rglob('*')))
                self.assertEqual(frozen, m.fingerprint(fixture))

    def make_campaign(self, root, candidate=8.0, endpoint=None, build_env=None):
        root = root.resolve()
        out = root/'out'; out.mkdir()
        manifest = dict(build_env=build_env or {'GOWORK': 'off'},
                        sources={name: dict(head=letter*40, binary=name, binary_sha256=letter*64)
                                for name, letter in [('A', 'a'), ('B', 'b')]}, receipts={})
        manifest['snapshot_restore'] = 'restore'
        manifest['sources']['restore'] = dict(head='r'*40, binary='restore', binary_sha256='r'*64)
        for role in ('source', 'build', 'native', 'runner'):
            m.save(out/(role+'-receipt.json'), {'reviewed': role})
            manifest['receipts'][role] = dict(path=role, sha256=m.unified.sha256(out/(role+'-receipt.json')))
        m.save(out/'manifest.json', manifest)
        plan = dict(cells=[])
        paths = {}
        entries = [('C1', 'A', 10.0, 1), ('C2', 'A', 10.1, 20), ('C3', 'A', 9.9, 40),
                   ('A1', 'A', 10., 100), ('B1', 'B', candidate, 120), ('B2', 'B', candidate, 140),
                   ('A2', 'A', 10., 160), ('A3', 'A', 10., 180), ('B3', 'B', candidate, 200)]
        for label, source, elapsed, offset in entries:
            directory = out/label; directory.mkdir(); (directory/'db').mkdir()
            m.save(directory/'fixture-receipt.json', dict(closed=True, verified=True, fixture_sha256='f'*64))
            cell = dict(label=label, source=source, fixture='/closed/fixture', mode='exhaustive', batch_size=8192,
                        fixture_receipt=dict(path='/receipt', sha256=m.unified.sha256(directory/'fixture-receipt.json')))
            if endpoint is not None:
                cell['endpoint'] = endpoint
            plan['cells'].append(cell)
            started = 1000000000000+offset*1000000000; finished = started+int(elapsed*1e9)
            rows = [dict(pid=5, process_start_ticks=1, started_unix_nano=started+200000000,
                         finished_unix_nano=started+200010000, rss_bytes=10000, anonymous_bytes=6000,
                         file_bytes=4000, shared_memory_bytes=0)]
            (directory/'rss_samples.jsonl').write_text(json.dumps(rows[0])+'\n')
            summary = dict(complete=True, cancelled=False, errors=[], interval_ms=200, samples=1)
            m.save(directory/'rss_samples.jsonl.summary.json', summary)
            (directory/'stderr.log').write_text('Maximum resident set size (kbytes): 12\n')
            m.save(directory/'stdout.json', settle_report() if endpoint == m.COMMAND_WAL_SETTLE_ENDPOINT else report())
            (directory/'ldd.stdout.txt').write_text('static linkage\n'); (directory/'ldd.stderr.txt').touch()
            (directory/'restore.ldd.stdout.txt').write_text('static linkage\n'); (directory/'restore.ldd.stderr.txt').touch()
            (directory/'restore.stderr.log').touch()
            m.save(directory/'restore.stdout.json', dict(rebound=True, stores=['maindb'], operation='RebindDurableRootSnapshotLayoutWithContextV1'))
            run = dict(schema=1, cell=cell, source=manifest['sources'][source], fixture=dict(sha256='f'*64, files=4, bytes=7128),
                       command=m.compact_command(str(root/'bin'/source), directory/'db', cell),
                       environment_policy=m.unified.ENVIRONMENT_POLICY,
                       env=m.unified.capture_environment(manifest['build_env'], root/'working-dbs', inherited={}),
                       harness=dict(collector='h'*64, unified_collector='u'*64, rss_observer='s'*64),
                       native_resolution=dict(libraries={}), started_ns=started, finished_ns=finished,
                       elapsed_seconds=elapsed, rc=0, rss_sampling=summary, validated=True)
            run['restored_fixture'] = dict(run['fixture'])
            run['snapshot_restore'] = dict(source=manifest['sources']['restore'], command=[str(root/'bin'/'restore'), str(directory/'db')], rc=0, native={})
            run['memory'] = m.rss_metrics(directory, summary, started, finished)
            run['artifacts'] = {p.name: m.unified.sha256(p) for p in directory.iterdir() if p.is_file()}
            m.save(directory/'run.json', run); paths[label] = directory/'run.json'
        m.save(out/'plan.json', plan)
        for path in paths.values():
            self.change(path, lambda r: r.update(manifest_sha256=m.unified.sha256(out/'manifest.json'),
                                                plan_sha256=m.unified.sha256(out/'plan.json')))
        with mock.patch.object(m.time, 'time_ns', return_value=1000000000000+80*1000000000):
            m.calibrate([paths[k] for k in ('C1', 'C2', 'C3')], 'elapsed_seconds', root/'noise.json')
        bundle = dict(calibration=str(root/'noise.json'), calibration_sha256=m.unified.sha256(root/'noise.json'),
                      pairs=[{**{k: str(paths[k+str(i)]) for k in ('A', 'B')},
                              **{k+'_sha256': m.unified.sha256(paths[k+str(i)]) for k in ('A', 'B')}}
                             for i in (1, 2, 3)])
        m.save(root/'bundle.json', bundle)
        return paths

    def test_failed_capture_preserves_copy_receipts_and_raw_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp).resolve(); fixture = root/'fixture'; fixture.mkdir()
            (fixture/'index.db').write_bytes(b'closed verified fixture')
            (fixture/'value').write_bytes(b'persistent payload')
            before = m.fingerprint(fixture)
            m.save(root/'fixture-receipt.json', dict(closed=True, verified=True, fixture_sha256=before['sha256']))
            (root/'bin').mkdir(); binary = root/'bin'/'treemap'; binary.write_bytes(b'\x7fELF')
            manifest = dict(build_env=dict(GOWORK='off', GOMEMLIMIT='2GiB', GOGC='25',
                                         TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES='77'), sources=dict(A=dict(head='a'*40, binary='treemap', binary_sha256=m.unified.sha256(binary))), libraries={}, receipts={})
            for role in ('source', 'build', 'native', 'runner'):
                m.save(root/(role+'.json'), {'reviewed': role})
                manifest['receipts'][role] = dict(path=role+'.json', sha256=m.unified.sha256(root/(role+'.json')))
            m.save(root/'manifest.json', manifest)
            cell = dict(label='failed', source='A', fixture=str(fixture), mode='exhaustive', batch_size=8192,
                        fixture_receipt=dict(path=str(root/'fixture-receipt.json'), sha256=m.unified.sha256(root/'fixture-receipt.json')))
            m.save(root/'plan.json', dict(output='capture', cells=[cell]))

            def failure(command, executable, sample_path, **kwargs):
                self.assertEqual(command[0:2], ['/usr/bin/time', '-v'])
                self.assertEqual(executable, binary); self.assertEqual(kwargs['interval_ms'], 200)
                # Ambient and manifest heap pressure must not reach the owned
                # offline command. Keep normal GC and the mapping-domain setting.
                self.assertEqual({k: kwargs['env'][k] for k in controls}, controls)
                self.assertNotIn('MALLOC_ARENA_MAX', kwargs['env'])
                self.assertNotIn('GH_TOKEN', kwargs['env'])
                effective_env.update(kwargs['env'])
                kwargs['stdout'].write('failed raw compact output\n'); kwargs['stderr'].write('preserved native diagnostic\n')
                sample_path.write_text('preserved sample\n')
                summary = dict(complete=False, cancelled=True, errors=[])
                m.save(str(sample_path)+'.summary.json', summary)
                return types.SimpleNamespace(returncode=-15), summary

            controls = dict(GOMEMLIMIT='off', GOGC='100', GOMAXPROCS='12',
                            TREEDB_VLOG_MAX_MAPPED_SEALED_BYTES='1073741824')
            effective_env = {}
            with mock.patch.dict(m.os.environ, GOMEMLIMIT='512MiB', GOGC='50', MALLOC_ARENA_MAX='1', GH_TOKEN='test-secret'), mock.patch.object(m, 'restore_snapshot') as restore, mock.patch.object(m.unified, 'validate_native') as native, mock.patch.object(m.owned_process_rss, 'run_with_rss', side_effect=failure):
                with self.assertRaisesRegex(ValueError, 'compact exit: -15'):
                    m.capture(root/'manifest.json', root/'plan.json')
            directory = root/'capture'/'failed'
            self.assertEqual(m.fingerprint(fixture), before); self.assertEqual(m.fingerprint(directory/'db'), before)
            self.assertEqual((directory/'stdout.json').read_text(), 'failed raw compact output\n')
            run = json.loads((directory/'run.json').read_bytes())
            self.assertIn('error', run); self.assertEqual(run['rc'], -15)
            self.assertEqual({k: run['env'][k] for k in controls}, controls)
            self.assertEqual(run['environment_policy'], m.unified.ENVIRONMENT_POLICY)
            self.assertEqual(run['env'], effective_env)
            self.assertEqual(restore.call_args.args[4], effective_env)
            self.assertEqual(native.call_args.args[3], effective_env)
            self.assertTrue(run['rss_sampling']['cancelled'])
            self.assertEqual(m.unified.sha256(directory/'stderr.log'), run['artifacts']['stderr.log'])

    @staticmethod
    def change(path, mutation):
        run = json.loads(path.read_bytes()); mutation(run); m.save(path, run)

    @staticmethod
    def seal_pairs(root):
        path = root/'bundle.json'; bundle = json.loads(path.read_bytes())
        for pair in bundle['pairs']:
            for role in ('A', 'B'):
                pair[role+'_sha256'] = m.unified.sha256(pathlib.Path(pair[role]))
        m.save(path, bundle)

    def test_material_and_negative_packets(self):
        for candidate, material in [(8., True), (9.8, False), (11., False)]:
            with self.subTest(candidate=candidate), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); self.make_campaign(root, candidate)
                result = m.analyze(root/'bundle.json')
                self.assertEqual(result['material'], material)
                self.assertAlmostEqual(result['E'], .02)
                self.assertEqual(len(result['pair_effects']), 3)

    def test_settle_current_manifest_revision_serializer_schema(self):
        # Current LeafGenerationGCStats serializes all 16 exported fields,
        # including a separate manifest census even when segment GC succeeds.
        raw = settle_report()
        stats = raw['leaf_gc']['stats']
        stats.update(GenerationsTotal=3, GenerationsWritable=1, GenerationsLive=1,
                     GenerationsEligible=1, GenerationsDeleted=1, FilesDeleted=1,
                     BytesEligible=473915, BytesDeleted=473915)
        original = copy.deepcopy(raw)
        cell = dict(mode='exhaustive', batch_size=8192, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
        measurement = m.validate_measurement(raw, cell)
        self.assertEqual(len(stats), 16)
        self.assertEqual(raw, original)
        self.assertTrue(m.policy_complete(measurement))
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp)
            paths = self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            run, loaded = m.load_run(paths['B1'])
            self.assertEqual(run['endpoint'], m.COMMAND_WAL_SETTLE_ENDPOINT)
            self.assertTrue(m.policy_complete(loaded))
            self.assertTrue(m.analyze(root/'bundle.json')['material'])

    def test_settle_capture_and_replay_preserve_current_stats(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp).resolve(); self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            campaign = root/'out'; fixture = root/'fixture'; fixture.mkdir()
            (fixture/'index.db').write_bytes(b'closed fixture')
            (fixture/'value').write_bytes(b'persistent payload')
            m.save(root/'fixture.json', dict(closed=True, verified=True, fixture_sha256=m.fingerprint(fixture)['sha256']))
            (campaign/'bin').mkdir(); binary = campaign/'bin'/'A'; binary.write_bytes(b'fake native binary')
            manifest = json.loads((campaign/'manifest.json').read_bytes())
            manifest['libraries'] = {}
            manifest['sources']['A']['binary_sha256'] = m.unified.sha256(binary)
            for role, receipt in manifest['receipts'].items():
                receipt['path'] = str(campaign/(role+'-receipt.json'))
            m.save(campaign/'manifest.json', manifest)
            cell = dict(label='current', source='A', fixture=str(fixture), mode='exhaustive', batch_size=8192,
                        endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT,
                        fixture_receipt=dict(path=str(root/'fixture.json'), sha256=m.unified.sha256(root/'fixture.json')))
            m.save(campaign/'capture-plan.json', dict(output='capture', cells=[cell]))
            raw = settle_report()

            def restore(root, manifest, database, directory, env, metadata):
                metadata['snapshot_restore'] = dict(source=manifest['sources']['restore'], rc=0, native={},
                    command=[str(root/'bin'/'restore'), str(database)])
                m.save(directory/'restore.stdout.json', dict(rebound=True, operation='RebindDurableRootSnapshotLayoutWithContextV1'))
                for name in ('restore.stderr.log', 'restore.ldd.stdout.txt', 'restore.ldd.stderr.txt'):
                    (directory/name).touch()

            def native(binary, source, libraries, env, root, directory, metadata):
                metadata['native_resolution'] = dict(libraries={})
                for name in ('ldd.stdout.txt', 'ldd.stderr.txt'):
                    (directory/name).touch()

            def execute(command, executable, sample_path, **kwargs):
                kwargs['stdout'].write(json.dumps(raw))
                kwargs['stderr'].write('Maximum resident set size (kbytes): 12\n')
                stamp = m.time.time_ns()
                sample_path.write_text(json.dumps(dict(pid=5, process_start_ticks=1,
                    started_unix_nano=stamp, finished_unix_nano=stamp, rss_bytes=10000,
                    anonymous_bytes=6000, file_bytes=4000, shared_memory_bytes=0))+'\n')
                summary = dict(complete=True, cancelled=False, errors=[], interval_ms=200, samples=1)
                m.save(str(sample_path)+'.summary.json', summary)
                return types.SimpleNamespace(returncode=0), summary

            # No native binary is executed: only filesystem capture and replay
            # run here, with loader, restore, and process operations replaced.
            with mock.patch.object(m, 'restore_snapshot', side_effect=restore), \
                 mock.patch.object(m.unified, 'validate_native', side_effect=native), \
                 mock.patch.object(m.owned_process_rss, 'run_with_rss', side_effect=execute), \
                 mock.patch('builtins.print'):
                m.capture(campaign/'manifest.json', campaign/'capture-plan.json')
            directory = campaign/'capture'/'current'
            self.assertEqual(json.loads((directory/'stdout.json').read_bytes()), raw)
            run, loaded = m.load_run(directory/'run.json')
            self.assertTrue(run['validated']); self.assertTrue(m.policy_complete(loaded))
            self.assertEqual(loaded['leaf_gc'], raw['leaf_gc'])
            self.assertEqual(m.unified.sha256(directory/'stdout.json'), run['artifacts']['stdout.json'])

    def test_settle_manifest_revision_counters_fail_closed(self):
        cell = dict(mode='exhaustive', batch_size=8192, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
        revision_fields = ('ManifestRevisionsTotal', 'ManifestRevisionsProtected', 'ManifestRevisionsEligible',
                           'ManifestRevisionsDeleted', 'ManifestRevisionBytesEligible', 'ManifestRevisionBytesDeleted')
        for name in revision_fields:
            for value in (-1, True, 1.5, '1', None):
                with self.subTest(name=name, value=value):
                    raw = settle_report(); raw['leaf_gc']['stats'][name] = value
                    with self.assertRaises(ValueError):
                        m.validate_measurement(raw, cell)
        for name in settle_report()['leaf_gc']['stats']:
            with self.subTest(missing=name):
                raw = settle_report(); del raw['leaf_gc']['stats'][name]
                with self.assertRaisesRegex(ValueError, 'invalid leaf GC counters'):
                    m.validate_measurement(raw, cell)
        for changes in [dict(UnexpectedCounter=0), dict(ManifestRevisionGCUnsupported=0),
                        dict(ManifestRevisionGCUnsupported=1), dict(ManifestRevisionGCUnsupported=None),
                        dict(ManifestRevisionGCUnsupported='false'), dict(ManifestRevisionsTotal=6),
                        dict(ManifestRevisionsProtected=6), dict(ManifestRevisionsDeleted=4),
                        dict(ManifestRevisionBytesDeleted=5555), dict(ManifestRevisionGCUnsupported=True)]:
            with self.subTest(changes=changes):
                raw = settle_report(); raw['leaf_gc']['stats'].update(changes)
                with self.assertRaises(ValueError):
                    m.validate_measurement(raw, cell)
        # A later protection check can retain initially eligible revisions.
        # Deleted counts and bytes are bounded, not required to equal eligibility.
        for changes in [dict(ManifestRevisionsDeleted=1, ManifestRevisionBytesDeleted=100),
                        {name: 0 for name in revision_fields}]:
            raw = settle_report(); raw['leaf_gc']['stats'].update(changes)
            measurement = m.validate_measurement(raw, cell)
            self.assertEqual(measurement['leaf_gc'], raw['leaf_gc'])
            self.assertTrue(m.policy_complete(measurement))
        for status in ('failed', 'not_started', 'cancelled'):
            raw = settle_report(); raw['leaf_gc']['status'] = status
            raw['leaf_gc']['stats'].update(ManifestRevisionsDeleted=1, ManifestRevisionBytesDeleted=100)
            with self.assertRaisesRegex(ValueError, 'incomplete leaf GC'):
                m.validate_measurement(raw, cell)

    def test_settle_unsupported_manifest_gc_is_diagnostic_and_nonqualifying(self):
        raw = settle_report()
        stats = raw['leaf_gc']['stats']
        for name in list(stats):
            if name.startswith('Manifest'):
                stats[name] = True if name == 'ManifestRevisionGCUnsupported' else 0
        cell = dict(mode='exhaustive', batch_size=8192, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
        for name in stats.keys()-{'ManifestRevisionGCUnsupported'}:
            if name.startswith('Manifest'):
                malformed = copy.deepcopy(raw); malformed['leaf_gc']['stats'][name] = 1
                with self.subTest(unsupported_census=name), self.assertRaisesRegex(ValueError, 'unsupported manifest GC census'):
                    m.validate_measurement(malformed, cell)
        original = copy.deepcopy(raw)
        measurement = m.validate_measurement(raw, cell)
        self.assertEqual(raw, original)
        self.assertTrue(measurement['policy_fully_compacted'])
        self.assertFalse(m.policy_complete(measurement))
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            for label in ('C2', 'B1'):
                path = paths[label]; m.save(path.parent/'stdout.json', raw)
                self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(path.parent/'stdout.json')}))
                _, loaded = m.load_run(path)
                self.assertEqual(loaded['leaf_gc']['stats'], stats)
                self.assertFalse(m.policy_complete(loaded))
            output = root/'rejected-noise.json'
            with self.assertRaisesRegex(ValueError, 'incomplete calibration run'):
                m.calibrate([paths[k] for k in ('C1', 'C2', 'C3')], 'elapsed_seconds', output)
            self.assertFalse(output.exists())
            m.save(paths['C2'].parent/'stdout.json', settle_report())
            self.change(paths['C2'], lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(paths['C2'].parent/'stdout.json')}))
            self.seal_pairs(root)
            result = m.analyze(root/'bundle.json')
            self.assertFalse(result['complete_runs']); self.assertFalse(result['material'])

    def test_settle_qualifies_final_completion_and_preserves_initial_report(self):
        raw = settle_report()
        raw['reports'][0]['before'][0]['bytes'] += 4096
        raw['reports'][0]['before'][-1]['bytes'] += 4096
        cell = dict(mode='exhaustive', batch_size=8192, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
        original = copy.deepcopy(raw)
        measurement = m.validate_measurement(raw, cell)
        self.assertEqual(raw, original)
        self.assertTrue(m.policy_complete(measurement))
        self.assertFalse(raw['reports'][0]['policy_fully_compacted'])
        self.assertEqual(measurement['before'], raw['reports'][0]['before'])
        self.assertEqual(measurement['after'], raw['reports'][1]['after'])
        self.assertEqual(len(measurement['phases']), 2)
        self.assertEqual(measurement['audit_phases'], raw['reports'][1]['phases'])
        self.assertEqual(m.compact_command('/bin/A', '/db', cell)[-1], '-command-wal-settle')
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            self.assertTrue(m.analyze(root/'bundle.json')['material'])

    def test_settle_noop_and_planned_audit_remain_distinct(self):
        cell = dict(mode='exhaustive', batch_size=8192, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
        raw = settle_report()
        raw['refresh']['result'] = copy.deepcopy(raw['refresh']['basis'])
        raw['refresh']['roots_after'] = copy.deepcopy(raw['refresh']['roots_before'])
        measurement = m.validate_measurement(raw, cell)
        self.assertTrue(m.policy_complete(measurement))
        # A legitimate unfinished audit remains diagnostic and is not promoted
        # into either completed policy debt or actually executed phases.
        raw['reports'][1]['remaining_debt']['index_vacuum_required'] = True
        raw['reports'][1].update(fully_compacted=False, policy_fully_compacted=False, byte_minimized=False)
        raw['reports'][1]['phases'][0].update(status='planned', required=True)
        measurement = m.validate_measurement(raw, cell)
        self.assertFalse(m.policy_complete(measurement))
        self.assertEqual(measurement['audit_phases'][0]['status'], 'planned')
        self.assertNotIn('planned', [p.get('status') for p in measurement['phases']])

    def test_settle_failed_audit_resealed_calibration_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            path = paths['C2']; raw = settle_report(); raw['audit_status'] = 'failed'
            m.save(path.parent/'stdout.json', raw)
            self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(path.parent/'stdout.json')}))
            noise_path = root/'noise.json'; noise = json.loads(noise_path.read_bytes())
            noise['hashes'][1] = m.unified.sha256(path); m.save(noise_path, noise)
            self.change(root/'bundle.json', lambda r: r.update(calibration_sha256=m.unified.sha256(noise_path)))
            with self.assertRaisesRegex(ValueError, 'incomplete settle operation'):
                m.analyze(root/'bundle.json')
            with self.assertRaises(ValueError):
                m.calibrate([paths[k] for k in ('C1', 'C2', 'C3')], 'elapsed_seconds', root/'invalid-noise.json')
            self.assertFalse((root/'invalid-noise.json').exists())

    def test_settle_command_cannot_drop_endpoint_flag(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            self.change(paths['B1'], lambda r: r['command'].pop())
            self.seal_pairs(root)
            with self.assertRaisesRegex(ValueError, 'CLI flag drift'):
                m.analyze(root/'bundle.json')

    def test_settle_malformed_bindings_fail_closed(self):
        cell = dict(mode='exhaustive', batch_size=8192, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
        mutations = [lambda r: r.update(schema=True), lambda r: r.update(refresh=None), lambda r: r.update(leaf_gc=None), lambda r: r.update(status='failed'),
                     lambda r: r.update(error='cleanup failed'), lambda r: r['reports'].pop(),
                     lambda r: r['refresh'].update(checkpoint_status='failed'),
                     lambda r: r['refresh']['result'].update(CommitSeq=27),
                     lambda r: r['refresh']['result'].update(RootPageID=101),
                     lambda r: r['refresh']['result'].update(SystemRootPageID=201),
                     lambda r: r['refresh']['result'].update(AppliedCommandLSN=1),
                     lambda r: r['refresh']['result'].update(MaxEntryRevision=301),
                     lambda r: r['reports'][1].update(dry_run=False),
                     lambda r: r['reports'][1]['phases'][0].update(status='succeeded'),
                     lambda r: r['reports'][1]['phases'][0].update(name='checkpoint'),
                     lambda r: r['reports'][1]['phases'][0].pop('status'),
                     lambda r: r['reports'][0]['phases'][0].update(status='planned'),
                     lambda r: r.update(initial_status='not_started'),
                     lambda r: r.update(audit_status='failed'),
                     lambda r: r.update(cleanup_status='failed'),
                     lambda r: r['refresh'].update(status='not_started'),
                     lambda r: r['refresh'].update(summary_after_status='failed'),
                     lambda r: r['refresh'].update(next_lsn_after=38),
                     lambda r: r['refresh'].update(next_lsn_before=36, next_lsn_after=36),
                     lambda r: r['leaf_gc'].update(status='failed'),
                     lambda r: r['leaf_gc']['stats'].update(BytesDeleted=-1),
                     lambda r: r['leaf_gc']['stats'].update(GenerationsDeleted=10),
                     lambda r: r['leaf_gc']['stats'].update(FilesDeleted=True),
                     lambda r: r['refresh']['basis'].update(CommitSeq=True),
                     lambda r: r['refresh']['roots_before'][0].update(UserRootPageID=101),
                     lambda r: r['refresh']['roots_after'][1].update(Durable=False)]
        for mutation in mutations:
            raw = settle_report(); mutation(raw)
            with self.assertRaises(ValueError):
                m.validate_measurement(raw, cell)
        with self.assertRaises(ValueError):
            m.validate_measurement(settle_report(), dict(mode='exhaustive'))
        with self.assertRaises(ValueError):
            m.validate_measurement(report(), cell)
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            path = paths['B1']; raw = settle_report()
            raw['refresh']['result']['SystemRootPageID'] = 201
            raw['refresh']['roots_after'][1]['SystemRootPageID'] = 201
            m.save(path.parent/'stdout.json', raw)
            self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(path.parent/'stdout.json')}))
            self.seal_pairs(root)
            with self.assertRaisesRegex(ValueError, 'refresh changed root/RID authority'):
                m.analyze(root/'bundle.json')

    def test_settle_initial_dispositions_block_calibration_and_qualification(self):
        for status in ('deferred', 'unsupported'):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); paths = self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
                for label in ('C2', 'B1'):
                    path = paths[label]; raw = settle_report()
                    raw['reports'][0]['phases'][0]['status'] = status
                    m.save(path.parent/'stdout.json', raw)
                    self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(path.parent/'stdout.json')}))
                output = root/'rejected-noise.json'
                with self.assertRaisesRegex(ValueError, 'incomplete calibration run'):
                    m.calibrate([paths[k] for k in ('C1', 'C2', 'C3')], 'elapsed_seconds', output)
                self.assertFalse(output.exists())
                # Restore calibration, retaining the rejected initial phase in B1.
                m.save(paths['C2'].parent/'stdout.json', settle_report())
                self.change(paths['C2'], lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(paths['C2'].parent/'stdout.json')}))
                self.seal_pairs(root)
                result = m.analyze(root/'bundle.json')
                self.assertFalse(result['complete_runs']); self.assertFalse(result['material'])

    def test_endpoint_identity_is_derived_from_raw_and_cannot_mix(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root)
            self.change(paths['B1'], lambda r: r.update(endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT))
            self.seal_pairs(root)
            # Forged metadata cannot promote a legacy command to a settle.
            run, _ = m.load_run(paths['B1'])
            self.assertEqual(run['endpoint'], m.SINGLE_COMPACT_ENDPOINT)
            self.assertTrue(m.analyze(root/'bundle.json')['material'])
            path = paths['B1']; raw = settle_report(); m.save(path.parent/'stdout.json', raw)
            self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(path.parent/'stdout.json')}))
            self.seal_pairs(root)
            with self.assertRaisesRegex(ValueError, 'unexpected endpoint receipt'):
                m.analyze(root/'bundle.json')
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root, endpoint=m.COMMAND_WAL_SETTLE_ENDPOINT)
            path = paths['B1']; parent = path.parent.parent
            plan = json.loads((parent/'plan.json').read_bytes())
            cell = next(c for c in plan['cells'] if c['label'] == 'B1'); del cell['endpoint']
            m.save(parent/'plan.json', plan)
            for run_path in paths.values():
                self.change(run_path, lambda r: r.update(plan_sha256=m.unified.sha256(parent/'plan.json')))
            self.change(path, lambda r: r.update(cell=cell, command=m.compact_command(r['command'][0], path.parent/'db', cell)))
            m.save(path.parent/'stdout.json', report())
            self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(path.parent/'stdout.json')}))
            # The single-compact packet is independently valid, but mismatches
            # the settle calibration contract even after every hash reseal.
            m.load_run(path)
            noise_path = root/'noise.json'; noise = json.loads(noise_path.read_bytes())
            noise['hashes'] = [m.unified.sha256(paths[k]) for k in ('C1', 'C2', 'C3')]; m.save(noise_path, noise)
            bundle_path = root/'bundle.json'; bundle = json.loads(bundle_path.read_bytes())
            bundle['calibration_sha256'] = m.unified.sha256(noise_path); m.save(bundle_path, bundle)
            self.seal_pairs(root)
            with self.assertRaisesRegex(ValueError, 'matched workload/environment/harness drift'):
                m.analyze(bundle_path)

    def test_raw_hash_and_calibration_drift_rejected(self):
        for label, filename in [('B1', 'stdout.json'), ('B3', 'rss_samples.jsonl'), ('C2', 'run.json')]:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); paths = self.make_campaign(root)
                path = paths[label].parent/filename; path.write_bytes(path.read_bytes()+b' ')
                with self.assertRaisesRegex(ValueError, 'drift'):
                    m.analyze(root/'bundle.json')

    def test_boundaries_reject_changed_controls(self):
        changes = {
            'fixture': lambda r: r['fixture'].update(sha256='e'*64),
            'source': lambda r: r['source'].update(head='c'*40),
            'flags': lambda r: r['command'].append('-rewrite-batch-size=256'),
            'harness': lambda r: r['harness'].update(collector='different'),
            'sample': lambda r: r['memory']['sampled_peak'].update(file_bytes=999),
            'env': lambda r: r['env'].update(GOMEMLIMIT='2GiB'),
            'order': lambda r: r.update(started_ns=1000000000000+100*1000000000),
            'failed': lambda r: r.update(validated=False),
        }
        for name, mutation in changes.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); paths = self.make_campaign(root)
                self.change(paths['B2'], mutation)
                self.seal_pairs(root)
                with self.assertRaises(ValueError):
                    m.analyze(root/'bundle.json')

    def test_full_environment_receipts_are_required_and_manifest_bound(self):
        mutations = [lambda r: r.pop('environment_policy'),
                     lambda r: r.update(environment_policy='allowlist-v0'),
                     lambda r: r.update(env=[]),
                     lambda r: r['env'].update(MALLOC_ARENA_MAX='8'),
                     lambda r: r['env'].pop('MALLOC_ARENA_MAX'),
                     lambda r: r['env'].update(UNDECLARED_PROCESS_CONTROL='1')]
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp)
                paths = self.make_campaign(root, build_env=dict(GOWORK='off', MALLOC_ARENA_MAX='2'))
                run, _ = m.load_run(paths['B1'])
                self.assertEqual(run['env']['MALLOC_ARENA_MAX'], '2')
                self.change(paths['B1'], mutation)
                self.seal_pairs(root)
                with self.assertRaisesRegex(ValueError, 'environment'):
                    m.analyze(root/'bundle.json')
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root)
            for label in ('C1', 'C2', 'C3'):
                self.change(paths[label], lambda r: r.pop('environment_policy'))
            with self.assertRaisesRegex(ValueError, 'incomplete environment receipt'):
                m.calibrate([paths[k] for k in ('C1', 'C2', 'C3')], 'elapsed_seconds', root/'new-noise.json')
            self.assertFalse((root/'new-noise.json').exists())

    def test_incomplete_sampling_and_policy_debt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root)
            path = paths['B1']; directory = path.parent
            broken = report(); broken['remaining_debt']['value_log_gc_bytes'] = 1
            broken.update(fully_compacted=False, policy_fully_compacted=False, byte_minimized=False)
            m.save(directory/'stdout.json', broken)
            self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(directory/'stdout.json')}))
            self.seal_pairs(root)
            result = m.analyze(root/'bundle.json')
            self.assertFalse(result['equal_completion']); self.assertFalse(result['material'])
            self.change(path, lambda r: r['rss_sampling'].update(complete=False))
            self.seal_pairs(root)
            with self.assertRaises(ValueError):
                m.analyze(root/'bundle.json')

    def test_equal_incomplete_runs_are_diagnostics(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root)
            for i, label in enumerate(('A1', 'B1', 'A2', 'B2', 'A3', 'B3')):
                path = paths[label]; directory = path.parent
                incomplete = report(); incomplete['remaining_debt']['value_log_gc_bytes'] = i+1
                incomplete.update(fully_compacted=False, policy_fully_compacted=False, byte_minimized=False)
                m.save(directory/'stdout.json', incomplete)
                self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(directory/'stdout.json')}))
            self.seal_pairs(root)
            result = m.analyze(root/'bundle.json')
            self.assertTrue(result['equal_completion']); self.assertFalse(result['complete_runs'])
            self.assertFalse(result['material']); self.assertTrue(result['all_favourable'])

    def test_every_matched_packet_requires_external_digest(self):
        for label in ('A1', 'B1', 'B2', 'A2', 'A3', 'B3'):
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); paths = self.make_campaign(root, candidate=11.)
                self.change(paths[label], lambda r: r.update(elapsed_seconds=8.))
                with self.assertRaisesRegex(ValueError, 'matched run digest drift'):
                    m.analyze(root/'bundle.json')
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); self.make_campaign(root)
            path = root/'bundle.json'; bundle = json.loads(path.read_bytes())
            del bundle['pairs'][0]['A_sha256']; m.save(path, bundle)
            with self.assertRaisesRegex(ValueError, 'matched run digest drift'):
                m.analyze(path)

    def test_incomplete_phase_dispositions_cannot_qualify(self):
        for status in ('deferred', 'unsupported'):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); paths = self.make_campaign(root)
                for label in ('A1', 'B1', 'B2', 'A2', 'A3', 'B3'):
                    path = paths[label]; directory = path.parent
                    incomplete = report(); incomplete['phases'][0]['status'] = status
                    m.save(directory/'stdout.json', incomplete)
                    self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(directory/'stdout.json')}))
                self.seal_pairs(root)
                result = m.analyze(root/'bundle.json')
                self.assertTrue(result['equal_completion']); self.assertTrue(result['all_favourable'])
                self.assertFalse(result['complete_phase_dispositions'])
                self.assertFalse(result['complete_runs']); self.assertFalse(result['material'])

    def test_separate_capture_directories_require_common_campaign_receipts(self):
        for role in (None, 'source', 'build', 'native', 'runner'):
            with self.subTest(role=role), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp).resolve(); self.make_campaign(root)
                other = root/'other'; m.shutil.copytree(root/'out', other)
                manifest = json.loads((other/'manifest.json').read_bytes())
                if role is not None:
                    receipt = other/(role+'-receipt.json')
                    m.save(receipt, {'reviewed': role, 'different_campaign': True})
                    manifest['receipts'][role]['sha256'] = m.unified.sha256(receipt)
                    m.save(other/'manifest.json', manifest)
                alternative = other/'B2'/'run.json'
                self.change(alternative, lambda r: r.update(
                    manifest_sha256=m.unified.sha256(other/'manifest.json'),
                    command=m.compact_command(r['command'][0], alternative.parent/'db', r['cell'])))
                self.change(alternative, lambda r: r['snapshot_restore'].update(
                    command=[r['snapshot_restore']['command'][0], str(alternative.parent/'db')]))
                # Each alternate packet is valid on its own, with independently
                # hashed manifest/receipts. The comparison is the failed gate.
                m.load_run(alternative)
                bundle_path = root/'bundle.json'; bundle = json.loads(bundle_path.read_bytes())
                bundle['pairs'][1].update(B=str(alternative), B_sha256=m.unified.sha256(alternative))
                m.save(bundle_path, bundle)
                if role is None:
                    self.assertTrue(m.analyze(bundle_path)['material'])
                else:
                    with self.assertRaisesRegex(ValueError, 'matched workload/environment/harness drift'):
                        m.analyze(bundle_path)

    def make_calibration_incomplete(self, path, status):
        incomplete = report()
        if status == 'policy_debt':
            incomplete['remaining_debt']['value_log_gc_bytes'] = 1
            incomplete.update(fully_compacted=False, policy_fully_compacted=False, byte_minimized=False)
        else:
            incomplete['phases'][0]['status'] = status
        m.save(path.parent/'stdout.json', incomplete)
        self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(path.parent/'stdout.json')}))
        m.load_run(path)  # Truthful incomplete reports remain valid diagnostics.

    def test_calibration_rejects_every_incomplete_characterization_before_write(self):
        for label in ('C1', 'C2', 'C3'):
            for status in ('deferred', 'unsupported', 'policy_debt'):
                with self.subTest(label=label, status=status), tempfile.TemporaryDirectory() as temp:
                    root = pathlib.Path(temp); paths = self.make_campaign(root)
                    self.make_calibration_incomplete(paths[label], status)
                    output = root/'rejected-noise.json'
                    with self.assertRaisesRegex(ValueError, 'incomplete calibration run'):
                        m.calibrate([paths[k] for k in ('C1', 'C2', 'C3')], 'elapsed_seconds', output)
                    self.assertFalse(output.exists())

    def test_analyze_revalidates_completion_of_hash_bound_calibration(self):
        for status in ('deferred', 'unsupported', 'policy_debt'):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); paths = self.make_campaign(root)
                self.make_calibration_incomplete(paths['C2'], status)
                noise_path = root/'noise.json'; noise = json.loads(noise_path.read_bytes())
                noise['hashes'][1] = m.unified.sha256(paths['C2']); m.save(noise_path, noise)
                bundle_path = root/'bundle.json'; bundle = json.loads(bundle_path.read_bytes())
                bundle['calibration_sha256'] = m.unified.sha256(noise_path); m.save(bundle_path, bundle)
                # Digest, chronology and numeric noise checks all still agree;
                # the invalid work-completion endpoint must reject the packet.
                with self.assertRaisesRegex(ValueError, 'incomplete calibration run'):
                    m.analyze(bundle_path)


if __name__ == '__main__':
    unittest.main()
