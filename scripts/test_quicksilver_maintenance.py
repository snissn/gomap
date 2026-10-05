import copy
import json
import pathlib
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


class MaintenanceTests(unittest.TestCase):
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

    def make_campaign(self, root, candidate=8.0):
        root = root.resolve()
        out = root/'out'; out.mkdir()
        manifest = dict(sources={name: dict(head=letter*40, binary=name, binary_sha256=letter*64)
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
            plan['cells'].append(cell)
            started = 1000000000000+offset*1000000000; finished = started+int(elapsed*1e9)
            rows = [dict(pid=5, process_start_ticks=1, started_unix_nano=started+200000000,
                         finished_unix_nano=started+200010000, rss_bytes=10000, anonymous_bytes=6000,
                         file_bytes=4000, shared_memory_bytes=0)]
            (directory/'rss_samples.jsonl').write_text(json.dumps(rows[0])+'\n')
            summary = dict(complete=True, cancelled=False, errors=[], interval_ms=200, samples=1)
            m.save(directory/'rss_samples.jsonl.summary.json', summary)
            (directory/'stderr.log').write_text('Maximum resident set size (kbytes): 12\n')
            m.save(directory/'stdout.json', report())
            (directory/'ldd.stdout.txt').write_text('static linkage\n'); (directory/'ldd.stderr.txt').touch()
            (directory/'restore.ldd.stdout.txt').write_text('static linkage\n'); (directory/'restore.ldd.stderr.txt').touch()
            (directory/'restore.stderr.log').touch()
            m.save(directory/'restore.stdout.json', dict(rebound=True, stores=['maindb'], operation='RebindDurableRootSnapshotLayoutWithContextV1'))
            run = dict(schema=1, cell=cell, source=manifest['sources'][source], fixture=dict(sha256='f'*64, files=4, bytes=7128),
                       command=m.compact_command('/bin/'+source, directory/'db', cell), env=dict(GOWORK='off'),
                       harness=dict(collector='h'*64, unified_collector='u'*64, rss_observer='s'*64),
                       native_resolution=dict(libraries={}), started_ns=started, finished_ns=finished,
                       elapsed_seconds=elapsed, rc=0, rss_sampling=summary, validated=True)
            run['restored_fixture'] = dict(run['fixture'])
            run['snapshot_restore'] = dict(source=manifest['sources']['restore'], command=['/bin/restore', str(directory/'db')], rc=0, native={})
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
                      pairs=[{k: str(paths[k+str(i)]) for k in ('A', 'B')} for i in (1, 2, 3)])
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
            manifest = dict(build_env=dict(GOWORK='off'), sources=dict(A=dict(head='a'*40, binary='treemap', binary_sha256=m.unified.sha256(binary))), libraries={}, receipts={})
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
                kwargs['stdout'].write('failed raw compact output\n'); kwargs['stderr'].write('preserved native diagnostic\n')
                sample_path.write_text('preserved sample\n')
                summary = dict(complete=False, cancelled=True, errors=[])
                m.save(str(sample_path)+'.summary.json', summary)
                return types.SimpleNamespace(returncode=-15), summary

            with mock.patch.object(m, 'restore_snapshot'), mock.patch.object(m.unified, 'validate_native'), mock.patch.object(m.owned_process_rss, 'run_with_rss', side_effect=failure):
                with self.assertRaisesRegex(ValueError, 'compact exit: -15'):
                    m.capture(root/'manifest.json', root/'plan.json')
            directory = root/'capture'/'failed'
            self.assertEqual(m.fingerprint(fixture), before); self.assertEqual(m.fingerprint(directory/'db'), before)
            self.assertEqual((directory/'stdout.json').read_text(), 'failed raw compact output\n')
            run = json.loads((directory/'run.json').read_bytes())
            self.assertIn('error', run); self.assertEqual(run['rc'], -15)
            self.assertTrue(run['rss_sampling']['cancelled'])
            self.assertEqual(m.unified.sha256(directory/'stderr.log'), run['artifacts']['stderr.log'])

    @staticmethod
    def change(path, mutation):
        run = json.loads(path.read_bytes()); mutation(run); m.save(path, run)

    def test_material_and_negative_packets(self):
        for candidate, material in [(8., True), (9.8, False), (11., False)]:
            with self.subTest(candidate=candidate), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); self.make_campaign(root, candidate)
                result = m.analyze(root/'bundle.json')
                self.assertEqual(result['material'], material)
                self.assertAlmostEqual(result['E'], .02)
                self.assertEqual(len(result['pair_effects']), 3)

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
            'env': lambda r: r['env'].update(GOMEMLIMIT='1GiB'),
            'order': lambda r: r.update(started_ns=1000000000000+100*1000000000),
            'failed': lambda r: r.update(validated=False),
        }
        for name, mutation in changes.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temp:
                root = pathlib.Path(temp); paths = self.make_campaign(root)
                self.change(paths['B2'], mutation)
                with self.assertRaises(ValueError):
                    m.analyze(root/'bundle.json')

    def test_incomplete_sampling_and_policy_debt(self):
        with tempfile.TemporaryDirectory() as temp:
            root = pathlib.Path(temp); paths = self.make_campaign(root)
            path = paths['B1']; directory = path.parent
            broken = report(); broken['remaining_debt']['value_log_gc_bytes'] = 1
            broken.update(fully_compacted=False, policy_fully_compacted=False, byte_minimized=False)
            m.save(directory/'stdout.json', broken)
            self.change(path, lambda r: r['artifacts'].update({'stdout.json': m.unified.sha256(directory/'stdout.json')}))
            result = m.analyze(root/'bundle.json')
            self.assertFalse(result['equal_completion']); self.assertFalse(result['material'])
            self.change(path, lambda r: r['rss_sampling'].update(complete=False))
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
            result = m.analyze(root/'bundle.json')
            self.assertTrue(result['equal_completion']); self.assertFalse(result['complete_runs'])
            self.assertFalse(result['material']); self.assertTrue(result['all_favourable'])


if __name__ == '__main__':
    unittest.main()
