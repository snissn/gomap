#!/usr/bin/env python3
"""Cheap evidence-gate tests; no Go compilation or runner activity."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

from capture_qualification import source_identity
import parse_qualification as parser


def synthetic_log(job):
    lines = []
    for name in job['names']:
        metrics = '10 ns/op 0 B/op 0 allocs/op'
        enabled = '/enabled=true' in name
        if name.startswith('BenchmarkNegativeLookup/'):
            metrics += f" {64 if name.endswith(('GetMany64', 'GetManyView64')) else 1} keys/op"
            if enabled:
                metrics += ' 10240 filter-bytes'
            if job['counters']:
                metrics += ' 1 descents/key 0 rejects/key'
        if name.startswith('BenchmarkNegativeLookupUpdate/'):
            metrics += ' 64 keys/op 1 publications/op'
        if name.startswith('BenchmarkNegativeFilterMembership/'):
            count = int(name.rsplit('=', 1)[1])
            metrics += f' 0.01 false-positives/miss 10304 retained-bytes {10240/count} bytes/key'
        if name.startswith('BenchmarkNegativeCoveragePrepare/'):
            metrics += f" {name.rsplit('=', 1)[1]} keys/op"
            if enabled:
                metrics += ' 64 token-bytes'
        lines.append(f'{name}-4 10 {metrics}')
    return '\n'.join(lines) + '\nPASS\n'


class ParserTests(unittest.TestCase):
    def test_source_inventory_rejects_changes_and_extra_sources(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / 'example.go'
            source.write_text('package example\n')
            manifest = root / 'source.sha256'
            manifest.write_text(parser.digest(source) + '  example.go\n')
            self.assertEqual(source_identity(root, manifest), parser.digest(manifest))
            source.write_text('package changed\n')
            with self.assertRaises(ValueError):
                source_identity(root, manifest)
            source.write_text('package example\n')
            (root / 'extra.go').write_text('package example\n')
            with self.assertRaises(ValueError):
                source_identity(root, manifest)

    def test_all_36_jobs_and_1090_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = dict(GOROOT='/go', GOWORK='off', GOMAXPROCS='4', GOMEMLIMIT='1GiB')
            manifest = dict(complete=True, source_before=dict(candidate='a'*64, reference='b'*64),
                            source_after=dict(candidate='a'*64, reference='b'*64), grant='test-only',
                            candidate_head='c'*40, reference_head='d'*40, environment=env, runs=[])
            preparation = root / 'preparation'
            preparation.mkdir()
            prepared = dict(complete=True, candidate_head=manifest['candidate_head'], reference_head=manifest['reference_head'],
                            source_before=manifest['source_before'], source_after=manifest['source_after'],
                            roots=dict(candidate='/synthetic/candidate', reference='/synthetic/reference'),
                            scripts_before=parser.script_identity(), scripts_after=parser.script_identity(),
                            environment=dict(GOROOT='/go', GOWORK='off', GOMAXPROCS='2', GOMEMLIMIT='2GiB'),
                            compiles=[], binaries_before={})
            for identifier, revision, package in parser.BINARY_SPECS:
                binary = preparation / (identifier + '.test')
                binary.write_bytes(b'synthetic binary identity; not executable')
                log = preparation / (identifier + '.compile.log')
                log.write_text('synthetic compile record; no Go invoked\n')
                prepared['binaries_before'][identifier] = dict(path=str(binary), sha256=parser.digest(binary))
                prepared['compiles'].append(dict(id=identifier, returncode=0, cwd=prepared['roots'][revision],
                                                 command=['/go/bin/go', 'test', '-c', '-o', str(binary), package],
                                                 source_manifest_sha256=prepared['source_before'][revision],
                                                 log_sha256=parser.digest(log)))
            prepared['binaries_after'] = copy.deepcopy(prepared['binaries_before'])
            preparation_file = preparation / 'prepare.json'
            preparation_file.write_text(json.dumps(prepared))
            manifest.update(preparation_directory=str(preparation), preparation_sha256=parser.digest(preparation_file),
                            binaries_before=prepared['binaries_before'], binaries_after=prepared['binaries_after'],
                            scripts_before=prepared['scripts_before'], scripts_after=prepared['scripts_after'])
            for job in parser.plan():
                log = root / (job['id'] + '.log')
                log.write_text(synthetic_log(job))
                binary = prepared['binaries_before'][parser.binary_id(job)]
                manifest['runs'].append(dict(job=job, returncode=0, command=parser.execution_command(job, binary),
                                            cwd=prepared['roots'][job['revision']], binary_sha256=binary['sha256'],
                                            log_sha256=parser.digest(log)))
            path = root / 'capture.json'
            path.write_text(json.dumps(manifest))
            parsed = parser.validate(root)
            self.assertEqual((parsed['processes'], parsed['performance_samples'], parsed['diagnostic_rows']), (36, 930, 160))
            for mutation in ('partial', 'source', 'failed', 'command', 'environment', 'log', 'binary', 'script', 'binding'):
                broken = copy.deepcopy(manifest)
                if mutation == 'partial':
                    broken['runs'].pop()
                elif mutation == 'source':
                    broken['source_after']['candidate'] = 'e'*64
                elif mutation == 'failed':
                    broken['runs'][0]['returncode'] = 1
                elif mutation == 'command':
                    broken['runs'][0]['command'][-3] = '-test.benchtime=1x'
                elif mutation == 'environment':
                    broken['environment']['GOMAXPROCS'] = '2'
                elif mutation == 'binary':
                    broken['binaries_after']['candidate-treedb']['sha256'] = 'e'*64
                elif mutation == 'script':
                    broken['scripts_after']['capture_qualification.py'] = 'e'*64
                elif mutation == 'binding':
                    broken['runs'][0]['binary_sha256'] = 'e'*64
                else:
                    broken['runs'][0]['log_sha256'] = 'f'*64
                path.write_text(json.dumps(broken))
                with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                    parser.validate(root)
            manifest_file = root / 'capture.json'
            manifest_file.write_text(json.dumps(manifest))
            binary = Path(prepared['binaries_before']['candidate-treedb']['path'])
            binary.write_bytes(b'changed binary')
            with self.assertRaises(ValueError):
                parser.validate(root)

    def test_rows_fail_closed(self):
        job = next(j for j in parser.plan() if j['id'] == 'public-1-true')
        log = synthetic_log(job)
        broken_logs = (log.split('\n', 1)[1], log + log.split('\n')[0] + '\n',
                       log.replace('10240 filter-bytes', '0 filter-bytes', 1),
                       log.replace('10 ns/op', 'nan ns/op', 1), log.replace('\nPASS\n', '\nFAIL\n'),
                       log.replace('1 keys/op', '64 keys/op', 1),
                       log.replace('0 allocs/op', '0 allocs/op 1 rejects/key', 1))
        for i, broken in enumerate(broken_logs):
            with self.subTest(mutation=i), self.assertRaises(ValueError):
                parser.parse_log(broken, job)
        counters = next(j for j in parser.plan() if j['counters'])
        with self.assertRaises(ValueError):
            parser.parse_log(synthetic_log(counters).replace(' 1 descents/key', '', 1), counters)


if __name__ == '__main__':
    unittest.main()
