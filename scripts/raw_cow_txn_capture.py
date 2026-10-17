#!/usr/bin/env python3
"""Fresh-process public raw COW transaction diagnostics (not benchprof inputs)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save_manifest(path, manifest):
    temporary = path.with_suffix('.tmp')
    with temporary.open('w') as stream:
        stream.write(json.dumps(manifest, indent=2) + '\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def benchmark_rows(output):
    return [match.group(1) for line in output.splitlines()
            if (match := re.match(r'^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+\S+\s+ns/op\b', line))]


def runtime_sources(root):
    listing = subprocess.check_output(['go', 'list', '-buildvcs=false', '-deps', '-test', '-json', './TreeDB'], cwd=root, text=True)
    decoder = json.JSONDecoder()
    paths = {Path(__file__).resolve(), root / 'go.mod', root / 'go.sum'}
    for name in ['go.work', 'go.work.sum']:
        if (root / name).exists():
            paths.add(root / name)
    while listing.strip():
        package, used = decoder.raw_decode(listing.lstrip())
        listing = listing.lstrip()[used:]
        directory = Path(package.get('Dir', '/')).resolve()
        if directory.is_relative_to(root):
            for field in ['GoFiles', 'CgoFiles', 'CFiles', 'CXXFiles', 'MFiles', 'HFiles',
                          'FFiles', 'SFiles', 'SwigFiles', 'SwigCXXFiles', 'SysoFiles',
                          'EmbedFiles', 'TestGoFiles', 'XTestGoFiles', 'TestEmbedFiles', 'XTestEmbedFiles']:
                paths.update(path for name in package.get(field, [])
                             if (path := (directory / name).resolve()).is_relative_to(root))
    return {str(p.relative_to(root)): sha(p) for p in sorted(paths)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--binary', type=Path, help='reuse an exact prebuilt TreeDB test binary')
    parser.add_argument('--binary-manifest', type=Path, help='required source/binary manifest for --binary')
    parser.add_argument('--bench', default='^BenchmarkPublicCOWTxn$')
    parser.add_argument('--expect-row', action='append', help='exact benchmark row name; required with custom --bench')
    parser.add_argument('--benchtime', default='128x')
    parser.add_argument('--count', type=int, default=3)
    parser.add_argument('--profiles', action='store_true', help='separate instrumented process; not timing evidence')
    args = parser.parse_args()
    if args.count < 1:
        parser.error('--count must be positive')
    if args.bench != '^BenchmarkPublicCOWTxn$' and not args.expect_row:
        parser.error('custom --bench requires --expect-row for each exact benchmark row')
    expected = sorted(args.expect_row or [
        'BenchmarkPublicCOWTxn/%s/%s/History%d' % (profile, shape, history)
        for profile in ['command_wal_durable', 'command_wal_relaxed', 'no_wal_fast']
        for shape in ['Point1', 'Point10', 'Scan', 'ReverseScan', 'ScanNewer2048', 'ReverseScanNewer2048']
        for history in [0, 2048] if history == 0 or not shape.endswith('Newer2048')])
    root = Path(__file__).resolve().parent.parent
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve() if args.binary else out / 'treedb.test'
    source_hashes = runtime_sources(root)
    if not args.binary:
        subprocess.run(['go', 'test', '-buildvcs=false', '-c', '-o', str(binary), './TreeDB'], cwd=root, check=True)
        if runtime_sources(root) != source_hashes:
            raise SystemExit('runtime source changed during build; no samples captured')
    provenance = None
    if args.binary:
        if not args.binary_manifest:
            parser.error('--binary requires --binary-manifest with binary_sha256 and sources')
        provenance = json.loads(args.binary_manifest.read_text())
        if provenance.get('binary_sha256') != sha(binary) or provenance.get('sources') != source_hashes:
            parser.error('external binary manifest does not match binary and current source/harness hashes')
    manifest = {
        'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'go': subprocess.check_output(['go', 'version'], cwd=root, text=True).strip(),
        'go_version_m': subprocess.check_output(['go', 'version', '-m', str(binary)], cwd=root, text=True),
        'go_env': json.loads(subprocess.check_output(['go', 'env', '-json', 'GOFLAGS', 'CGO_ENABLED', 'GOOS', 'GOARCH', 'GOWORK'], cwd=root, text=True)),
        'runtime_env': {name: os.environ.get(name, '<unset>') for name in ['GOMAXPROCS', 'GOGC', 'GOMEMLIMIT']},
        'build_argv': None if args.binary else ['go', 'test', '-buildvcs=false', '-c', '-o', str(binary), './TreeDB'],
        'binary_sha256': sha(binary),
        'sources': source_hashes,
        'source_inputs': 'go list -deps -test local source/assembly/embed inputs, module/workspace config and runner',
        'external_binary_manifest_sha256': sha(args.binary_manifest) if args.binary else None,
        'boundary': 'benchmark ACK loop; setup/final Close excluded; profiles instrumented separately',
        'commands': [],
    }
    (out / 'source.patch').write_bytes(subprocess.check_output(['git', 'diff', '--binary', 'HEAD'], cwd=root))
    command = [str(binary), '-test.run=^$', '-test.bench=' + args.bench,
               '-test.benchtime=' + args.benchtime, '-test.benchmem', '-test.count=1']
    for n in range(args.count + int(args.profiles)):
        profile = n == args.count
        name = 'diagnostic' if profile else 'sample-%02d' % (n + 1)
        argv = command + (['-test.cpuprofile=' + str(out / 'cpu.pprof'),
                           '-test.memprofile=' + str(out / 'allocs.pprof'),
                           '-test.memprofilerate=1'] if profile else [])
        result = subprocess.run(argv, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        with (out / (name + '.txt')).open('wb') as log:
            log.write(result.stdout)
            log.flush()
            os.fsync(log.fileno())
        path = out / (name + '.txt')
        if path.read_bytes() != result.stdout:
            raise SystemExit('stdout readback mismatch; no valid sample recorded')
        output = result.stdout.decode()
        rows = benchmark_rows(output)
        complete = sorted(rows) == expected and output.rstrip().endswith('PASS')
        manifest['commands'].append({'argv': argv, 'exit': result.returncode, 'instrumented': profile,
                                     'stdout_sha256': hashlib.sha256(result.stdout).hexdigest(), 'stdout': output,
                                     'rows': rows, 'complete': complete})
        save_manifest(out / 'manifest.json', manifest)
        if result.returncode or not complete:
            raise SystemExit(result.returncode or 'incomplete benchmark output; retained as invalid evidence')
    print(out)


if __name__ == '__main__':
    main()
