#!/usr/bin/env python3
"""Validate the complete bounded #342 packet; counter timings are diagnostic."""
import argparse
import hashlib
import itertools
import json
import math
from pathlib import Path
import re

PAYLOADS = ("compressible256", "random4096")
ROUTES = ("Get", "GetAppend", "GetMany64", "GetManyView64", "OldSnapshot")


def public_names(modes):
    return {f"BenchmarkNegativeLookup/{p}/enabled={e}/{d}/miss={m}/{r}"
            for p, e, d, m, r in itertools.product(
                PAYLOADS, modes, ("uniform", "zipf"), (0, 50, 90, 99), ROUTES)}


def plan():
    jobs = []

    def add(name, revision, package, pattern, duration, names, counters=False):
        jobs.append(dict(id=name, revision=revision, package=package,
                         pattern=pattern, duration=duration, names=sorted(names),
                         counters=counters))

    for round_number in range(1, 6):
        modes = ("false", "true") if round_number % 2 else ("true", "false")
        for mode in modes:
            add(f"public-{round_number}-{mode}", "candidate", "./TreeDB",
                f"^BenchmarkNegativeLookup$/(compressible256|random4096)/enabled={mode}$",
                "500ms", public_names((mode,)))
    canonical = {f"{b}/{r}" for b, r in itertools.product(
        ("BenchmarkDBCheckpointedValueLogGet", "BenchmarkSnapshotValueLogGet"),
        ("Get", "GetAppend", "GetUnsafe"))}
    for round_number in range(1, 6):
        revisions = ("reference", "candidate") if round_number % 2 else ("candidate", "reference")
        for revision in revisions:
            add(f"canonical-{round_number}-{revision}", revision, "./TreeDB",
                "^Benchmark(DBCheckpointed|Snapshot)ValueLogGet$", "500ms", canonical)
    updates = {f"BenchmarkNegativeLookupUpdate/enabled={e}/{r}"
               for e, r in itertools.product(("false", "true"), ("WriteSync", "WriteSyncCheckpoint"))}
    updates |= {f"BenchmarkNegativeLookupBootstrap/enabled={e}" for e in ("false", "true")}
    for round_number in range(1, 6):
        add(f"updates-{round_number}", "candidate", "./TreeDB",
            "^BenchmarkNegativeLookup(Update|Bootstrap)$", "10x", updates)
    add("counters", "candidate", "./TreeDB", "^BenchmarkNegativeLookup$",
        "10000x", public_names(("false", "true")), True)
    for round_number in range(1, 6):
        add(f"membership-{round_number}", "candidate", "./TreeDB/tree",
            "^BenchmarkNegativeFilterMembership$", "500ms",
            {f"BenchmarkNegativeFilterMembership/covered={n}" for n in (8192, 81920)})
        add(f"prepare-{round_number}", "candidate", "./TreeDB/db",
            "^BenchmarkNegativeCoveragePrepare$", "500ms",
            {f"BenchmarkNegativeCoveragePrepare/enabled={e}/keys={n}"
             for e, n in itertools.product(("false", "true"), (1, 64, 256))})
    return jobs


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def parse_log(text, job):
    if not re.search(r"^PASS$", text, re.M) or not re.search(
            r"^ok\s+github\.com/snissn/gomap/TreeDB(?:/(?:tree|db))?\s", text, re.M):
        raise ValueError(f"{job['id']}: missing successful Go completion")
    if re.search(r"^(?:FAIL|panic:|fatal error:)", text, re.M):
        raise ValueError(f"{job['id']}: failure in log")
    rows = {}
    for line in text.splitlines():
        match = re.match(r"^(Benchmark\S+)-4\s+(\d+)\s+(.*)$", line)
        if not match:
            continue
        name, iterations, rest = match.groups()
        fields = rest.split()
        if name in rows or name not in job['names'] or int(iterations) < 1 or len(fields) % 2:
            raise ValueError(f"{job['id']}: invalid/duplicate/unexpected row {name}")
        metrics = {}
        for value, unit in zip(fields[::2], fields[1::2]):
            number = float(value)
            if not math.isfinite(number) or number < 0 or unit in metrics:
                raise ValueError(f"{job['id']}: invalid metric {name} {unit}")
            metrics[unit] = number
        if not {'ns/op', 'B/op', 'allocs/op'} <= metrics.keys() or metrics['ns/op'] <= 0:
            raise ValueError(f"{job['id']}: missing timing/allocation metrics {name}")
        enabled = '/enabled=true' in name
        if name.startswith('BenchmarkNegativeLookup/'):
            keys = 64 if name.endswith(('GetMany64', 'GetManyView64')) else 1
            if metrics.get('keys/op') != keys or metrics.get('filter-bytes', 0) != (10240 if enabled else 0):
                raise ValueError(f"{job['id']}: active coverage/key count mismatch {name}")
            counters = {'descents/key', 'rejects/key'}
            if job['counters'] and not counters <= metrics.keys():
                raise ValueError(f"{job['id']}: missing counters {name}")
            if not job['counters'] and counters & metrics.keys():
                raise ValueError(f"{job['id']}: throughput contaminated by counters {name}")
            if job['counters'] and not enabled and metrics['rejects/key'] != 0:
                raise ValueError(f"{job['id']}: disabled route rejected a key {name}")
        if name.startswith('BenchmarkNegativeLookupUpdate/'):
            if metrics.get('keys/op') != 64 or 'publications/op' not in metrics:
                raise ValueError(f"{job['id']}: missing publication evidence {name}")
            if name.endswith('WriteSyncCheckpoint') and metrics['publications/op'] < 1:
                raise ValueError(f"{job['id']}: checkpoint failed to publish {name}")
        if name.startswith('BenchmarkNegativeFilterMembership/'):
            count = int(name.rsplit('=', 1)[1])
            if not (0 <= metrics.get('false-positives/miss', -1) <= 1
                    and 10240 < metrics.get('retained-bytes', 0) <= 11000
                    and metrics.get('bytes/key') == 10240 / count):
                raise ValueError(f"{job['id']}: invalid fixed-memory proof {name}")
        if name.startswith('BenchmarkNegativeCoveragePrepare/'):
            if metrics.get('keys/op') != int(name.rsplit('=', 1)[1]):
                raise ValueError(f"{job['id']}: invalid mutation count {name}")
            if enabled and not (0 < metrics.get('token-bytes', 0) <= 256):
                raise ValueError(f"{job['id']}: missing bounded token storage {name}")
            if not enabled and metrics.get('token-bytes', 0) != 0:
                raise ValueError(f"{job['id']}: disabled token storage {name}")
        rows[name] = dict(iterations=int(iterations), metrics=metrics)
    if set(rows) != set(job['names']):
        raise ValueError(f"{job['id']}: expected {len(job['names'])}, got {len(rows)}; missing {sorted(set(job['names'])-rows.keys())}")
    return rows


def validate(directory):
    directory = Path(directory)
    manifest = json.loads((directory / 'capture.json').read_text())
    jobs = plan()
    if manifest.get('complete') is not True or len(manifest.get('runs', [])) != len(jobs):
        raise ValueError('incomplete capture')
    if manifest.get('source_before') != manifest.get('source_after'):
        raise ValueError('source changed during capture')
    identities = manifest.get('source_before', {})
    if set(identities) != {'candidate', 'reference'} or any(
            not re.fullmatch(r'[0-9a-f]{64}', value) for value in identities.values()):
        raise ValueError('missing frozen source inventories')
    if not manifest.get('grant') or any(not re.fullmatch(r'[0-9a-f]{40}', manifest.get(k, ''))
                                        for k in ('candidate_head', 'reference_head')):
        raise ValueError('missing grant/revision identities')
    env = manifest.get('environment', {})
    if any(env.get(k) != v for k, v in {'GOWORK': 'off', 'GOMAXPROCS': '4', 'GOMEMLIMIT': '1GiB'}.items()):
        raise ValueError('wrong timing environment')
    parsed = []
    for job, record in zip(jobs, manifest['runs']):
        if record.get('job') != job or record.get('returncode') != 0:
            raise ValueError(f"wrong/failed execution {job['id']}")
        expected_command = [env.get('GOROOT', '') + '/bin/go', 'test', job['package'],
                            '-run', '^$', '-bench', job['pattern'], '-benchmem',
                            '-benchtime=' + job['duration'], '-count=1', '-timeout=20m']
        if record.get('command') != expected_command:
            raise ValueError(f"wrong command {job['id']}")
        log = directory / (job['id'] + '.log')
        if digest(log) != record.get('log_sha256'):
            raise ValueError(f"changed raw log {job['id']}")
        parsed.append(dict(job=job, rows=parse_log(log.read_text(), job)))
    performance = sum(len(p['rows']) for p in parsed if not p['job']['counters'])
    diagnostic = sum(len(p['rows']) for p in parsed if p['job']['counters'])
    if (performance, diagnostic) != (930, 160):
        raise ValueError(f'wrong total sample counts {performance}/{diagnostic}')
    return dict(classification='bounded PR qualification; authoritative campaign waits landed H',
                performance_samples=performance, diagnostic_rows=diagnostic, processes=len(jobs),
                candidate_head=manifest['candidate_head'], reference_head=manifest['reference_head'],
                parsed=parsed)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory')
    args = parser.parse_args()
    print(json.dumps(validate(args.directory), indent=2, sort_keys=True))
