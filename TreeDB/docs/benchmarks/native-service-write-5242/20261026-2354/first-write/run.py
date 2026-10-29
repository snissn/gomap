import datetime, importlib.util, json, pathlib, re, signal, statistics, subprocess, sys

ROOT = pathlib.Path('/mnt/fast4tb/gomap-mongo-execution-20261024')
WORK = ROOT / 'consumer-canonical'
PACKET = pathlib.Path(__file__).parent
spec = importlib.util.spec_from_file_location('owned_process', ROOT / 'evidence/native-query-work-witness1/run.py')
H = importlib.util.module_from_spec(spec)
spec.loader.exec_module(H)
signal.signal(signal.SIGTERM, H.interrupted)
signal.signal(signal.SIGINT, H.interrupted)
plan = json.loads((PACKET / 'public-write-plan.json').read_text())
GO_ROOT = pathlib.Path(plan['toolchain'])
GO = GO_ROOT / 'bin/go'

def source():
    paths = subprocess.check_output(['git', 'diff', '--name-only', 'HEAD'], cwd=WORK, text=True).splitlines()
    paths += subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=WORK, text=True).splitlines()
    return {'head': H.git(WORK, 'rev-parse', 'HEAD'), 'tree': H.git(WORK, 'rev-parse', 'HEAD^{tree}'),
            'staged_tree': H.git(WORK, 'write-tree'), 'status': H.git(WORK, 'status', '--porcelain'),
            'files': {n: H.digest(WORK / n) if (WORK / n).exists() else None for n in sorted(set(paths))},
            'harness_sha256': H.digest(WORK / plan['harness'])}

start = source()
assert start['head'] == plan['source_head'] and start['harness_sha256'] == plan['harness_sha256']
assert not (PACKET / 'result.json').exists()
elf = PACKET / 'documentservice.test'
assert not elf.exists()
H.write_json(PACKET / 'source-binding.json', start)
env = H.safe_env({'GOROOT': str(GO_ROOT), 'GOTOOLCHAIN': 'local', 'GOMAXPROCS': str(plan['gomaxprocs']),
                  'GOCACHE': str(ROOT / 'gocache'), 'TMPDIR': str(ROOT / 'tmp')})
build = H.run_process([str(GO), 'test', '-c', '-p=1', '-o', str(elf), './TreeDB/documentservice'], str(WORK), env, PACKET / 'build.log', plan['build_wall_limit_seconds'])
H.write_json(PACKET / 'build-receipt.json', build)
assert build['process_ok'] and source() == start, 'build/source failure; no benchmark executed'
binary_sha = H.digest(elf)
H.write_json(PACKET / 'binary-binding.json', {'source': start, 'elf_sha256': binary_sha, 'go_sha256': H.digest(GO),
             'plan_sha256': H.digest(PACKET / 'public-write-plan.json'), 'runner_sha256': H.digest(pathlib.Path(__file__))})
rows, receipts = [], []
for i, arm in enumerate(plan['fixed_order'], 1):
    assert source() == start and H.digest(elf) == binary_sha
    log = PACKET / f'cell-{i:02}-{arm}.log'
    assert not log.exists()
    argv = [str(elf), '-test.v', '-test.run=^$', '-test.bench=' + plan['public_bench'],
            '-test.benchmem', '-test.benchtime=1x', '-test.count=1', '-test.timeout=3m']
    receipt = H.run_process(argv, str(WORK), dict(env, GOMAP_SERVICE_BENCH_BACKEND=arm,
                            GOMAP_SERVICE_BENCH_PROFILE=plan['profile']), log, plan['process_wall_limit_seconds'])
    raw = log.read_text()
    parsed = []
    for line in raw.splitlines():
        match = re.match(r'^(BenchmarkServiceSharedCOWWriteLoad/writers(1|4)-\d+)\s+(\d+)\s+(.+)$', line)
        if match:
            fields = match.group(4).split()
            metrics = {fields[j + 1]: float(fields[j]) for j in range(0, len(fields) - 1, 2)}
            parsed.append({'cell': i, 'arm': arm, 'writers': int(match.group(2)), 'iterations': int(match.group(3)), 'metrics': metrics, 'raw': line})
    activation = all(f'backend={arm} profile=command_wal_durable requested_memtable=cow_btree resolved_strategy=column_graph metric=cosine rows=4096 batch=128 writers={n} defer_rebuild=true' in raw for n in [1, 4])
    completions = all(f'backend={arm} profile=command_wal_durable writers={n} loads=1 attempted=32 completed=32 request_retries=0' in raw for n in [1, 4])
    valid = receipt['process_ok'] and source() == start and activation and completions and len(parsed) == 2 and {r['writers'] for r in parsed} == {1, 4} and all(r['iterations'] == 1 and r['metrics'].get('requests/op') == 32 and r['metrics'].get('records/op') == 4096 for r in parsed)
    receipt.update(arm=arm, elf_sha256=binary_sha, source_unchanged=source() == start,
                   activation_verified=activation, durable_request_completions_verified=completions, rows=parsed, valid=valid)
    H.write_json(PACKET / f'cell-{i:02}-{arm}-receipt.json', receipt)
    rows.extend(parsed)
    receipts.append(receipt)
    print(json.dumps({'cell': i, 'arm': arm, 'valid': valid, 'elapsed_seconds': receipt['elapsed_seconds'],
                      'rows': parsed, 'log_sha256': receipt['log_sha256']}), flush=True)

valid = all(r['valid'] for r in receipts)
medians = {}
if valid:
    for writers in [1, 4]:
        medians[writers] = {}
        for metric in sorted(rows[0]['metrics']):
            pair = {arm: statistics.median(r['metrics'][metric] for r in rows if r['arm'] == arm and r['writers'] == writers) for arm in ['legacy', 'native']}
            medians[writers][metric] = dict(pair, change_percent=(pair['native'] / pair['legacy'] - 1) * 100 if pair['legacy'] else None)
result = {'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'valid': valid, 'source_unchanged': source() == start,
          'rows': rows, 'medians': medians, 'numeric_gate': None, 'qualification': None, 'retries': 0,
          'plan_sha256': H.digest(PACKET / 'public-write-plan.json')}
H.write_json(PACKET / 'source-after.json', source())
H.write_json(PACKET / 'result.json', result)
print(json.dumps(result), flush=True)
sys.exit(0 if valid and result['source_unchanged'] else 1)
