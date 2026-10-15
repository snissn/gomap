"""Fixed 18-call diagnostic. Reuses unchanged public Go benchmarks; no retries."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import threading
import time

root = Path('/mnt/fast4tb/gomap-mvcc-4871-20261015')
data = root / 'performance-185'
out = data / 'short-guard-01'
out.mkdir()  # A prior invocation must remain intact.
runtime = out / 'tmp'
runtime.mkdir()
roots = {'baseline': root / 'coordinator', 'candidate': data / 'candidate'}
expected_heads = {'baseline': '5bca87ffe6e1bfba2d693264d2dc82936adc234f',
                  'candidate': json.loads((data / 'candidate-source.json').read_text())['head']}
binaries = json.loads((data / 'binaries.json').read_text())
env = os.environ.copy()
env.update(GOROOT=str(root / 'toolchains/go1.26.3'), GOTOOLCHAIN='local',
           GOWORK='off', GOPROXY='off', GOSUMDB='off', CGO_ENABLED='1',
           GOMAXPROCS='1', TMPDIR=str(runtime))

def write(name, value):
    (out / name).write_text(json.dumps(value, indent=2, sort_keys=True))

def sources():
    result = {}
    for label, path in roots.items():
        def git(*args):
            return subprocess.check_output(['git', *args], cwd=path, text=True).strip()
        assert git('rev-parse', 'HEAD') == expected_heads[label]
        assert not git('status', '--porcelain'), label + ' source drift'
        result[label] = {'head': git('rev-parse', 'HEAD'), 'tree': git('rev-parse', 'HEAD^{tree}')}
    return result

before = sources()
for binary in binaries.values():
    assert hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() == binary['sha256']
write('binding.json', {'sources': before, 'binaries': binaries,
    'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    'scope': 'Bounded viability diagnostic only; no final qualification or cross-host attribution',
    'pairs': 'AB/BA/AB; three pairs per benchmark, all raw results retained without replacement',
    'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'affinity': [0], 'env': {k: env[k] for k in ['GOROOT', 'GOTOOLCHAIN', 'GOWORK',
        'GOPROXY', 'GOSUMDB', 'CGO_ENABLED', 'GOMAXPROCS', 'TMPDIR']}})

def processes():
    result = {}
    for folder in Path('/proc').iterdir():
        if not folder.name.isdigit():
            continue
        try:
            raw = (folder / 'stat').read_text()
            fields = raw[raw.rindex(')') + 2:].split()
            key = folder.name + ':' + fields[19]
            result[key] = {'pid': int(folder.name), 'comm': raw[raw.index('(')+1:raw.rindex(')')],
                           'ticks': int(fields[11]) + int(fields[12])}
        except (OSError, ValueError, IndexError):
            pass
    return result

ticks = os.sysconf('SC_CLK_TCK')
allowed = {os.getpid()}
samples = []

def delta(a, b, elapsed):
    active = []
    for key, item in b.items():
        if key in a and item['pid'] not in allowed:
            cpu = (item['ticks'] - a[key]['ticks']) * 100 / ticks / elapsed
            if cpu >= 2.0:
                active.append({**item, 'cpu_pct': cpu})
    return active

start = time.monotonic()
first = processes()
time.sleep(5)
second = processes()
quiet = delta(first, second, time.monotonic() - start)
write('preflight.json', {'nonrunner_active': quiet, 'threshold_pct': 2.0,
    'loadavg': Path('/proc/loadavg').read_text(), 'meminfo': Path('/proc/meminfo').read_text(),
    'diskstats': Path('/proc/diskstats').read_text(), 'cpu': Path('/proc/stat').read_text()})
assert not quiet, 'Host is not quiet; no measurements started. See preflight.json.'

stop = threading.Event()
def monitor():
    previous = processes()
    last = time.monotonic()
    while not stop.wait(1):
        current = processes()
        now = time.monotonic()
        samples.append({'elapsed_s': now - start, 'nonrunner_active': delta(previous, current, now-last),
                        'loadavg': Path('/proc/loadavg').read_text()})
        previous, last = current, now

thread = threading.Thread(target=monitor, daemon=True)
thread.start()
rows = []
try:
    for benchmark, duration, package in [
        ('GetVersioned', '500ms', 'db'),
        ('ConditionalTxnBaselineBatchWrite', '1000x', 'db'),
        ('CommitAtPrimaryOnlyAfterDataGeneration', '128x', 'primary')]:
        for repeat, order in enumerate([['baseline', 'candidate'], ['candidate', 'baseline'],
                                         ['baseline', 'candidate']], 1):
            for label in order:
                name = f'{benchmark}-{repeat}-{label}'
                argv = ['taskset', '-c', '0', binaries[label + '-' + package]['path'],
                        '-test.run=^$', '-test.bench=^Benchmark' + benchmark + '$',
                        '-test.benchtime=' + duration, '-test.count=1', '-test.benchmem',
                        '-test.timeout=90s']
                receipt = {'argv': argv, 'cwd': str(roots[label]), 'benchmark': benchmark,
                           'repeat': repeat, 'label': label}
                write(name + '.command.json', receipt)
                began = time.monotonic()
                with (out / (name + '.raw')).open('wb') as stream:
                    process = subprocess.Popen(argv, cwd=roots[label], env=env, stdout=stream,
                        stderr=subprocess.STDOUT, start_new_session=True)
                    allowed.add(process.pid)
                    try:
                        status = process.wait(timeout=100)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                        status = 124
                receipt.update(exit=status, elapsed_s=time.monotonic()-began)
                raw = (out / (name + '.raw')).read_text()
                receipt['raw_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
                write(name + '.receipt.json', receipt)
                print(name, status, flush=True)
                assert status == 0, name
                matches = [line.split() for line in raw.splitlines()
                           if line.startswith('Benchmark' + benchmark)]
                assert len(matches) == 1, (name, matches)
                fields = matches[0]
                metrics = {fields[i+1]: float(fields[i]) for i in range(2, len(fields)-1, 2)}
                assert all(key in metrics for key in ['ns/op', 'B/op', 'allocs/op'])
                rows.append({**receipt, 'metrics': metrics})
finally:
    stop.set()
    thread.join()
    write('host-during.json', samples)
    write('rows.json', rows)
    write('after.json', sources())

assert len(rows) == 18
summary = {}
for benchmark in dict.fromkeys(row['benchmark'] for row in rows):
    values = {label: sorted([row for row in rows if row['benchmark'] == benchmark and row['label'] == label],
                           key=lambda row: row['repeat']) for label in ['baseline', 'candidate']}
    medians = {label: {key: statistics.median(row['metrics'][key] for row in items)
                      for key in items[0]['metrics']} for label, items in values.items()}
    pair_changes = [(c['metrics']['ns/op']/b['metrics']['ns/op']-1)*100
                    for b, c in zip(values['baseline'], values['candidate'])]
    summary[benchmark] = {'median': medians, 'paired_ns_delta_pct': pair_changes,
        'paired_median_ns_delta_pct': statistics.median(pair_changes),
        'median_ns_delta_pct': (medians['candidate']['ns/op']/medians['baseline']['ns/op']-1)*100,
        'ops_per_second': {key: 1e9/value['ns/op'] for key, value in medians.items()}}
write('summary.json', summary)
interference = [sample for sample in samples if sample['nonrunner_active']]
write('qualification.json', {'complete': True, 'quiet': not interference,
    'interference_windows': len(interference), 'scope': 'Short diagnostic only; final gates remain open'})
print(json.dumps({'complete': True, 'interference_windows': len(interference), 'summary': summary}), flush=True)
