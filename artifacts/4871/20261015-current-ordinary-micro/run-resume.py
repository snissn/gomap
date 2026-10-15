from pathlib import Path
import datetime
import hashlib
import json
import os
import re
import statistics
import subprocess
import time

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
repo = base / 'ordinary-5111'
old = base / 'sprint-performance-20261014'
out = base / 'evidence/ordinary-current-micro-v2-20261015'
toolchain = base / 'toolchains/go1.26.3-from-185-20261014'
head = 'a934ea4c7a90b95bc15d9feb6ee9a3edde6502eb'
assert out.exists() and not (out / 'receipt.json').exists()

def sha(data):
    return hashlib.sha256(data).hexdigest()

def git(*args):
    return subprocess.check_output(['git', '-C', str(repo), *args])

env = dict(os.environ, GOROOT=str(toolchain), GOWORK='off', GOTOOLCHAIN='local', CGO_ENABLED='1', GOMAXPROCS='4')
assert git('rev-parse', 'HEAD').decode().strip() == head
assert not git('status', '--porcelain=v1', '-uall')
index = Path(git('rev-parse', '--git-path', 'index').decode().strip())
if not index.is_absolute():
    index = repo / index
index_sha = sha(index.read_bytes())
procs = subprocess.check_output(['ps', '-eo', 'pid,comm,args']).decode()
(out / 'processes-resume.txt').write_text(procs)
assert not any(line.split()[1] in ('go', 'compile', 'link', 'mvcc.test') or line.split()[1].endswith('.test') for line in procs.splitlines()[1:] if len(line.split()) >= 2)
receipts = json.loads((out / 'commands.json').read_text())

def command(name, argv, localenv=env, timeout=90):
    log = out / (name + '.log')
    if log.exists():
        prior = [row for row in receipts if row['name'] == name]
        assert len(prior) == 1 and prior[0]['argv'] == argv and prior[0]['exit'] == 0
        assert sha(log.read_bytes()) == prior[0]['raw_sha256']
        print(json.dumps({'name':name, 'retained':True}), flush=True)
        return log
    started = time.monotonic()
    with log.open('wb') as f:
        try:
            code = subprocess.run(argv, cwd=repo, env=localenv, stdout=f, stderr=subprocess.STDOUT, timeout=timeout).returncode
        except subprocess.TimeoutExpired:
            code = 124
    result = dict(name=name, argv=argv, env={key:localenv[key] for key in ['GOROOT','GOWORK','GOTOOLCHAIN','CGO_ENABLED','GOMAXPROCS'] + (['TMPDIR'] if 'TMPDIR' in localenv else [])},
        at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), exit=code, wall_seconds=time.monotonic()-started, raw_sha256=sha(log.read_bytes()))
    receipts.append(result)
    (out / 'commands.json').write_text(json.dumps(receipts, indent=2) + '\n')
    print(json.dumps({key:result[key] for key in ['name','exit','wall_seconds']}), flush=True)
    assert code == 0, log.read_text()[-4000:]
    return log

def census():
    argv = [str(toolchain / 'bin/go'), 'list', '-deps', '-test', '-json', './TreeDB/db', './TreeDB/caching', './TreeDB']
    data = subprocess.check_output(argv, cwd=repo, env=env).decode()
    decoder = json.JSONDecoder()
    paths = {'go.mod', 'go.sum'}
    while data.strip():
        package, count = decoder.raw_decode(data.lstrip())
        data = data.lstrip()[count:]
        directory = Path(package.get('Dir', '/'))
        if not directory.is_relative_to(repo):
            continue
        for key in ['GoFiles','CgoFiles','CFiles','HFiles','SFiles','SysoFiles','EmbedFiles','TestGoFiles','XTestGoFiles']:
            for name in package.get(key, []):
                p = directory / name
                if p.is_file() and p.is_relative_to(repo):
                    paths.add(str(p.relative_to(repo)))
    return [dict(path=path, bytes=(repo/path).stat().st_size, sha256=sha((repo/path).read_bytes())) for path in sorted(paths)]

before = json.loads((out / 'current-compiled-inputs-before.json').read_text())
assert census() == before
binaries = {}
prior_hashes = json.loads((old / 'evidence/premeasurement-binaries.json').read_text())
for label in ['before-sprint', 'recent-main']:
    for package in ['db', 'caching', 'treedb']:
        path = old / 'binaries' / (label + '-raw-' + package + '.test')
        assert sha(path.read_bytes()) == prior_hashes[path.name]
        binaries[label,package] = path
for package, importpath in [('db','./TreeDB/db'),('caching','./TreeDB/caching'),('treedb','./TreeDB')]:
    binary = out / 'binaries' / ('current-raw-' + package + '.test')
    command('build-current-' + package, [str(toolchain/'bin/go'), 'test', '-c', '-trimpath', '-buildvcs=false', '-o', str(binary), importpath])
    binaries['current',package] = binary
hashes = json.loads((out / 'binary-identities.json').read_text())
assert hashes == {label + '/' + package:sha(path.read_bytes()) for (label,package),path in binaries.items()}
groups = [('get_versioned','db','^BenchmarkGetVersioned$','200ms'),
    ('batch_write','db','^BenchmarkConditionalTxnBaselineBatchWrite$','100x'),
    ('snapshot_seek','treedb','^BenchmarkSnapshotIteratorSeekNext/keys=1024/snapshot_seek$','200ms'),
    ('repeated_iterator','caching','^BenchmarkRepeatedIterator$','200ms'),
    ('durable_sync','treedb','^BenchmarkPublicCommandWALDurableTinyBatchWriteSync/placement=inline/shape=dirty_batch/ops=1$','200ms')]
rows = []
measured_wall = 0
for sample in [1,2]:
    for group, package, regex, duration in groups:
        order = ['before-sprint','recent-main','current'] if sample == 1 else ['current','recent-main','before-sprint']
        for label in order:
            name = 'micro-' + str(sample) + '-' + group + '-' + label
            temp = out / 'temp' / name
            temp.mkdir(exist_ok=True)
            localenv = dict(env, GOMAXPROCS='1', TMPDIR=str(temp))
            argv = ['taskset','-c','0',str(binaries[label,package]),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime='+duration,'-test.count=1','-test.v','-test.timeout=30s']
            log = command(name,argv,localenv,35)
            measured_wall += next(row['wall_seconds'] for row in receipts if row['name'] == name)
            assert measured_wall < 60
            found = [line for line in log.read_text().splitlines() if re.match(r'^Benchmark\S+\s+\d+\s+', line)]
            assert len(found) == 1, found
            fields = found[0].split()
            metrics = {fields[i+1]:float(fields[i]) for i in range(2,len(fields),2)}
            assert all(key in metrics for key in ['ns/op','B/op','allocs/op']), fields
            row = dict(label=label, sample=sample, group=group, raw=found[0], ns=metrics['ns/op'], bytes=metrics['B/op'], allocs=metrics['allocs/op'], binary_sha256=hashes[label+'/'+package])
            rows.append(row)
            (out / 'rows.json').write_text(json.dumps(rows, indent=2) + '\n')
after = census()
assert after == before
(out / 'current-compiled-inputs-after.json').write_text(json.dumps(after, indent=2) + '\n')
assert git('rev-parse','HEAD').decode().strip() == head and not git('status','--porcelain=v1','-uall')
assert sha(index.read_bytes()) == index_sha
assert hashes == {label+'/'+package:sha(path.read_bytes()) for (label,package),path in binaries.items()}
(out / 'processes-after.txt').write_bytes(subprocess.check_output(['ps','-eo','pid,comm,args']))
summary = {}
for group,_,_,_ in groups:
    summary[group] = {}
    for label in ['before-sprint','recent-main','current']:
        sample_rows = [row for row in rows if row['label']==label and row['group']==group]
        summary[group][label] = {metric:statistics.median(row[metric] for row in sample_rows) for metric in ['ns','bytes','allocs']}
    for other in ['before-sprint','recent-main']:
        summary[group]['current_vs_'+other+'_percent'] = 100*(summary[group]['current']['ns']/summary[group][other]['ns']-1)
result = dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), candidate=head,
    baseline='28aea01e19f1d62743dec5625ec3d22c1acb6703', control='3f87d74b98e8b1e809d151611be03785f85e2283',
    host='192.168.0.111', compiler=toolchain.name, build_flags=['-trimpath','-buildvcs=false'],
    current_compiled_input_rows=len(before), current_compiled_inputs_sha256=sha((out/'current-compiled-inputs-before.json').read_bytes()),
    current_source_unchanged=True, current_head_index_status_preserved=True, invocations=len(rows), measured_wall_seconds=measured_wall,
    retained_baseline_control_binding='Exact same hashes as retained854ce8c artifact; no rebuild; current clean a934ea4 rebuilt actual packages.',
    qualification='Bounded two-balanced-round raw micro diagnostic only. No M0/Dgraph/native/whole-production acceptance. All noisy/failing rows retained.', summary=summary)
(out / 'receipt.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result,indent=2), flush=True)
