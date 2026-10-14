from pathlib import Path
import hashlib, json, os, re, shutil, subprocess, time

base = Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
perf = base / 'sprint-performance-20261014'
ordinary = base / 'ordinary-5111'
evidence = base / 'evidence/root-cow-point-lane-affinity-20261014'
out = evidence / 'causal-comparison'
out.mkdir(exist_ok=False)
go = base / 'toolchains/go1.26.3-from-185-20261014'
env = dict(os.environ, GOROOT=str(go), GOWORK='off', GOTOOLCHAIN='local', CGO_ENABLED='1', GOMAXPROCS='4')
def sha(b): return hashlib.sha256(b).hexdigest()
source = 'TreeDB/caching/db.go'
before = perf / 'sources/causal-after'
after = perf / 'sources/lane-affinity-candidate'
assert sha((before / source).read_bytes()) == '9d38862a7a8eae58196b73b6816d39fa6b3bad657000bd72d897c376925bd1be'
assert sha((ordinary / source).read_bytes()) == '376af70da069dcea65e622453709219c3fe274ed1aa3bf199511907bd4fda01e'
assert (before / source).read_bytes() == (evidence / 'db.go.before').read_bytes()
assert (ordinary / source).read_bytes() == (evidence / 'db.go.after').read_bytes()
shutil.copytree(before, after, copy_function=os.link)
p = after / source
p.unlink()
p.write_bytes((ordinary / source).read_bytes())
def census(root):
    return {str(p.relative_to(root)): sha(p.read_bytes()) for p in sorted(root.rglob('*.go')) if not p.name.endswith('_test.go')}
bc, ac = census(before), census(after)
assert bc.keys() == ac.keys()
assert [p for p in bc if bc[p] != ac[p]] == [source]
for name, digest in ac.items(): assert sha((ordinary / name).read_bytes()) == digest, name
harness = 'TreeDB/mvcc/sprintperf/benchmark_test.go'
assert sha((before / harness).read_bytes()) == 'b63439b1c72421396c5fd3763c0bf6e0e15570a6f9d7428219a00dd8687ffde6'
assert (before / harness).read_bytes() == (after / harness).read_bytes()
(out / 'production-census.json').write_text(json.dumps({'before': bc, 'after': ac, 'changed_paths': [source]}, indent=1) + '\n')
bindings = {'before': {'binary': str(perf / 'binaries/causal-after-mvcc.test'), 'binary_sha256': 'f8ef21478881ee87166670191c2f8e7278d31f75f33d20bd9f2e987bb32bc816', 'source': str(before), 'retained_build': '0bb0dd5b26db071ef5777614ce62463c75bd98f1/artifacts/4878/20261014-primary-empty-fastpath/evidence/causal-comparison/build-bindings.json'}}
binary = perf / 'binaries/lane-affinity-candidate-mvcc.test'
argv = [str(go / 'bin/go'), 'test', '-c', '-trimpath', '-buildvcs=false', '-o', str(binary), './TreeDB/mvcc/sprintperf']
log = out / 'build.log'
start = time.monotonic()
with log.open('wb') as f: result = subprocess.run(argv, cwd=after, env=env, stdout=f, stderr=subprocess.STDOUT, timeout=180)
bindings['after'] = {'argv': argv, 'exit': result.returncode, 'seconds': time.monotonic()-start, 'log_sha256': sha(log.read_bytes()), 'source': str(after), 'harness_sha256': sha((after/harness).read_bytes())}
if result.returncode == 0: bindings['after'].update(binary=str(binary), binary_sha256=sha(binary.read_bytes()))
(out/'build-bindings.json').write_text(json.dumps(bindings, indent=1)+'\n')
print(json.dumps({'build': bindings['after']}), flush=True)
if result.returncode: print(log.read_text()[-4000:]); raise SystemExit(result.returncode)
processes = subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,pmem,args','--sort=-pcpu'], text=True)
(out/'timing-start-processes.txt').write_text(processes)
for line in processes.splitlines()[1:]:
    command = ' '.join(line.split()[5:])
    if re.match(r'.*(?:/bin/go (?:test|build)|/pkg/tool/[^ ]+/(?:compile|link)|\.test(?: |$)|unified-bench(?: |$))', command): raise SystemExit('producer active: '+line)
tmp = perf/'lane-affinity-timed-temp'
tmp.mkdir(exist_ok=False)
receipts, wall_total = [], 0
cells = [('command_wal_relaxed','forced_pointer','point'), ('command_wal_relaxed','inline','point'), ('command_wal_relaxed','forced_pointer','group_versions'), ('command_wal_durable','forced_pointer','point')]
for sample in [1,2]:
    for profile, placement, operation in cells:
        for label in (['before','after'] if sample == 1 else ['after','before']):
            binary = Path(bindings[label]['binary'])
            assert sha(binary.read_bytes()) == bindings[label]['binary_sha256']
            name = f'{sample}-{profile}-{placement}-{operation}-{label}'
            temp = tmp/name
            temp.mkdir()
            local = dict(env,TMPDIR=str(temp),SPRINT_MEMTABLE='cow_btree')
            regex = f'^BenchmarkSprintMVCC$/^{profile}$/^cow_btree$/^{placement}$/^{operation}$'
            argv = ['taskset','-c','0-3',str(binary),'-test.run','^$','-test.bench',regex,'-test.benchmem','-test.benchtime=64x','-test.count=1','-test.v','-test.timeout=30s']
            log = out/(name+'.log')
            start, stamp = time.monotonic(), time.time()
            with log.open('wb') as f:
                try: result = subprocess.run(argv,env=local,stdout=f,stderr=subprocess.STDOUT,timeout=35).returncode
                except subprocess.TimeoutExpired: result = 124
            wall = time.monotonic()-start
            wall_total += wall
            rows = [line for line in log.read_text().splitlines() if re.match(r'^Benchmark\S+\s+\d+\s+',line)]
            probes = []
            for line in log.read_text().splitlines():
                m = re.search(r'lane_probe n=64 phase=(\S+) key=(\S+) value=(\S+)',line)
                if m: probes.append({'phase':m[1],'key':m[2],'value':m[3]})
            receipt = {'label':label,'sample':sample,'profile':profile,'placement':placement,'operation':operation,'argv':argv,'env':{k:local[k] for k in ['GOROOT','GOWORK','GOTOOLCHAIN','CGO_ENABLED','GOMAXPROCS','SPRINT_MEMTABLE','TMPDIR']},'exit':result,'unix_start':stamp,'wall_seconds':wall,'log':log.name,'raw_sha256':sha(log.read_bytes()),'binary_sha256':sha(binary.read_bytes()),'rows':rows,'lane_probes':probes}
            receipts.append(receipt)
            (out/'measurement-receipts.json').write_text(json.dumps({'diagnostic_only':True,'wall_seconds':wall_total,'receipts':receipts},indent=1)+'\n')
            print(json.dumps({'run':name,'exit':result,'rows':rows,'lane_probes':len(probes),'seconds':wall}),flush=True)
            if result or len(rows)!=1 or not probes: print(log.read_text()[-4000:]); raise SystemExit('FAILED_OR_UNMATCHED')
            assert wall_total < 60, 'bounded comparison wall exceeded'
for label,binding in bindings.items(): assert sha(Path(binding['binary']).read_bytes()) == binding['binary_sha256']
(out/'timing-end-processes.txt').write_text(subprocess.check_output(['ps','-eo','pid,ppid,etime,pcpu,pmem,args','--sort=-pcpu'],text=True))
print(json.dumps({'COMPLETE':True,'invocations':len(receipts),'wall_seconds':wall_total}),flush=True)
