"""Diagnostic profiles of actual material C2 costs; never replacement timings.

Uses the retained, source-bound cost binaries. Package CPU/heap profiles include
fixture setup/teardown and StartTimer/StopTimer bookkeeping. Sampling and larger
fixed counts differ from the cost matrix. Keep these limitations with findings.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time

from collect import normalized_identity, now, sha, write_json
from analyze import parse_row as parse_raw
from analyze_mvcc import parse_row as parse_mvcc


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--identity', type=Path, required=True)
    p.add_argument('--raw-packet', type=Path, required=True)
    p.add_argument('--mvcc-packet', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    source, out = a.source.resolve(), a.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    identity = normalized_identity(json.loads(a.identity.read_text()))
    write_json(out/'source-identity.json', identity)
    def drift():
        expected = {x['path'] for x in identity['files']}
        bad = [x['path'] for x in identity['files'] if not (source/x['path']).is_file()
               or (source/x['path']).is_symlink()
               or sha(source/x['path']) != x['sha256']]
        actual = {x.relative_to(source).as_posix() for x in source.rglob('*')
                  if x.is_file() or x.is_symlink()}
        return bad + ['EXTRA:'+x for x in sorted(actual-expected)]
    before = drift()
    write_json(out/'hydration.json', {'at':now(),'drift':before})
    if before:
        raise ValueError('source drift before profiles')
    jobs=[]
    for layout, operation in [('Inline64','Forward16'),('Pointer4096','CaptureReadRelease'),('Pointer4096','IncrementalWrite')]:
        for mode in ('btree','cow_btree'):
            jobs.append(dict(kind='raw',mode=mode,layout=layout,operation=operation,count=8192,
                pattern=f'^BenchmarkCOWPublicDirtyCost$/^no_wal_fast$/^{mode}$/^{layout}$/^N=2048$/^{operation}$'))
    for mode in ('btree','cow_btree'):
        jobs.append(dict(kind='mvcc',mode=mode,read='point',count=4096,
            pattern=f'^BenchmarkCOWIntegratedPublicMVCC$/^no_wal_fast$/^{mode}$/^point$'))
    root='/home/mikers/.gvm/gos/go1.26.3'
    go=root+'/bin/go'
    env=os.environ.copy()
    original={k:env.pop(k,None) for k in ('GOGC','GOMEMLIMIT')}
    env.update(GOROOT=root,GOWORK='off',GOMAXPROCS='12',
        GOCACHE='/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache',
        GOMODCACHE='/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod')
    write_json(out/'policy.json',dict(at=now(),candidate=identity['commit'],
        source_tree_sha256=identity['source_tree_sha256'],jobs=jobs,host=platform.uname()._asdict(),
        original_memory_controls_removed=original,GOMAXPROCS=12,
        scope=__doc__,timing_acceptance=False,
        exclusivity='coordinator serial handoff after matched45runs; no other owned185jobs'))
    packets={'raw':a.raw_packet.resolve(),'mvcc':a.mvcc_packet.resolve()}
    binaries={}
    for kind,packet in packets.items():
        valid=json.loads((packet/'analysis-validation.json').read_text())
        if valid['source_tree_sha256'] != identity['source_tree_sha256']:
            raise ValueError('cost packet source mismatch')
        binary=packet/('treedb-cost.test' if kind=='raw' else 'mvcc-cost.test')
        binding=json.loads((packet/'binary.json').read_text())
        if sha(binary) != binding['sha256']:
            raise ValueError('retained binary hash mismatch')
        binaries[kind]=binary
    write_json(out/'binary-bindings.json',{k:dict(path=str(v),sha256=sha(v)) for k,v in binaries.items()})
    receipts=[]
    for job in jobs:
        label='-'.join([job['kind'],job['mode'],job.get('layout',''),job.get('operation',job.get('read',''))]).strip('-')
        cpu,heap=out/(label+'.cpu.pprof'),out/(label+'.heap.pprof')
        command=[str(binaries[job['kind']]),'-test.run=^$', '-test.bench='+job['pattern'],
            '-test.benchtime='+str(job['count'])+'x','-test.count=1','-test.timeout=300s',
            '-test.cpuprofile='+str(cpu),'-test.memprofile='+str(heap)]
        log,err=out/(label+'.log'),out/(label+'.stderr.log')
        start,clock,load=now(),time.monotonic(),os.getloadavg()
        with log.open('w') as f,err.open('w') as e:
            child=subprocess.run(command,cwd=source,env=env,stdout=f,stderr=e)
        rows=[r for line in log.read_text().splitlines() if (r:=(parse_raw(line) if job['kind']=='raw' else parse_mvcc(line))) is not None]
        validation=[]
        if child.returncode or not re.search(r'^PASS$',log.read_text(),re.M): validation.append('exit/noPASS')
        if re.search(r'WARNING: DATA RACE|fatal error:',err.read_text()): validation.append('runtimefailure')
        if len(rows)!=1 or rows[0]['iterations']!=job['count'] or rows[0]['mode']!=job['mode'] or rows[0]['profile']!='no_wal_fast': validation.append('wrongcase/count')
        if rows and job['kind']=='raw' and (rows[0]['layout']!=job['layout'] or rows[0]['records']!=2048 or rows[0]['operation']!=job['operation']): validation.append('wrongrawcase')
        if rows and job['kind']=='mvcc' and rows[0]['read']!='point': validation.append('wrongmvcccase')
        if not cpu.is_file() or not heap.is_file(): validation.append('missingprofile')
        changed=drift()
        receipt=dict(label=label,command=command,started_utc=start,completed_utc=now(),
            elapsed_seconds=time.monotonic()-clock,load_before=load,exit_code=child.returncode,
            validation_errors=validation,source_drift_after=changed,
            log_sha256=sha(log),stderr_sha256=sha(err),diagnostic_only=True)
        receipts.append(receipt);write_json(out/'receipts.json',receipts)
        if validation or changed: raise ValueError('retained failed profile: '+label)
        for suffix,flags,profile in [('cpu-top',['-top'],cpu),('cpu-cum',['-top','-cum'],cpu),('alloc-top',['-top','-alloc_space'],heap),('alloc-cum',['-top','-cum','-alloc_space'],heap)]:
            with (out/(label+'.'+suffix+'.txt')).open('w') as stream:
                subprocess.run([go,'tool','pprof',*flags,str(binaries[job['kind']]),str(profile)],env=env,cwd=source,stdout=stream,stderr=subprocess.STDOUT,check=True)
        receipt['output_hashes']={x.name:sha(x) for x in out.glob(label+'.*') if x.is_file()}
        write_json(out/'receipts.json',receipts)
        print(json.dumps(receipt),flush=True)
    write_json(out/'completion.json',dict(at=now(),runs=len(receipts),source_drift=drift(),timing_acceptance=False))


if __name__=='__main__':
    main()
