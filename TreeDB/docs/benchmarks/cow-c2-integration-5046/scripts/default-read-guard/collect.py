"""Collect six serial default-read guards AFTER root releases the cost runner."""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import subprocess
import time
from contract import COMMITS, FIXTURES, PATTERN, ORDERS, OPERATIONS, sha, write, identity, drift, validate_run


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def main():
    p=argparse.ArgumentParser()
    for name in ('main-source','main-identity','candidate-source','candidate-identity','out'):
        p.add_argument('--'+name,type=Path,required=True)
    args=p.parse_args()
    sources={'main':args.main_source.resolve(),'candidate':args.candidate_source.resolve()}
    identities={'main':identity(args.main_identity,'main'), 'candidate':identity(args.candidate_identity,'candidate')}
    out=args.out.resolve();out.mkdir(parents=True,exist_ok=False)
    for role in COMMITS:
        write(out/(role+'-source-identity.json'),identities[role])
    def all_drift():
        return {r:drift(sources[r],identities[r]) for r in COMMITS}
    before=all_drift();write(out/'hydration.json',{'at':now(),'drift':before})
    if any(before.values()):
        raise SystemExit('source mismatch or extra files; retained hydration')
    env=os.environ.copy();controls={k:env.pop(k,None) for k in ('GOGC','GOMEMLIMIT')}
    env.update(GOROOT='/home/mikers/.gvm/gos/go1.26.3',GOWORK='off',GOMAXPROCS='12',
               GOCACHE='/mnt/fast4tb/gomap-cow-execution-o2nauuzm/cache',
               GOMODCACHE='/home/mikers/.gvm/pkgsets/go1.25/global/pkg/mod',TREEDB_BENCH_SHARDS='2')
    go=env['GOROOT']+'/bin/go'
    write(out/'policy.json',{'created_utc':now(),'commits':COMMITS,'fixture_hashes':FIXTURES,
        'pattern':PATTERN,'orders':ORDERS,'operations':OPERATIONS,'GOMAXPROCS':12,'TREEDB_BENCH_SHARDS':2,
        'removed_memory_controls':controls,'host':platform.uname()._asdict(),'cpu_count':os.cpu_count(),
        'noise_policy':'retain all three paired repeats; descriptive spread, no significance or sustained claim',
        'runner_gate':'root serializes after raw27+MVCC18; no other owned jobs',
        'scope':'unchanged default cached W0 and checkpointed pointer Get/GetAppend; benchmark setup/Close outside timer',
        'limitation':'unchanged fixtures ignore final Close errors: no persistence/drain/Close proof from this diagnostic'})
    with (out/'toolchain.txt').open('w') as stream:
        subprocess.run([go,'version'],env=env,stdout=stream,check=True)
    binaries={}
    for role in COMMITS:
        folder=out/role;folder.mkdir()
        with (folder/'goenv.json').open('w') as stream:
            subprocess.run([go,'env','-json'],cwd=sources[role],env=env,stdout=stream,check=True)
        with (folder/'compiled-dependencies.json').open('w') as stream:
            subprocess.run([go,'list','-deps','-json','./TreeDB'],cwd=sources[role],env=env,stdout=stream,check=True)
        binary=folder/'default-read.test';binaries[role]=binary
        with (folder/'build.log').open('w') as stream:
            subprocess.run([go,'test','-c','-o',str(binary),'./TreeDB'],cwd=sources[role],env=env,stdout=stream,stderr=subprocess.STDOUT,check=True)
        with (folder/'binary-buildinfo.txt').open('w') as stream:
            subprocess.run([go,'version','-m',str(binary)],env=env,stdout=stream,check=True)
        write(folder/'binary.json',{'sha256':sha(binary),'bytes':binary.stat().st_size})
    afterbuild=all_drift();write(out/'build-source-drift.json',afterbuild)
    if any(afterbuild.values()):raise SystemExit('build mutated source')
    receipts=[]
    for repeat,order in enumerate(ORDERS,1):
        for role in order:
            label=f'r{repeat}-{role}'
            pred=all_drift()
            if any(pred.values()):raise SystemExit('source changed before run')
            (out/(label+'-processes-before.txt')).write_text(subprocess.check_output(['ps','-eo','pid,ppid,etimes,pcpu,rss,args'],text=True))
            command=[str(binaries[role]),'-test.run=^$','-test.bench='+PATTERN,
                     '-test.benchmem','-test.benchtime=100000x','-test.count=1','-test.timeout=600s']
            stdout,stderr=out/(label+'.log'),out/(label+'.stderr.log')
            start,clock,load=now(),time.monotonic(),os.getloadavg()
            with stdout.open('w') as stream,stderr.open('w') as errors:
                child=subprocess.Popen(command,cwd=sources[role],env=env,stdout=stream,stderr=errors)
                _,status,usage=os.wait4(child.pid,0);child.returncode=os.waitstatus_to_exitcode(status)
            error,rows=None,None
            if child.returncode==0:
                try:rows=validate_run(stdout,stderr)
                except (ValueError,OSError) as exc:error=str(exc)
            post=all_drift()
            receipt={'label':label,'role':role,'repeat':repeat,'commit':COMMITS[role],
                'source_tree_sha256':identities[role]['source_tree_sha256'],
                'binary_sha256':sha(binaries[role]),'goenv_sha256':sha(out/role/'goenv.json'), 'command':command,
                'started_utc':start,'completed_utc':now(),'exit_code':child.returncode,
                'elapsed_seconds':time.monotonic()-clock,'load_before':load,'user_seconds':usage.ru_utime,
                'system_seconds':usage.ru_stime,'child_max_rss_kib':usage.ru_maxrss,
                'log_sha256':sha(stdout),'stderr_sha256':sha(stderr),'validation_error':error,
                'case_validation':{'rows':len(rows),'exact_case_set':True} if rows is not None else None,
                'source_drift_before':pred,'source_drift_after':post}
            receipts.append(receipt);write(out/'receipts.json',receipts);print(json.dumps(receipt),flush=True)
            if child.returncode or error or any(post.values()):raise SystemExit('retained failed run; stop collection')
    write(out/'completion.json',{'completed_utc':now(),'runs':6,'source_drift':all_drift(),
        'qualification':'bounded default-read guard; ignored fixture Close errors, not a persistence/drain or C3/C4 proof'})


if __name__=='__main__':main()
