import datetime, hashlib, json, os, pathlib, subprocess, time

source = pathlib.Path('/private/tmp/gomap-cow-execution-o2nauuzm/OWNED/c2-windows-contract-repair')
out = pathlib.Path('/private/tmp/gomap-cow-execution-o2nauuzm/artifacts/c2/windows-contract-repair/crosscompile-v4')
out.mkdir(parents=True, exist_ok=True)

def identity():
    raw = subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'], cwd=source)
    files = {}
    for name in sorted(set(raw.decode().strip('\0').split('\0'))):
        path = source / name
        if path.is_file():
            files[name] = hashlib.sha256(path.read_bytes()).hexdigest()
    encoded = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
    return {'head': subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip(), 'files': files, 'file_count': len(files), 'aggregate_sha256': hashlib.sha256(encoded).hexdigest()}

before = identity()
(out/'source-identity-before.json').write_text(json.dumps(before, indent=2)+'\n')
(out/'runtime.patch').write_bytes(subprocess.check_output(['git','diff','--binary'],cwd=source))
env = dict(os.environ, GOWORK='off', GOTOOLCHAIN='go1.26.8', GOMAXPROCS='4', GOCACHE='/private/tmp/gomap-cow-execution-o2nauuzm/cache/windows-repair-darwin')
for key in ['GOGC','GOMEMLIMIT','GOOS','GOARCH','GOROOT']:
    env.pop(key, None)
receipts = []
env.update(GOOS='windows', GOARCH='amd64', CGO_ENABLED='0', GOCACHE='/private/tmp/gomap-cow-execution-o2nauuzm/cache/windows-contract-repair')
for name, package in [('valuelog','./TreeDB/internal/valuelog'),('cache','./TreeDB/caching'),('public','./TreeDB')]:
    label='windows-'+name
    command=['go','test','-c','-o',str(out/(name+'.test.exe'))]
    if name=='valuelog':
        command+=['-gcflags=github.com/snissn/gomap/TreeDB/internal/valuelog=-m=2']
    command+=[package]
    started=datetime.datetime.now(datetime.timezone.utc).isoformat()
    tick=time.monotonic()
    with (out/(label+'.stdout')).open('wb') as stdout, (out/(label+'.stderr')).open('wb') as stderr:
        result=subprocess.run(command,cwd=source,env=env,stdout=stdout,stderr=stderr,timeout=600)
    receipt={'label':label,'command':command,'started':started,'finished':datetime.datetime.now(datetime.timezone.utc).isoformat(),'elapsed_seconds':time.monotonic()-tick,'exit':result.returncode,'source_aggregate_sha256':before['aggregate_sha256'],'binary_sha256':hashlib.sha256((out/(name+'.test.exe')).read_bytes()).hexdigest() if result.returncode==0 else None}
    receipts.append(receipt)
    (out/'receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
    print(json.dumps(receipt),flush=True)
after=identity()
(out/'source-identity-after.json').write_text(json.dumps(after,indent=2)+'\n')
drift={name:[before['files'].get(name),after['files'].get(name)] for name in sorted(set(before['files'])|set(after['files'])) if before['files'].get(name)!=after['files'].get(name)}
(out/'source-drift.json').write_text(json.dumps(drift,indent=2)+'\n')
raise SystemExit(0 if not drift and all(r['exit']==0 for r in receipts) else 1)
