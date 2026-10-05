"""Retain exact small profile originals and hash-bind native-only large artifacts."""
import hashlib, json, os, pathlib, subprocess, tarfile, time

root = pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
campaign = root/'offsets-3m-profile-paired'
out = root/'m-profile-analysis-v2'
out.mkdir(exist_ok=False)
go = pathlib.Path('/home/mikers/.gvm/gos/go1.26.3/bin/go')
env = dict(os.environ)
env.update(json.load(open(root/'qualified-manifest-offsets.json'))['build_env'])
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
inventory, analyses = [], []
for cell in sorted(p for p in campaign.iterdir() if p.is_dir()):
    variant = 'candidate' if 'candidate' in cell.name else 'baseline'
    binary = root/'bin'/('unified-bench-'+variant)
    for p in sorted(cell.iterdir()):
        if p.suffix == '.pprof' or p.name == 'trace.out':
            inventory.append(dict(path=str(p.relative_to(root)), bytes=p.stat().st_size, sha256=sha(p)))
    for name, sample in [('cpu_quicksilver_mixed_treedb.pprof',None), ('allocs_quicksilver_mixed_treedb.pprof','alloc_space'), ('mutex.pprof',None), ('block.pprof',None)]:
        argv = [str(go),'tool','pprof','-top','-nodecount=20']
        if sample: argv += ['-sample_index='+sample]
        argv += [str(binary),str(cell/name)]
        started=time.time(); p=subprocess.run(argv,capture_output=True,text=True,env=env)
        dest=out/(cell.name+'-'+name+'.top.txt'); dest.write_text(p.stdout+p.stderr)
        assert p.returncode == 0,p.stderr
        analyses.append(dict(command=argv,started=started,finished=time.time(),rc=p.returncode,output=str(dest.relative_to(root)),output_sha256=sha(dest),binary_sha256=sha(binary),profile_sha256=sha(cell/name)))
(out/'inventory.json').write_text(json.dumps(inventory,indent=2)+'\n')
(out/'analyses.json').write_text(json.dumps(dict(go_sha256=sha(go),rows=analyses),indent=2)+'\n')
files=[]
for p in campaign.rglob('*'):
    if p.is_file() and p.name in {'manifest.json','plan.json','source-receipt.json','build-receipt.json','runner-receipt.json','native-receipt.json','run.json','stdout.json','stderr.log','ldd.stdout.txt','ldd.stderr.txt'}: files.append(p)
files += list(p for p in out.iterdir() if p.is_file())
files += list(p for p in (root/'m-profile-analysis').iterdir() if p.is_file())
files += [root/'offsets-3m-profile-paired-memory.jsonl',root/'qualified-manifest-offsets.json',root/'offsets-3m-profile-paired-plan.json',pathlib.Path(__file__)]
files += list((root/'receipts-offsets').iterdir())
archive=root/'m-profile-3m-raw.tar.gz'
with tarfile.open(archive,'w:gz') as t:
    for p in sorted(set(files)): t.add(p,arcname=str(p.relative_to(root)),recursive=False)
print(json.dumps(dict(archive=str(archive),bytes=archive.stat().st_size,sha256=sha(archive),files=len(set(files)),analyses=len(analyses))))
