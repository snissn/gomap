"""Root-owned, source-verified Linux builds in the existing bounded envelope."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import hashlib, json, pathlib, shlex, subprocess, time

import sys,re,inspect
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from source_paths import isolate_paths

def main(argv=None):
    if not __debug__:
        raise RuntimeError('ordinary Python required; assertions are fail-closed guards')
    args=sys.argv[1:] if argv is None else list(argv)
    assert len(args)==6, 'head tree source inventory prereview local-root required'
    HEAD,TREE,SOURCE,inventory_path,review_path,root_name=args
    assert re.fullmatch('[0-9a-f]{40}',HEAD) and re.fullmatch('[0-9a-f]{40}',TREE)
    assert re.fullmatch('gomap-5021-sustained-runtime-build-root-v[1-9][0-9]*',root_name)
    LOCAL=pathlib.Path('/tmp')/root_name
    REMOTE='/home/mikers/'+root_name
    INVENTORY=pathlib.Path(inventory_path);REVIEW=pathlib.Path(review_path)
    assert INVENTORY.is_file() and not INVENTORY.is_symlink() and REVIEW.is_file() and not REVIEW.is_symlink()
    inv=json.loads(INVENTORY.read_bytes());review=json.loads(REVIEW.read_bytes())
    assert inv['head']==HEAD and inv['tree']==TREE
    assert review['state']=='MERGED_SOURCE_ACCEPTED_FOR_BOUNDED_BUILD'
    assert review['head']==HEAD and review['tree']==TREE
    assert review['current_head_ci_passed'] is True and review['required_review_passed'] is True
    assert review['source_inventory_sha256']==hashlib.sha256(INVENTORY.read_bytes()).hexdigest()
    assert review['issue']==5021 and review['known_findings']==[]
    assert re.fullmatch(r'/home/mikers/gomap-5021-sustained-final-source-root-v[1-9][0-9]*/source',SOURCE)
    isolate_paths([LOCAL],[pathlib.Path(__file__).resolve().parents[1],INVENTORY,REVIEW])
    LOCAL.mkdir(exist_ok=False)
    (LOCAL/'git-source-inventory.json').write_bytes(INVENTORY.read_bytes())
    SSH = ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','mikers@192.168.0.111']

    def call(name, program, data=None, timeout=60):
        started = time.time()
        r = subprocess.run(SSH+[shlex.join(['python3','-c',program])],input=data,capture_output=True,timeout=timeout)
        (LOCAL/(name+'.stdout')).write_bytes(r.stdout)
        (LOCAL/(name+'.stderr')).write_bytes(r.stderr)
        (LOCAL/(name+'.exit.json')).write_text(json.dumps({'exit':r.returncode,'started_unix':started,'finished_unix':time.time()})+'\n')
        r.check_returncode()
        return r.stdout

    verify = '''
def verify_source(source,inventory):
 assert source.is_dir() and not source.is_symlink()
 expected={item['path'] for item in inventory['rows']}
 assert len(expected)==len(inventory['rows']) and expected
 actual=set()
 for directory,dirs,files in os.walk(source,followlinks=False):
  for name in dirs+files:
   p=pathlib.Path(directory)/name;assert not p.is_symlink(),str(p)
  for name in files:actual.add((pathlib.Path(directory)/name).relative_to(source).as_posix())
 assert actual==expected,{'missing':sorted(expected-actual)[:10],'extra':sorted(actual-expected)[:10]}
 for item in inventory['rows']:
  rel=pathlib.PurePosixPath(item['path']);assert not rel.is_absolute() and '..' not in rel.parts and str(rel)==item['path']
  assert item['mode'] in ('100644','100755')
  p=source/rel;assert p.is_file() and not p.is_symlink(),str(p)
  data=p.read_bytes();blob=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\\0'+data).hexdigest()
  assert blob==item['git_blob'],item['path']
  assert p.stat().st_mode&0o777==(0o755 if item['mode']=='100755' else 0o644),item['path']
'''
    prefix = 'if not __debug__: raise RuntimeError(\'ordinary Python required; assertions must run\')\nimport hashlib,json,os,pathlib,subprocess,sys\nROOT='+repr(REMOTE)+'\nSOURCE='+repr(SOURCE)+'\nHEAD='+repr(HEAD)+'\nTREE='+repr(TREE)+'\nBINDING_SHA='+repr(hashlib.sha256(REVIEW.read_bytes()).hexdigest())+'\n'+verify+inspect.getsource(isolate_paths)+'\n'
    prepare = prefix+'''
root=pathlib.Path(ROOT);assert not root.exists()
isolate_paths([root],[SOURCE])
template=pathlib.Path(SOURCE)/'docs/benchmarks/fixed_cluster_sustained_20261005/harness/sources/native-runner'
for name,pin in {'gomap-1242-bounded-runner-prepare.py':'1993a5de4d50dece596e884afd7b3f9566765787f65e41c8ec7cc50dd1ee5229','run.sh':'e3e4570f7f66ff465dd57675a5f7b0d6aea61d58d6619836407e1383d79a4e5a','inner.sh':'0bd4c035dc654e4776c4c6a69ce6b29e4a339bc540576c181a9ab7690c144c98'}.items():
 p=template/name;assert p.is_file() and not p.is_symlink() and p.stat().st_size<=256*1024
 assert hashlib.sha256(p.read_bytes()).hexdigest()==pin,name
disk=os.statvfs('/home/mikers');assert disk.f_bavail*disk.f_frsize>=50*1024**3
mem={l.split(':')[0]:int(l.split()[1])*1024 for l in pathlib.Path('/proc/meminfo').read_text().splitlines() if ':' in l}
assert mem['MemAvailable']>=16*1024**3 and os.getloadavg()[0]<4
assert not subprocess.run(['pgrep','-af','(^|/)(go|compile|link|[^ ]+\\.test)( |$)'],capture_output=True,text=True).stdout.strip()
assert not subprocess.check_output(['docker','ps','-q'],text=True).strip()
raw=sys.stdin.buffer.read();inventory=json.loads(raw);assert inventory['head']==HEAD and inventory['tree']==TREE
verify_source(pathlib.Path(SOURCE),inventory)
root.mkdir();subprocess.run(['cp','-a','--reflink=auto',SOURCE,str(root/'source')],check=True)
verify_source(root/'source',inventory)
(root/'git-source-inventory.json').write_bytes(raw)
cfg={'unit_prefix':'gomap-5021-sustained-runtime-build-','outer_timeout_seconds':1100,'provenance':{'head':HEAD,'tree':TREE,'git_inventory_sha256':hashlib.sha256(raw).hexdigest()},'go_commands':[
 {'name':'fixed-peer-build','args':['build','-o',str(root/'receipts/treedb-fixed-peer'),'./cmd/treedb-fixed-peer'],'timeout_seconds':480},
 {'name':'query-driver-build','args':['build','-o',str(root/'receipts/treedb-query-under-write'),'./cmd/treedb-query-under-write'],'timeout_seconds':480}]}
(root/'config.json').write_text(json.dumps(cfg,indent=2)+'\\n')
subprocess.run(['python3','-B',str(template/'gomap-1242-bounded-runner-prepare.py'),str(root)],check=True)
print(json.dumps({'state':'PREPARED_NOT_RUN','head':HEAD,'tree':TREE,'git_inventory_sha256':hashlib.sha256(raw).hexdigest(),'tracked_files':len(inventory['rows'])}))
'''
    print(call('prepare',prepare,INVENTORY.read_bytes(),120).decode().strip(),flush=True)
    run = prefix+'''
root=pathlib.Path(ROOT)
raise SystemExit(subprocess.run(['/bin/bash',str(root/'run.sh')],cwd=root).returncode)
'''
    call('bounded-build',run,timeout=1140)
    finish = prefix+'''
root=pathlib.Path(ROOT);raw=(root/'git-source-inventory.json').read_bytes();inventory=json.loads(raw);verify_source(root/'source',inventory)
assert (root/'receipts/scope.exit').read_text().strip()=='0'
assert all(int(x.split()[1])==0 for x in (root/'receipts/end-memory.events.txt').read_text().splitlines() if x.split()[0] in ('oom','oom_kill'))
out={}
for i,name in enumerate(['treedb-fixed-peer','treedb-query-under-write']):
 label=['fixed-peer-build','query-driver-build'][i]
 assert (root/'receipts'/f'{i:02d}-{label}.exit').read_text().strip()=='0'
 p=root/'receipts'/name;assert p.is_file() and not p.is_symlink()
 rawelf=p.read_bytes();assert rawelf[:4]==b'\\x7fELF'
 meta={}
 for label,argv in [('file',['file',str(p)]),('ldd',['ldd',str(p)]),('go-version-m',['/home/mikers/gomap-q5-evidence/toolchains/go1.26.0-linux-amd64/bin/go','version','-m',str(p)])]:
  r=subprocess.run(argv,capture_output=True,text=True,timeout=15)
  (root/'receipts'/(name+'.'+label+'.stdout')).write_text(r.stdout);(root/'receipts'/(name+'.'+label+'.stderr')).write_text(r.stderr)
  assert r.returncode==0 or (label=='ldd' and r.returncode==1 and ('not a dynamic executable' in r.stderr or 'statically linked' in r.stdout)),(label,r.returncode,r.stderr)
  meta[label]={'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr}
 out[name]={'path':str(p),'sha256':hashlib.sha256(rawelf).hexdigest(),'bytes':len(rawelf),'metadata':meta}
receipt={'state':'VERIFIED_ELFS_NOT_IMAGES_OR_RUNTIME','head':HEAD,'tree':TREE,'source_inventory_sha256':hashlib.sha256(raw).hexdigest(),'source_verified_before_after':True,'landed_source_verified':True,'current_head_ci_passed':True,'source_binding_sha256':BINDING_SHA,'tracked_files':len(inventory['rows']),'issue':5021,'ELFs':out,'peak_bytes':int((root/'receipts/end-memory.peak.txt').read_text()),'memory_events':(root/'receipts/end-memory.events.txt').read_text(),'source':str(root/'source')}
(root/'build-proof.json').write_text(json.dumps(receipt,indent=2)+'\\n');print(json.dumps(receipt))
'''
    raw = call('finish',finish,timeout=120)
    receipt = json.loads(raw)
    (LOCAL/'build-proof.json').write_text(json.dumps(receipt,indent=2)+'\n')
    # Retain every small command, environment and cgroup receipt, without copying ELFs.
    capture = prefix+'''
root=pathlib.Path(ROOT);out={}
for p in sorted((root/'receipts').iterdir()):
 if p.name in ('treedb-fixed-peer','treedb-query-under-write'):continue
 assert p.is_file() and not p.is_symlink() and p.stat().st_size<=4*1024*1024,p.name
 out[p.name]={'raw':p.read_text(),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
for name in ('config.json','run.sh','inner.sh','source-inventory.json','git-source-inventory.json'):
 p=root/name;out[name]={'raw':p.read_text(),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}
print(json.dumps(out))
'''
    (LOCAL/'raw-build-receipts.json').write_bytes(call('raw-receipts',capture,timeout=60))
    print(json.dumps({'state':receipt['state'],'head':HEAD,'tree':TREE,'ELFs':{k:v['sha256'] for k,v in receipt['ELFs'].items()},'peak_bytes':receipt['peak_bytes']}),flush=True)

if __name__=='__main__':
    main()
