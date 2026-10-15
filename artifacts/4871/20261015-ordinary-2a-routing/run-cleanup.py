from pathlib import Path
import hashlib,json,os,stat,subprocess,time
B=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013')
E=B/'evidence/ordinary-2a-routing-20261015';C=E/'cleanup';H=B/'ordinary-2a-routing-20261015';G=Path('/home/mikers/dev/snissn/gomap/.git');N=B/'native-4878-traversal'
assert not C.exists();C.mkdir()
def sha(d):return hashlib.sha256(d).hexdigest()
def dump(name,v):(C/name).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def desc(p):
 s=p.lstat();v=dict(dev=s.st_dev,inode=s.st_ino,bytes=s.st_size,mtime_ns=s.st_mtime_ns,mode=s.st_mode)
 if p.is_symlink():v['symlink']=os.readlink(p)
 elif p.is_file():v['sha256']=sha(p.read_bytes())
 return v
def git(*args):return subprocess.check_output(['git','--git-dir',str(G),*args])
snapshot=json.loads((E/'cleanup-inventory.json').read_text())
assert sha((E/'cleanup-inventory.json').read_bytes())=='03fa3a0aba818ab0a13bc753ad31933622848313b79356f8f67cdf4c969f3225'
source=json.loads((E/'source-preflight.json').read_text());local=json.loads((E/'checkpoint-local-receipt.json').read_text());readback=json.loads((E/'checkpoint-github-readback.json').read_text())
assert readback['status']=='PASS' and readback['commit']==local['commit'] and readback['published_members']==120 and readback['unique_decoded_blobs']==87
bins=json.loads((E/'binaries.json').read_text());privateindex=E/'checkpoint-private.index';released_files=[Path(r['path']) for r in bins]+[privateindex]
# Full root/member identity equality before any normalization.
assert not H.is_symlink() and H.resolve()==H
hs=H.stat();assert dict(dev=hs.st_dev,inode=hs.st_ino)==snapshot['root_identity']
current=[]
for p in sorted(H.rglob('*')):
 row=dict(path=str(p.relative_to(H)),**desc(p));current.append(row)
assert current==snapshot['members'],'full helper snapshot changed'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=H,text=True).strip()==snapshot['head']
assert subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all','--ignored'],cwd=H,text=True)==snapshot['status']
assert sha(Path(snapshot['index']['path']).read_bytes())==snapshot['index']['sha256']
for row in bins:
 p=Path(row['path']);s=p.stat()
 assert not p.is_symlink() and p.resolve()==p and dict(dev=s.st_dev,inode=s.st_ino,bytes=s.st_size,mtime_ns=s.st_mtime_ns,sha256=sha(p.read_bytes()))=={k:v for k,v in row.items() if k not in ['path','role']}
for p in released_files:assert p.parent==E/'public-comparison' or p==privateindex
registrations=git('worktree','list','--porcelain').decode()
roots=[line[9:] for line in registrations.splitlines() if line.startswith('worktree ')]
assert roots.count(str(H))==1 and not any(r.startswith(str(H)+'/') for r in roots)
block=next(b for b in registrations.strip().split('\n\n') if b.startswith('worktree '+str(H)+'\n'))
assert '\nlocked' not in block and '\nprunable' not in block
mounts=[]
for line in Path('/proc/self/mountinfo').read_text().splitlines():
 mp=line.split()[4].replace('\\040',' ').replace('\\134','\\')
 if mp==str(H) or mp.startswith(str(H)+'/'):mounts.append(mp)
assert not mounts,'mount boundary'
protected=[Path(x) for x in snapshot['protected']]
for p in protected:assert p.exists() and p.resolve()!=H and not p.resolve().is_relative_to(H) and not H.is_relative_to(p.resolve()),'protected overlap'
# Exact already-published contents prove all normalization paths.
objects={r['path']:r for r in local['objects']}
for r in source['owned_paths']:
 assert sha((H/r).read_bytes())==objects[r]['sha256']
 if r in source['production_files']:
  assert (E/'preimages'/r).read_bytes()==git('show',snapshot['head']+':'+r)
 else:assert (H/r).read_bytes()==(E/'candidate-source'/r).read_bytes()
# Save release and exact normalization authority before mutations.
authority=dict(root_release='Explicit parent release exacthelper+2ELFs; privateindex maydelete after complete byteproof',root_independent_receipt_sha256='bc7136e628ba1763c9511543df5400bed36fdb29047c4b0690944dabc51afae5',checkpoint=local['commit'],readback_sha256=sha((E/'checkpoint-github-readback.json').read_bytes()),root=str(H),snapshot_sha256='03fa3a0aba818ab0a13bc753ad31933622848313b79356f8f67cdf4c969f3225',restore_tracked_exact=source['production_files'],unlink_untracked_exact=[r for r in source['owned_paths'] if r not in source['production_files']],unlink_exact_files=[dict(path=str(p),**desc(p)) for p in released_files],no_force=True,no_branch_deletion=True)
dump('release-authority.json',authority)
# Protect retained evidence and active inputs, including current foreign demo.
ep={str(p.relative_to(E)):desc(p) for p in sorted(E.rglob('*')) if p.is_file() and not p.is_relative_to(C) and p not in released_files}
P=json.loads((E/'protection-before.json').read_text())
active={}
for n,old in P['protected_roots'].items():
 w=B/n;v=dict(head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=w,text=True).strip(),index_sha256=sha(Path(old['index']).read_bytes()))
 assert v['head']==old['head'] and v['index_sha256']==old['index_sha256']
 if n.startswith('ordinary'):
  v['status']=subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all'],cwd=w,text=True).strip();assert v['status']==old['status']
  v['source']={r:sha((w/r).read_bytes()) for r in old['tracked_go_module']};assert v['source']==old['tracked_go_module']
  v['ignored']={r:dict(dev=(w/r).stat().st_dev,ino=(w/r).stat().st_ino,size=(w/r).stat().st_size,mtime_ns=(w/r).stat().st_mtime_ns) for r in old['ignored_custody']};assert v['ignored']==old['ignored_custody']
 active[n]=v
foreign=list(json.loads((E/'writer-frozen/final.receipt.json').read_text())['protected'])+['cmd/treedb_column_graph_demo/main.go']
foreign_before={r:desc(N/r) for r in foreign}
assert foreign_before[foreign[-1]]['sha256']=='176e418116243bfc126641b4270c89a27bfdb12dad1f210dfde09c77ef4187ad' and foreign_before[foreign[-1]]['bytes']==15130
refs_before=git('for-each-ref','--format=%(refname) %(objectname)').decode()
# Relevant consumer scan; unrelated denied daemon internals aren't target use.
targets=[str(H)]+[r['path'] for r in bins];anc=set();pid=os.getpid()
while pid>1:
 anc.add(pid)
 try:pid=int(next(x for x in (Path('/proc')/str(pid)/'status').read_text().splitlines() if x.startswith('PPid:')).split()[1])
 except (OSError,StopIteration):break
hits=[];denied=[]
for p in Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  if p.stat().st_uid!=os.getuid():continue
  pid=int(p.name);cmd=(p/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace');matches=[]
  for f in ['cwd','exe']:
   try:
    v=os.readlink(p/f)
    if any(v==t or v.startswith(t+'/') for t in targets):matches.append(dict(field=f,value=v))
   except OSError as ex:denied.append(dict(pid=pid,field=f,cmd=cmd,error=type(ex).__name__))
  try:
   for fd in (p/'fd').iterdir():
    try:
     v=os.readlink(fd)
     if any(v==t or v.startswith(t+'/') for t in targets):matches.append(dict(field='fd',value=v))
    except OSError:pass
  except OSError:pass
  try:
   for l in (p/'maps').read_text().splitlines():
    if any(t in l for t in targets):matches.append(dict(field='maps',value=l))
  except OSError as ex:denied.append(dict(pid=pid,field='maps',cmd=cmd,error=type(ex).__name__))
  if any(t in cmd for t in targets):matches.append(dict(field='cmdline',value=cmd))
  if matches:hits.append(dict(pid=pid,inspector_or_ancestor=pid in anc,cmd=cmd,matches=matches))
 except OSError:pass
assert not any(not x['inspector_or_ancestor'] for x in hits),'live target consumer'
# Only known unrelated service/SSH ancestors may have unreadable process internals.
for r in denied:assert r['pid'] in anc or any(x in r['cmd'] for x in ['/usr/lib/systemd/systemd --user','(sd-pam)','/usr/bin/ssh-agent -D']),'uncertain relevant unreadable process'
dump('preflight.json',dict(full_snapshot_equal=True,registered_worktree=block,no_nested_worktree=True,no_nested_mount=True,protected_overlap=False,consumers=hits,denials=denied,protected_evidence_count=len(ep),protected_evidence_fingerprint=sha(json.dumps(ep,sort_keys=True).encode()),foreign_before=foreign_before,protected_path_identity={str(p):dict(dev=p.stat().st_dev,inode=p.stat().st_ino) for p in protected},private_index=desc(privateindex)))
free_before=os.statvfs(B).f_bavail*os.statvfs(B).f_frsize
start=time.monotonic()
q=subprocess.run(['git','restore','--source='+snapshot['head'],'--worktree','--',*source['production_files']],cwd=H,capture_output=True)
dump('normalize-tracked.receipt.json',dict(argv=['git','restore','--source='+snapshot['head'],'--worktree','--',*source['production_files']],exit=q.returncode,stdout=q.stdout.decode(),stderr=q.stderr.decode()));assert q.returncode==0
for r in authority['unlink_untracked_exact']:assert sha((H/r).read_bytes())==objects[r]['sha256'];(H/r).unlink()
assert not subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all','--ignored'],cwd=H)
assert sha(Path(snapshot['index']['path']).read_bytes())==snapshot['index']['sha256']
clean=[]
for p in sorted(H.rglob('*')):clean.append(dict(path=str(p.relative_to(H)),**desc(p)))
dump('clean-snapshot.json',dict(root=str(H),root_identity=dict(dev=H.stat().st_dev,inode=H.stat().st_ino),head=snapshot['head'],status='',members=clean))
q=subprocess.run(['git','--git-dir',str(G),'worktree','remove',str(H)],capture_output=True)
dump('remove-worktree.receipt.json',dict(argv=['git','--git-dir',str(G),'worktree','remove',str(H)],exit=q.returncode,stdout=q.stdout.decode(),stderr=q.stderr.decode()));assert q.returncode==0
for row in authority['unlink_exact_files']:
 p=Path(row['path']);assert desc(p)=={k:v for k,v in row.items() if k!='path'};p.unlink()
free_after=os.statvfs(B).f_bavail*os.statvfs(B).f_frsize
assert not H.exists() and not Path(snapshot['private_git_dir']).exists()
assert str(H) not in [l[9:] for l in git('worktree','list','--porcelain').decode().splitlines() if l.startswith('worktree ')]
assert all(not p.exists() for p in released_files)
assert git('for-each-ref','--format=%(refname) %(objectname)').decode()==refs_before
for rel,row in ep.items():assert desc(E/rel)==row,'retained evidence drift '+rel
for r,row in foreign_before.items():assert desc(N/r)==row,'foreign drift '+r
for n,old in P['protected_roots'].items():
 w=B/n;assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=w,text=True).strip()==active[n]['head'];assert sha(Path(old['index']).read_bytes())==active[n]['index_sha256']
 if n.startswith('ordinary'):
  assert subprocess.check_output(['git','status','--porcelain=v1','--untracked-files=all'],cwd=w,text=True).strip()==active[n]['status']
  assert {r:sha((w/r).read_bytes()) for r in old['tracked_go_module']}==active[n]['source']
  for r,v in active[n]['ignored'].items():
   s=(w/r).stat();assert dict(dev=s.st_dev,ino=s.st_ino,size=s.st_size,mtime_ns=s.st_mtime_ns)==v
for p in protected:assert p.exists()
result=dict(status='DONE',removed_worktree=str(H),removed_elfs=[r['path'] for r in bins],removed_private_index=str(privateindex),normalized_tracked=source['production_files'],removed_preserved_untracked=authority['unlink_untracked_exact'],checkpoint=local['commit'],logical_bytes=dict(helper_before_normalization=snapshot['logical_file_bytes'],elfs=sum(r['bytes'] for r in bins),private_index=authority['unlink_exact_files'][-1]['bytes']),free_before_bytes=free_before,free_after_bytes=free_after,observed_free_delta_bytes=free_after-free_before,delta_note='Observed filesystem delta includes concurrent host changes and filesystem metadata; logical estimates are separate attribution.',helper_absent=True,unregistered=True,private_git_dir_absent=True,elfs_absent=True,private_index_absent=True,all_branches_and_other_refs_unchanged=True,protected_evidence_count=len(ep),protected_evidence_fingerprint_before=sha(json.dumps(ep,sort_keys=True).encode()),protected_evidence_fingerprint_after=sha(json.dumps({r:desc(E/r) for r in ep},sort_keys=True).encode()),original_ordinary_and_integration_unchanged=True,native_head_index_protected4_unchanged=True,foreign_demo=foreign_before[foreign[-1]],elapsed_seconds=time.monotonic()-start,no_go=True)
dump('result.json',result)
print(json.dumps(dict(result=result,result_sha256=sha((C/'result.json').read_bytes())),indent=2))
