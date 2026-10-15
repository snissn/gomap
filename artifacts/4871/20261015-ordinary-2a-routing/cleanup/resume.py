from pathlib import Path
import hashlib,json,subprocess,os,time
B=Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013');E=B/'evidence/ordinary-2a-routing-20261015';C=E/'cleanup';H=B/'ordinary-2a-routing-20261015';G=Path('/home/mikers/dev/snissn/gomap/.git');N=B/'native-4878-traversal'
def sha(d):return hashlib.sha256(d).hexdigest()
def desc(p):
 s=p.lstat();r=dict(dev=s.st_dev,inode=s.st_ino,bytes=s.st_size,mtime_ns=s.st_mtime_ns,mode=s.st_mode)
 if p.is_symlink():r['symlink']=os.readlink(p)
 elif p.is_file():r['sha256']=sha(p.read_bytes())
 return r
def dump(n,v):(C/n).write_text(json.dumps(v,indent=2,sort_keys=True)+'\n')
def git(*a):return subprocess.check_output(['git','--git-dir',str(G),*a])
def wgit(w,*a):return subprocess.check_output(['git',*a],cwd=w)
inv=json.loads((E/'cleanup-inventory.json').read_text());a=json.loads((C/'release-authority.json').read_text());pre=json.loads((C/'preflight.json').read_text());P=json.loads((E/'protection-before.json').read_text());source=json.loads((E/'source-preflight.json').read_text());loc=json.loads((E/'checkpoint-local-receipt.json').read_text());bins=json.loads((E/'binaries.json').read_text())
released=[Path(x['path']) for x in a['unlink_exact_files']]
expected={r['path']:r for r in inv['members']};removed=set(a['unlink_untracked_exact']);restored=set(a['restore_tracked_exact']);dirs={str(Path(r).parent) for r in removed|restored}
current={str(p.relative_to(H)):dict(path=str(p.relative_to(H)),**desc(p)) for p in sorted(H.rglob('*'))}
assert set(current)==set(expected)-removed
assert dict(dev=H.stat().st_dev,inode=H.stat().st_ino)==inv['root_identity']
for rel,row in current.items():
 if rel in restored:assert (H/rel).read_bytes()==(E/'preimages'/rel).read_bytes()
 elif rel in dirs:assert all(row[k]==expected[rel][k] for k in ['dev','inode','mode'])
 else:assert row==expected[rel],rel
assert not wgit(H,'status','--porcelain=v1','--untracked-files=all','--ignored')
assert wgit(H,'write-tree').decode().strip()==source['tree'] and wgit(H,'rev-parse','HEAD').decode().strip()==inv['head']
ep={str(p.relative_to(E)):desc(p) for p in sorted(E.rglob('*')) if p.is_file() and not p.is_relative_to(C) and p not in released}
assert sha(json.dumps(ep,sort_keys=True).encode())==pre['protected_evidence_fingerprint']
for row in a['unlink_exact_files']:assert desc(Path(row['path']))=={k:v for k,v in row.items() if k!='path'}
def protect():
 for rel,row in pre['foreign_before'].items():assert desc(N/rel)==row,'foreign '+rel
 for n,old in P['protected_roots'].items():
  w=B/n;assert wgit(w,'rev-parse','HEAD').decode().strip()==old['head'];assert sha(Path(old['index']).read_bytes())==old['index_sha256']
  if n.startswith('ordinary'):
   assert wgit(w,'status','--porcelain=v1','--untracked-files=all').decode().strip()==old['status'];assert {r:sha((w/r).read_bytes()) for r in old['tracked_go_module']}==old['tracked_go_module']
   for r,v in old['ignored_custody'].items():
    s=(w/r).stat();assert dict(dev=s.st_dev,ino=s.st_ino,size=s.st_size,mtime_ns=s.st_mtime_ns)==v
 for p,ident in pre['protected_path_identity'].items():
  s=Path(p).stat();assert dict(dev=s.st_dev,inode=s.st_ino)==ident
protect()
regs=git('worktree','list','--porcelain').decode();assert ('worktree '+str(H)+'\n') in regs
assert not any(r[9:].startswith(str(H)+'/') for r in regs.splitlines() if r.startswith('worktree '))
assert not any(l.split()[4]==str(H) or l.split()[4].startswith(str(H)+'/') for l in Path('/proc/self/mountinfo').read_text().splitlines())
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
    if any(v==t or v.startswith(t+'/') for t in targets):matches.append((f,v))
   except OSError as ex:denied.append(dict(pid=pid,field=f,cmd=cmd,error=type(ex).__name__))
  try:
   for fd in (p/'fd').iterdir():
    try:
     v=os.readlink(fd)
     if any(v==t or v.startswith(t+'/') for t in targets):matches.append(('fd',v))
    except OSError:pass
  except OSError:pass
  try:
   for l in (p/'maps').read_text().splitlines():
    if any(t in l for t in targets):matches.append(('maps',l))
  except OSError as ex:denied.append(dict(pid=pid,field='maps',cmd=cmd,error=type(ex).__name__))
  if any(t in cmd for t in targets):matches.append(('cmdline',cmd))
  if matches:hits.append(dict(pid=pid,inspector_or_ancestor=pid in anc,cmd=cmd,matches=matches))
 except OSError:pass
assert not any(not r['inspector_or_ancestor'] for r in hits)
for r in denied:assert r['pid'] in anc or any(x in r['cmd'] for x in ['/usr/lib/systemd/systemd --user','(sd-pam)','/usr/bin/ssh-agent -D'])
dump('resume-preflight.json',dict(exact_normalized_state=True,index_tree_equals2a=True,staged_diff_empty=True,all_non_normalization_members_equal=True,normalized_parent_directory_identity_equal=True,evidence_equal=True,foreign4_equal=True,consumers=hits,denials=denied,note='Authorized restore updates helper-only Git stat cache and restored/unlinked files parent mtimes; source/index tree exactly2a. No target worktree/ELF deletion before this fresh verification.'))
dump('clean-snapshot.json',dict(root=str(H),root_identity=inv['root_identity'],head=inv['head'],tree=source['tree'],status='',members=list(current.values())))
refs_before=git('for-each-ref','--format=%(refname) %(objectname)').decode();free_before=os.statvfs(B).f_bavail*os.statvfs(B).f_frsize;started=time.monotonic()
q=subprocess.run(['git','--git-dir',str(G),'worktree','remove',str(H)],capture_output=True);dump('remove-worktree.receipt.json',dict(argv=['git','--git-dir',str(G),'worktree','remove',str(H)],exit=q.returncode,stdout=q.stdout.decode(),stderr=q.stderr.decode()));assert q.returncode==0
for row in a['unlink_exact_files']:
 p=Path(row['path']);assert desc(p)=={k:v for k,v in row.items() if k!='path'};p.unlink()
free_after=os.statvfs(B).f_bavail*os.statvfs(B).f_frsize
assert not H.exists() and not Path(inv['private_git_dir']).exists()
assert str(H) not in [l[9:] for l in git('worktree','list','--porcelain').decode().splitlines() if l.startswith('worktree ')]
assert all(not p.exists() for p in released);assert git('for-each-ref','--format=%(refname) %(objectname)').decode()==refs_before
for rel,row in ep.items():assert desc(E/rel)==row
protect()
result=dict(status='DONE',removed_worktree=str(H),removed_elfs=[r['path'] for r in bins],removed_private_index=a['unlink_exact_files'][-1]['path'],normalized_tracked=a['restore_tracked_exact'],removed_preserved_untracked=a['unlink_untracked_exact'],checkpoint=loc['commit'],logical_bytes=dict(helper_before_normalization=inv['logical_file_bytes'],helper_clean_before_remove=sum(x['bytes'] for x in current.values() if 'sha256'in x),elfs=sum(r['bytes'] for r in bins),private_index=a['unlink_exact_files'][-1]['bytes']),free_before_bytes=free_before,free_after_bytes=free_after,observed_free_delta_bytes=free_after-free_before,delta_note='Observed filesystem delta includes filesystem metadata/concurrent host changes; logical estimates are separate attribution.',helper_absent=True,unregistered=True,private_git_dir_absent=True,elfs_absent=True,private_index_absent=True,all_branches_and_other_refs_unchanged=True,protected_evidence_count=len(ep),protected_evidence_fingerprint_before=sha(json.dumps(ep,sort_keys=True).encode()),protected_evidence_fingerprint_after=sha(json.dumps({r:desc(E/r) for r in ep},sort_keys=True).encode()),original_ordinary_and_integration_unchanged=True,native_head_index_protected4_unchanged=True,foreign_demo=pre['foreign_before']['cmd/treedb_column_graph_demo/main.go'],elapsed_seconds=time.monotonic()-started,no_go=True,normalization_index_stat_refresh='Retained failed assertion and subsequent exact2a index-tree proof; no staged/source difference. Restored/unlinked paths parent-directory mtime changes explicitly classified.')
dump('result.json',result);print(json.dumps(dict(result=result,result_sha256=sha((C/'result.json').read_bytes())),indent=2))
