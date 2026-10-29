"""Strict sampled census: only volatile TMPDIR ENOENT permits complete rescan.
Deadlines are checked around filesystem operations; not a hard syscall preemption.
"""
import errno,json,os,pathlib,stat,time
MAX_ATTEMPTS=3
MAX_SECONDS=1.0
MAX_ENTRIES=100000
CAP_BYTES=2*1024**3
FREE_BYTES=20*1024**3
class CensusError(RuntimeError):pass
class Gap(Exception):
 def __init__(self,path,exc):self.path=path;self.exc=exc

def _receipt(root,actor,**fields):
 row=dict(actor=actor,monotonic=time.monotonic(),**fields)
 fd=os.open(root/'capacity-census.jsonl',os.O_WRONLY|os.O_APPEND|os.O_CREAT|os.O_NOFOLLOW,0o600)
 try:
  st=os.fstat(fd)
  if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.getuid():raise CensusError('unsafe census receipt')
  payload=(json.dumps(row,sort_keys=True)+'\n').encode()
  if os.write(fd,payload)!=len(payload):raise CensusError('short census receipt')
 finally:os.close(fd)

def _walk(root,check):
 total=0;count=0
 def failure(rel,exc):
  if exc.errno==errno.ENOENT and rel.parts and rel.parts[0]=='tmp':raise Gap(str(rel),exc) from exc
  raise CensusError('census filesystem error at '+str(rel)+': '+repr(exc)) from exc
 def visit(fd,rel):
  nonlocal total,count
  check()
  try:
   with os.scandir(fd) as entries:
    for entry in entries:
     check();count+=1
     if count>MAX_ENTRIES:raise CensusError('census entry budget unresolved')
     child=rel/entry.name
     try:
      st=entry.stat(follow_symlinks=False);check()
      if stat.S_ISLNK(st.st_mode):raise CensusError('census unsafe symlink '+str(child))
      if stat.S_ISDIR(st.st_mode):
       sub=os.open(entry.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
       try:
        check()
        now=os.fstat(sub)
        if (now.st_dev,now.st_ino)!=(st.st_dev,st.st_ino):raise CensusError('census changed directory identity '+str(child))
        visit(sub,child)
       finally:os.close(sub)
      elif stat.S_ISREG(st.st_mode):total+=st.st_size
      else:raise CensusError('census unsupported file type '+str(child))
     except OSError as exc:failure(child,exc)
  except OSError as exc:failure(rel,exc)
 check()
 try:fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 except OSError as exc:raise CensusError('census root unavailable: '+repr(exc)) from exc
 try:
  identity=os.fstat(fd);visit(fd,pathlib.Path())
  try:end=os.stat(root,follow_symlinks=False)
  except OSError as exc:raise CensusError('census root disappeared: '+repr(exc)) from exc
  if not stat.S_ISDIR(end.st_mode) or (end.st_dev,end.st_ino)!=(identity.st_dev,identity.st_ino):raise CensusError('census root changed identity')
  check();return total,count
 finally:os.close(fd)

def enforce(root,actor,*,deadline=None):
 root=pathlib.Path(root);began=time.monotonic();expires=min(began+MAX_SECONDS,deadline if deadline is not None else float('inf'))
 def check():
  if time.monotonic()>=expires:raise CensusError('census time budget unresolved')
 for attempt in range(1,MAX_ATTEMPTS+1):
  try:
   check();total,count=_walk(root,check);check()
  except Gap as gap:
   _receipt(root,actor,kind='volatile_enoent_gap',attempt=attempt,path=gap.path,errno=gap.exc.errno,complete=False)
   if attempt==MAX_ATTEMPTS:raise CensusError('volatile census rescan budget unresolved') from gap
   continue
  except BaseException as exc:
   _receipt(root,actor,kind='unresolved',attempt=attempt,error=repr(exc),complete=False)
   raise
  # Only a complete successful traversal supplies the cap total. Rescans never
  # accumulate partial sizes, and cap failure is never interpreted as a gap.
  _receipt(root,actor,kind='complete',attempt=attempt,bytes=total,entries=count,complete=True)
  check()
  if total>CAP_BYTES:raise CensusError('2GiB output cap')
  v=os.statvfs(root);check()
  if v.f_bavail*v.f_frsize<FREE_BYTES:raise CensusError('20GiB hosted free floor')
  return total
 raise AssertionError('unreachable')
