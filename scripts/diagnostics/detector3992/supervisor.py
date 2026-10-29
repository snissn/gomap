"""Linux-only, single-command supervisor. No poll/wait before group drain.
SIGKILL of this supervisor cannot be made recoverable by an in-process lease.
"""
import ctypes, errno, os, signal, subprocess, time, sys
from pathlib import Path
from dataclasses import dataclass,field

class Cancelled(BaseException):
 def __init__(self,signum):self.signum=signum;super().__init__(f'cancelled by {signum}')
class LifecycleError(RuntimeError):pass

def read_identity(pid):
 text=Path(f'/proc/{pid}/stat').read_text();fields=text[text.rfind(')')+2:].split()
 return {'state':fields[0],'ppid':int(fields[1]),'pgid':int(fields[2]),'sid':int(fields[3]),'start':int(fields[19])}

def proc_table():
 table={}
 for name in os.listdir('/proc'):
  if not name.isdigit():continue
  try:
   text=open('/proc/'+name+'/stat').read();fields=text[text.rfind(')')+2:].split()
   table[int(name)]={'state':fields[0],'ppid':int(fields[1]),'pgid':int(fields[2]),'sid':int(fields[3]),'start':int(fields[19])}
  except (OSError,ValueError,IndexError):continue
 return table

@dataclass(slots=True)
class Owned:
 process:object
 leader:int
 start:int|None
 baseline:set
 tracked:dict=field(default_factory=dict)
 done:object=None
 escaped:bool=False
 authority:bool=True
 reaped:bool=False

class Supervisor:
 def __init__(self,event=None,term_grace=2.0,allow_nested_sessions=False):
  self.allow_nested_sessions=allow_nested_sessions
  self.active=None;self.event=event or (lambda **kw:None);self.term_grace=term_grace
  self.cleaning=False;self.cancelled=None;self.old_handlers={};self.event_error=None
  libc=ctypes.CDLL(None,use_errno=True)
  if libc.prctl(36,1,0,0,0)!=0:raise OSError(ctypes.get_errno(),'PR_SET_CHILD_SUBREAPER')
 def emit(self,kind,**kw):
  try:self.event(kind=kind,**kw)
  except BaseException as exc:self.event_error=exc
 def install_handlers(self):
  for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP):self.old_handlers[sig]=signal.signal(sig,self._handler)
 def restore_handlers(self):
  for sig,old in self.old_handlers.items():signal.signal(sig,old)
  self.old_handlers.clear()
 def _handler(self,sig,frame):
  self.cancelled=sig
  # Only latch here: raising from arbitrary Python/C cleanup sites can be
  # swallowed or interrupt ownership registration. Poll at explicit safe points.
 def _leader_status(self,owned):
  if not owned.authority or owned.reaped:raise LifecycleError('leader authority already ended')
  if owned.process is None or owned.leader!=owned.process.pid:raise LifecycleError('direct-child handoff incomplete')
  try:status=os.waitid(os.P_PID,owned.leader,os.WEXITED|os.WNOHANG|os.WNOWAIT)
  except ChildProcessError:
   owned.authority=False
   # Retain unresolved registry/lease: loss of leader ownership is NOT drain.
   raise LifecycleError('owned leader unexpectedly reaped; signaling forbidden')
  if status is not None:
   if owned.done is None:self.emit('leader_zombie',pid=owned.leader,status=status.si_status)
   owned.done=status
  return owned.done
 def _owned_direct(self,owned,table):
  # Kernel child list closes the /proc enumeration birth/adoption window.
  # With the leader zombie, any remaining owned subtree has an adopted root.
  children=Path(f'/proc/self/task/{os.getpid()}/children').read_text().split()
  return {int(pid) for pid in children if int(pid)!=owned.leader and (int(pid),table.get(int(pid),{}).get('start')) not in owned.baseline}
 def _discover(self,owned):
  table=proc_table();leader=table.get(owned.leader)
  if leader is None or leader['start']!=owned.start:raise LifecycleError('pinned leader identity lost')
  descendants={owned.leader}
  changed=True
  while changed:
   changed=False
   for pid,row in table.items():
    if row['ppid'] in descendants and pid not in descendants:descendants.add(pid);changed=True
  # The process is a subreaper; newly adopted children belong to this command.
  descendants|={pid for pid,row in table.items() if row['ppid']==os.getpid() and (pid,row['start']) not in owned.baseline}
  descendants|={pid for pid,row in table.items() if row['pgid']==owned.leader}
  descendants|=self._owned_direct(owned,table)
  descendants.discard(owned.leader)
  for pid in descendants:
   row=table.get(pid)
   if row is None:continue
   old=owned.tracked.get(pid)
   if old is not None:
    if old['start']!=row['start']:raise LifecycleError('tracked descendant PID reused')
    continue
   try:fd=os.pidfd_open(pid,0)
   except ProcessLookupError:continue
   again=proc_table().get(pid)
   if again is None or again['start']!=row['start']:os.close(fd);continue
   owned.tracked[pid]={'start':row['start'],'fd':fd}
   self.emit('descendant_owned',pid=pid,start=row['start'],pgid=row['pgid'],sid=row['sid'])
   if not self.allow_nested_sessions and (row['pgid']!=owned.leader or row['sid']!=owned.leader):
    owned.escaped=True;self.emit('escape_detected',pid=pid)
  # Track/reap adopted zombies by exact PID. No wait(-1), no unrelated reaping.
  for pid,item in list(owned.tracked.items()):
   row=table.get(pid)
   if row is None:
    os.close(item['fd']);del owned.tracked[pid];continue
   if row['start']!=item['start']:raise LifecycleError('tracked identity changed')
   if not self.allow_nested_sessions and (row['pgid']!=owned.leader or row['sid']!=owned.leader):owned.escaped=True
   if row['ppid']==os.getpid() and row['state']=='Z':
    waited,status=os.waitpid(pid,os.WNOHANG)
    if waited:
     os.close(item['fd']);del owned.tracked[pid];self.emit('descendant_reaped',pid=pid)
  table=proc_table()
  # Re-scan on every drain iteration: exits can expose previously unseen orphans.
  residual={pid for pid,row in table.items() if pid!=owned.leader and (row['pgid']==owned.leader or pid in owned.tracked or (row['ppid']==os.getpid() and (pid,row['start']) not in owned.baseline))}
  residual|=self._owned_direct(owned,table)
  return residual
 def _signal(self,owned,sig):
  if self.active is not owned or not owned.authority or owned.reaped:raise LifecycleError('post-reap signaling forbidden')
  self._leader_status(owned)
  if os.getpgid(owned.leader)!=owned.leader or os.getsid(owned.leader)!=owned.leader:raise LifecycleError('leader/session identity mismatch')
  if owned.start is not None:
   row=read_identity(owned.leader)
   if row['start']!=owned.start:raise LifecycleError('leader start identity mismatch')
  self.emit('group_signal',pid=owned.leader,signal=int(sig))
  os.killpg(owned.leader,sig)
  # Escaped descendants are rejected, but drain their exact pidfd identities too.
  for pid,item in list(owned.tracked.items()):
   try:signal.pidfd_send_signal(item['fd'],sig,None,0)
   except ProcessLookupError:pass
 def _reap(self,owned):
  if self.active is not owned or not owned.authority or owned.reaped:raise LifecycleError('invalid final reap')
  if self._leader_status(owned) is None or self._discover(owned):raise LifecycleError('group not drained')
  # Revoke the registry BEFORE sole final reap, with handled signals blocked.
  oldmask=signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGINT,signal.SIGTERM,signal.SIGHUP})
  previous=self.cleaning;self.cleaning=True
  try:
   owned.authority=False;self.active=None;self.emit('registry_cleared',pid=owned.leader)
   waited,status=os.waitpid(owned.leader,0)
   if waited!=owned.leader:raise LifecycleError('wrong final reap')
   owned.reaped=True;owned.process.returncode=os.waitstatus_to_exitcode(status)
   self.emit('leader_reaped',pid=owned.leader,returncode=owned.process.returncode)
  finally:
   signal.pthread_sigmask(signal.SIG_SETMASK,oldmask)
   self.cleaning=previous
  return owned.process.returncode

 def finalize(self):
  owned=self.active
  if owned is None:return
  if owned.process is None:
   # Popen never returned a child. No child authority was obtained or signaled.
   owned.authority=False;self.active=None;self.emit('spawn_not_returned')
   return
  if not owned.leader:owned.leader=owned.process.pid
  was_cleaning=self.cleaning;self.cleaning=True
  try:
   if owned.start is None:
    # Unreaped direct-child + start_new_session pins this pending PGID even
    # before a census succeeds. Stop it first; then resolve exact starttime.
    self._signal(owned,signal.SIGTERM)
    row=read_identity(owned.leader)
    if row['pgid']!=owned.leader or row['sid']!=owned.leader:raise LifecycleError('pending session mismatch')
    owned.start=row['start'];self.emit('pending_identity_promoted',pid=owned.leader,start=owned.start)
   self._discover(owned);self._signal(owned,signal.SIGTERM)
   deadline=time.monotonic()+self.term_grace;killed=False;kill_seen=set()
   while True:
    done=self._leader_status(owned);residual=self._discover(owned)
    if done is not None and not residual:break
    if not killed and time.monotonic()>=deadline:
     self._signal(owned,signal.SIGKILL);killed=True
     kill_seen|={(pid,row['start']) for pid,row in owned.tracked.items()}
    if killed:
     new={(pid,row['start']) for pid,row in owned.tracked.items()}-kill_seen
     if new:
      self._signal(owned,signal.SIGKILL);kill_seen|=new
    time.sleep(.01)
   self._reap(owned)
  except BaseException:
   # If a census/identity read cannot establish drain, stop the still-owned
   # child/group where kernel identity permits, retain it unreaped, and fail.
   # The caller must keep its lease until attended cleanup proves drain.
   if self.active is owned and owned.authority and not owned.reaped:
    try:
     self._signal(owned,signal.SIGTERM)
     time.sleep(self.term_grace)
     self._signal(owned,signal.SIGKILL)
    except BaseException as stopping_error:
     self.emit('cleanup_stop_unproved',error=repr(stopping_error))
    self.emit('cleanup_unproved',pid=owned.leader,pending=owned.start is None)
   raise
  finally:self.cleaning=was_cleaning
 def capture(self,argv,env,cwd,path,*,pass_fds=(),tick=None,timeout=None,poll_interval=.01):
  if self.active is not None:raise LifecycleError('one active owned command only')
  if self.cancelled is not None:raise Cancelled(self.cancelled)
  baseline={(pid,row['start']) for pid,row in proc_table().items() if row['ppid']==os.getpid()}
  # Allocate the complete pending record BEFORE spawn. A returned Popen is
  # stored directly into its existing slot; no census/setup intervenes.
  owned=Owned(None,0,None,baseline)
  oldmask=signal.pthread_sigmask(signal.SIG_BLOCK,{signal.SIGINT,signal.SIGTERM,signal.SIGHUP})
  self.active=owned
  try:
   with open(path,'x') as out:
    owned.process=subprocess.Popen([sys.executable,'-B',str(Path(__file__).with_name('leader_exec.py')),'--parent',str(os.getpid()),'--',*map(str,argv)],env=env,cwd=cwd,stdout=out,stderr=subprocess.STDOUT,start_new_session=True,pass_fds=pass_fds)
    owned.leader=owned.process.pid
    self.emit('leader_pending',pid=owned.leader)
    row=proc_table()[owned.leader]
    if row['pgid']!=owned.leader or row['sid']!=owned.leader:raise LifecycleError('spawn session mismatch')
    owned.start=row['start']
    self.emit('leader_owned',pid=owned.leader,start=owned.start,oldmask=[int(x) for x in oldmask])
    signal.pthread_sigmask(signal.SIG_SETMASK,oldmask)
    command_deadline=None if timeout is None else time.monotonic()+timeout
    while True:
     if tick is not None:tick()
     if command_deadline is not None and time.monotonic()>=command_deadline:raise LifecycleError("owned command deadline exceeded")
     if self.cancelled is not None:raise Cancelled(self.cancelled)
     if self.event_error is not None:raise LifecycleError('event sink failed: '+repr(self.event_error))
     done=self._leader_status(owned);residual=self._discover(owned)
     if self.cancelled is not None:raise Cancelled(self.cancelled)
     if owned.escaped:raise LifecycleError('owned command escaped session/group')
     if done is not None:
      if residual:raise LifecycleError('leader exited with residual descendants')
      code=self._reap(owned)
      if self.cancelled is not None:raise Cancelled(self.cancelled)
      if self.event_error is not None:raise LifecycleError('event sink failed: '+repr(self.event_error))
      if code:raise LifecycleError(f'command exited {code}')
      return
     time.sleep(poll_interval)
  finally:
   try:self.finalize()
   finally:signal.pthread_sigmask(signal.SIG_SETMASK,oldmask)
