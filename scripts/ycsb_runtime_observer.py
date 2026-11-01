"""Derived from runtime_observer_bound.py SHA256 807633ff3f79d57d8e42a17ded97ea6f0016235c3e1b9f56586454c42ff66238.
Owned benchmark-child observation; inert import, no signals/ptrace or child execution.
Sampling may overlap measured work. It cannot prove an unperturbed timed interval.
"""
import errno, hashlib, os, struct, time
from datetime import datetime, timezone
from pathlib import Path
STARTUP_EMPTY_MAX_READS=8
STARTUP_READY_SECONDS=2.0
ENVIRONMENT_MAX_BYTES=1024*1024
STARTUP_POLL_SECONDS=0.001
EXIT_RECHECK_SECONDS=0.05

def sample_interval(observer, steady_interval):
 """Do not sleep through startup when no complete runtime witness exists."""
 return steady_interval if observer.samples else min(STARTUP_POLL_SECONDS, steady_interval)

class RuntimeMappingDisappeared(RuntimeError):
 """A previously witnessed mapping is absent; only pinned exit may excuse it."""

def sample_owned_or_exited(observer, child, exited, recheck_seconds=EXIT_RECHECK_SECONDS):
 """Permit an exit race only after a complete witness, pinned-child exit proof."""
 try:
  return observer.sample(child)
 except (OSError, RuntimeMappingDisappeared) as error:
  proc_prefix='/proc/'+str(child.pid)+'/'
  mapping_disappeared=isinstance(error,RuntimeMappingDisappeared)
  last_attempt=getattr(observer,'last_attempt',None) or {}
  mapping_identity_matches=bool(mapping_disappeared and observer.identity is not None
      and last_attempt.get('pid')==child.pid
      and (last_attempt.get('parent_pid'),last_attempt.get('start_ticks'))==observer.identity[1]
      and last_attempt.get('exe_identity')==list(observer.elf_identity))
  disappeared=((mapping_disappeared and mapping_identity_matches) or
               (isinstance(error,OSError) and (error.errno==errno.ESRCH or
                (error.errno==errno.ENOENT and isinstance(error.filename,str) and error.filename.startswith(proc_prefix)))))
  sample_count=len(observer.samples)
  identity_matches=observer.identity is not None and observer.identity[0]==child.pid
  eligible=bool(disappeared and sample_count and identity_matches)
  check={'errno':getattr(error,'errno',None),'filename':getattr(error,'filename',None),'error_type':type(error).__name__,
         'mapping_disappeared':mapping_disappeared,'mapping_identity_matches':mapping_identity_matches,
         'expected_pid':child.pid,'disappeared_predicate':disappeared,'sample_count':sample_count,
         'identity_matches':identity_matches,'eligible':eligible,'pidfd_ready_initial':None,
         'pidfd_ready_after_wait':None,'pidfd_check_error':None,'wait_bound_seconds':recheck_seconds,'wait_elapsed_seconds':0.0}
  observer.exit_race_check=check
  if not eligible:
   raise
  require(0 <= recheck_seconds <= EXIT_RECHECK_SECONDS,'invalid exit recheck bound')
  try:
   check['pidfd_ready_initial']=bool(exited(0))
   if not check['pidfd_ready_initial']:
    started=time.monotonic()
    try:check['pidfd_ready_after_wait']=bool(exited(recheck_seconds))
    finally:check['wait_elapsed_seconds']=time.monotonic()-started
  except OSError as check_error:
   check['pidfd_check_error']={'errno':check_error.errno,'type':type(check_error).__name__,'message':str(check_error)[:256]}
   raise
  if not (check['pidfd_ready_initial'] or check['pidfd_ready_after_wait']):raise
  # Keep the failed partial sample as diagnostic; the earlier complete sample
  # remains the only proof and no interrupted read contributes to it.
  observer.terminal_exit_race=observer.observation_error
  observer.observation_error=None
  return None

def require(ok,why):
 if not ok:raise RuntimeError(why)
def mapping_pending(mapped, started, has_samples):
 if mapped:return False
 if has_samples:raise RuntimeMappingDisappeared('runtime mapping disappeared after observation')
 require(time.monotonic()-started<=STARTUP_READY_SECONDS,'runtime mapping startup deadline exceeded')
 return True
def gomax_symbol(data, expected_sha, expected_address):
 require(isinstance(expected_sha,str) and len(expected_sha)==64 and all(c in '0123456789abcdef' for c in expected_sha),'expected ELF SHA schema')
 require(type(expected_address) is int and expected_address>0,'expected symbol address schema')
 require(hashlib.sha256(data).hexdigest()==expected_sha,'exact benchmark ELF hash')
 require(data[:7]==b'\x7fELF\x02\x01\x01','ELF64 little endian')
 h=struct.unpack_from('<16sHHIQQQIHHHHHH',data)
 require(h[1]==2 and h[2]==62 and h[11]==64,'ET_EXEC x86_64 sections')
 sections=[struct.unpack_from('<IIQQQQIIQQ',data,h[6]+i*h[11]) for i in range(h[12])]
 symbols=[]
 for section in sections:
  if section[1]!=2:continue
  strings=sections[section[6]]; names=data[strings[4]:strings[4]+strings[5]]
  require(section[9]==24,'symbol entry size')
  for offset in range(section[4],section[4]+section[5],24):
   name,info,other,index,value,size=struct.unpack_from('<IBBHQQ',data,offset)
   if names[name:names.find(b'\0',name)]==b'runtime.gomaxprocs':symbols.append((info,index,value,size))
 require(len(symbols)==1,'unique runtime.gomaxprocs')
 info,index,address,size=symbols[0]
 require(info&15==1 and address==expected_address and size==4 and index<len(sections),'runtime.gomaxprocs object identity')
 section=sections[index]
 require(section[2]&3==3 and section[3]<=address and address+4<=section[3]+section[5],'writable allocated runtime object')
 return address

def start_identity(pid):
 text=Path('/proc',str(pid),'stat').read_text();f=text[text.rindex(')')+2:].split()
 return (int(f[1]),int(f[19]))

def recheck_identity(pid,proc,identity,expected_elf):
 require(start_identity(pid)==identity,'child identity changed during observation')
 st=(proc/'exe').stat()
 require((st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)==expected_elf,'child ELF identity changed during observation')

def validate_cpulist(value):
 require(bool(value),'empty effective cpuset')
 previous=-1
 for item in value.split(','):
  fields=item.split('-')
  require(len(fields) in (1,2) and all(field.isascii() and field.isdigit() for field in fields),'cpuset schema')
  low=int(fields[0]);high=int(fields[-1])
  require(previous<low<=high<1024*1024,'cpuset order/range')
  previous=high
 return value

def ancestor_policy(root,current):
 """Read actual controller interfaces twice; absence needs kernel delegation evidence.

 cpuset files exist in children only when their parent enables cpuset in
 cgroup.subtree_control. An undelegated child uses the nearest ancestor's
 effective set. CPU quota interfaces likewise require CPU delegation from the
 parent; an absent local file does not mean unlimited quota. Preserve every
 actual exported ancestor constraint without inventing a local numeric value.
 Affinity is recorded separately and is never a substitute.
 """
 require(current==root or root in current.parents,'cgroup outside full root')
 def collect():
  rows=[];node=current
  while True:
   controllers=(node/'cgroup.controllers').read_text().strip().split()
   delegated=(node/'cgroup.subtree_control').read_text().strip().split()
   require(len(controllers)==len(set(controllers)) and len(delegated)==len(set(delegated)),'duplicate controller schema')
   require(set(delegated)<=set(controllers),'delegation outside available controllers')
   try:quota=(node/'cpu.max').read_text().strip() if node!=root else None
   except FileNotFoundError:quota=None
   if quota is not None:
    fields=quota.split();require(len(fields)==2,'cpu.max schema')
    q,period=fields;require(period.isascii() and period.isdigit() and int(period)>0 and (q=='max' or (q.isascii() and q.isdigit() and int(q)>0)),'cpu.max schema')
   try:cpuset=validate_cpulist((node/'cpuset.cpus.effective').read_text().strip())
   except FileNotFoundError:cpuset=None
   rows.append({'path':str(node),'cpu.max':quota,'cpuset.cpus.effective':cpuset,
                'cgroup.controllers':controllers,'cgroup.subtree_control':delegated})
   if node==root:break
   node=node.parent
  require(rows[-1]['cpuset.cpus.effective'] is not None,'no exported root effective cpuset')
  inherited=None;source=None
  for index in range(len(rows)-1,-1,-1):
   row=rows[index];present=row['cpuset.cpus.effective'] is not None
   if index<len(rows)-1:
    parent=rows[index+1];enabled='cpuset' in parent['cgroup.subtree_control']
    cpu_enabled='cpu' in parent['cgroup.subtree_control']
    require((row['cpu.max'] is not None)==cpu_enabled,'cpu.max interface/delegation mismatch: '+row['path'])
    require(('cpu' in row['cgroup.controllers'])==cpu_enabled,'CPU availability/delegation mismatch: '+row['path'])
    require(present==enabled,'cpuset interface/delegation mismatch')
    require(('cpuset' in row['cgroup.controllers'])==enabled,'cpuset availability/delegation mismatch')
   constraints=[{'path':ancestor['path'],'cpu.max':ancestor['cpu.max']}
                for ancestor in rows[index:] if ancestor['cpu.max'] is not None]
   row.update(cpu_interface_state=('not exported at cgroup root' if index==len(rows)-1
              else 'exported' if row['cpu.max'] is not None else 'not delegated by authenticated parent'),
              cpu_exported_ancestor_constraints=constraints)
   if present:inherited=row['cpuset.cpus.effective'];source=row['path']
   require(inherited is not None,'unresolved effective cpuset')
   row.update(cpuset_effective_cpus=inherited,cpuset_effective_source=source,
              cpuset_interface_state='exported' if present else 'not delegated by authenticated parent')
  return rows
 first=collect();second=collect()
 require(first==second,'cgroup controller policy changed during snapshot')
 return first

def quota_snapshot(pid):
 proc=Path('/proc',str(pid))
 for namespace in ('cgroup','mnt'):
  require(os.readlink(proc/'ns'/namespace)==os.readlink(Path('/proc/self/ns')/namespace),'namespace differs: '+namespace)
 lines=(proc/'cgroup').read_text().splitlines()
 unified=[line for line in lines if line.startswith('0::/')]
 require(len(unified)==1,'unified cgroup v2 required')
 require(not any(set(line.split(':')[1].split(',')) & {'cpu','cpuacct','cpuset'} for line in lines if not line.startswith('0::')),'legacy CPU controller unsupported')
 relative=unified[0][3:];require('..' not in Path(relative).parts,'unsafe cgroup path')
 mounts=[line.split(' - ',1) for line in (proc/'mountinfo').read_text().splitlines() if ' - cgroup2 ' in line]
 require(len(mounts)==1,'unique cgroup2 mount')
 fields=mounts[0][0].split();require(fields[3]=='/' and fields[4]=='/sys/fs/cgroup','unambiguous cgroup root')
 root=Path('/sys/fs/cgroup');current=root/relative.lstrip('/')
 rows=ancestor_policy(root,current)
 require((proc/'cgroup').read_text().splitlines()==lines,'cgroup membership changed during snapshot')
 return {'membership':relative,'ancestors':rows,'root_cpu_max':'not exported at cgroup root; no parent outside authenticated same namespace'}

class RuntimeObserver:
 def __init__(self,binary,expected_sha,expected_address,expected_gomaxprocs=None):
  require(expected_gomaxprocs in (None,'4'),'unsupported runtime arm'); self.expected_gomaxprocs=expected_gomaxprocs
  self.binary=Path(binary).resolve()
  def identity(s):return (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
  with self.binary.open('rb') as binary_file:
   before=identity(os.fstat(binary_file.fileno()));data=binary_file.read();after=identity(os.fstat(binary_file.fileno()))
  require(before==after==identity(self.binary.stat()),'binary identity changed during hash/symbol binding')
  self.address=gomax_symbol(data,expected_sha,expected_address);self.expected_sha=expected_sha;s=self.binary.stat()
  require(identity(s)==before,'binary identity changed after binding')
  self.elf_identity=(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns);self.samples=[];self.pending=0;self.identity=None
  self.environment_validated=False;self.startup_started=None;self.startup_empty_reads=0
  self.environment_first=None;self.environment_last=None;self.last_attempt=None;self.observation_error=None
  self.terminal_exit_race=None
  self.exit_race_check=None
 def diagnostics(self):
  return {'environment_validated':self.environment_validated,'startup_empty_reads':self.startup_empty_reads,
          'startup_empty_max_reads':STARTUP_EMPTY_MAX_READS,'startup_ready_seconds':STARTUP_READY_SECONDS,
          'first_environment':self.environment_first,'last_environment':self.environment_last,
          'last_attempt':self.last_attempt,'error':self.observation_error,'terminal_exit_race':self.terminal_exit_race,'exit_race_check':self.exit_race_check}
 def sample(self,child):
  self.last_attempt={'pid':child.pid,'GOMAXPROCS_expected':self.expected_gomaxprocs,'state':'started'}
  try:return self._sample(child)
  except BaseException as error:
   self.observation_error={'type':type(error).__name__,'message':str(error)[:256]}
   self.last_attempt['state']='failed'
   raise
 def _sample(self,child):
  pid=child.pid;proc=Path('/proc',str(pid))
  # A direct parent owns this still-live Popen child; never inspect an unrelated PID.
  if child.poll() is not None:self.last_attempt['state']='exited';return None
  identity=start_identity(pid);require(identity[0]==os.getpid(),'observer requires direct owned child')
  self.last_attempt.update(parent_pid=identity[0],start_ticks=identity[1])
  if self.identity is None:self.identity=(pid,identity)
  require(self.identity==(pid,identity),'child PID identity changed')
  s=(proc/'exe').stat();actual_elf=(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
  self.last_attempt['exe_identity']=list(actual_elf)
  require(actual_elf==self.elf_identity,'actual child ELF identity')
  now=time.monotonic()
  if self.startup_started is None:self.startup_started=now
  if not self.environment_validated:require(now-self.startup_started<=STARTUP_READY_SECONDS,'initial child environment readiness deadline exceeded')
  raw_environment=(proc/'environ').read_bytes()
  environ=raw_environment.split(b'\0')
  values=[entry.partition(b'=')[2] for entry in environ if entry.partition(b'=')[0]==b'GOMAXPROCS']
  environment={'byte_length':len(raw_environment),'GOMAXPROCS_expected':self.expected_gomaxprocs,
               'GOMAXPROCS_occurrences':len(values),'GOMAXPROCS_values':[
                {'byte_length':len(value),'bytes_repr':repr(value[:64]),'truncated':len(value)>64} for value in values[:8]],
               'values_truncated':len(values)>8,'pid':pid,'start_ticks':identity[1],'exe_identity':list(actual_elf)}
  self.environment_last=environment
  if self.environment_first is None:self.environment_first=environment
  require(len(raw_environment)<=ENVIRONMENT_MAX_BYTES,'actual child environment exceeds bounded read contract')
  require(start_identity(pid)==identity,'child identity changed during environment read')
  if not raw_environment:
   require(not self.environment_validated,'actual child environment became empty after validation')
   self.startup_empty_reads+=1
   require(self.startup_empty_reads<=STARTUP_EMPTY_MAX_READS,'initial child environment remained empty beyond bounded reads')
   recheck_identity(pid,proc,identity,self.elf_identity)
   self.last_attempt['state']='initial-environment-empty-pending'
   return None
  require(any(entry for entry in environ) and all(b'=' in entry and entry.partition(b'=')[0] for entry in environ if entry),'nonempty initial environment has invalid entries')
  validate_environment(environ,self.expected_gomaxprocs)
  self.environment_validated=True
  self.last_attempt['state']='environment-validated'
  maps=(proc/'maps').read_text().splitlines();mapped=[]
  for line in maps:
   fields=line.split();lo,hi=(int(v,16) for v in fields[0].split('-'))
   if lo<=self.address and self.address+4<=hi:mapped.append(fields)
  # The dynamic loader can expose the correct /proc/exe before it maps the
  # executable's data. Only an absent mapping during bounded startup is pending.
  if mapping_pending(mapped,self.startup_started,bool(self.samples)):
   recheck_identity(pid,proc,identity,self.elf_identity)
   self.last_attempt['state']='runtime-map-pending'
   return None
  require(len(mapped)==1 and 'r' in mapped[0][1] and 'w' in mapped[0][1],'runtime object live mapped writable memory')
  fd=os.open(proc/'mem',os.O_RDONLY|os.O_CLOEXEC)
  try:raw=os.pread(fd,4,self.address)
  finally:os.close(fd)
  require(len(raw)==4,'short runtime read');value=struct.unpack('<i',raw)[0]
  require(value>=0 and value<=1024*1024,'runtime value range')
  require(start_identity(pid)==identity,'child identity changed during read')
  if value==0:
   require(not self.samples and time.monotonic()-self.startup_started<=STARTUP_READY_SECONDS,'runtime remained zero or reverted to zero')
   recheck_identity(pid,proc,identity,self.elf_identity)
   self.pending+=1;self.last_attempt['state']='runtime-zero-pending';return None # Pre-runtime startup, never accepted as effective value.
  if self.expected_gomaxprocs is not None: require(value==4,'fixed arm actual runtime differs from 4')
  affinity=sorted(os.sched_getaffinity(pid));require(bool(affinity),'empty leader affinity')
  cgroup=quota_snapshot(pid)
  require(affinity==sorted(os.sched_getaffinity(pid)),'leader affinity changed during CPU policy snapshot')
  recheck_identity(pid,proc,identity,self.elf_identity)
  record={'utc':datetime.now(timezone.utc).isoformat(),'monotonic_seconds':time.monotonic(),'pid':pid,'start_ticks':identity[1],'runtime_gomaxprocs':value,'GOMAXPROCS_present':self.expected_gomaxprocs is not None,'GOMAXPROCS_expected':self.expected_gomaxprocs,'leader_affinity':affinity,'cgroup':cgroup,'address':hex(self.address),'read_bytes':4,'measurement_relation':'earliest/live sample; timed overlap possible; no stop/ptrace'}
  if self.samples:
   for key in ('runtime_gomaxprocs','leader_affinity','cgroup'):require(record[key]==self.samples[0][key],'runtime/cpu policy changed: '+key)
  self.samples.append(record);self.last_attempt['state']='validated-runtime-sample';return record
 def validate(self):
  require(self.observation_error is None,'observer failed; no result can validate')
  require(self.environment_validated,'no validated nonempty initial actual-child environment; fail closed')
  require(bool(self.samples),'no validated nonzero actual-child runtime observation; fail closed')
  return {'status':'actual-child-runtime-observed','sample_count':len(self.samples),'pre_runtime_reads':self.pending,'first':self.samples[0],'last':self.samples[-1],'terminal_exit_race':self.terminal_exit_race,'exit_race_check':self.exit_race_check,'whole_interval_verified':False,'unperturbed_timing_verified':False,'thread_affinity_scope':'leader only; do not infer all thread affinities','permission_feasibility':'verified only when sample succeeds on the actual owned child'}


def validate_environment(environ,expected):
 entries=[v for v in environ if v.partition(b'=')[0]==b'GOMAXPROCS']
 require(all(b'=' in entry for entry in entries),'malformed actual child GOMAXPROCS environment entry')
 values=[v.partition(b'=')[2] for v in entries]
 require(values==([] if expected is None else [expected.encode()]),'actual child GOMAXPROCS environment mismatch')
 for key in (b'TREEDB_NATIVE_DEBUG_LOG', b'TREEDB_NATIVE_SLOW_REQUEST'):
  require(not any(v.partition(b'=')[0]==key for v in environ),
          'actual child environment variable must be absent: '+key.decode())


def startup_evidence(observer, child):
 """Convert the actual live, direct-child v5 sample to the barrier contract.

 Caller must hold the child at the trusted startup gate and fail closed on
 missing/pending evidence; the release operation is deliberately separate.
 """
 require(observer.observation_error is None, 'prior observer failure is terminal')
 sample = observer.sample(child)
 require(sample is not None, 'startup runtime sample pending')
 result = observer.validate()
 require(result.get('status')=='actual-child-runtime-observed' and observer.observation_error is None,
         'observer did not validate or encountered an error')
 return {'pid': sample['pid'], 'start_ticks': sample['start_ticks'],
         'elf_sha256': observer.expected_sha, 'environment_nonempty': observer.environment_validated,
         'elf_identity': observer.elf_identity,
         'gomaxprocs_present': sample['GOMAXPROCS_present'],
         'runtime_gomaxprocs': sample['runtime_gomaxprocs'],
         'leader_affinity': sample['leader_affinity'], 'quota_policy': sample['cgroup'],
         'direct_child_verified': True}
