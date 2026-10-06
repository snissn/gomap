"""Exact accepted campaign profiles, bound once in an immutable source packet.
This describes source construction; it grants no review, admission or runtime authority.
"""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from dataclasses import dataclass
MATCHED_ORDER=(1,4,4,1,1,4)
PROFILE=('rf4trial24mixedchangingc1', 48, 300, 6, 1)
@dataclass(frozen=True)
class Workload:
 campaign: str
 originals: int
 duration: int
 spacing: int
 concurrency: int
 @property
 def prefixes(self):return self.originals+1
 @property
 def duration_ns(self):return self.duration*1000000000
 @property
 def spacing_ns(self):return self.spacing*1000000000
 @property
 def query_run(self):return self.campaign+'mixedc'+str(self.concurrency)+'v1'
 @property
 def short(self):return self.campaign.removeprefix('rf4')
 @property
 def stage_short(self):return 'trial24' if self.issue=='5021' else self.short
 @property
 def issue(self):return '5021' if self.campaign=='rf4trial24mixedchangingc1' else '5068'
 @property
 def local_input(self):return '/tmp/gomap-4997-4998-'+self.campaign+'-inputs-root-v1'
 @property
 def remote_input(self):return '/home/mikers/gomap-4997-4998-'+self.campaign+'-inputs-root-v1'
 @property
 def remote_root(self):return '/home/mikers/gomap-4250-twohost-'+self.campaign
 @property
 def output(self):return '/tmp/gomap-4997-4998-'+self.short+'-window-root-v1'
 @property
 def gate(self):return '/home/mikers/gomap-4997-4998-'+self.short+'-window-resource-root-v1'
 @property
 def name(self):return 'treedb-4250-'+self.campaign+'-mixed-window-c'+str(self.concurrency)+'-v1'
 def declaration(self):
  return dict(Originals=self.originals,Prefixes=self.prefixes,DurationSeconds=self.duration,MinSpacingSeconds=self.spacing,RPCTimeoutSeconds=3,OverallTimeoutSeconds=420,CollectorOuterSeconds=480,Warmup=64,MaxAttempts=65536,OutputBytes=134217728,Concurrency=self.concurrency,InitialRows=10005,FinalRows=10002)
def accepted(campaign,workload):
 if campaign=='rf4trial24mixedchangingc1':values=(campaign,48,300,6,1)
 else:
  allowed={'rf4matched5068w%02dc%d'%(i,c):(campaign,6,60,5,c) for i,c in enumerate(MATCHED_ORDER,1)}
  if campaign not in allowed:raise ValueError('accepted campaign namespace and arm/order')
  values=allowed[campaign]
 w=Workload(*values)
 if not isinstance(workload,dict) or workload!=w.declaration() or any(type(v) is not int for v in workload.values()):raise ValueError('exact accepted workload profile and integer caps')
 return w
W=accepted(PROFILE[0],Workload(*PROFILE).declaration())
