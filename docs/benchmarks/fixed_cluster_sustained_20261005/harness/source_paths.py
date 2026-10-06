"""Resolve only the enumerated source copies; evidence paths are unchanged."""
if not __debug__: raise RuntimeError('ordinary Python required; assertions must run')
import sys
sys.dont_write_bytecode=True
from pathlib import Path
import hashlib,importlib.util
WORKLOAD_SHA='55bfcf3156faa277ba5db32c68627d88442141bd4e2c6b3785a9525d412ab6f5'
def load_workload():
 path=Path(__file__).resolve().parent/'workload_profile.py'
 if not path.is_file() or path.is_symlink() or path.stat().st_size>16384:raise ValueError('bounded immutable workload source')
 raw=path.read_bytes()
 if hashlib.sha256(raw).hexdigest()!=WORKLOAD_SHA:raise ValueError('authenticated workload source')
 spec=importlib.util.spec_from_file_location('authenticated_workload_'+WORKLOAD_SHA,path)
 module=importlib.util.module_from_spec(spec)
 sys.modules[spec.name]=module
 exec(compile(raw,str(path),'exec'),module.__dict__)
 return module
workload_source=load_workload()
W=workload_source.W
SOURCE_PATHS={'/tmp/gomap-4994-mixed-window-artifact-verify-root-v7.py': 'sources/gomap-4994-mixed-window-artifact-verify-root-v7.py', '/tmp/gomap-4975-trial13c1-paced-window-artifact-verify-root-v1.py': 'sources/gomap-4975-trial13c1-paced-window-artifact-verify-root-v1.py', '/tmp/gomap-4975-trial13c1-paired-artifact-verify-root-v1.py': 'sources/gomap-4975-trial13c1-paired-artifact-verify-root-v1.py', '/tmp/gomap-4994-mixed-window-resource-accounting-root-v5.py': 'sources/gomap-4994-mixed-window-resource-accounting-root-v5.py', '/tmp/gomap-4956-5fa-fixed-cluster.py': 'sources/gomap-4956-5fa-fixed-cluster.py', '/tmp/gomap-4994-trial14mixedc1-post-input-manifest-prepare-root-v1.py': 'sources/gomap-4994-trial14mixedc1-post-input-manifest-prepare-root-v1.py', '/tmp/gomap-4994-trial14mixedc1-growth-admission-root-v2.py': 'sources/gomap-4994-trial14mixedc1-growth-admission-root-v2.py', '/tmp/gomap-4994-trial14mixedc1-growth-query-root-v2.py': 'sources/gomap-4994-trial14mixedc1-growth-query-root-v2.py', '/tmp/gomap-4994-trial14mixedc1-growth-lifecycle-root-v2.py': 'sources/gomap-4994-trial14mixedc1-growth-lifecycle-root-v2.py', '/tmp/gomap-4994-trial14mixedc1-growth-exact-validate-root-v2.py': 'sources/gomap-4994-trial14mixedc1-growth-exact-validate-root-v2.py', '/tmp/gomap-4994-trial14mixedc1-growth-artifact-verify-root-v2.py': 'sources/gomap-4994-trial14mixedc1-growth-artifact-verify-root-v2.py', '/tmp/gomap-trial17-native-runner-source-context-root-v1/gomap-1242-bounded-runner-prepare.py': 'sources/native-runner/gomap-1242-bounded-runner-prepare.py', '/tmp/gomap-trial17-native-runner-source-context-root-v1/run.sh': 'sources/native-runner/run.sh', '/tmp/gomap-trial17-native-runner-source-context-root-v1/inner.sh': 'sources/native-runner/inner.sh', '/tmp/gomap-trial17-native-runner-source-context-root-v1': 'sources/native-runner', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/shared_read.py': 'shared_read.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/shared_audit.py': 'shared_audit.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-rf4trial24-credential-preactivation-guard-root-v1.py': 'gomap-rf4trial24-credential-preactivation-guard-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py': 'gomap-4997-4998-trial24mixedchangingc1-collector-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-bootstrap-plan-prepare-root-v1.py': 'gomap-4997-4998-trial24-bootstrap-plan-prepare-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-bootstrap-preflight-prepare-root-v1.py': 'gomap-4997-4998-trial24-bootstrap-preflight-prepare-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-bootstrap-lifecycle-prepare-root-v1.py': 'gomap-4997-4998-trial24-bootstrap-lifecycle-prepare-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-pre-input-freeze-root-v1.py': 'gomap-4997-4998-trial24-pre-input-freeze-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-growth-prepare-root-v1.py': 'gomap-4997-4998-trial24-growth-prepare-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-4997-4998-trial24-post-input-manifest-prepare-root-v1.py': 'gomap-4997-4998-trial24-post-input-manifest-prepare-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-trial24-prefix-oracle-prepare-root-v1.py': 'gomap-trial24-prefix-oracle-prepare-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-trial24-native-oracle-runner-root-v1.py': 'gomap-trial24-native-oracle-runner-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-trial24-permission-stage-prepare-root-v1.py': 'gomap-trial24-permission-stage-prepare-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-trial24-read-audit-accounting-root-v1.py': 'gomap-trial24-read-audit-accounting-root-v1.py', '/tmp/gomap-5021-sustained-consumers-provisional-root-v1/gomap-trial24-resource-accounting-root-v1.py': 'gomap-trial24-resource-accounting-root-v1.py'}
def source_path(path):
 value=str(path)
 if value in SOURCE_PATHS:return str(Path(__file__).resolve().parent/SOURCE_PATHS[value])
 return value

def isolate_paths(outputs, protected):
 """Reject resolved overlap before owned writes; preserve exclusive leaf checks."""
 from pathlib import Path
 def resolved(value):
  p=Path(value)
  if not p.is_absolute() or '..' in p.parts or p.is_symlink():
   raise ValueError('absolute non-symlink path without dot-dot required')
  return p.resolve(strict=False)
 targets=[resolved(p) for p in outputs]
 roots=[resolved(p) for p in protected]
 for i,target in enumerate(targets):
  for root in roots+targets[:i]:
   if target==root or target.is_relative_to(root) or root.is_relative_to(target):
    raise ValueError('output overlaps protected path')

def reviewer_identity(review):
 identity=review.get('reviewer')
 import re
 if not (isinstance(identity,str) and 0<len(identity)<=160 and re.fullmatch(r'(?:[A-Za-z0-9][A-Za-z0-9_-]*(?:\[bot\])?|/root/[a-z0-9_]+(?:/[a-z0-9_]+)*)',identity)):raise ValueError('stable raw reviewer identity')
 # Verified REST/GraphQL aliases for GitHub user 199175422 only.
 value=identity.casefold()
 return 'chatgpt-codex-connector[bot]' if value=='chatgpt-codex-connector' else value
