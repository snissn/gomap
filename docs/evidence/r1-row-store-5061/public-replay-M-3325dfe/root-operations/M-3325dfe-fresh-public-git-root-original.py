import datetime, hashlib, json, os, subprocess
from pathlib import Path
O=Path('/home/mikers/gomap-r1-final-observers-20261006')
p=Path('/home/mikers/gomap-r1-public-git-M-3325dfe-20261006')
assert not p.exists()
p.mkdir()
env=dict(os.environ, GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL='/dev/null', GIT_TERMINAL_PROMPT='0')
commands=[['git','init','--quiet'],['git','remote','add','origin','https://github.com/snissn/gomap.git'],
 ['git','-c','credential.helper=','fetch','--quiet','--no-tags','--depth=1','origin','24ac6866992dd89ab222b8bf03d60e6c0c49a2d5'],
 ['git','sparse-checkout','init','--cone'],['git','sparse-checkout','set','docs/evidence/r1-row-store-5061/_artifacts'],
 ['git','checkout','--quiet','--detach','24ac6866992dd89ab222b8bf03d60e6c0c49a2d5']]
rows=[]
for i,argv in enumerate(commands):
    row={'argv':argv,'cwd':str(p),'start_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    r=subprocess.run(argv,cwd=p,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=600)
    row.update(exit=r.returncode,end_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),log_sha256=hashlib.sha256(r.stdout).hexdigest())
    with (O/f'M-3325dfe-public-git-command-{i}-original.log').open('xb') as f:f.write(r.stdout)
    rows.append(row)
    if r.returncode:break
with (O/'M-3325dfe-public-git-acquisition-original.json').open('x') as f:
    json.dump({'commands':rows,'private_source_used':False},f,indent=2);f.write('\n')
assert all(r['exit']==0 for r in rows) and len(rows)==len(commands),rows
print(json.dumps({'public_git_checkout':str(p),'commands':len(rows),'all_exit_zero':True}))
