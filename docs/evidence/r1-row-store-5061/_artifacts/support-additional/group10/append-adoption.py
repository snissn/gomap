import hashlib,json,stat
from pathlib import Path
base=Path('/tmp/gomap-r1-execution-20261005');out=base/'5061-postmeta-and-root-capture-support-preparation'
p=out/'inventory.json';x=json.loads(p.read_bytes());original=base/'5071-0245-root-adoption.json';adoption=json.loads(original.read_bytes())
for name,expected in adoption['files'].items():
 q=base/name;assert hashlib.sha256(q.read_bytes()).hexdigest()==expected,name
for name in ['5071-0245-root-adoption.json','0245-post-body-original.json','0245-hosted-request-original.log']:
 q=base/name;s=q.lstat();assert stat.S_ISREG(s.st_mode) and not q.is_symlink() and not stat.S_IMODE(s.st_mode)&0o111
 b=q.read_bytes();assert b'\0' not in b;b.decode('utf-8');copy='originals/'+name;dest=out/copy
 with dest.open('xb') as f:f.write(b)
 dest.chmod(0o644);h=hashlib.sha256(b).hexdigest()
 x['files'].append(dict(group='root-capture-repair-root-adoption',sourceSHA=x['exact_candidate'],classification='ORIGINAL_ROOT_ADOPTION_AND_REQUEST_SNAPSHOT_NOT_HOSTED_PASS_OR_LANDING',rationale='Root adopted focused six-line fixture barrier and bound original proof; recorded current CI pending and one hosted request, not a hosted clean or actual gate result.',original=str(q),original_relative=name,copy=copy,original_sha256=h,copy_sha256=h,original_bytes=len(b),copy_bytes=len(b),original_mode=f'{stat.S_IMODE(s.st_mode):04o}',copy_mode='0644',original_nonsymlink=True,copy_nonsymlink=True,utf8_non_nul_exact=True,source_kind='PREEXISTING_RETAINED_TEXT'))
x['retained_text_original_count']=len(x['files']);x['original_bytes']=sum(v['original_bytes'] for v in x['files']);x['root_source_adoption_original']='originals/5071-0245-root-adoption.json'
p.write_text(json.dumps(x,indent=2,sort_keys=True)+'\n')
rp=out/'readiness.json';r=json.loads(rp.read_bytes());r.update(files=len(x['files']),bytes=x['original_bytes'],root_adoption=False,root_source_adoption_original_included=True);rp.write_text(json.dumps(r,indent=2,sort_keys=True)+'\n')
print('Copied three original adoption/request texts; source root-adopted, support-group unadopted; current CI/hosted remains pending snapshot.')
