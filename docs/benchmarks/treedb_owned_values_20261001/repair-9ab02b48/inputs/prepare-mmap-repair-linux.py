#!/usr/bin/env python3
"""Bounded untimed preparation. Root's grant is external; this script grants nothing."""
from pathlib import Path
import datetime,hashlib,json,os,re,shutil,subprocess,sys
p=Path(sys.argv[1]);logs=p/'logs'; processes=[]
commits={'base':'a68a84c7195e0c5d8c339343a385b40acf818d03','previous':'ab2e9124fcca72710b896cf78577fa6bd37a11b0','candidate':'9ab02b48ce2397410940a7e42cc22bbf95a96c5a'}
env=dict(os.environ,GOROOT='/home/mikers/.gvm/gos/go1.26.3',GOWORK='off',GOMAXPROCS='4',GOMEMLIMIT='1GiB',TREEDB_HOT_PATH_STATS='1',TMPDIR=str(p/'tmp'))
for k in ['TREEDB_OWNED_VLOG_FIXTURE','TREEDB_OWNED_VLOG_FIXTURE_EXPORT','TREEDB_OWNED_VLOG_VALIDATE_ONLY','TREEDB_OWNED_VLOG_EXPECT_MAPPED']:env.pop(k,None)
go=env['GOROOT']+'/bin/go'
def digest(q):return hashlib.sha256(q.read_bytes()).hexdigest()
def inventory(d):return {str(q.relative_to(d)):{'bytes':q.stat().st_size,'sha256':digest(q)} for q in sorted(d.rglob('*')) if q.is_file()}
def write(name,obj):(logs/name).write_text(json.dumps(obj,indent=2)+'\n')
def run(name,argv,cwd,extra=None):
 e=dict(env,**(extra or {})); so=logs/(name+'.stdout.txt');se=logs/(name+'.stderr.txt');rss=logs/(name+'.rss.txt')
 started=datetime.datetime.now(datetime.timezone.utc).isoformat()
 with so.open('wb') as out,se.open('wb') as err:
  r=subprocess.run(['/usr/bin/time','-v','-o',str(rss),*argv],cwd=cwd,env=e,stdout=out,stderr=err)
 text=rss.read_text(); status=re.search(r'Exit status: (\d+)',text);peak=re.search(r'Maximum resident set size \(kbytes\): (\d+)',text)
 processes.append({'name':name,'argv':argv,'wrapper_argv':['/usr/bin/time','-v','-o',str(rss),*argv],'cwd':str(cwd),'env':{k:e.get(k) for k in ['GOROOT','GOWORK','GOMAXPROCS','GOMEMLIMIT','TREEDB_HOT_PATH_STATS','TMPDIR','TREEDB_OWNED_VLOG_FIXTURE','TREEDB_OWNED_VLOG_VALIDATE_ONLY','TREEDB_OWNED_VLOG_EXPECT_MAPPED']},'started_UTC':started,'completed_UTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'process_exit_status':r.returncode,'time_exit_status':int(status[1]) if status else None,'positive_peak_RSS_KiB':int(peak[1]) if peak else None,'stdout':str(so.relative_to(p)),'stderr':str(se.relative_to(p)),'rss':str(rss.relative_to(p))})
 write('preparation-process-manifest.json',processes)
 assert r.returncode==0 and status and int(status[1])==0 and peak and int(peak[1])>0,(name,r.returncode,se.read_text())
 print(name+' PASS',flush=True)
 return so.read_text()
fixture=inventory(p/'fixture'); original=json.loads((logs/'fixture-manifest.json').read_text());assert fixture==original['files']
write('fixture-preparation-before.json',fixture)
for rev in commits:
 src=p/'src'/rev
 before=inventory(src);write('source-'+rev+'-original.json',before)
 overlay=src/'TreeDB/internal/valuelog/mmap_qualification_overlay_test.go'; assert not overlay.exists();shutil.copyfile(p/'mmap_qualification_overlay_test.go',overlay)
 allowed={'TreeDB/internal/valuelog/mmap_qualification_overlay_test.go'}
 if rev=='base':
  for donor,dest in [('baseline-benchmark-overlay.go.txt','TreeDB/internal/valuelog/read_append_owned_bench_overlay_test.go'),('value_log_owned_bench_test.go','TreeDB/value_log_owned_bench_test.go')]:
   target=src/dest;assert not target.exists();shutil.copyfile(p/donor,target);allowed.add(dest)
 after=inventory(src);write('source-'+rev+'-prepared.json',after)
 delta={k:{'before':before.get(k),'after':after.get(k)} for k in sorted(set(before)|set(after)) if before.get(k)!=after.get(k)}
 assert set(delta)==allowed and all(v['before'] is None for v in delta.values()),(rev,delta)
 write('source-'+rev+'-overlay-delta.json',delta)
 write('source-'+rev+'-prepared-fingerprint.json',{'commit':commits[rev],'file_count':len(after),'inventory_sha256':digest(logs/('source-'+rev+'-prepared.json'))})
version=run('toolchain',[go,'version'],p).strip();assert version=='go version go1.26.3 linux/amd64',version
run('go-env',[go,'env','-json'],p/'src/candidate')
for rev in commits:run('compile-valuelog-'+rev,[go,'test','-c','-o',str(p/'bin'/('valuelog-'+rev+'.test')),'./TreeDB/internal/valuelog'],p/'src'/rev)
for rev in ['base','candidate']:run('compile-treedb-'+rev,[go,'test','-c','-o',str(p/'bin'/('treedb-'+rev+'.test')),'./TreeDB'],p/'src'/rev)
for rev in commits:
 binary=str(p/'bin'/('valuelog-'+rev+'.test'))
 run('validate-internal-'+rev,[binary,'-test.run','^TestOwnedMmapQualificationInternalRoutes$','-test.count','1','-test.v'],p/'src'/rev,{'TREEDB_OWNED_VLOG_EXPECT_MAPPED':str(int(rev!='base'))})
 regex='^BenchmarkOwnedMmapBudgetDenied512$' if rev=='base' else '^BenchmarkOwnedMmap(BudgetDenied512|ConcurrentFirstAdmission)$'
 out=run('validate-overlay-'+rev,[binary,'-test.run','^$','-test.bench',regex,'-test.benchtime','1x','-test.count','1','-test.v'],p/'src'/rev,{'TREEDB_OWNED_VLOG_VALIDATE_ONLY':'1'})
 assert out.count('--- SKIP: BenchmarkOwnedMmap')==(2 if rev=='base' else 3),out
for rev in ['base','candidate']:
 out=run('validate-public-'+rev,[str(p/'bin'/('treedb-'+rev+'.test')),'-test.run','^$','-test.bench','^BenchmarkDBOwnedValueLogRoute$','-test.benchtime','1x','-test.count','1','-test.v'],p/'src'/rev,{'TREEDB_OWNED_VLOG_VALIDATE_ONLY':'1','TREEDB_OWNED_VLOG_FIXTURE':str(p/'fixture')})
 vector=f"pointer=1 inline=0 transport=map[cache_hits/op:1 crc_checks/op:1 fallbacks/op:{int(rev=='base')} mmap_hits/op:{int(rev=='candidate')}] retained=9389893 budget=67108864"
 assert vector in out and '--- SKIP: BenchmarkDBOwnedValueLogRoute' in out,out
for q in sorted((p/'bin').iterdir()):
 run('build-id-'+q.name,[go,'tool','buildid',str(q)],p)
 run('version-m-'+q.name,[go,'version','-m',str(q)],p)
for rev in commits:assert inventory(p/'src'/rev)==json.loads((logs/f'source-{rev}-prepared.json').read_text()),rev
assert inventory(p/'fixture')==fixture
write('fixture-preparation-after.json',fixture)
inputs=[q for d in [p/'archives',p/'bin',logs] for q in sorted(d.rglob('*')) if q.is_file()]
inputs += [p/n for n in ['qualify-mmap-repair.sh','analyze-mmap-repair.py','mmap_qualification_overlay_test.go','baseline-benchmark-overlay.go.txt','value_log_owned_bench_test.go','prepare-mmap-repair-linux.py','mmap-repair-qualification-plan.md','mmap-repair-freeze-proposal.json']]
manifest={'status':'PREPARED_UNTIMED_NO_OFFICIAL_CAPTURE','commits':commits,'toolchain':version,'freeze_UTC':datetime.datetime.now(datetime.timezone.utc).isoformat(),'capture_env':{k:env[k] for k in ['GOROOT','GOWORK','GOMAXPROCS','GOMEMLIMIT','TREEDB_HOT_PATH_STATS','TMPDIR']},'input_sha256':{str(q.relative_to(p)):digest(q) for q in inputs},'process_manifest_sha256':digest(logs/'preparation-process-manifest.json'),'source_deltas':'Only identical declared test/benchmark overlays added; runtime files byte identical to exact archives','immutable_fixture_origin':'final-f9d8b01d/fixture; a217 canonical fully verified/closed32768x256B source; actual17-file inventory unchanged','official_capture_started':False}
write('repair-source-freeze.json',manifest)
print('FINAL_UNTIMED_SOURCE_FREEZE_SHA256='+digest(logs/'repair-source-freeze.json'),flush=True)
