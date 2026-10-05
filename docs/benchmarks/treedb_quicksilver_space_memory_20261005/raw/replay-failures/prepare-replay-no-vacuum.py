"""Clone the retained failure, use existing explicit restore, seal a new diagnostic substrate."""
import hashlib,importlib.util,json,os,pathlib,subprocess,time
root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
v2=root/'5004-replay-diagnostic-v2'
spec=importlib.util.spec_from_file_location('inplace',v2/'inplace.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
original=root/'working-dbs/replay-3m-inplace';copy=root/'working-dbs/replay-3m-postfailure-vacuumoff';out=root/'replay-3m-no-vacuum';out.mkdir()
assert not copy.exists()
old=json.loads((root/'replay-3m/1-baseline/guard/post/census.json').read_text());assert m.census(original)==old
helper=root/'bin/rebind-owned-copy-restore-r2';assert m.digest(helper)=='ae237f6af2abc82fa9857a33e93c9c8ee64a2d7a5f350a15a1525574d5dc77c3'
commands=[]
def run(argv):
 started=time.time();p=subprocess.run(argv,capture_output=True,text=True);commands.append(dict(command=argv,started=started,finished=time.time(),rc=p.returncode,stdout=p.stdout,stderr=p.stderr));(out/'prepare.json').write_text(json.dumps(dict(diagnostic_only=True,commands=commands,method_sha256=m.digest(pathlib.Path(__file__))),indent=2)+'\n');assert p.returncode==0,p.stderr
run(['cp','-a','--reflink=auto',str(original),str(copy)])
run([str(helper),str(copy)])
assert m.census(original)==old,'retained failed-original changed'
run(['python3',str(v2/'inplace.py'),'seal','--owned-root',str(root),'--manifest',str(out/'seal.json'),'--db',str(copy),'--backup',str(root/'replay-3m-vacuumoff-backup')])
assert m.census(original)==old,'retained failed-original changed'
(out/'original-preserved.json').write_text(json.dumps(dict(original=str(original),failed_post_census_sha256=m.digest(root/'replay-3m/1-baseline/guard/post/census.json'),current_matches_failed_census=True,restore_helper_sha256=m.digest(helper),restore_source='eaa0019ed65b2dc90d9406ca7371502cf17c53eb',restore_landed_source='723e5625f4b46945886d0696c0122db2e2c928cf'),indent=2)+'\n')
print('PREPARED_NEW_POSTFAILURE_CLONE_SEAL_ORIGINAL_UNCHANGED',flush=True)
