"""Reuse explicit restore on a fresh owned copy; never reopen the failed original."""
import hashlib,json,os,pathlib,subprocess,sys,time
r=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005');variant=sys.argv[1];assert variant in ('defaults','pack')
p=r/('churn-3m-1-'+variant)/('1-treedb-churn-'+variant);m=json.loads((p/'run.json').read_text());d=json.loads((p/'stdout.json').read_text());original=pathlib.Path(d[0]['data_dir']);copy=r/'working-dbs'/('churn-3m-'+variant+'-manifest-l-copy');packet=r/('churn-3m-'+variant+'-manifest-l-input');assert original.parent==r/'working-dbs' and not copy.exists() and not packet.exists();packet.mkdir()
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def census():return [(str(f.relative_to(original)),s.st_dev,s.st_ino,s.st_size,s.st_blocks,s.st_mtime_ns) for f in sorted(original.rglob('*')) if f.is_file() for s in [f.stat()]]
before=census();assert not any(f.is_symlink() for f in original.rglob('*'));env=os.environ.copy();env.update(m['env'])
rm=json.loads((r/'provisional-manifest-restore-r2.json').read_text());rb=r/rm['receipts']['build']['path'];assert sha(rb)==rm['receipts']['build']['sha256'];rr=json.loads(rb.read_text());helper=r/'bin/rebind-owned-copy-restore-r2';assert rr['head']=='eaa0019ed65b2dc90d9406ca7371502cf17c53eb' and sha(helper)==rr['binaries'][helper.name]['binary_sha256']
commands=[]
for label,argv in [('copy',['cp','-a','--reflink=auto',str(original),str(copy)]),('explicit-restore',[str(helper),str(copy)])]:
    started=time.time()
    with (packet/(label+'-stdout.txt')).open('w') as stdout,(packet/(label+'-stderr.txt')).open('w') as stderr:run=subprocess.run(argv,env=env,stdout=stdout,stderr=stderr,timeout=600)
    commands.append(dict(label=label,command=argv,started=started,finished=time.time(),rc=run.returncode));(packet/'prepare.json').write_text(json.dumps(commands,indent=2)+'\n');assert run.returncode==0
after=census();assert before==after,'failed original changed';(packet/'original-preserved.json').write_text(json.dumps(dict(original=str(original),before=before,after=after,unchanged=True),indent=2)+'\n')
d[0]['data_dir']=str(copy);(packet/'stdout.json').write_text(json.dumps(d)+'\n');m['command'][0]=str(r/'bin/unified-bench-manifest-l');m['source']='manifest-l';m['derived_input_only']=True;m['candidate_manifest']=dict(path='provisional-manifest-manifest-l.json',sha256=sha(r/'provisional-manifest-manifest-l.json'));m['derived_copy']=dict(source_packet=str(p),original=str(original),copy=str(copy),restore_helper_sha256=sha(helper),restore_source=rr['head'],runtime_head='f5a82a6f7ac81e6a2959b730e030e6468e75444b',method_sha256=sha(pathlib.Path(__file__)));(packet/'run.json').write_text(json.dumps(m,indent=2)+'\n')
subprocess.run(['python3','-u',str(r/'maintenance-churn-manifest-l-full.py'),str(packet),'maintenance-churn-3m-'+variant+'-manifest-l-full8192','8192'],env=env,check=True)
