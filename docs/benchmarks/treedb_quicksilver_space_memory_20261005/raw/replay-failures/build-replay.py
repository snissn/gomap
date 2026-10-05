"""Build diagnostic suite overlay against the two unchanged frozen sources."""
import hashlib,json,os,pathlib,subprocess,time
root=pathlib.Path('/mnt/fast4tb/quicksilver-space-memory-20261005')
packet=root/'5004-replay-diagnostic'
public=json.load(open(root/'qualified-manifest-offsets.json'))
env={k:v for k,v in os.environ.items() if not k.startswith('TREEDB_')}
env.update(public['build_env'],GOMAXPROCS='12',TMPDIR=str(root/'tmp'))
def sha(p):return hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
rows=[]
for label in ('baseline','candidate'):
 source=root/('source-'+label)
 subprocess.run(['python3',str(packet/'generate.py'),'--build-root',str(source),'--label',label],check=True)
 overlay=packet/(label+'-overlay.json');binary=root/'bin'/('unified-bench-replay-'+label)
 argv=['go','build','-p','1','-buildvcs=false','-tags','lmdb rocksdb','-overlay',str(overlay),'-o',str(binary),'./cmd/unified_bench']
 started=time.time();p=subprocess.run(argv,cwd=source,env=env,capture_output=True,text=True)
 log=packet/(label+'-build.txt');log.write_text(p.stdout+p.stderr)
 rec=dict(source=public['sources'][label],command=argv,environment=env,started=started,finished=time.time(),rc=p.returncode,output_sha256=sha(log),overlay_sha256=sha(overlay),suite_overlay_sha256=sha(packet/'suite_quicksilver.go'),suite_original_sha256=sha(source/'cmd/unified_bench/suite_quicksilver.go'),binary_sha256=sha(binary) if binary.exists() else None)
 rows.append(rec);(packet/'native-builds.json').write_text(json.dumps(rows,indent=2)+'\n')
 assert p.returncode==0,p.stderr
 print('BUILT_REPLAY',label,rec['binary_sha256'],flush=True)
