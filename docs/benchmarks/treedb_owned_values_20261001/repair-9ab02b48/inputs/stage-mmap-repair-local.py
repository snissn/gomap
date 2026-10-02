from pathlib import Path
import io,shlex,subprocess,tarfile
host='mikers@192.168.0.185';root='/mnt/fast4tb/treedb-quicksilver-4891-values-20261001/final-9ab02b48'
subprocess.run(['ssh',host, 'python3 - '+shlex.quote(root)],input=b'import pathlib,sys\np=pathlib.Path(sys.argv[1]);p.mkdir()\nfor n in ["archives","src","bin","logs","tmp"]:(p/n).mkdir()\n',check=True)
commits={'base':'a68a84c7195e0c5d8c339343a385b40acf818d03','previous':'ab2e9124fcca72710b896cf78577fa6bd37a11b0','candidate':'9ab02b48ce2397410940a7e42cc22bbf95a96c5a'}
for rev,sha in commits.items():
 archive=subprocess.Popen(['git','archive','--format=tar',sha],stdout=subprocess.PIPE)
 command='mkdir '+shlex.quote(root+'/src/'+rev)+'\ncat > '+shlex.quote(root+'/archives/'+rev+'.tar')+'\ntar -xf '+shlex.quote(root+'/archives/'+rev+'.tar')+' -C '+shlex.quote(root+'/src/'+rev)
 remote=subprocess.run(['ssh',host,command],stdin=archive.stdout);archive.stdout.close();assert archive.wait()==0 and remote.returncode==0
 print('archived '+rev+' '+sha,flush=True)
files={n:'tmp/'+n for n in ['qualify-mmap-repair.sh','analyze-mmap-repair.py','mmap_qualification_overlay_test.go','prepare-mmap-repair-linux.py','mmap-repair-qualification-plan.md','mmap-repair-freeze-proposal.json']}
files.update({'baseline-benchmark-overlay.go.txt':'docs/benchmarks/treedb_owned_values_20261001/baseline-benchmark-overlay.go.txt','value_log_owned_bench_test.go':'TreeDB/value_log_owned_bench_test.go','logs/fixture-manifest.json':'docs/benchmarks/treedb_owned_values_20261001/raw/fixture-manifest.json'})
stream=io.BytesIO()
with tarfile.open(fileobj=stream,mode='w') as t:
 for target,source in files.items():t.add(source,arcname=target)
subprocess.run(['ssh',host,'tar -xf - -C '+shlex.quote(root)],input=stream.getvalue(),check=True)
script='import pathlib,shutil,sys\np=pathlib.Path(sys.argv[1]);shutil.copytree(p.parent/"final-f9d8b01d/fixture",p/"fixture")\n'
subprocess.run(['ssh',host,'python3 - '+shlex.quote(root)],input=script.encode(),check=True)
print('staged declared overlays/scripts/unchanged fixture; '+root,flush=True)
