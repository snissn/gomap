import pathlib,json,subprocess,hashlib,zipfile,stat
A=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-main-integration');D=A/'raw-primary-current';D.mkdir(exist_ok=False)
a=next(x for x in json.loads((A/'treedb-artifacts-current.json').read_text())['artifacts'] if x['name']=='mvcc-raw-path-gate-6d3fc3a6e3582810d7c80fc46b784a9b30fada80');(D/'artifact-api.json').write_text(json.dumps(a,indent=2)+'\n');z=D/'artifact.zip'
with z.open('wb') as f:subprocess.run(['gh','api',f"repos/snissn/gomap/actions/artifacts/{a['id']}/zip"],stdout=f,check=True,timeout=600)
h=hashlib.sha256(z.read_bytes()).hexdigest();assert z.stat().st_size==a['size_in_bytes'] and a['digest']=='sha256:'+h
out=D/'packet';out.mkdir();seen=set();files={}
with zipfile.ZipFile(z) as f:
 for i in f.infolist():
  p=pathlib.PurePosixPath(i.filename);assert not p.is_absolute() and '..' not in p.parts and i.filename not in seen and not stat.S_ISLNK(i.external_attr>>16);seen.add(i.filename)
  if i.is_dir():continue
  data=f.read(i);dest=out/p;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data);files[str(p)]={'size':len(data),'sha256':hashlib.sha256(data).hexdigest()}
(D/'download-receipt.json').write_text(json.dumps({'artifact_id':a['id'],'zip_sha256':h,'api_size_and_digest_match':True,'files':files},indent=2)+'\n');print('verified',len(files),'files',sum(x['size'] for x in files.values()),'bytes',h)
with (D/'job.log').open('wb') as f:subprocess.run(['gh','api','repos/snissn/gomap/actions/jobs/116763456195/logs'],stdout=f,check=True,timeout=120)
print('joblog', (D/'job.log').stat().st_size)
