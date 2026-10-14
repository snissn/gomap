import pathlib,json,hashlib,subprocess,os,sys
wt=pathlib.Path('/home/mikers/benchmarks/mvcc-4871-repair-20261013/native-4878-traversal');packet=wt/'artifacts/4878/20261014-native-shared-physical-cleanup'
manifest=json.loads((packet/'source.json').read_text());manifest['production_paths']=sum(not r['path'].endswith('_test.go') for r in manifest['source']);manifest['test_paths']=len(manifest['source'])-manifest['production_paths'];manifest['merged_pin_head']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=wt,text=True).strip()
for row in manifest['source']:
 if hashlib.sha256((wt/row['path']).read_bytes()).hexdigest()!=row['sha256']:raise RuntimeError('changed source '+row['path'])
(packet/'source.json').write_text(json.dumps(manifest,indent=2)+'\n')
env=os.environ.copy();env.update(GOWORK='off',GOMAXPROCS='2',CGO_ENABLED='1',GOPROXY='off',GOSUMDB='off',GOTOOLCHAIN='local');go='/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go'
cmd=[go,'list','-mod=readonly','-m','-json','github.com/snissn/compress'];r=subprocess.run(cmd,cwd=wt,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
if r.returncode:raise RuntimeError(r.stderr)
module=json.loads(r.stdout)
if module.get('Replace') or module['Version']!='v1.18.2-snissn.0.0.20261014112447-88a027b09341':raise RuntimeError('unexpected merged module')
bind={'module':module,'consumer_pin_receipt_sha256':'e4a95d9df1e0f6a193d310f006394e7843066c9b7153486bd44ea2840068b1a1','files':[],'provider_sources':[]}
for f in ['go.mod','go.sum']:
 b=(wt/f).read_bytes();bind['files'].append({'path':f,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
for f in ['zstd/decoder.go','zstd/decoder_bounded.go']:
 b=(pathlib.Path(module['Dir'])/f).read_bytes();bind['provider_sources'].append({'path':f,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
cmd=[go,'test','-mod=readonly','-p','2','./TreeDB/internal/retainedalloc','./TreeDB/pager','./TreeDB/internal/primaryarena','-count=1'];bind['command']=cmd
with (packet/'merged-module-normal.log').open('w') as log:
 log.write(json.dumps({'command':cmd,'env':{k:env[k] for k in ['GOWORK','GOMAXPROCS','CGO_ENABLED','GOPROXY','GOSUMDB','GOTOOLCHAIN']}})+'\n');log.flush();code=subprocess.run(cmd,cwd=wt,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
bind['exit_code']=code
(packet/'merged-module-binding.json').write_text(json.dumps(bind,indent=2)+'\n')
for row in manifest['source']:
 if hashlib.sha256((wt/row['path']).read_bytes()).hexdigest()!=row['sha256']:raise RuntimeError('changed source after run')
print('MERGED_MODULE_NORMAL',code,'SOURCE_MANIFEST',hashlib.sha256((packet/'source.json').read_bytes()).hexdigest(),flush=True)
sys.exit(code)
