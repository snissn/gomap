import pathlib,subprocess,json,hashlib,re,shutil,datetime
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'ordinary-repair';O=D/'ordinary-5699-diagnosis'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=W,text=True).splitlines()
identity={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip(),'files':{f:sha(W/f) for f in files if (W/f).is_file()}}
changed=subprocess.check_output(['git','diff','--name-only'],cwd=W,text=True).splitlines()
assert set(changed)=={'.gitattributes','TreeDB/docs/design/ordinary-primary-allocation-5111.md','TreeDB/mongo_gateway/standalone_test.go','TreeDB/power_loss_oracle_test.go'},changed
checks={}
for label in ['coherent-normal-01','coherent-race-01','coherent-windows-build-01']:
 p=O/'logs'/label;b=json.loads((p/'before.json').read_text());a=json.loads((p/'after.json').read_text());result=json.loads((p/'result.json').read_text());raw=(p/'raw.txt').read_text()
 assert b==a==identity,label
 assert result['exit']==0,(label,result)
 assert 'WARNING: DATA RACE' not in raw,label
 checks[label]={**result,'source_before_after_equal':True,'matches_freeze':True,'command':json.loads((p/'command.json').read_text()),'package_results':re.findall(r'^(?:ok\s+|\?\s+).*$',raw,re.M),'top_level_pass_events':len(re.findall(r'^--- PASS:',raw,re.M)),'subtest_pass_events':len(re.findall(r'^\s+--- PASS:',raw,re.M)),'data_race_reports':raw.count('WARNING: DATA RACE'),'raw_sha256':sha(p/'raw.txt')}
controls={}
for label,code in [('cut-positive-01',0),('cut-negative-01',1),('gateway-diagnostics-negative-01',1)]:
 p=O/'logs'/label;b=json.loads((p/'before.json').read_text());a=json.loads((p/'after.json').read_text());result=json.loads((p/'result.json').read_text());assert b==a==identity,label;assert result['exit']==code,label
 controls[label]={**result,'expected_exit':code,'source_before_after_equal':True,'matches_freeze':True,'raw_sha256':sha(p/'raw.txt')}
subprocess.run(['git','diff','--check'],cwd=W,check=True,stdout=subprocess.PIPE)
gofmt=subprocess.check_output([str(D/'toolchains/go1.26.3/bin/gofmt'),'-l',*[f for f in changed if f.endswith('.go')]],cwd=W,text=True);assert not gofmt,gofmt
(O/'freeze-final.json').write_text(json.dumps(identity,sort_keys=True,indent=2))
(O/'changed-files-final.json').write_text(json.dumps({'head':identity['head'],'production_benchmark_gate_and_archive_payload_bytes_equal_head':True,'files':{f:{'owner':'root' if f in ['.gitattributes','TreeDB/docs/design/ordinary-primary-allocation-5111.md'] else 'ordinary_completion','sha256':identity['files'][f],'head_sha256':hashlib.sha256(subprocess.check_output(['git','show','HEAD:'+f],cwd=W)).hexdigest()} for f in changed}},indent=2))
(O/'validation-final.json').write_text(json.dumps({'head':identity['head'],'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_frozen':True,'runtime_released':True,'checks':checks,'causal_controls':controls,'gofmt_clean':True,'diff_check_clean':True,'windows_binaries':{str(p.relative_to(O)):sha(p) for p in (O/'windows-build').glob('*.exe')},'limits':['focused local validation only','Windows compilation only; hosted execution remains required','gateway hosted delay remains unattributed; diagnostics-only change','M0 variability remains root-owned; no timing runtime in this packet','no ordinary/native/graph acceptance claim']},indent=2))
(O/'final.patch').write_bytes(subprocess.check_output(['git','diff','--binary'],cwd=W))
for f in changed:
 dest=O/'frozen-source'/f;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(W/f,dest)
print(json.dumps({'head':identity['head'],'source_files':len(identity['files']),'changed':changed,'checks':{k:{x:v[x] for x in ['exit','elapsed','top_level_pass_events','subtest_pass_events','source_before_after_equal']} for k,v in checks.items()}},indent=2))
