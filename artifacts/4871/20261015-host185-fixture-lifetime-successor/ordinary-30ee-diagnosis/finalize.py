import pathlib,subprocess,json,hashlib,re,shutil,datetime
D=pathlib.Path('/mnt/fast4tb/gomap-mvcc-4871-20261015');W=D/'ordinary-repair';O=D/'ordinary-30ee-diagnosis'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=W,text=True).splitlines()
identity={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=W,text=True).strip(),'files':{f:sha(W/f) for f in files if (W/f).is_file()}}
changed=subprocess.check_output(['git','diff','--name-only'],cwd=W,text=True).splitlines()
assert len(changed)==5 and all(f.endswith('_test.go') for f in changed),changed
checks={}
for label in ['coherent-normal-01','coherent-race-01','coherent-windows-build-01']:
 p=O/'logs'/label;b=json.loads((p/'before.json').read_text());a=json.loads((p/'after.json').read_text());result=json.loads((p/'result.json').read_text());raw=(p/'raw.txt').read_text()
 assert b==a==identity,label
 assert result['exit']==0,(label,result)
 checks[label]={**result,'source_before_after_equal':True,'matches_freeze':True,'command':json.loads((p/'command.json').read_text()),'package_results':re.findall(r'^(?:ok\s+|\?\s+).*$',raw,re.M),'top_level_pass_events':len(re.findall(r'^--- PASS:',raw,re.M)),'top_level_skip_events':len(re.findall(r'^--- SKIP:',raw,re.M)),'subtest_pass_events':len(re.findall(r'^\s+--- PASS:',raw,re.M)),'data_race_reports':raw.count('WARNING: DATA RACE'),'raw_sha256':sha(p/'raw.txt')}
subprocess.run(['git','diff','--check'],cwd=W,check=True,stdout=subprocess.PIPE)
gofmt=subprocess.check_output([str(D/'toolchains/go1.26.3/bin/gofmt'),'-l',*changed],cwd=W,text=True);assert not gofmt,gofmt
(O/'freeze-final.json').write_text(json.dumps(identity,sort_keys=True,indent=2))
(O/'changed-files-final.json').write_text(json.dumps({'head':identity['head'],'test_only':True,'production_and_harness_bytes_equal_head':True,'files':{f:{'sha256':identity['files'][f],'head_sha256':hashlib.sha256(subprocess.check_output(['git','show','HEAD:'+f],cwd=W)).hexdigest()} for f in changed}},indent=2))
(O/'validation-final.json').write_text(json.dumps({'head':identity['head'],'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_frozen':True,'runtime_released':True,'checks':checks,'gofmt_clean':True,'diff_check_clean':True,'windows_binaries':{str(p.relative_to(O)):sha(p) for p in (O/'windows-build').glob('*.exe')},'limits':['focused local validation only','Windows compilation only; successor hosted execution required','hosted grower failing phase remains unattributed','SAME dictionary control demonstrates baseline mutation, not exact hosted delta reproduction','no performance runtime or successor qualification claim']},indent=2))
(O/'final.patch').write_bytes(subprocess.check_output(['git','diff','--binary'],cwd=W))
for f in changed:
 dest=O/'frozen-source'/f;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(W/f,dest)
print(json.dumps({'head':identity['head'],'source_files':len(identity['files']),'changed':changed,'checks':{k:{x:v[x] for x in ['exit','elapsed','top_level_pass_events','subtest_pass_events','source_before_after_equal']} for k,v in checks.items()}},indent=2))
