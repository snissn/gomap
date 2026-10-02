"""Local stdlib check; does not build Go or run the benchmark."""
import copy
import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile

script=pathlib.Path(__file__).with_name('negative-741-iterator-repro.py')
spec=importlib.util.spec_from_file_location('repro',script)
repro=importlib.util.module_from_spec(spec)
spec.loader.exec_module(repro)
with tempfile.TemporaryDirectory(prefix='iterator-repro-check-')as directory:
 repro.ROOT=pathlib.Path(directory)
 archives=[];records={}
 repro.inventory=lambda source:{'inputs':{str(source/'input'):repro.sha(source/'input')}}
 for name in ('base','candidate'):
  source=repro.ROOT/name;source.mkdir();(source/'input').write_bytes(b'input')
  archive=repro.ROOT/(name+'.tar.gz');archive.write_bytes(name.encode())
  binary=repro.ROOT/(name+'-caching.test');binary.write_bytes(b'binary')
  item={'name':name,'head':name+'-head','sha256':repro.sha(archive)};archives.append(item)
  records[name]={'original_head':item['head'],'archive_sha256':item['sha256'],'source':str(source),'identity':repro.inventory(source),'binary_sha256':repro.sha(binary)}
 repro.validate_records(records,archives)
 def rejected(bad_records=records,bad_archives=archives):
  try:repro.validate_records(bad_records,bad_archives)
  except (ValueError,KeyError):return
  raise AssertionError('invalid identity accepted')
 for bad in ({},{'base':records['base']},{'candidate':records['candidate']},dict(records,extra=records['base'])):rejected(bad)
 for bad in ([],archives[:1],archives+[archives[0]],[archives[0],archives[0]]):rejected(bad_archives=bad)
 for name in records:
  for field in ('original_head','archive_sha256','source','identity','binary_sha256'):
   bad=copy.deepcopy(records);bad[name][field]='wrong';rejected(bad)
  for path in (repro.ROOT/(name+'.tar.gz'),repro.ROOT/(name+'-caching.test'),repro.ROOT/name/'input'):
   original=path.read_bytes();path.write_bytes(b'tampered');rejected();path.write_bytes(original)
 repro.ROWS.append({'pair':1,'name':'base'})
 command=[sys.executable,'-c',"import sys,time;sys.stdout.buffer.write(b'raw\\x00stdout');sys.stdout.flush();sys.stderr.buffer.write(b'raw\\x00stderr');sys.stderr.flush();time.sleep(10)"]
 for phase in ('captured','redirected'):
  kwargs={'stdout':subprocess.PIPE,'stderr':subprocess.PIPE}
  if phase=='redirected':
   out=(repro.ROOT/'row.stdout').open('xb');err=(repro.ROOT/'row.stderr').open('xb');kwargs={'stdout':out,'stderr':err}
  try:
   try:repro.run(phase,command,timeout=0.5,**kwargs)
   except subprocess.TimeoutExpired:pass
   else:raise AssertionError('timeout did not fire')
  finally:
   if phase=='redirected':out.close();err.close()
  progress=json.loads((repro.ROOT/'capture-incomplete.json').read_text())
  assert progress['complete']is False and progress['failed_phase']==phase
  assert progress['completed_rows']==1 and progress['rows']==repro.ROWS
  for stream in ('stdout','stderr'):
   path=repro.ROOT/('timeout-'+phase+'.'+stream if phase=='captured'else'row.'+stream)
   assert path.read_bytes()==b'raw\x00'+stream.encode()
   assert progress[stream+'_sha256']==repro.sha(path)
print('PASS: exact preparation/archive identities, actual hashes, timeout progress and raw streams')
