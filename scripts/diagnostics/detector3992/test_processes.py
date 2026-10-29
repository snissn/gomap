"""Real inert OS lifecycle tests. No product, benchmark or remote execution."""
import fcntl,json,os,pathlib,signal,sys,tempfile,threading,time,unittest
from unittest.mock import patch
import hosted,census,subprocess
from supervisor import read_identity,Cancelled,LifecycleError

class ProcessContracts(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='3992-inert-',dir=pathlib.Path(__file__).resolve().parent);self.addCleanup(self.temp.cleanup);self.out=pathlib.Path(self.temp.name);(self.out/'tmp').mkdir()
  self.fd=os.open(self.out/'job.lock',os.O_CREAT|os.O_EXCL|os.O_RDWR,0o600);fcntl.flock(self.fd,fcntl.LOCK_EX);self.addCleanup(os.close,self.fd)
  self.env=dict(PATH=os.environ['PATH'],TMPDIR=str(self.out/'tmp'))
  self.commands=hosted.Commands(self.out,self.env,self.fd)
  self.addCleanup(self.commands.finish)
 def test_actual_success_and_error(self):
  self.assertEqual(self.commands('ok',[sys.executable,'-c','print("inert")']),'inert\n')
  with self.assertRaises(LifecycleError):self.commands('bad',[sys.executable,'-c','raise SystemExit(7)'])
  self.assertIsNone(self.commands.owner.active)
  rows=json.loads((self.out/'commands.json').read_text());self.assertEqual(rows[0]['status'],0);self.assertEqual(rows[1]['status'],'INVALID');self.assertTrue(all(x['drained'] for x in rows))
 def test_deadline_with_residual_grandchildren(self):
  markers=self.out/'markers'
  code='''import os,time,pathlib
p=pathlib.Path(%r)
def mark():
 s=pathlib.Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19]
 with p.open('a') as f:f.write(str(os.getpid())+' '+s+'\\n')
mark()
if os.fork()==0:
 if os.fork()==0:mark();time.sleep(60);os._exit(0)
 mark();time.sleep(60);os._exit(0)
time.sleep(60)
'''%str(markers)
  with self.assertRaises(LifecycleError):self.commands('tree',[sys.executable,'-c',code],timeout=.2)
  self.assertIsNone(self.commands.owner.active)
  ids=[x.split() for x in markers.read_text().splitlines()];self.assertEqual(len(ids),3)
  for pid,start in ids:
   try:r=read_identity(int(pid))
   except FileNotFoundError:continue
   self.assertNotEqual(str(r['start']),start,'owned process remained after drain')
 def test_actual_interrupt_is_latched_and_drained(self):
  def interrupt():time.sleep(.1);os.kill(os.getpid(),signal.SIGTERM)
  t=threading.Thread(target=interrupt);t.start()
  try:
   with self.assertRaises(Cancelled):self.commands('interrupt',[sys.executable,'-c','import time;time.sleep(60)'])
  finally:t.join()
  self.assertIsNone(self.commands.owner.active)
 def test_lease_path_replacement_stops_before_spawn(self):
  (self.out/'job.lock').unlink();(self.out/'job.lock').write_text('foreign')
  with self.assertRaises(RuntimeError):self.commands('never',[sys.executable,'-c','print("must not run")'])
  self.assertIsNone(self.commands.owner.active)
 def test_capacity_and_symlinks_fail_closed(self):
  (self.out/'bad').symlink_to('/tmp')
  with self.assertRaises(census.CensusError):hosted.census(self.out)
  (self.out/'bad').unlink()
  with patch.object(census,'CAP_BYTES',0):
   (self.out/'large').write_bytes(b'x')
   with self.assertRaises(census.CensusError):hosted.census(self.out)

class SourceCleanupContracts(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(prefix='3992-owned-sources-',dir=pathlib.Path(__file__).resolve().parent);self.addCleanup(self.temp.cleanup)
  self.root=pathlib.Path(self.temp.name);self.repo=self.root/'repo';self.repo.mkdir()
  self.git_run(['git','init','-q'],self.repo)
  self.git_run(['git','-c','user.name=Inert Test','-c','user.email=inert@example.invalid','commit','-q','--allow-empty','-m','inert fixture'],self.repo)
  self.failure=None;self.sources=hosted.OwnedSources(self.root,'123',self.repo,self.owner)
 def git_run(self,argv,cwd):return subprocess.check_output(list(map(str,argv)),cwd=cwd,stderr=subprocess.STDOUT,text=True)
 def owner(self,label,argv,cwd):
  if label=='worktree-A' and self.failure=='partial-directory':
   (self.sources.paths['A']/'partial').write_text('inert')
   raise RuntimeError('injected add failure before registration')
  result=self.git_run(argv,cwd)
  if label=='worktree-A' and self.failure=='observer':raise RuntimeError('injected observer failure after real registration')
  if label=='remove-A' and self.failure=='remove-observer':raise RuntimeError('injected observer failure after removal')
  if label=='remove-A' and self.failure=='post-remove-replacement':
   src=self.sources.paths['A'];src.mkdir();(src/'foreign').write_text('preserve')
  return result
 def test_successful_owned_sources_removed(self):
  self.sources.prepare()
  for label in ['A','C']:self.sources.add(label,'HEAD')
  self.assertTrue(self.sources.cleanup()['complete']);self.assertFalse(self.sources.root.exists())
 def test_partial_add_directory_is_preserved(self):
  self.sources.prepare();self.failure='partial-directory'
  with self.assertRaises(RuntimeError):self.sources.add('A','HEAD')
  self.assertFalse(self.sources.cleanup()['complete']);self.assertEqual((self.sources.paths['A']/'partial').read_text(),'inert')
 def test_registered_add_observer_failure_is_preserved(self):
  self.sources.prepare();self.failure='observer'
  with self.assertRaises(RuntimeError):self.sources.add('A','HEAD')
  self.assertFalse(self.sources.cleanup()['complete']);self.assertIn(self.sources.paths['A'],self.sources.registrations('test-list'))
 def test_missing_directory_registration_is_preserved(self):
  self.sources.prepare();self.sources.add('A','HEAD');self.sources.add('C','HEAD')
  import shutil
  shutil.rmtree(self.sources.paths['A'])
  self.assertFalse(self.sources.cleanup()['complete']);self.assertIn(self.sources.paths['A'],self.sources.registrations('test-list'));self.assertFalse(self.sources.paths['C'].exists())
 def test_preexisting_private_parent_is_never_claimed(self):
  self.sources.root.mkdir();(self.sources.root/'foreign').write_text('preserve')
  with self.assertRaises(FileExistsError):self.sources.prepare()
  self.assertFalse(self.sources.cleanup()['complete']);self.assertEqual((self.sources.root/'foreign').read_text(),'preserve')
 def test_post_prepare_path_collision_is_preserved(self):
  self.sources.prepare();src=self.sources.paths['A'];src.mkdir();(src/'foreign').write_text('preserve')
  with self.assertRaises(FileExistsError):self.sources.add('A','HEAD')
  self.assertFalse(self.sources.cleanup()['complete']);self.assertEqual((src/'foreign').read_text(),'preserve')
 def test_post_prepare_foreign_registration_is_preserved(self):
  self.sources.prepare();src=self.sources.paths['A'];self.git_run(['git','worktree','add','--detach',src,'HEAD'],self.repo)
  with self.assertRaises(RuntimeError):self.sources.add('A','HEAD')
  self.assertFalse(self.sources.cleanup()['complete']);self.assertIn(src,self.sources.registrations('test-list'));self.assertTrue((src/'.git').exists())
 def test_source_directory_replacement_is_preserved(self):
  self.sources.prepare();src=self.sources.add('A','HEAD');src.rename(self.sources.root/'original-A');src.mkdir();(src/'foreign').write_text('preserve')
  self.assertFalse(self.sources.cleanup()['complete']);self.assertEqual((src/'foreign').read_text(),'preserve');self.assertIn(src,self.sources.registrations('test-list'))
 def test_private_parent_replacement_is_preserved(self):
  self.sources.prepare();self.sources.add('A','HEAD');self.sources.root.rename(self.root/'original-parent');self.sources.root.mkdir();src=self.sources.paths['A'];src.mkdir();(src/'foreign').write_text('preserve')
  self.assertFalse(self.sources.cleanup()['complete']);self.assertEqual((src/'foreign').read_text(),'preserve')
 def test_registration_link_replacement_is_preserved(self):
  self.sources.prepare();src=self.sources.add('A','HEAD');link=src/'.git';replacement=src/'replacement';replacement.write_bytes(link.read_bytes());replacement.replace(link)
  self.assertFalse(self.sources.cleanup()['complete']);self.assertTrue(link.exists());self.assertIn(src,self.sources.registrations('test-list'))
 def test_registration_metadata_replacement_is_preserved(self):
  self.sources.prepare();src=self.sources.add('A','HEAD');metadata=pathlib.Path((src/'.git').read_text().removeprefix('gitdir: ').strip())
  original=metadata.with_name(metadata.name+'-original');metadata.rename(original)
  import shutil
  shutil.copytree(original,metadata)
  self.assertFalse(self.sources.cleanup()['complete']);self.assertTrue(metadata.exists());self.assertTrue(src.exists())
 def test_source_symlink_replacement_does_not_follow(self):
  self.sources.prepare();src=self.sources.add('A','HEAD');src.rename(self.sources.root/'original-A')
  foreign=self.root/'foreign';foreign.mkdir();(foreign/'keep').write_text('preserve');src.symlink_to(foreign,target_is_directory=True)
  self.assertFalse(self.sources.cleanup()['complete']);self.assertTrue(src.is_symlink());self.assertEqual((foreign/'keep').read_text(),'preserve')
 def test_residue_created_after_remove_is_preserved(self):
  self.sources.prepare();src=self.sources.add('A','HEAD');self.failure='post-remove-replacement'
  self.assertFalse(self.sources.cleanup()['complete']);self.assertEqual((src/'foreign').read_text(),'preserve')
 def test_removal_observer_failure_cannot_claim_complete(self):
  self.sources.prepare();self.sources.add('A','HEAD');self.sources.add('C','HEAD');self.failure='remove-observer'
  self.assertFalse(self.sources.cleanup()['complete'])
  for src in self.sources.paths.values():self.assertFalse(src.exists());self.assertNotIn(src,self.sources.registrations('test-list'))
 def test_final_registration_query_failure_is_incomplete(self):
  self.sources.prepare();original=self.sources.owner
  def fail(label,argv,cwd):
   if label=='cleanup-final-registrations':raise RuntimeError('injected observation failure')
   return original(label,argv,cwd)
  self.sources.owner=fail;result=self.sources.cleanup();self.assertFalse(result['complete']);self.assertIn('SOURCE_NAMESPACE_UNVERIFIED',result['remaining'])

if __name__=='__main__':unittest.main()
