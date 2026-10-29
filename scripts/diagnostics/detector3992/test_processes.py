"""Real inert OS lifecycle tests. No product, benchmark or remote execution."""
import fcntl,json,os,pathlib,signal,sys,tempfile,threading,time,unittest
from unittest.mock import patch
import hosted,census
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

if __name__=='__main__':unittest.main()
