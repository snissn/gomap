"""Bounded Linux observer tests using the current Python ELF, no benchmark."""
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

import owned_process_rss as observer


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux /proc sampler')
class OwnedProcessRSSTest(unittest.TestCase):
    def test_owned_direct_and_time_child_only(self):
        unrelated = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(10)'])
        try:
            for prefix in ([], ['/usr/bin/time','-v']):
                with tempfile.TemporaryDirectory() as temp, open(os.devnull,'w') as sink:
                    path = pathlib.Path(temp)/'samples.jsonl'
                    command = prefix+[sys.executable,'-c','import time; time.sleep(.12)']
                    result, summary = observer.run_with_rss(command, sys.executable, path, interval_ms=5, stdout=sink,stderr=sink)
                    rows = [json.loads(line) for line in path.read_text().splitlines()]
                    self.assertEqual(result.returncode,0)
                    self.assertTrue(summary['complete'],summary)
                    self.assertEqual(summary['samples'],len(rows))
                    self.assertNotEqual(summary['observed_pid'],unrelated.pid)
                    self.assertEqual({r['pid'] for r in rows},{summary['observed_pid']})
                    self.assertTrue(all(r['finished_unix_nano'] >= r['started_unix_nano'] and r['rss_bytes'] >= 0 for r in rows))
                    if prefix:
                        self.assertNotEqual(summary['launcher_pid'],summary['observed_pid'])
        finally:
            unrelated.terminate();unrelated.wait()

    def test_process_exit_race(self):
        with tempfile.TemporaryDirectory() as temp:
            path=pathlib.Path(temp)/'samples.jsonl'
            result,summary=observer.run_with_rss([sys.executable,'-c','pass'],sys.executable,path,interval_ms=5)
            self.assertEqual(result.returncode,0)
            self.assertEqual(summary['errors'],[])
            self.assertEqual(summary['complete'],summary['samples']>0)

    def test_cancel_reaps_launcher_without_sampler(self):
        for prefix in ([], ['/usr/bin/time','-v']):
            with tempfile.TemporaryDirectory() as temp, open(os.devnull,'w') as sink:
                event=threading.Event();event.set()
                path=pathlib.Path(temp)/'samples.jsonl'
                before={t.ident for t in threading.enumerate()}
                result,summary=observer.run_with_rss(prefix+[sys.executable,'-c','import time; time.sleep(60)'],sys.executable,path,cancel_event=event,stdout=sink,stderr=sink)
                self.assertTrue(summary['cancelled'])
                self.assertFalse(summary['complete'])
                self.assertNotEqual(result.returncode,0)
                self.assertEqual({t.ident for t in threading.enumerate()},before)
                with self.assertRaises(ProcessLookupError):
                    os.kill(summary['launcher_pid'],0)

    def test_failure_and_wrong_binary_do_not_launch(self):
        with tempfile.TemporaryDirectory() as temp:
            path=pathlib.Path(temp)/'samples.jsonl'
            with mock.patch.object(observer.subprocess,'Popen',side_effect=OSError('failed launch')):
                with self.assertRaises(OSError):
                    observer.run_with_rss([sys.executable,'-c','pass'],sys.executable,path)
            summary=json.loads(pathlib.Path(str(path)+'.summary.json').read_text())
            self.assertFalse(summary['complete'])
            self.assertEqual(summary['samples'],0)
            with mock.patch.object(observer.subprocess,'Popen') as launch:
                with self.assertRaises(ValueError):
                    observer.run_with_rss(['/usr/bin/true'],sys.executable,path)
                launch.assert_not_called()

    def test_cancel_exit_between_timeout_and_kill_is_reaped(self):
        with tempfile.TemporaryDirectory() as temp:
            path=pathlib.Path(temp)/'samples.jsonl'
            event=threading.Event();event.set()
            process=mock.Mock(pid=123456,returncode=-15)
            process.poll.return_value=None
            process.wait.side_effect=[subprocess.TimeoutExpired('owned',5),-15]
            with mock.patch.object(observer.subprocess,'Popen',return_value=process), mock.patch.object(observer.os,'killpg',side_effect=[None,ProcessLookupError]):
                result,summary=observer.run_with_rss([sys.executable,'-c','pass'],sys.executable,path,cancel_event=event)
            self.assertEqual(result.returncode,-15)
            self.assertEqual(process.wait.call_count,2)
            self.assertTrue(summary['cancelled'])
            self.assertFalse(summary['complete'])
            self.assertEqual(json.loads(pathlib.Path(str(path)+'.summary.json').read_text()),summary)

    def test_identity_rejection_never_records_unrelated_process(self):
        identity=os.stat(sys.executable)
        with self.assertRaises(ValueError):
            observer._sample(os.getpid(),-1,(identity.st_dev,identity.st_ino),None)
        with self.assertRaises(ValueError):
            observer._sample(os.getpid(),os.getppid(),(-1,-1),None)


if __name__=='__main__':
    unittest.main()
