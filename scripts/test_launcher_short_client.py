"""Synthetic launcher/short-child integration; never runs a product client."""
import argparse
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
import ycsb_compare_validate as compare

@unittest.skipUnless(sys.platform=='linux' and hasattr(os,'pidfd_open'), 'Linux /proc and pidfd controls')
class LauncherControls(unittest.TestCase):
    def test_fast_client_pending_then_witness_and_no_witness(self):
        class SimulatedObserver:
            produce=True
            def __init__(self,*args):
                self.samples=[]
                self.start=time.monotonic()
            def sample(self, child):
                if self.produce and time.monotonic()-self.start >= .025:
                    pid=child.pid
                    self.samples.append(dict(pid=pid,start_ticks=compare.observe_core(pid)['start_ticks'],GOMAXPROCS_present=False,runtime_gomaxprocs=12,address=hex(compare.CONTROLLED_GOMAX_SYMBOL),leader_affinity=[0],cgroup=dict(membership='/',ancestors=[{'cpu.max':'max 100000'}])))
                    return self.samples[-1]
                return None
            def validate(self):
                if not self.samples: raise RuntimeError('no validated nonzero actual-child runtime observation')
                return dict(status='actual-child-runtime-observed',sample_count=len(self.samples),first=self.samples[0],last=self.samples[-1],whole_interval_verified=False,unperturbed_timing_verified=False)
            def diagnostics(self):return dict(error='synthetic')
        with tempfile.TemporaryDirectory() as root:
            cell=pathlib.Path(root)/'cell';cell.mkdir()
            (pathlib.Path(root)/'manifest.json').write_text(json.dumps(dict(campaign_id='synthetic',profile='p',client=dict(build_id='synthetic',exe_sha256=compare.CONTROLLED_CLIENT_SHA))))
            for produce,stem in ((True,'witness'),(False,'missing')):
                SimulatedObserver.produce=produce
                args=argparse.Namespace(directory=str(cell),stem=stem,timeout=3,cleanup_timeout=.5,command=[sys.executable,'-c','import time; time.sleep(.20)'])
                with mock.patch.object(compare,'exe_digest',return_value=compare.CONTROLLED_CLIENT_SHA),mock.patch.object(compare.runtime_observer,'RuntimeObserver',SimulatedObserver):
                    if produce:
                        self.assertEqual(compare.launch(args),0)
                        evidence=compare.read(cell/(stem+'.launch.json'))['runtime_observation']
                        self.assertGreaterEqual(evidence['sample_count'],1)
                        self.assertEqual(evidence['startup_sample_interval_seconds'],.001)
                        self.assertEqual(evidence['sample_interval_seconds'],.25)
                        self.assertEqual(evidence['observer_source_sha256'],compare.RUNTIME_HELPER_SHA)
                    else:
                        with self.assertRaisesRegex(RuntimeError,'no validated'):
                            compare.launch(args)
                        failure=compare.read(cell/(stem+'.failure.json'))
                        self.assertEqual(failure['runtime_samples'],[])
                        self.assertTrue(failure['cleanup']['reaped'])
                        self.assertFalse(pathlib.Path('/proc',str(failure['pid'])).exists())
                        self.assertFalse((cell/(stem+'.launch.json')).exists())

if __name__=='__main__': unittest.main()
