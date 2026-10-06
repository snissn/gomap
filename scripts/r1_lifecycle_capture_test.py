"""Exercise the environment actually received by a child, without running Go."""
import os
import subprocess
import unittest
from unittest.mock import patch

from r1_lifecycle_capture import benchmark_environment


class BenchmarkEnvironmentTests(unittest.TestCase):
    def test_unreported_caller_settings_do_not_reach_actual_child(self):
        frozen = {'GOMAXPROCS': '16', 'GOGC': '100', 'GOMEMLIMIT': 'off', 'GOFLAGS': ''}
        inherited = {'PATH': '/usr/bin:/bin', 'HOME': '/tmp/r1-home',
                     'GODEBUG': 'gctrace=1', 'GOTRACEBACK': 'crash',
                     'GOMAP_UNREPORTED_SETTING': '1',
                     'GOMAP_R1_LIFECYCLE_DOCUMENTS': '99999',
                     'GOMAP_R1_LIFECYCLE_CALLS_PER_EPOCH': '99999'}
        with patch.dict(os.environ, inherited, clear=True):
            recorded = benchmark_environment(frozen, '/tmp/r1-benchmark-tmp', 32, 8)
        actual = dict(line.split('=', 1) for line in subprocess.check_output(
            ['/usr/bin/env'], env=recorded, text=True).splitlines())
        self.assertEqual(actual, recorded)
        self.assertEqual(recorded['GODEBUG'], '')
        self.assertEqual(recorded['GOMAP_R1_LIFECYCLE_DOCUMENTS'], '32')
        self.assertEqual(recorded['GOMAP_R1_LIFECYCLE_CALLS_PER_EPOCH'], '8')
        self.assertNotIn('GOMAP_UNREPORTED_SETTING', recorded)
        self.assertNotIn('GOTRACEBACK', recorded)


if __name__ == '__main__':
    unittest.main()
