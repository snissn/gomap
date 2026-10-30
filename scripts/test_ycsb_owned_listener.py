import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

import ycsb_compare_validate as c
import ycsb_owned_listener as guard


@unittest.skipUnless(sys.platform=='linux' and hasattr(os,'pidfd_open'), 'Linux /proc and pidfd controls')
class OwnedListenerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.children = []

    def tearDown(self):
        for child in self.children:
            child.terminate()
            child.wait(timeout=5)
            child.stdout.close()
        self.tmp.cleanup()

    def child(self, listening=True, host='127.0.0.1'):
        code = ('import socket,time; s=socket.socket(); s.bind((' + repr(host) + ',0)); '
                's.listen(); print(s.getsockname()[1],flush=True); time.sleep(20)' if listening else
                'import time; print("ready",flush=True); time.sleep(20)')
        process = subprocess.Popen([sys.executable, '-u', '-c', code], stdout=subprocess.PIPE, text=True)
        self.children.append(process)
        line = process.stdout.readline().strip()
        self.assertTrue(line)
        return process, int(line) if listening else None

    def test_owned_listener_and_cli_receipt(self):
        child, port = self.child()
        identity = c.observe_core(child.pid)
        addr = '127.0.0.1:' + str(port)
        inode = guard.listener(child.pid, addr, identity)
        self.assertTrue(inode.isdigit())
        path = pathlib.Path(self.tmp.name)
        launch, receipt = path / 'launch.json', path / 'listener.json'
        launch.write_text(json.dumps({'process': identity}))
        cmd = [sys.executable, str(pathlib.Path(guard.__file__)), str(launch), addr, str(receipt)]
        subprocess.run(cmd, check=True, capture_output=True, timeout=5)
        subprocess.run(cmd + ['--verify'], check=True, capture_output=True, timeout=5)
        self.assertEqual(json.loads(receipt.read_text())['inode'], inode)

    def test_occupied_port_owned_by_other_process_is_rejected(self):
        _, port = self.child()
        wrong, _ = self.child(False)
        with self.assertRaisesRegex(ValueError, 'not owned'):
            guard.listener(wrong.pid, '127.0.0.1:' + str(port), c.observe_core(wrong.pid))

    def test_process_start_identity_and_executable_are_enforced(self):
        child, port = self.child()
        identity = c.observe_core(child.pid)
        identity['start_ticks'] += 1
        with self.assertRaisesRegex(ValueError, 'process changed'):
            guard.listener(child.pid, '127.0.0.1:' + str(port), identity)
        identity = c.observe_core(child.pid)
        identity['exe_sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'process changed'):
            guard.listener(child.pid, '127.0.0.1:' + str(port), identity)

    def test_wildcard_listener_is_not_accepted(self):
        child, port = self.child(host='0.0.0.0')
        with self.assertRaisesRegex(ValueError, 'missing or ambiguous'):
            guard.listener(child.pid, '127.0.0.1:' + str(port), c.observe_core(child.pid))

    def test_invalid_non_loopback_address_rejected(self):
        child, port = self.child()
        with self.assertRaisesRegex(ValueError, 'explicit IPv4 loopback'):
            guard.listener(child.pid, '0.0.0.0:' + str(port), c.observe_core(child.pid))


if __name__ == '__main__':
    unittest.main()
