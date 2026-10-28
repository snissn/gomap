import importlib.util
from pathlib import Path
import tempfile
import unittest
import os
import hashlib
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('storage', Path(__file__).resolve().parents[2] / 'scripts/mvcc_storage_context.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class StorageContextTest(unittest.TestCase):
    def test_bounds_symlink_and_secrets(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            secret = root / 'secret'
            secret.write_text('credentials')
            link = root / 'link'
            link.symlink_to(secret)
            self.assertEqual(m.read_counter(link)['status'], 'unavailable')
            secret.write_bytes(b'x' * (m.LIMIT + 1))
            self.assertEqual(m.read_counter(secret)['reason'], 'truncated')
            for output in (secret, link):
                with self.assertRaises(FileExistsError):
                    m.write_fresh(output, {})
            self.assertEqual(secret.stat().st_size, m.LIMIT + 1)
            out = root / 'fresh'
            m.write_fresh(out, {'qualification': 'unavailable'})
            self.assertEqual(out.stat().st_mode & 0o777, 0o600)

    def test_mount_unknown_and_escape(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / 'self').mkdir()
            table = root / 'self/mountinfo'
            table.write_text('1 0 8:0 / /x\\040y rw - ext4 SECRET rw,password=SECRET\n')
            rows = m.mount_table(root)
            self.assertEqual(rows[0]['mount'], '/x y')
            self.assertNotIn('SECRET', str(rows))
            table.write_text('broken')
            self.assertIsNone(m.mount_table(root))
            table.write_bytes(b'x' * (m.LIMIT + 1))
            self.assertIsNone(m.mount_table(root))

    def test_scope_nested_mount_and_missing_device(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            device = root.stat().st_dev
            dev = f'{os.major(device)}:{os.minor(device)}'
            rows = [{'id': 1, 'mount': '/', 'device': dev, 'filesystem': 'ext4'},
                    {'id': 2, 'mount': d, 'device': dev, 'filesystem': 'ext4'}]
            result = m.inventory(root, rows, root)
            self.assertEqual(result['mount_id'], 2)
            self.assertEqual(result['physical_isolation'], 'unavailable')
            self.assertEqual(result['device_stat']['status'], 'unavailable')
            self.assertEqual(m.inventory(root, None, root)['status'], 'unavailable')
            rows.append(dict(rows[-1], id=3))
            self.assertEqual(m.inventory(root, rows, root)['reason'], 'ambiguous_mount')
            rows.pop()
            rows.append(dict(rows[0], id=4))
            self.assertEqual(m.inventory(root, rows, root)['reason'], 'ambiguous_mount')
            rows.pop()
            rows[-1]['device'] = '9:9'
            self.assertEqual(m.inventory(root, rows, root)['reason'], 'device_mismatch')

    def test_machinewide_and_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / 'meminfo').write_text('Dirty: 7 kB\nWriteback: 2 kB\nSecret: ignored\n')
            self.assertEqual(m.memory(root)['Dirty'], 7)
            self.assertNotIn('Secret', str(m.memory(root)))
            result = m.collect([root], stage="before-build", proc=root, sysroot=root)
            self.assertEqual(result['qualification'], 'unavailable')
            self.assertTrue(result['diagnostic_only'])
            self.assertEqual(result['paths'][0]['status'], 'unavailable')
            with self.assertRaises(ValueError):
                m.collect([root] * 17, stage="before-build")
            (root / 'meminfo').write_text('Dirty: 7 kB\n')
            self.assertEqual(m.memory(root)['status'], 'unavailable')

    def test_required_labels_and_identity_limits(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            file = root / 'binary'
            file.write_bytes(b'abc')
            with self.assertRaises(TypeError):
                m.collect([root])
            with self.assertRaises(ValueError):
                m.collect([root], stage='secret stage')
            with self.assertRaises(ValueError):
                m.collect([root], stage='before-build', source_sha='bad')
            result = m.collect([root], stage='collection-end', source_sha='a' * 40,
                               binary=file, proc=root, sysroot=root)
            self.assertEqual(result['identity']['binary']['sha256'], hashlib.sha256(b'abc').hexdigest())
            self.assertEqual(result['identity']['harness']['reason'], 'not_provided')
            self.assertEqual(result['qualification'], 'unavailable')
            self.assertNotIn(str(file), str(result))
            link = root / 'link'
            link.symlink_to(file)
            self.assertEqual(m.file_digest(link)['status'], 'unavailable')
            with patch.object(m, 'MAX_DIGEST_BYTES', 2):
                self.assertEqual(m.file_digest(file)['reason'], 'size_limit')
            with patch.object(m, 'MAX_OUTPUT', 1):
                with self.assertRaises(ValueError):
                    m.write_fresh(root / 'oversize', result)
                self.assertFalse((root / 'oversize').exists())


if __name__ == '__main__':
    unittest.main()
