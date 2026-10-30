"""Synthetic controls for the actual server /proc environment observation."""
import pathlib
import unittest
from unittest import mock

import ycsb_compare_validate as compare


class ServerEnvironmentTest(unittest.TestCase):
    def observe(self, raw, after=None):
        before = dict(pid=1234, start_ticks=42, exe_sha256="a" * 64)
        with mock.patch.object(compare, "observe_core", side_effect=[before.copy(), (after or before).copy()]) as identity, \
             mock.patch.object(pathlib.Path, "read_bytes", return_value=raw) as read:
            actual = compare.observe(1234)
            self.assertEqual(identity.call_count, 2)
            read.assert_called_once()
            return actual

    def test_valid_nonempty_unset_environment(self):
        result = self.observe(b"PATH=/bin\0OTHER=value=with=equals\0EMPTY=\0")
        self.assertEqual(result, dict(pid=1234, start_ticks=42, exe_sha256="a" * 64, gomaxprocs_unset=True))

    def test_empty_and_malformed_environments_fail(self):
        for raw in (b"", b"\0", b"PATH=/bin", b"PATH=/bin\0\0", b"PATH=/bin\0NOEQUALS\0", b"=bad\0", b"GOMAXPROCS\0"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                self.observe(raw)

    def test_any_override_fails_including_empty_value(self):
        for raw in (b"GOMAXPROCS=4\0PATH=/bin\0", b"PATH=/bin\0GOMAXPROCS=\0", b"GOMAXPROCS=1\0GOMAXPROCS=2\0"):
            with self.subTest(raw=raw), self.assertRaisesRegex(ValueError, "GOMAXPROCS"):
                self.observe(raw)

    def test_identity_change_rejected(self):
        for key, value in (("pid", 1235), ("start_ticks", 43), ("exe_sha256", "b" * 64)):
            after = dict(pid=1234, start_ticks=42, exe_sha256="a" * 64)
            after[key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "process changed"):
                self.observe(b"PATH=/bin\0", after)

    def test_proc_read_failure_is_not_absence(self):
        with mock.patch.object(compare, "observe_core", return_value=dict(pid=1234, start_ticks=42, exe_sha256="a" * 64)), \
             mock.patch.object(pathlib.Path, "read_bytes", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                compare.observe(1234)


if __name__ == "__main__":
    unittest.main()
