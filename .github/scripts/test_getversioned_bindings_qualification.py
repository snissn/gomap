#!/usr/bin/env python3
"""Pure fixed event/source controls for the #5396 hosted qualification."""
import importlib.util
import json
import os
from unittest import mock
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("binding_qualification", HERE / "getversioned_bindings_qualification.py")
Q = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(Q)


def complete():
    records = []
    for pkg, tops in Q.TOPS.items():
        records.append({"Action": "start", "Package": pkg})
        for name in sorted(tops | Q.NEW[pkg]):
            records.extend([{"Action": "run", "Package": pkg, "Test": name},
                            {"Action": "pass", "Package": pkg, "Test": name}])
        records.append({"Action": "pass", "Package": pkg})
    return records


class FixedQualificationTests(unittest.TestCase):
    def consume(self, records):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "events.jsonl"
            p.write_text("".join(json.dumps(e) + "\n" for e in records))
            return Q.events(p)

    def test_exact_inventory(self):
        r = self.consume(complete())
        self.assertEqual((r["tops"], r["new_outcomes"], r["new_leaves"]), (29, 9, 7))

    def test_missing_and_unfinished(self):
        records = complete()
        name = next(iter(Q.TOPS[Q.DB]))
        with self.assertRaises(ValueError):
            self.consume([e for e in records if e.get("Test") != name])
        with self.assertRaises(ValueError):
            self.consume([e for e in records if not (e.get("Test") == name and e["Action"] == "pass")])

    def test_skip_fail_and_race(self):
        for action in ("skip", "fail"):
            r = complete()
            r[2]["Action"] = action
            with self.assertRaises(ValueError):
                self.consume(r)
        r = complete()
        r.insert(1, {"Action": "output", "Package": Q.DB, "Output": "WARNING: DATA RACE"})
        with self.assertRaises(ValueError):
            self.consume(r)

    def test_duplicate_and_foreign(self):
        for extra in ({"Action": "pass", "Package": Q.DB},
                      {"Action": "start", "Package": "foreign"},
                      {"Action": "run", "Package": Q.DB, "Test": "TestForeign"}):
            with self.assertRaises(ValueError):
                self.consume(complete() + [extra])

    def test_new_descendant_and_malformed(self):
        r = complete()
        r.insert(1, {"Action": "run", "Package": Q.DB, "Test": "TestOneShotAppendBindingsUseCurrentOwner/foreign"})
        with self.assertRaises(ValueError):
            self.consume(r)
        with self.assertRaises(ValueError):
            self.consume([{}])

    def test_compiler_transport_population(self):
        r = complete()
        r.insert(0, {"Action": "build-output", "ImportPath": Q.DB + ".test", "Output": "compiler text"})
        self.consume(r)
        r[0]["ImportPath"] = "foreign.test"
        with self.assertRaises(ValueError):
            self.consume(r)
        r[0] = {"Action": "build-fail", "ImportPath": Q.DB}
        with self.assertRaises(ValueError):
            self.consume(r)

    def test_bounded_log(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "large.jsonl"
            with p.open("wb") as f:
                f.truncate(Q.LOG_LIMIT + 1)
            with self.assertRaises(ValueError):
                Q.events(p)

    def test_directory_boundary_and_failure_ledger(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"RUNNER_TEMP": ""}):
            os.environ["RUNNER_TEMP"] = d
            root = Path(d) / "getversioned-bindings-qualification"
            root.mkdir()
            self.assertEqual(Q.bounds(root)["bytes"], 0)
            p = root / "raw.log"
            with p.open("wb") as f:
                f.truncate(Q.LOG_LIMIT + 1)
            with self.assertRaises(ValueError):
                Q.bounds(root)
            with mock.patch("sys.argv", ["consumer", "manifest", str(root)]), self.assertRaises(ValueError):
                Q.main()
            record = json.loads((root / "artifact-manifest.json").read_text())
            self.assertEqual(record["bounds"], "qualification output bound exceeded")
            self.assertIn("raw.log", record["files"])
            p.unlink()
            (root / "artifact-manifest.json").unlink()
            binary = root / "db.test"
            with binary.open("wb") as f:
                f.truncate(Q.OUTPUT_LIMIT + 1)
            with self.assertRaises(ValueError):
                Q.bounds(root)

    def test_fixed_workflow_source(self):
        source = (HERE.parent / "workflows/treedb-tests.yml").read_text()
        job = source.split("  getversioned-bindings-qualification:\n", 1)[1].split("  mvcc-raw-path-gate:\n", 1)[0]
        self.assertIn("timeout-minutes: 25", job)
        self.assertEqual(job.count("go test -json -tags=treedb_test -count=1"), 1)
        self.assertEqual(job.count("go test -race -json -tags=treedb_test -count=1"), 1)
        self.assertEqual(job.count("-gcflags='github.com/snissn/gomap/TreeDB/...=-m=2'"), 2)
        self.assertNotIn("${QUALIFICATION_OUT}", job)
        self.assertNotIn("-gcflags='all=-l'", job)
        self.assertIn('mkdir "$out"', job)
        self.assertIn('manifest "$QUALIFICATION_OUT"', job)
        self.assertIn("INCONCLUSIVE: required optimized symbol text absent", job)
        self.assertEqual(len(set(Q.DB_TOPS + Q.TREE_TOPS)), 29)


if __name__ == "__main__":
    unittest.main()
