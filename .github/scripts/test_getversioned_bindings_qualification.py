#!/usr/bin/env python3
"""Pure fixed event/source controls for the #5396 hosted qualification."""
import importlib.util
import hashlib
import io
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
        for arm in ("db", "tree"):
            self.assertEqual(job.count("-gcflags='github.com/snissn/gomap/TreeDB/" + arm + "=-m=2'"), 1)
            self.assertIn("compiler-" + arm + ' "$out"', job)
            self.assertIn('"${exits[0]}" > "$out/' + arm + '-compiler.exit"', job)
            self.assertIn('"${exits[1]}" > "$out/' + arm + '-selection.exit"', job)
        self.assertEqual(job.count('exits=("${PIPESTATUS[@]}")'), 2)
        self.assertEqual(job.count('test "${exits[0]}" -eq 0'), 2)
        self.assertEqual(job.count('test "${exits[1]}" -eq 0'), 2)
        self.assertNotIn("TreeDB/...=-m=2", job)
        self.assertNotIn("${QUALIFICATION_OUT}", job)
        self.assertNotIn("-gcflags='all=-l'", job)
        self.assertIn('mkdir "$out"', job)
        self.assertIn('manifest "$QUALIFICATION_OUT"', job)
        self.assertIn("INCONCLUSIVE: required optimized symbol text absent", job)
        self.assertEqual(len(set(Q.DB_TOPS + Q.TREE_TOPS)), 29)

    def test_compiler_selection_preserves_scope_errors_and_unknowns(self):
        sources = Q.COMPILER_SOURCES["db"]
        kept = [b"TreeDB/db/value_reader.go:65:10: can inline selected\n",
                b"./TreeDB/db/db.go:1940:4: inlining call to selected\n",
                b"/checkout/TreeDB/db/db.go:1940:4:   flow: x = y:\n",
                b"TreeDB/db/db.go:1940:4:     from x (return) at x.go:1:2\n",
                b"TreeDB/db/other.go:1:2: x escapes to heap, not allowed in runtime\n",
                b"TreeDB/db/other.go:1:2: undefined: missing\n",
                b"TreeDB/db/other.go:1:2:   warning: truncated explanation\n",
                b"# package heading\n", b"unknown future compiler transport\n", b"\xff\n"]
        for line in kept:
            self.assertFalse(Q.omit_optimization(line, sources), line)
        for line in (b"TreeDB/db/other.go:1:2: can inline irrelevant with cost 3\n",
                     b"TreeDB/db/db.go:2500:4: x escapes to heap in f:\n",
                     b"TreeDB/db/db.go:2500:4:   flow: x = y:\n",
                     b"TreeDB/db/db.go:2500:4:     from x (return) at x.go:1:2\n"):
            self.assertTrue(Q.omit_optimization(line, sources), line)

    def test_compiler_stream_identity_digest_and_failure_receipt(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"RUNNER_TEMP": ""}):
            os.environ["RUNNER_TEMP"] = d
            root = Path(d) / "getversioned-bindings-qualification"
            root.mkdir()
            repository = HERE.parents[1]
            raw = (b"TreeDB/db/other.go:1:2: can inline irrelevant\n"
                   b"TreeDB/db/value_reader.go:65:2: r does not escape\n"
                   b"TreeDB/db/other.go:1:2: undefined: missing\n")
            result = Q.compiler_stream("db", root, io.BytesIO(raw), repository)
            self.assertTrue(result["raw_eof"])
            self.assertEqual(result["raw_bytes"], len(raw))
            self.assertEqual(result["raw_sha256"], hashlib.sha256(raw).hexdigest())
            self.assertEqual(result["omitted_optimization_lines"], 1)
            self.assertEqual((root / "db-compiler.selected.stderr").read_bytes(), raw.split(b"\n", 1)[1])
            # An EOF-complete stream still needs the independently retained
            # compiler exit: preserving its error text is not compiler PASS.
            self.assertTrue(result["compiler_exit_required_separately"])
            for p in root.iterdir():
                p.unlink()
            with mock.patch.object(Q, "LOG_LIMIT", 8), self.assertRaises(ValueError):
                Q.compiler_stream("db", root, io.BytesIO(b"unknown diagnostic too long\n"), repository)
            failed = json.loads((root / "db-compiler-stream.json").read_text())
            self.assertFalse(failed["raw_eof"])
            self.assertEqual(failed["status"], "INCOMPLETE")
            self.assertNotIn("raw_sha256", failed)
            self.assertIn("consumed_prefix_sha256", failed)
            mismatched = {"db": {"TreeDB/db/db.go": ("0" * 64, ((1787, 1968),))}}
            with mock.patch.object(Q, "COMPILER_SOURCES", mismatched), self.assertRaises(ValueError):
                Q.compiler_scope("db", repository)


if __name__ == "__main__":
    unittest.main()
