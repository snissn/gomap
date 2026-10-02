#!/usr/bin/env python3
"""Structural tests for the base/head benchmark-group pairing contract."""

from pathlib import Path
import re
import json
import os
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


def normalized(path: Path) -> str:
    return re.sub(r"\s+", " ", path.read_text(encoding="utf-8"))


def assert_in_order(test: unittest.TestCase, source: str, fragments: list[str]) -> None:
    position = 0
    for fragment in fragments:
        found = source.find(fragment, position)
        test.assertNotEqual(found, -1, f"missing or out of order: {fragment}")
        position = found + len(fragment)


class BenchmarkPairingTests(unittest.TestCase):
    def test_raw_gate_pairs_each_individual_group(self) -> None:
        source = normalized(ROOT / "scripts" / "mvcc_raw_path_gate.sh")
        self.assertNotIn("run_revision()", source)
        self.assertIn("GET_VERSIONED_BENCH_REGEX='^BenchmarkGetVersioned$'", source)
        self.assertIn(
            "BATCH_WRITE_BENCH_REGEX='^BenchmarkConditionalTxnBaselineBatchWrite$'",
            source,
        )
        self.assertIn(
            'BATCH_WRITE_BENCHTIME="${BATCH_WRITE_BENCHTIME:-1000x}"', source
        )
        for package in ("DB", "CACHING", "TREEDB"):
            self.assertIn(
                f'--baseline-{package.lower()}-binary "$BASELINE_{package}_BIN"',
                source,
            )
            self.assertIn(
                f'--candidate-{package.lower()}-binary "$CANDIDATE_{package}_BIN"',
                source,
            )
        assert_in_order(
            self,
            source,
            [
                'run_pair "$sample" get_versioned',
                'run_pair "$sample" batch_write "$BASELINE_DB_BIN" "$CANDIDATE_DB_BIN" "$BATCH_WRITE_BENCH_REGEX" "$BATCH_WRITE_BENCHTIME"',
                'run_pair "$sample" snapshot_seek',
                'run_pair "$sample" repeated_iterator',
                'run_pair "$sample" durable_sync',
            ],
        )

    def test_raw_gate_pair_order_is_abba_by_sample(self) -> None:
        source = normalized(ROOT / "scripts" / "mvcc_raw_path_gate.sh")
        assert_in_order(
            self,
            source,
            [
                'if ((sample % 2 == 1)); then run_sample baseline',
                'run_sample candidate',
                'else run_sample candidate',
                'run_sample baseline',
            ],
        )

    def test_iterator_diagnostics_preserve_gate_status_and_samples(self) -> None:
        script = (ROOT / "scripts/mvcc_raw_path_gate.sh").read_text()
        tail = script[script.index("if python3 .github/scripts/check_mvcc_raw_path_gate.py"):]
        for status, timing_pass, equivalent, diagnostic_status in ((0, True, False, 0), (1, False, False, 0), (1, False, False, 124), (1, False, True, 0), (2, True, False, 0)):
            with self.subTest(status=status, timing_pass=timing_pass, equivalent=equivalent), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                summary = {"results": [{"benchmark": "BenchmarkRepeatedIterator", "timing_pass": timing_pass, "binary_equivalent": equivalent}]}
                (root / "summary.json").write_text(json.dumps(summary))
                for revision in ("baseline", "candidate"):
                    (root / f"{revision}-caching.test").write_bytes(revision.encode())
                shim = root / "shim"
                shim.mkdir()
                fake_go = shim / "go"
                fake_go.write_text('#!/bin/bash\necho "$*" >> "$OUT_DIR/go-calls"\n')
                fake_go.chmod(0o755)
                prefix = r'''set -euo pipefail
python3() { if [[ "$1" == .github/scripts/check_mvcc_raw_path_gate.py ]]; then return "$MOCK_STATUS"; else command python3 "$@"; fi; }
taskset() { echo "$*" >> "$OUT_DIR/profile-calls"; return "$MOCK_DIAGNOSTIC_STATUS"; }
TMP_ROOT="$OUT_DIR"
SUMMARY_JSON="$OUT_DIR/summary.json"
SUMMARY_MD="$OUT_DIR/summary.md"
BASELINE_LOG="$OUT_DIR/baseline.txt"
CANDIDATE_LOG="$OUT_DIR/candidate.txt"
BASELINE_SHA=base
CANDIDATE_SHA=head
for revision in BASELINE CANDIDATE; do
  for package in DB CACHING TREEDB; do printf -v "${revision}_${package}_BIN" dummy; done
done
RUNS=8
MAX_REGRESSION_PERCENT=5
MAX_BYTES_REGRESSION_PERCENT=1
MAX_BYTES_REGRESSION_ABSOLUTE=64
SCRIPT_GOWORK=off
CPUSET=0
CACHING_BENCH_REGEX='^BenchmarkRepeatedIterator$'
'''
                result = subprocess.run(["bash", "-c", prefix + tail], env={**os.environ, "OUT_DIR": tmp, "MOCK_STATUS": str(status), "MOCK_DIAGNOSTIC_STATUS": str(diagnostic_status), "PATH": str(shim) + os.pathsep + os.environ["PATH"]}, capture_output=True, text=True)
                self.assertEqual(result.returncode, status, result.stderr)
                calls = root / "profile-calls"
                if status and not timing_pass and not equivalent:
                    rows = calls.read_text().splitlines()
                    self.assertEqual(len(rows), 4)
                    self.assertEqual([re.search(r"/(baseline|candidate)-caching.test", row).group(1) for row in rows], ["baseline", "candidate", "candidate", "baseline"])
                    for revision in ("baseline", "candidate"):
                        self.assertEqual((root / f"iterator-diagnostics/{revision}-caching.test").read_bytes(), revision.encode())
                else:
                    self.assertFalse(calls.exists())
                self.assertFalse((root / "baseline.txt").exists())
                self.assertFalse((root / "candidate.txt").exists())

    def test_raw_batch_write_excludes_async_publication(self) -> None:
        source = normalized(
            ROOT / "TreeDB" / "db" / "conditional_kv_contract_bench_test.go"
        )
        body = source.split(
            "func BenchmarkConditionalTxnBaselineBatchWrite", 1
        )[1].split("func BenchmarkConditionalTxnBaselineGet1BatchWrite", 1)[0]
        self.assertIn("rootPublicationFixedDelay: 100 * time.Millisecond", body)
        self.assertIn("const foregroundGroup = 8", body)
        assert_in_order(
            self,
            body,
            [
                "before := coordinator.Stats() b.StartTimer()",
                "batch.Write()",
                "b.StopTimer() after := coordinator.Stats()",
                "after.PublishCalls != before.PublishCalls",
                "d.Checkpoint()",
                "drained.PendingCommits != 0",
            ],
        )

    def test_adapter_gate_pairs_each_group(self) -> None:
        source = normalized(ROOT / "scripts" / "mvcc_adapter_overhead_gate.sh")
        self.assertNotIn("run_revision()", source)
        assert_in_order(
            self,
            source,
            [
                'run_pair "$sample" "$BASELINE_BIN" "$CANDIDATE_BIN" "$COMMIT_REGEX"',
                'run_pair "$sample" "$BASELINE_BIN" "$CANDIDATE_BIN" "$GET_REGEX"',
                'run_pair "$sample" "$BASELINE_BIN" "$CANDIDATE_BIN" "$ITER_REGEX"',
            ],
        )

    def test_adapter_gate_pair_order_is_abba_by_sample(self) -> None:
        source = normalized(ROOT / "scripts" / "mvcc_adapter_overhead_gate.sh")
        assert_in_order(
            self,
            source,
            [
                'if ((sample % 2 == 1)); then run_group baseline',
                'run_group candidate',
                'else run_group candidate',
                'run_group baseline',
            ],
        )

    def test_raw_and_adapter_require_balanced_even_sample_counts(self) -> None:
        for name in ("mvcc_raw_path_gate.sh", "mvcc_adapter_overhead_gate.sh"):
            source = normalized(ROOT / "scripts" / name)
            with self.subTest(script=name):
                self.assertIn('RUNS="${RUNS:-8}"', source)
                self.assertIn("if ((RUNS % 2 != 0)); then", source)
                self.assertIn("RUNS must be even to balance AB/BA sample order", source)

    def test_closeout_prune_stops_timer_before_all_setup(self) -> None:
        source = normalized(ROOT / "TreeDB" / "mvcc" / "closeout_bench_test.go")
        body = source.split("func benchmarkCloseoutPrune", 1)[1].split(
            "func openCloseoutBench", 1
        )[0]
        self.assertRegex(body, r"^\(b .*\) \{ b\.StopTimer\(\) var total")
        assert_in_order(
            self,
            body,
            [
                "b.StopTimer()",
                "parentDir := b.TempDir()",
                "b.StartTimer() stats, err := store.PruneVersions",
                "b.StopTimer() if err := db.Close()",
            ],
        )


if __name__ == "__main__":
    unittest.main()
