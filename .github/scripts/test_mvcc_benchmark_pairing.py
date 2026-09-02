#!/usr/bin/env python3
"""Structural tests for the base/head benchmark-group pairing contract."""

from pathlib import Path
import re
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
