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
        assert_in_order(
            self,
            source,
            [
                'run_pair "$sample" get_versioned',
                'run_pair "$sample" batch_write',
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


if __name__ == "__main__":
    unittest.main()
