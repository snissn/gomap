package main

import (
	"math"
	"testing"
	"time"
)

func TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail(t *testing.T) {
	prevMemtableMode := *treedbMemtableMode
	prevForcePointers := *treedbForceValuePointers
	prevValueLogThreshold := *treedbValueLogThreshold
	prevReadWorkers := *readWorkers
	prevFlushThreshold := *treedbFlushThreshold
	prevMaxQueuedMems := *treedbMaxQueuedMems
	defer func() {
		*treedbMemtableMode = prevMemtableMode
		*treedbForceValuePointers = prevForcePointers
		*treedbValueLogThreshold = prevValueLogThreshold
		*readWorkers = prevReadWorkers
		*treedbFlushThreshold = prevFlushThreshold
		*treedbMaxQueuedMems = prevMaxQueuedMems
	}()

	*treedbMemtableMode = "append_only"
	*treedbForceValuePointers = true
	*treedbValueLogThreshold = 1
	*readWorkers = 4
	*treedbFlushThreshold = 8 << 20
	*treedbMaxQueuedMems = 2

	run, err := runBenchmark(BenchConfig{
		Keys:           40_000,
		ValueSize:      256,
		BatchSize:      256,
		ReadWorkers:    4,
		ReadRequireHit: true,
		RangeQueries:   0,
		RangeSpan:      0,
		DBsArg:         "treedb",
		TestsArg:       "sequential_write,random_read_parallel,random_read_parallel_acquire_snapshot",
		KeepDir:        false,
		Progress:       false,
		SeedUsed:       1,
		MaxWall:        30 * time.Second,
	})
	if err != nil {
		t.Fatalf("runBenchmark append-only snapshot guardrail: %v", err)
	}

	parallelRead := run.Results["random_read_parallel"]["TreeDB"]
	if math.IsNaN(parallelRead) || parallelRead <= 0 {
		t.Fatalf("expected random_read_parallel > 0 for TreeDB, got %v", parallelRead)
	}
	snapshotRead := run.Results["random_read_parallel_acquire_snapshot"]["TreeDB"]
	if math.IsNaN(snapshotRead) || snapshotRead <= 0 {
		t.Fatalf("expected random_read_parallel_acquire_snapshot > 0 for TreeDB, got %v", snapshotRead)
	}

	const minSnapshotToParallelRatio = 0.002
	ratio := snapshotRead / parallelRead
	if ratio < minSnapshotToParallelRatio {
		t.Fatalf("snapshot read throughput ratio too low: got %.6f want >= %.6f (snapshot=%.0f parallel=%.0f)",
			ratio, minSnapshotToParallelRatio, snapshotRead, parallelRead)
	}
	t.Logf("append-only snapshot guardrail ops/s: parallel=%.0f snapshot=%.0f ratio=%.6f",
		parallelRead, snapshotRead, ratio)
}
