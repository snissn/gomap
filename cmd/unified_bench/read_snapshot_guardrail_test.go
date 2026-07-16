package main

import (
	"fmt"
	"math"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestRunBenchmark_ReadSnapshotAppendOnlyGuardrail(t *testing.T) {
	withTreeDBFastReadRequireHitFlags(t)

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

func TestRunBenchmark_ProfileFastReadRequireHitBeforeCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("profile-fast read-require-hit regression guardrail is intentionally heavy")
	}
	withTreeDBFastReadRequireHitFlags(t)

	for attempt := 1; attempt <= 3; attempt++ {
		attempt := attempt
		t.Run(fmt.Sprintf("attempt_%d", attempt), func(t *testing.T) {
			_, err := runBenchmark(BenchConfig{
				Keys:           160_000,
				ValueSize:      128,
				BatchSize:      8_000,
				ReadWorkers:    8,
				ReadRequireHit: true,
				RangeQueries:   0,
				RangeSpan:      0,
				DBsArg:         "treedb",
				TestsArg:       "sequential_write,random_read_parallel",
				Profile:        "fast",
				KeepDir:        false,
				Progress:       false,
				SeedUsed:       1,
				MaxWall:        30 * time.Second,
			})
			if err != nil {
				t.Fatalf("TreeDB read-after-write hit contract failed before checkpoint: %v", err)
			}
		})
	}
}

func withTreeDBFastReadRequireHitFlags(t *testing.T) {
	t.Helper()

	prevReadWorkers := *readWorkers
	prevAllowUnsafe := *treedbAllowUnsafe
	prevDisableWAL := *treedbDisableWAL
	prevRelaxedSync := *treedbRelaxedSync
	prevDisableReadChecksum := *treedbDisableReadChecksum
	prevIndexOptimizations := *treedbIndexOptimizations
	prevIndexOuterLeavesInVlog := *treedbIndexOuterLeavesInVlog
	prevPreferAppendAlloc := *treedbPreferAppendAlloc
	prevVlogCompression := *treedbVlogCompression
	prevVlogBlockCodec := *treedbVlogBlockCodec
	prevVlogAutoPolicy := *treedbVlogAutoPolicy
	prevVlogCompressionAutotune := *treedbVlogCompressionAutotune
	prevVlogDictIncompressibleHoldBytes := *treedbVlogDictIncompressibleHoldBytes
	prevVlogDictProbeIntervalBytes := *treedbVlogDictProbeIntervalBytes
	prevVlogGenerationPolicy := *treedbVlogGenerationPolicy
	prevFlushThreshold := *treedbFlushThreshold
	prevMaxQueuedMems := *treedbMaxQueuedMems
	prevMemtableMode := *treedbMemtableMode
	prevForcePointers := *treedbForceValuePointers
	prevValueLogThreshold := *treedbValueLogThreshold

	t.Cleanup(func() {
		*readWorkers = prevReadWorkers
		*treedbAllowUnsafe = prevAllowUnsafe
		*treedbDisableWAL = prevDisableWAL
		*treedbRelaxedSync = prevRelaxedSync
		*treedbDisableReadChecksum = prevDisableReadChecksum
		*treedbIndexOptimizations = prevIndexOptimizations
		*treedbIndexOuterLeavesInVlog = prevIndexOuterLeavesInVlog
		*treedbPreferAppendAlloc = prevPreferAppendAlloc
		*treedbVlogCompression = prevVlogCompression
		*treedbVlogBlockCodec = prevVlogBlockCodec
		*treedbVlogAutoPolicy = prevVlogAutoPolicy
		*treedbVlogCompressionAutotune = prevVlogCompressionAutotune
		*treedbVlogDictIncompressibleHoldBytes = prevVlogDictIncompressibleHoldBytes
		*treedbVlogDictProbeIntervalBytes = prevVlogDictProbeIntervalBytes
		*treedbVlogGenerationPolicy = prevVlogGenerationPolicy
		*treedbFlushThreshold = prevFlushThreshold
		*treedbMaxQueuedMems = prevMaxQueuedMems
		*treedbMemtableMode = prevMemtableMode
		*treedbForceValuePointers = prevForcePointers
		*treedbValueLogThreshold = prevValueLogThreshold
	})

	applyTreeDBProfileIfUnset(treedb.ProfileBenchUnsafe, map[string]bool{})

	*readWorkers = 8
	*treedbAllowUnsafe = true
	*treedbVlogGenerationPolicy = "default"
	*treedbFlushThreshold = 64 << 20
	*treedbMaxQueuedMems = 0
	*treedbMemtableMode = ""
	*treedbForceValuePointers = false
	*treedbValueLogThreshold = 0
}
