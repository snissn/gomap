package caching

import (
	"bytes"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

// Regression guard for the "high-watermark growth under KeepRecent churn" case.
//
// The intent is not to prove perfect compaction, but to ensure that enabling
// backend micro-batching during *background flush* materially reduces append
// allocations (and thus index.db growth) versus a single giant backend commit.
func TestCachedHighWatermark_MicroBatchReducesAppend(t *testing.T) {
	keys := 50000
	if v := os.Getenv("TREEDB_TEST_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keys = n
		}
	}

	run := func(t *testing.T, flushBackendMaxEntries int) (appendDeltaRandom uint64, appendAlloc uint64, size int64) {
		t.Helper()

		dir := t.TempDir()
		backend, err := db.Open(db.Options{
			Dir:               dir,
			PreferAppendAlloc: false,
			KeepRecent:        1,
			// Use cached-mode friendly pruning defaults (background enabled).
		})
		if err != nil {
			t.Fatalf("backend open: %v", err)
		}

		cached, err := Open(dir, backend, Options{
			// Force frequent memtable rotations so the phases create queued work that
			// must be drained by background flush (sync=false). This matches the
			// unified-bench random_write pressure case.
			FlushThreshold:         1 << 20, // 1MiB
			DisableWAL:             true,
			RelaxedSync:            true,
			AllowUnsafe:            true,
			FlushBackendMaxEntries: flushBackendMaxEntries,
		})
		if err != nil {
			_ = backend.Close()
			t.Fatalf("cached open: %v", err)
		}

		val := bytes.Repeat([]byte("a"), 128)

		flushBackground := func() {
			// Drain queued memtables via background flush (sync=false). This is where
			// backend micro-batching should reduce peak allocator demand.
			cached.flushSomeBlocking(false, 1<<30, 0)
		}

		logPhase := func(phase string) {
			rep, err := backend.FragmentationReport()
			if err != nil {
				t.Logf("phase=%s frag_error=%v", phase, err)
				return
			}
			stats := backend.Stats()
			var head uint64
			var reclaimable uint64
			if fl, ferr := backend.FreelistStats(); ferr == nil {
				head = fl.Head
				reclaimable = fl.ReclaimablePages()
			}
			info, _ := os.Stat(filepath.Join(dir, "index.db"))
			var fsz int64
			if info != nil {
				fsz = info.Size()
			}
			t.Logf("phase=%s index.db=%d alloc.append=%s alloc.freelist=%s freelist.head=%d reclaimable=%d user.pages=%s leaf.avg=%s leaf.p50=%s leaf.min=%s pages.total=%s span_ratio=%s",
				phase,
				fsz,
				stats["treedb.alloc.append"],
				stats["treedb.alloc.freelist"],
				head,
				reclaimable,
				rep["treedb.user.pages"],
				rep["treedb.user.leaf_fill_ppm_avg"],
				rep["treedb.user.leaf_fill_ppm_p50"],
				rep["treedb.user.leaf_fill_ppm_min"],
				rep["treedb.pages.total"],
				rep["treedb.user.pages.span_ratio_ppm"],
			)
		}

		seedBatchesAsync := func(keys int) {
			const batchSize = 256
			for base := 0; base < keys; base += batchSize {
				b := cached.NewBatch()
				limit := base + batchSize
				if limit > keys {
					limit = keys
				}
				for i := base; i < limit; i++ {
					k := []byte{byte(i >> 8), byte(i)}
					if err := b.Set(k, val); err != nil {
						t.Fatalf("set: %v", err)
					}
				}
				if err := b.Write(); err != nil {
					t.Fatalf("write: %v", err)
				}
				_ = b.Close()
			}
		}

		applyRandomUpdatesAsync := func(keys, rounds int) {
			rng := rand.New(rand.NewSource(1))
			for round := 0; round < rounds; round++ {
				b := cached.NewBatch()
				for i := 0; i < keys; i++ {
					k := []byte{byte(rng.Intn(keys) >> 8), byte(rng.Intn(keys))}
					if err := b.Set(k, val); err != nil {
						t.Fatalf("random set: %v", err)
					}
				}
				if err := b.Write(); err != nil {
					t.Fatalf("random write: %v", err)
				}
				_ = b.Close()
			}
		}

		deleteAllAsync := func(keys int) {
			b := cached.NewBatch()
			for i := 0; i < keys; i++ {
				k := []byte{byte(i >> 8), byte(i)}
				if err := b.Delete(k); err != nil {
					t.Fatalf("delete: %v", err)
				}
			}
			if err := b.Write(); err != nil {
				t.Fatalf("delete write: %v", err)
			}
			_ = b.Close()
		}

		// Phase 1: batch write (async) -> background flush -> checkpoint
		seedBatchesAsync(keys)
		flushBackground()
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("checkpoint after batch write: %v", err)
		}
		logPhase("after_batch_write")

		statsBefore := backend.Stats()
		appendBefore, _ := strconv.ParseUint(statsBefore["treedb.alloc.append"], 10, 64)

		// Phase 2: random write (async) -> background flush -> checkpoint
		applyRandomUpdatesAsync(keys, 1)
		flushBackground()
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("checkpoint after random write: %v", err)
		}
		logPhase("after_random_write")

		statsAfter := backend.Stats()
		appendAfter, _ := strconv.ParseUint(statsAfter["treedb.alloc.append"], 10, 64)
		appendDeltaRandom = appendAfter - appendBefore

		// Phase 3: batch delete (async) -> background flush -> checkpoint
		deleteAllAsync(keys)
		flushBackground()
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("checkpoint after delete: %v", err)
		}
		logPhase("after_delete")

		// Phase 4: rewrite (async) -> background flush -> checkpoint
		seedBatchesAsync(keys)
		flushBackground()
		if err := cached.Checkpoint(); err != nil {
			t.Fatalf("checkpoint after rewrite: %v", err)
		}
		logPhase("after_rewrite")

		stats := backend.Stats()
		appendStr := stats["treedb.alloc.append"]
		appendAlloc, _ = strconv.ParseUint(appendStr, 10, 64)

		info, err := os.Stat(filepath.Join(dir, "index.db"))
		if err != nil {
			t.Fatalf("stat index.db: %v", err)
		}
		size = info.Size()

		_ = cached.Close()
		_ = backend.Close()
		return appendDeltaRandom, appendAlloc, size
	}

	// "Disabled" micro-batching: a single giant backend commit per flush.
	appendNoRandom, appendNo, sizeNo := run(t, -1)
	// "Enabled" micro-batching: smaller commits so pages retired earlier become
	// eligible for reuse sooner under KeepRecent.
	appendYesRandom, appendYes, sizeYes := run(t, 4096)

	// The exact amounts depend on OS/filesystem, but micro-batching should
	// materially reduce append allocations for this churny workload.
	if appendYes > appendNo {
		t.Fatalf("expected micro-batching to not increase append allocs (disabled=%d enabled=%d)", appendNo, appendYes)
	}
	// Require at least a modest improvement so we catch regressions.
	if appendNo > 0 && appendYes*100 > appendNo*90 {
		t.Fatalf("expected micro-batching to reduce append allocs by >=10%% (disabled=%d enabled=%d)", appendNo, appendYes)
	}
	if appendYesRandom > appendNoRandom {
		t.Fatalf("expected micro-batching to not increase random_write append delta (disabled=%d enabled=%d)", appendNoRandom, appendYesRandom)
	}
	if sizeYes > sizeNo {
		t.Fatalf("expected micro-batching to not increase index.db size (disabled=%d enabled=%d)", sizeNo, sizeYes)
	}
}
