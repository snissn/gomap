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

// Integration-style regression test that mirrors the unified-bench phases and
// flags excessive bloat if vacuum shrinks the index by a large factor.
func TestCachedBenchBloatVacuum(t *testing.T) {
	keys := 20000
	if v := os.Getenv("TREEDB_TEST_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keys = n
		}
	}

	dir := t.TempDir()
	backend, err := db.Open(db.Options{
		Dir:               dir,
		PreferAppendAlloc: false,
		KeepRecent:        1,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cached, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cached open: %v", err)
	}

	val := bytes.Repeat([]byte("a"), 128)

	// Phase 1: batch write
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after batch write: %v", err)
	}

	// Phase 2: random write
	applyRandomUpdates(t, cached, keys, val, 1)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after random write: %v", err)
	}

	// Phase 3: batch delete
	{
		b := cached.NewBatch()
		for i := 0; i < keys; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Delete(k); err != nil {
				t.Fatalf("delete: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("delete write: %v", err)
		}
		_ = b.Close()
	}
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after delete: %v", err)
	}

	// Phase 4: rewrite (reinsert) so the final on-disk size should roughly
	// correspond to a "full" working set again. If the file is still vastly
	// larger than vacuum, that suggests we are not reclaiming/reusing as expected.
	seedBatches(t, cached, keys, val)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after rewrite: %v", err)
	}

	// Capture diagnostics before closing/vacuuming.
	diagStats := backend.Stats()
	var diagHead uint64
	var diagReclaimable uint64
	if fl, ferr := backend.FreelistStats(); ferr == nil {
		diagHead = fl.Head
		diagReclaimable = fl.ReclaimablePages()
	} else {
		t.Logf("diag: freelist stats error before close: %v", ferr)
	}

	_ = cached.Close()
	_ = backend.Close()

	indexPath := filepath.Join(dir, "index.db")
	infoBefore, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat index.db before vacuum: %v", err)
	}
	sizeBefore := infoBefore.Size()
	if sizeBefore == 0 {
		t.Fatalf("expected non-zero index.db size")
	}

	if err := db.VacuumIndexOffline(db.Options{Dir: dir, KeepRecent: 1}); err != nil {
		t.Fatalf("vacuum offline: %v", err)
	}

	infoAfter, err := os.Stat(indexPath)
	if err != nil {
		t.Fatalf("stat index.db after vacuum: %v", err)
	}
	sizeAfter := infoAfter.Size()
	if sizeAfter == 0 {
		t.Fatalf("expected non-zero index.db size after vacuum")
	}

	// Flag bloat if the vacuum shrinks by more than 2x.
	if sizeBefore > sizeAfter*2 {
		t.Logf("diag(before close): freelist.head=%d reclaimable=%d alloc.freelist=%s alloc.append=%s prune.enabled=%s prune.pages_freed=%s pages.total=%s",
			diagHead,
			diagReclaimable,
			diagStats["treedb.alloc.freelist"],
			diagStats["treedb.alloc.append"],
			diagStats["treedb.prune.enabled"],
			diagStats["treedb.prune.pages_freed"],
			diagStats["treedb.pages.total"],
		)

		t.Fatalf("index bloat detected: before=%d after=%d ratio=%.2f", sizeBefore, sizeAfter, float64(sizeBefore)/float64(sizeAfter))
	}
}

func seedBatches(t *testing.T, cached *DB, keys int, value []byte) {
	t.Helper()
	const batchSize = 256
	for base := 0; base < keys; base += batchSize {
		b := cached.NewBatch()
		limit := base + batchSize
		if limit > keys {
			limit = keys
		}
		for i := base; i < limit; i++ {
			k := []byte{byte(i >> 8), byte(i)}
			if err := b.Set(k, value); err != nil {
				t.Fatalf("set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = b.Close()
	}
}

func applyRandomUpdates(t *testing.T, cached *DB, keys int, value []byte, rounds int) {
	t.Helper()
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < rounds; round++ {
		b := cached.NewBatch()
		for i := 0; i < keys; i++ {
			k := []byte{byte(rng.Intn(keys) >> 8), byte(rng.Intn(keys))}
			if err := b.Set(k, value); err != nil {
				t.Fatalf("random set: %v", err)
			}
		}
		if err := b.WriteSync(); err != nil {
			t.Fatalf("random write: %v", err)
		}
		_ = b.Close()
	}
}
