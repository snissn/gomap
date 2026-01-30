package caching

import (
	"bytes"
	"math/rand"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func TestCachedReuseIncreasesFreelistAllocations(t *testing.T) {
	dir := t.TempDir()

	backend, err := db.Open(db.Options{
		Dir:                    dir,
		PreferAppendAlloc:      false,
		KeepRecent:             1,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	defer backend.Close()

	cached, err := Open(dir, backend, Options{FlushThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("cached open: %v", err)
	}
	defer cached.Close()

	const keys = 2000
	valA := bytes.Repeat([]byte("a"), 64)
	valB := bytes.Repeat([]byte("b"), 64)

	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after seed: %v", err)
	}
	backend.Prune()

	applyRandomUpdatesReuse(t, cached, keys, valB, 1)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after random: %v", err)
	}
	backend.Prune()

	// Delete all keys.
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
	backend.Prune()

	rep, err := backend.FragmentationReport()
	if err != nil {
		t.Fatalf("FragmentationReport: %v", err)
	}
	reclaimable := parseReportUintReuse(t, rep, "treedb.freelist.reclaimable_pages")
	if reclaimable == 0 {
		t.Fatalf("expected reclaimable pages after deletes")
	}

	beforeStats := backend.Stats()
	freelistBefore := parseReportUintReuse(t, beforeStats, "treedb.alloc.freelist")

	seedBatchesReuse(t, cached, keys, valA)
	if err := cached.Checkpoint(); err != nil {
		t.Fatalf("checkpoint after reuse: %v", err)
	}
	backend.Prune()

	afterStats := backend.Stats()
	freelistAfter := parseReportUintReuse(t, afterStats, "treedb.alloc.freelist")
	if freelistAfter <= freelistBefore {
		t.Fatalf("expected freelist reuse to increase (before=%d after=%d)", freelistBefore, freelistAfter)
	}
}

func seedBatchesReuse(t *testing.T, cached *DB, keys int, value []byte) {
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

func applyRandomUpdatesReuse(t *testing.T, cached *DB, keys int, value []byte, rounds int) {
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

func parseReportUintReuse(t *testing.T, rep map[string]string, key string) uint64 {
	t.Helper()
	valStr := rep[key]
	if valStr == "" {
		t.Fatalf("missing %s", key)
	}
	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		t.Fatalf("parse %s: %v", key, err)
	}
	return val
}
