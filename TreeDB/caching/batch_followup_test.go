package caching

import (
	"bytes"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
)

func statUint64Followup(t *testing.T, stats map[string]string, key string) uint64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing stats key %q", key)
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", key, raw, err)
	}
	return n
}

func TestBatchEntryHint_AdaptiveDecayAndClamp(t *testing.T) {
	var cache DB

	if got := cache.batchEntriesCapHint(0); got != batchDefaultEntriesCap {
		t.Fatalf("initial cap hint=%d want %d", got, batchDefaultEntriesCap)
	}

	cache.observeBatchEntries(4096)
	high := int(cache.batchEntryHint.Load())
	if high != 4096 {
		t.Fatalf("high hint=%d want 4096", high)
	}

	for i := 0; i < 24; i++ {
		cache.observeBatchEntries(batchDefaultEntriesCap)
	}
	low := int(cache.batchEntryHint.Load())
	if low >= high {
		t.Fatalf("hint did not decay: high=%d low=%d", high, low)
	}
	if low < batchDefaultEntriesCap {
		t.Fatalf("hint dropped below default: %d", low)
	}

	cache.observeBatchEntries(batchHintEntriesMax * 8)
	if got := int(cache.batchEntryHint.Load()); got > batchHintEntriesMax {
		t.Fatalf("hint exceeded max: got=%d max=%d", got, batchHintEntriesMax)
	}

	cache.batchEntryHint.Store(512)
	b := cache.NewBatch()
	defer b.Close()
	if cap(b.entries) < 512 {
		t.Fatalf("NewBatch cap=%d want >=512", cap(b.entries))
	}
}

func TestBatchWrite_DeleteOnlyFastPath_SortedAndUnsorted(t *testing.T) {
	run := func(t *testing.T, order []string) {
		t.Helper()
		dir := t.TempDir()
		backend := NewMockBackend()
		cache, err := Open(dir, backend, Options{
			DisableWAL:     true,
			AllowUnsafe:    true,
			FlushThreshold: 1 << 20,
		})
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer cache.Close()

		for _, key := range []string{"a", "b", "c"} {
			if err := cache.Set([]byte(key), []byte("v-"+key)); err != nil {
				t.Fatalf("seed Set(%q): %v", key, err)
			}
		}

		b := cache.NewBatch()
		for _, key := range order {
			if err := b.Delete([]byte(key)); err != nil {
				_ = b.Close()
				t.Fatalf("Delete(%q): %v", key, err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("Write: %v", err)
		}
		_ = b.Close()

		for _, key := range []string{"a", "b"} {
			val, err := cache.Get([]byte(key))
			if err != nil {
				t.Fatalf("Get(%q): %v", key, err)
			}
			if val != nil {
				t.Fatalf("Get(%q)=%q want nil after delete", key, val)
			}
		}
		valC, err := cache.Get([]byte("c"))
		if err != nil {
			t.Fatalf("Get(c): %v", err)
		}
		if !bytes.Equal(valC, []byte("v-c")) {
			t.Fatalf("Get(c)=%q want %q", valC, []byte("v-c"))
		}
	}

	t.Run("sorted", func(t *testing.T) { run(t, []string{"a", "b"}) })
	t.Run("unsorted", func(t *testing.T) { run(t, []string{"b", "a"}) })
}

func TestBatchWrite_DeleteOnlyFastPath_WALOnWritesDeleteOpsOnly(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:    true,
		FlushThreshold: 1 << 20,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	b := cache.NewBatch()
	for _, key := range []string{"k3", "k1", "k2"} {
		if err := b.Delete([]byte(key)); err != nil {
			_ = b.Close()
			t.Fatalf("Delete(%q): %v", key, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		_ = b.Close()
		t.Fatalf("WriteSync: %v", err)
	}
	_ = b.Close()

	inlineOps, ridOps, deleteOps := countCommitLogOpsInDir(t, filepath.Join(dir, "wal"))
	if inlineOps != 0 || ridOps != 0 {
		t.Fatalf("unexpected non-delete commit ops: inline=%d rid=%d", inlineOps, ridOps)
	}
	if deleteOps != 3 {
		t.Fatalf("delete ops=%d want 3", deleteOps)
	}
}

func TestBatchWrite_DeleteOnlyFastPath_WALOffDoesNotEnqueueDeferredStats(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{
		Dir:                dir,
		IndexOuterLeafMode: db.IndexOuterLeafModeV2FencePtr,
		ValueLog: db.ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		AllowUnsafe:              true,
		DisableWAL:               true,
		FlushThreshold:           1 << 20,
		IndexOuterLeafMode:       db.IndexOuterLeafModeV2FencePtr,
		ValueLogPointerThreshold: 1,
		ForceValueLogPointers:    true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}
	defer cache.Close()

	before := cache.Stats()
	beforeKeys := statUint64Followup(t, before, "treedb.cache.v2_fenceptr.deferred_enqueued_keys")
	beforeBytes := statUint64Followup(t, before, "treedb.cache.v2_fenceptr.deferred_enqueued_bytes")

	if err := cache.Set([]byte("seed"), bytes.Repeat([]byte("v"), 256)); err != nil {
		t.Fatalf("seed Set: %v", err)
	}
	mid := cache.Stats()
	midKeys := statUint64Followup(t, mid, "treedb.cache.v2_fenceptr.deferred_enqueued_keys")
	midBytes := statUint64Followup(t, mid, "treedb.cache.v2_fenceptr.deferred_enqueued_bytes")
	if midKeys <= beforeKeys || midBytes <= beforeBytes {
		t.Fatalf("expected put path to advance deferred counters: before=(%d,%d) mid=(%d,%d)", beforeKeys, beforeBytes, midKeys, midBytes)
	}

	b := cache.NewBatch()
	if err := b.Delete([]byte("seed")); err != nil {
		_ = b.Close()
		t.Fatalf("Delete(seed): %v", err)
	}
	if err := b.Delete([]byte("missing")); err != nil {
		_ = b.Close()
		t.Fatalf("Delete(missing): %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()

	after := cache.Stats()
	afterKeys := statUint64Followup(t, after, "treedb.cache.v2_fenceptr.deferred_enqueued_keys")
	afterBytes := statUint64Followup(t, after, "treedb.cache.v2_fenceptr.deferred_enqueued_bytes")
	if afterKeys != midKeys || afterBytes != midBytes {
		t.Fatalf("delete-only batch changed deferred counters: before-delete=(%d,%d) after=(%d,%d)", midKeys, midBytes, afterKeys, afterBytes)
	}
}
