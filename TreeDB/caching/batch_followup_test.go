package caching

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/db"
)

func batchFollowupBE8Key(i uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, i)
	return key
}

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

func TestBatchWrite_DeleteOnlyEmptyDBNoOp_WALOff(t *testing.T) {
	run := func(t *testing.T, useView bool) {
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

		b := cache.NewBatch()
		for _, key := range []string{"k3", "k1", "k2"} {
			var err error
			if useView {
				err = b.DeleteView([]byte(key))
			} else {
				err = b.Delete([]byte(key))
			}
			if err != nil {
				_ = b.Close()
				t.Fatalf("Delete(%q): %v", key, err)
			}
		}
		if err := b.Write(); err != nil {
			_ = b.Close()
			t.Fatalf("Write: %v", err)
		}
		_ = b.Close()

		if got := backend.writeCalls; got != 0 {
			t.Fatalf("backend write calls=%d want 0", got)
		}
		if got := cache.mutableBytes.Load(); got != 0 {
			t.Fatalf("mutableBytes=%d want 0", got)
		}

		cache.mu.RLock()
		queueLen := len(cache.queue)
		backendRangeKnown := cache.backendRangeKnown
		backendRangeValid := cache.backendRange.valid
		cache.mu.RUnlock()

		if queueLen != 0 {
			t.Fatalf("queue len=%d want 0", queueLen)
		}
		if !backendRangeKnown || backendRangeValid {
			t.Fatalf("backend range known=%t valid=%t want known empty backend", backendRangeKnown, backendRangeValid)
		}

		for i := range cache.mutableShards {
			shard := &cache.mutableShards[i]
			shard.mu.Lock()
			got := shard.mem.Len()
			shard.mu.Unlock()
			if got != 0 {
				t.Fatalf("mutable shard %d len=%d want 0", i, got)
			}
		}
	}

	t.Run("delete", func(t *testing.T) { run(t, false) })
	t.Run("delete_view", func(t *testing.T) { run(t, true) })
}

func TestBatchWrite_DeleteOnlyEmptyDBNoOp_LockOrder(t *testing.T) {
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

	b := cache.NewBatch()
	if err := b.Delete([]byte("missing")); err != nil {
		_ = b.Close()
		t.Fatalf("Delete: %v", err)
	}

	cache.flushMu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- b.Write()
	}()

	heldWriteMu := false
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if cache.writeMu.TryLock() {
			cache.writeMu.Unlock()
			time.Sleep(time.Millisecond)
			continue
		}
		heldWriteMu = true
		break
	}
	cache.flushMu.Unlock()

	if err := <-done; err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()
	if heldWriteMu {
		t.Fatalf("delete-only empty DB fast path acquired writeMu before flushMu")
	}
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

func TestBatchWrite_SortedBatchCreatesSingleMutableRoute(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                true,
		DisableWAL:                 true,
		RelaxedSync:                true,
		MemtableMode:               "append_only",
		MemtableShards:             8,
		FlushThreshold:             1 << 30,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	b := cache.NewBatchWithSize(512)
	defer func() { _ = b.Close() }()
	for i := uint64(0); i < 512; i++ {
		key := batchFollowupBE8Key(i)
		value := append([]byte("v-"), key...)
		if err := b.Set(key, value); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	if err := b.WriteSync(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	route := cache.currentMutableRoute.Load()
	if route == nil {
		t.Fatal("expected current mutable route after sorted batch")
	}
	routeEntries := route.normalizedEntries()
	if len(routeEntries) != 1 {
		t.Fatalf("route entries=%d want 1", len(routeEntries))
	}

	var crossShardKey []byte
	crossShardRoute := -1
	for _, entry := range routeEntries {
		for i := uint64(0); i < 512; i++ {
			key := batchFollowupBE8Key(i)
			if !keyInRange(entry.rng, key) {
				continue
			}
			if cache.hashShardIndex(key) != int(entry.shardID) {
				crossShardKey = key
				crossShardRoute = int(entry.shardID)
				break
			}
		}
		if crossShardKey != nil {
			break
		}
	}
	if crossShardKey == nil {
		t.Fatal("expected at least one routed key whose hash shard differs from route shard")
	}

	if got := cache.shardIndexForWrite(crossShardKey); got != crossShardRoute {
		t.Fatalf("shardIndexForWrite=%d want route shard %d", got, crossShardRoute)
	}
	if _, _, found := cache.mutableShards[cache.hashShardIndex(crossShardKey)].mem.Get(crossShardKey); found {
		t.Fatal("hashed shard unexpectedly owns routed key")
	}
	if _, _, found := cache.mutableShards[crossShardRoute].mem.Get(crossShardKey); !found {
		t.Fatal("route shard does not own routed key")
	}

	got, err := cache.Get(crossShardKey)
	if err != nil {
		t.Fatalf("Get(routed): %v", err)
	}
	if want := append([]byte("v-"), crossShardKey...); !bytes.Equal(got, want) {
		t.Fatalf("Get(routed)=%q want %q", got, want)
	}
}

func TestBatchWrite_SortedBatchRotatesSingleRoutedShard(t *testing.T) {
	dir := t.TempDir()
	backend, err := db.Open(db.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}
	cache, err := Open(dir, backend, Options{
		AllowUnsafe:                true,
		DisableWAL:                 true,
		RelaxedSync:                true,
		MemtableMode:               "append_only",
		MemtableShards:             8,
		FlushThreshold:             1 << 10,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cache.Close()

	b := cache.NewBatchWithSize(512)
	defer func() { _ = b.Close() }()
	value := bytes.Repeat([]byte("x"), 128)
	for i := uint64(0); i < 512; i++ {
		key := batchFollowupBE8Key(i)
		if err := b.Set(key, value); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	cache.flushMu.Lock()
	defer cache.flushMu.Unlock()
	if err := b.WriteSync(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	cache.mu.RLock()
	queueLen := len(cache.queue)
	queueShardIDs := append([]uint16(nil), cache.queueShardIDs...)
	queueRouteModes := append([]uint8(nil), cache.queueRouteModes...)
	cache.mu.RUnlock()

	if queueLen != 1 {
		t.Fatalf("queue len=%d want 1", queueLen)
	}
	if len(queueRouteModes) != queueLen {
		t.Fatalf("queue route modes len=%d want %d", len(queueRouteModes), queueLen)
	}
	for i, mode := range queueRouteModes {
		if mode != memtableRouteRanged {
			t.Fatalf("queue route mode[%d]=%d want %d", i, mode, memtableRouteRanged)
		}
	}
	if route := cache.currentMutableRoute.Load(); route != nil {
		t.Fatalf("expected current mutable route to clear after routed rotation, got entries=%d", len(route.normalizedEntries()))
	}

	var routedKey []byte
	for _, queuedShardID := range queueShardIDs {
		queuedShard := int(queuedShardID)
		for i := uint64(0); i < 512; i++ {
			key := batchFollowupBE8Key(i)
			if cache.hashShardIndex(key) != queuedShard {
				routedKey = key
				break
			}
		}
		if routedKey != nil {
			break
		}
	}
	if routedKey == nil {
		t.Fatal("expected queued routed key whose hash shard differs from queued shard")
	}

	got, err := cache.Get(routedKey)
	if err != nil {
		t.Fatalf("Get(queued routed): %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get(queued routed) len=%d want %d", len(got), len(value))
	}
}
