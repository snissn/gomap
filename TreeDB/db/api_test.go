package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCRUD(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Errorf("Get mismatch: %s", val)
	}

	has, err := db.Has([]byte("key1"))
	if err != nil || !has {
		t.Fatalf("Has failed: err=%v has=%v", err, has)
	}

	if err := db.Set([]byte("key2"), []byte("val2")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()

	count := 0
	for ; it.Valid(); it.Next() {
		count++
	}
	if count != 2 {
		t.Fatalf("Iterator expected 2 items, got %d", count)
	}

	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatal(err)
	}

	val, err = db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Get deleted key returned error: %v", err)
	}
	if val != nil {
		t.Fatalf("Get deleted key should return nil")
	}

	has, err = db.Has([]byte("key1"))
	if err != nil {
		t.Fatalf("Has deleted key returned error: %v", err)
	}
	if has {
		t.Fatalf("Has deleted key should be false")
	}
}

func TestConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v1")); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup

	snap := db.AcquireSnapshot()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer snap.Close()

		for i := 0; i < 100; i++ {
			val, err := snap.Get([]byte(fmt.Sprintf("k%d", i)))
			if err != nil {
				t.Errorf("Snapshot.Get failed: %v", err)
				return
			}
			if !bytes.Equal(val, []byte("v1")) {
				t.Errorf("Snapshot isolation failed for k%d: got %q, want v1", i, val)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = db.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v2"))
		}
	}()

	wg.Wait()

	val, err := db.Get([]byte("k0"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, []byte("v2")) {
		t.Fatalf("Final state should be v2, got %q", val)
	}
}

func TestSnapshotGet_ReturnsSafeCopyForValueLogPointer(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k")
	val := bytes.Repeat([]byte("v"), 4096)
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	vlogPath := filepath.Join(walDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(vlogPath, fileID)
	if err != nil {
		t.Fatalf("new valuelog writer: %v", err)
	}
	ptr, err := vw.Append(0, key, 1, val)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("append valuelog: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("close valuelog writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, vlogPath, fileID)
	rawBatch := db.NewBatch()
	type pointerBatch interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
		Write() error
		Close() error
	}
	b, ok := rawBatch.(pointerBatch)
	if !ok {
		t.Skipf("NewBatch() returned %T, which does not support SetPointer", rawBatch)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("Close pointer batch: %v", err)
		}
	})
	if err := b.SetPointer(key, ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write pointer batch: %v", err)
	}

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer snap.Close()

	got, err := snap.Get(key)
	if err != nil {
		t.Fatalf("Snapshot.Get failed: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("Snapshot.Get mismatch")
	}

	got[0] = 'x'

	gotAgain, err := snap.Get(key)
	if err != nil {
		t.Fatalf("Snapshot.Get second read failed: %v", err)
	}
	if !bytes.Equal(gotAgain, val) {
		t.Fatalf("Snapshot.Get did not return a safe copy")
	}

	unsafeVal, err := snap.GetUnsafe(key)
	if err != nil {
		t.Fatalf("Snapshot.GetUnsafe failed: %v", err)
	}
	if !bytes.Equal(unsafeVal, val) {
		t.Fatalf("Snapshot.GetUnsafe mismatch")
	}
}

func TestIteratorKeyValueViewsAndCopyOwnership(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("k1"), []byte("value1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	if !it.Valid() {
		t.Fatalf("iterator invalid")
	}

	keyView := it.Key()
	valueView := it.Value()
	if !sameBacking(keyView, it.UnsafeKey()) {
		t.Fatalf("Key() should return the current iterator view")
	}
	if !sameBacking(valueView, it.UnsafeValue()) {
		t.Fatalf("Value() should return the current iterator view")
	}

	keyCopy := it.KeyCopy(make([]byte, 0, 16))
	valueCopy := it.ValueCopy(make([]byte, 0, 16))
	if sameBacking(keyCopy, keyView) {
		t.Fatalf("KeyCopy() must return caller-owned bytes")
	}
	if sameBacking(valueCopy, valueView) {
		t.Fatalf("ValueCopy() must return caller-owned bytes")
	}

	keyCopy[0] ^= 0x1
	valueCopy[0] ^= 0x1
	if !bytes.Equal(it.UnsafeKey(), []byte("k1")) {
		t.Fatalf("mutating KeyCopy() changed iterator key view")
	}
	if !bytes.Equal(it.UnsafeValue(), []byte("value1")) {
		t.Fatalf("mutating ValueCopy() changed iterator value view")
	}
}

func TestStatsIncludesWatermarkLagDriftMetric(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stats := db.Stats()
	if _, ok := stats["treedb.publish.watermark.lag_drift_bytes_per_sec"]; !ok {
		t.Fatalf("missing treedb.publish.watermark.lag_drift_bytes_per_sec")
	}
	if _, ok := stats["treedb.publish.system_root.warm_attempts"]; !ok {
		t.Fatalf("missing treedb.publish.system_root.warm_attempts")
	}
	if _, ok := stats["treedb.publish.system_root.warm_rebuild_fallbacks"]; !ok {
		t.Fatalf("missing treedb.publish.system_root.warm_rebuild_fallbacks")
	}
	if _, ok := stats["treedb.root_probe.has_any_sorted.fallback_items"]; !ok {
		t.Fatalf("missing treedb.root_probe.has_any_sorted.fallback_items")
	}
	if _, ok := stats["treedb.root_probe.has_prefixes.fallback_items"]; !ok {
		t.Fatalf("missing treedb.root_probe.has_prefixes.fallback_items")
	}
	if _, ok := stats["treedb.native_fastpath.per_item_key_probe_fallback_count"]; !ok {
		t.Fatalf("missing treedb.native_fastpath.per_item_key_probe_fallback_count")
	}
	if _, ok := stats["treedb.native_fastpath.per_item_prefix_probe_fallback_count"]; !ok {
		t.Fatalf("missing treedb.native_fastpath.per_item_prefix_probe_fallback_count")
	}
	if _, ok := stats["treedb.process.read_path.backend_tree.get_append_inline_hits_total"]; !ok {
		t.Fatalf("missing treedb.process.read_path.backend_tree.get_append_inline_hits_total")
	}
	if _, ok := stats["treedb.process.read_path.backend_tree.get_append_pointer_hits_total"]; !ok {
		t.Fatalf("missing treedb.process.read_path.backend_tree.get_append_pointer_hits_total")
	}
	if _, ok := stats["treedb.process.read_path.backend_tree.getmany.grouped_calls_total"]; !ok {
		t.Fatalf("missing treedb.process.read_path.backend_tree.getmany.grouped_calls_total")
	}
	if _, ok := stats["treedb.process.read_path.backend_tree.getmany.leaf_loads_saved_total"]; !ok {
		t.Fatalf("missing treedb.process.read_path.backend_tree.getmany.leaf_loads_saved_total")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.loads_total"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.loads_total")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hits_total"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache_potential.capacity_1024_hits_total")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache.conflict_evictions"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache.conflict_evictions")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache.capacity_evictions"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache.capacity_evictions")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache.buckets"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache.buckets")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache.ways"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache.ways")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache.read_miss_admission_skips"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache.read_miss_admission_skips")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache.read_miss_admission_lock_skips"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache.read_miss_admission_lock_skips")
	}
	if _, ok := stats["treedb.process.read_path.outer_leaf.cache.read_miss_admission_stores"]; !ok {
		t.Fatalf("missing treedb.process.read_path.outer_leaf.cache.read_miss_admission_stores")
	}
	if _, ok := stats["treedb.vlog.writer_append_buf.pool.max_entries"]; !ok {
		t.Fatalf("missing treedb.vlog.writer_append_buf.pool.max_entries")
	}
	if _, ok := stats["treedb.vlog.writer_append_buf.pool.retained_bytes"]; !ok {
		t.Fatalf("missing treedb.vlog.writer_append_buf.pool.retained_bytes")
	}
}

func TestIteratorOptions_SnapshotCompatibility(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: page.DefaultInlineThreshold,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("k-inline"), []byte("inline")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("encode file id: %v", err)
	}
	vlogPath := filepath.Join(walDir, "value-l0-000001.log")
	vw, err := valuelog.NewWriter(vlogPath, fileID)
	if err != nil {
		t.Fatalf("new valuelog writer: %v", err)
	}
	large := bytes.Repeat([]byte("p"), 8*1024)
	ptr, err := vw.Append(0, []byte("k-pointer"), 1, large)
	if err != nil {
		_ = vw.Close()
		t.Fatalf("append valuelog: %v", err)
	}
	if err := vw.Close(); err != nil {
		t.Fatalf("close valuelog writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, vlogPath, fileID)
	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k-pointer"), ptr); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write pointer batch: %v", err)
	}
	_ = b.Close()

	keysOnly, err := db.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModeKeysOnly})
	if err != nil {
		t.Fatalf("IteratorWithOptions keys-only: %v", err)
	}
	defer keysOnly.Close()

	proj, err := db.IteratorWithOptions(nil, nil, IteratorOptions{Mode: IteratorModePointerProjection})
	if err != nil {
		t.Fatalf("IteratorWithOptions projection: %v", err)
	}
	defer proj.Close()

	if err := db.Delete([]byte("k-pointer")); err != nil {
		t.Fatalf("Delete after iterator acquire: %v", err)
	}

	var seenKeys []string
	for ; keysOnly.Valid(); keysOnly.Next() {
		seenKeys = append(seenKeys, string(keysOnly.Key()))
		if val := keysOnly.Value(); val != nil {
			t.Fatalf("keys-only iterator returned value for key %q", string(keysOnly.Key()))
		}
	}
	if err := keysOnly.Error(); err != nil {
		t.Fatalf("keys-only iterator error: %v", err)
	}
	if len(seenKeys) != 2 {
		t.Fatalf("expected snapshot iterator to see 2 keys, got %d (%v)", len(seenKeys), seenKeys)
	}

	seenPointer := false
	for ; proj.Valid(); proj.Next() {
		val, ptr, flags := proj.UnsafeEntry()
		if string(proj.Key()) == "k-pointer" {
			seenPointer = true
			if flags&node.FlagPointer == 0 {
				t.Fatalf("expected pointer flag for k-pointer")
			}
			if val != nil {
				t.Fatalf("projection expected nil value for pointer key")
			}
			if !page.IsValueLogFileID(ptr.FileID) {
				t.Fatalf("projection expected value-log pointer for k-pointer, got file %d", ptr.FileID)
			}
		}
	}
	if err := proj.Error(); err != nil {
		t.Fatalf("projection iterator error: %v", err)
	}
	if !seenPointer {
		t.Fatalf("projection iterator did not observe k-pointer")
	}
}

func TestValuePlacement_PerDomainThreshold_DefaultFallback(t *testing.T) {
	domains := NormalizeValueLogDomainThresholds([]ValueLogDomainThreshold{
		{Prefix: []byte("hot/"), InlineThreshold: 16},
		{Prefix: []byte("hot/user/"), InlineThreshold: 8},
		{Prefix: []byte("cold/"), InlineThreshold: 1024},
	})
	base := 256

	if got := ResolveInlineThresholdForKey(base, []byte("hot/user/001"), domains); got != 8 {
		t.Fatalf("expected longest-prefix threshold=8, got %d", got)
	}
	if got := ResolveInlineThresholdForKey(base, []byte("hot/other"), domains); got != 16 {
		t.Fatalf("expected hot prefix threshold=16, got %d", got)
	}
	if got := ResolveInlineThresholdForKey(base, []byte("cold/key"), domains); got != 1024 {
		t.Fatalf("expected cold prefix threshold=1024, got %d", got)
	}
	if got := ResolveInlineThresholdForKey(base, []byte("neutral/key"), domains); got != base {
		t.Fatalf("expected fallback threshold=%d, got %d", base, got)
	}
}

func TestNewBatchWithSize_AppliesPerDomainInlineThresholds(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 256,
			DomainInlineThresholds: []ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: 16},
				{Prefix: []byte("cold/"), InlineThreshold: 1024},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	hotKey := []byte("hot/key")
	coldKey := []byte("cold/key")
	defaultKey := []byte("other/key")
	hotValue := bytes.Repeat([]byte("h"), 64)
	coldValue := bytes.Repeat([]byte("c"), 64)
	defaultValue := bytes.Repeat([]byte("d"), 300)

	b := db.NewBatchWithSize(3).(*Batch)
	if err := b.Set(coldKey, coldValue); err != nil {
		_ = b.Close()
		t.Fatalf("batch.Set cold: %v", err)
	}
	if err := b.Set(hotKey, hotValue); !errors.Is(err, batchpkg.ErrValueTooLarge) {
		_ = b.Close()
		t.Fatalf("batch.Set hot err = %v, want %v", err, batchpkg.ErrValueTooLarge)
	}
	if err := b.Set(defaultKey, defaultValue); !errors.Is(err, batchpkg.ErrValueTooLarge) {
		_ = b.Close()
		t.Fatalf("batch.Set default err = %v, want %v", err, batchpkg.ErrValueTooLarge)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch.Close: %v", err)
	}
}

func TestNewBatchWithSize_ResolverUsesBatchSnapshot(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			PointerThreshold: 256,
			DomainInlineThresholds: []ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: 16},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatchWithSize(2).(*Batch)
	// Mutate effective threshold source after batch creation; this should not
	// affect an in-flight batch's threshold resolution for default-domain keys.
	db.adaptive = nil
	db.policy.InlineThreshold = 8

	defaultKey := []byte("other/key")
	defaultValue := bytes.Repeat([]byte("d"), 64)
	if err := b.Set(defaultKey, defaultValue); err != nil {
		_ = b.Close()
		t.Fatalf("batch.Set default with snapshot threshold: %v", err)
	}

	hotKey := []byte("hot/key")
	hotValue := bytes.Repeat([]byte("h"), 64)
	if err := b.Set(hotKey, hotValue); !errors.Is(err, batchpkg.ErrValueTooLarge) {
		_ = b.Close()
		t.Fatalf("batch.Set hot err = %v, want %v", err, batchpkg.ErrValueTooLarge)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("batch.Close: %v", err)
	}
}

func TestNewBatchWithSize_ForcePointersOverridesDomainThresholds(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Dir: dir,
		ValueLog: ValueLogOptions{
			ForcePointers: true,
			DomainInlineThresholds: []ValueLogDomainThreshold{
				{Prefix: []byte("hot/"), InlineThreshold: 1024},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	b := db.NewBatchWithSize(2).(*Batch)
	hotKey := []byte("hot/key")
	hotValue := bytes.Repeat([]byte("h"), 64)
	if err := b.Set(hotKey, hotValue); !errors.Is(err, batchpkg.ErrValueTooLarge) {
		_ = b.Close()
		t.Fatalf("batch.Set hot err = %v, want %v", err, batchpkg.ErrValueTooLarge)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("batch.Close: %v", err)
	}
}

func forceStalePublishedValueLogSetForReadRetryTest(t *testing.T, db *DB) {
	t.Helper()
	if db == nil || db.valueLogManager == nil {
		t.Fatalf("missing db/value log manager")
	}
	db.mu.Lock()
	oldState := db.state.Load()
	if oldState == nil {
		db.mu.Unlock()
		t.Fatalf("missing db state")
	}
	staleSet := &valuelog.Set{Files: map[uint32]*valuelog.File{}}
	staleSet.RefCount.Store(1)
	stale := &DBState{
		CommitSeq:        oldState.CommitSeq,
		RootPageID:       oldState.RootPageID,
		SystemRootPageID: oldState.SystemRootPageID,
		ValueLogSet:      staleSet,
	}
	db.state.Store(stale)
	db.publishSnapshotView(db.idx.Load(), stale, db.valueLogManager)
	db.mu.Unlock()
	if oldState.ValueLogSet != nil {
		if err := db.valueLogManager.Release(oldState.ValueLogSet); err != nil {
			t.Fatalf("release old set: %v", err)
		}
	}
}

func TestGet_RetriesAfterRefreshingStaleValueLogSet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k-retry")
	want := bytes.Repeat([]byte("r"), 256)
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	vlogPath := filepath.Join(walDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(vlogPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptr, err := w.Append(0, nil, 1, want)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, vlogPath, fileID)

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer(key, ptr); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()
	if _, err := db.Get(key); err != nil {
		t.Fatalf("initial Get: %v", err)
	}

	forceStalePublishedValueLogSetForReadRetryTest(t, db)
	before := db.valueLogManager.RefreshScanCount()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get after stale state: %v", err)
	}
	after := db.valueLogManager.RefreshScanCount()
	if after <= before {
		t.Fatalf("expected Get retry to refresh value-log set: before=%d after=%d", before, after)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get mismatch after retry")
	}
}

func TestGetMany_RetriesAfterRefreshingStaleValueLogSet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	keys := make([][]byte, 160)
	want := make([][]byte, len(keys))
	ptrs := make([]page.ValuePtr, len(keys))
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	vlogPath := filepath.Join(walDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(vlogPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("k-many-%03d", i))
		want[i] = bytes.Repeat([]byte{byte('a' + (i % 26))}, 192)
		ptr, appendErr := w.Append(0, nil, uint64(i+1), want[i])
		if appendErr != nil {
			_ = w.Close()
			t.Fatalf("Append %d: %v", i, appendErr)
		}
		ptrs[i] = ptr
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, vlogPath, fileID)

	b := db.NewBatch().(*Batch)
	for i := range keys {
		if err := b.SetPointer(keys[i], ptrs[i]); err != nil {
			_ = b.Close()
			t.Fatalf("SetPointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()

	forceStalePublishedValueLogSetForReadRetryTest(t, db)
	before := db.valueLogManager.RefreshScanCount()
	got, err := db.GetMany(keys)
	if err != nil {
		t.Fatalf("GetMany after stale state: %v", err)
	}
	after := db.valueLogManager.RefreshScanCount()
	if after <= before {
		t.Fatalf("expected GetMany retry to refresh value-log set: before=%d after=%d", before, after)
	}
	if len(got) != len(want) {
		t.Fatalf("GetMany len mismatch: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("GetMany[%d] mismatch", i)
		}
	}
}

func TestGet_ConcurrentStaleReadRetry_DedupesRefresh(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := []byte("k-retry-concurrent")
	want := bytes.Repeat([]byte("x"), 320)
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	vlogPath := filepath.Join(walDir, "value-l0-000001.log")
	w, err := valuelog.NewWriter(vlogPath, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptr, err := w.Append(0, nil, 1, want)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	registerTestValueLogProducer(t, dir, vlogPath, fileID)

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer(key, ptr); err != nil {
		_ = b.Close()
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		_ = b.Close()
		t.Fatalf("Write: %v", err)
	}
	_ = b.Close()

	// Baseline read before forcing stale publication.
	if got, err := db.Get(key); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("initial Get: err=%v match=%v", err, bytes.Equal(got, want))
	}

	forceStalePublishedValueLogSetForReadRetryTest(t, db)
	beforeScans := db.valueLogManager.RefreshScanCount()

	const workers = 48
	start := make(chan struct{})
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			got, err := db.Get(key)
			if err != nil {
				errCh <- err
				return
			}
			if !bytes.Equal(got, want) {
				errCh <- fmt.Errorf("value mismatch after concurrent retry")
				return
			}
			errCh <- nil
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent Get: %v", err)
		}
	}

	afterScans := db.valueLogManager.RefreshScanCount()
	if delta := afterScans - beforeScans; delta != 1 {
		t.Fatalf("expected one refresh scan for concurrent stale retries: before=%d after=%d delta=%d leaders=%d followers=%d skipped_epoch=%d",
			beforeScans, afterScans, delta,
			db.readRetryRefreshLeaderCount.Load(),
			db.readRetryRefreshFollowerCount.Load(),
			db.readRetryRefreshSkippedEpoch.Load(),
		)
	}
	if leaders := db.readRetryRefreshLeaderCount.Load(); leaders != 1 {
		t.Fatalf("expected one read-retry refresh leader call, got %d", leaders)
	}
}
