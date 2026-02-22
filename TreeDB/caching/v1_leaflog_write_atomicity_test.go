package caching

import (
	"bytes"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

type failingCommitWriter struct {
	err error
}

func (w *failingCommitWriter) Append(commitlog.Record) error        { return w.err }
func (w *failingCommitWriter) AppendBatch([]commitlog.Record) error { return w.err }
func (*failingCommitWriter) RotateTo(string) error                  { return nil }
func (*failingCommitWriter) Size() int64                            { return 0 }
func (*failingCommitWriter) Flush() error                           { return nil }
func (*failingCommitWriter) Sync() error                            { return nil }
func (*failingCommitWriter) Close() error                           { return nil }

func openV1LeafLogAtomicityCache(t *testing.T) (*DB, *MockBackend) {
	t.Helper()
	dir := t.TempDir()
	backend := NewMockBackend()
	cache, err := Open(dir, backend, Options{
		FlushThreshold:           1 << 30,
		MemtableShards:           1,
		JournalLanes:             1,
		ForceValueLogPointers:    true,
		ValueLogPointerThreshold: 1,
		IndexOuterLeafMode:       backenddb.IndexOuterLeafModeV1LeafLog,
	})
	if err != nil {
		t.Fatalf("Open cache: %v", err)
	}
	return cache, backend
}

func installFailingWALWriter(t *testing.T, cache *DB, err error) {
	t.Helper()
	if cache == nil {
		t.Fatalf("cache nil")
	}
	if len(cache.lanes) == 0 {
		t.Fatalf("cache lanes unavailable")
	}
	ln := &cache.lanes[0]
	ln.walMu.Lock()
	ln.wal = &failingCommitWriter{err: err}
	ln.walMu.Unlock()
}

func assertKeyNotVisibleAnywhere(t *testing.T, cache *DB, backend *MockBackend, key []byte) {
	t.Helper()
	got, err := cache.Get(key)
	if err != nil {
		t.Fatalf("cache Get(%q): %v", key, err)
	}
	if got != nil {
		t.Fatalf("cache key %q unexpectedly visible", key)
	}
	backendVal, err := backend.Get(key)
	if err != nil {
		t.Fatalf("backend Get(%q): %v", key, err)
	}
	if backendVal != nil {
		t.Fatalf("backend key %q unexpectedly visible", key)
	}
	shard := cache.shardForKey(key)
	shard.mu.Lock()
	_, found, deleted := shard.mem.Get(key)
	shard.mu.Unlock()
	if found || deleted {
		t.Fatalf("memtable key %q unexpectedly published (found=%v deleted=%v)", key, found, deleted)
	}
}

func TestCachingDB_V1LeafLog_SetAtomicOnWALError(t *testing.T) {
	cache, backend := openV1LeafLogAtomicityCache(t)
	defer cache.Close()

	injectedErr := errors.New("injected wal append failure")
	installFailingWALWriter(t, cache, injectedErr)

	key := []byte("atomic-set-key")
	value := bytes.Repeat([]byte("v"), 1024) // pointer-backed under threshold=1
	if err := cache.Set(key, value); !errors.Is(err, injectedErr) {
		t.Fatalf("Set error=%v, want %v", err, injectedErr)
	}
	assertKeyNotVisibleAnywhere(t, cache, backend, key)
}

func TestCachingDB_V1LeafLog_SetSyncAtomicOnWALError(t *testing.T) {
	cache, backend := openV1LeafLogAtomicityCache(t)
	defer cache.Close()

	injectedErr := errors.New("injected wal append failure")
	installFailingWALWriter(t, cache, injectedErr)

	key := []byte("atomic-setsync-key")
	value := bytes.Repeat([]byte("v"), 1024) // pointer-backed under threshold=1
	if err := cache.SetSync(key, value); !errors.Is(err, injectedErr) {
		t.Fatalf("SetSync error=%v, want %v", err, injectedErr)
	}
	assertKeyNotVisibleAnywhere(t, cache, backend, key)
}

func TestCachingDB_V1LeafLog_BatchWriteAtomicOnWALError(t *testing.T) {
	cache, backend := openV1LeafLogAtomicityCache(t)
	defer cache.Close()

	injectedErr := errors.New("injected wal append failure")
	installFailingWALWriter(t, cache, injectedErr)

	b := cache.NewBatchWithSize(2)
	defer b.Close()
	keyA := []byte("atomic-batch-a")
	keyB := []byte("atomic-batch-b")
	if err := b.Set(keyA, bytes.Repeat([]byte("a"), 1024)); err != nil {
		t.Fatalf("batch Set(a): %v", err)
	}
	if err := b.Set(keyB, bytes.Repeat([]byte("b"), 1024)); err != nil {
		t.Fatalf("batch Set(b): %v", err)
	}
	if err := b.Write(); !errors.Is(err, injectedErr) {
		t.Fatalf("batch Write error=%v, want %v", err, injectedErr)
	}

	assertKeyNotVisibleAnywhere(t, cache, backend, keyA)
	assertKeyNotVisibleAnywhere(t, cache, backend, keyB)
}

func TestCachingDB_V1LeafLog_BatchWriteSyncAtomicOnWALError(t *testing.T) {
	cache, backend := openV1LeafLogAtomicityCache(t)
	defer cache.Close()

	injectedErr := errors.New("injected wal append failure")
	installFailingWALWriter(t, cache, injectedErr)

	b := cache.NewBatchWithSize(2)
	defer b.Close()
	keyA := []byte("atomic-batchsync-a")
	keyB := []byte("atomic-batchsync-b")
	if err := b.Set(keyA, bytes.Repeat([]byte("a"), 1024)); err != nil {
		t.Fatalf("batch Set(a): %v", err)
	}
	if err := b.Set(keyB, bytes.Repeat([]byte("b"), 1024)); err != nil {
		t.Fatalf("batch Set(b): %v", err)
	}
	if err := b.WriteSync(); !errors.Is(err, injectedErr) {
		t.Fatalf("batch WriteSync error=%v, want %v", err, injectedErr)
	}

	assertKeyNotVisibleAnywhere(t, cache, backend, keyA)
	assertKeyNotVisibleAnywhere(t, cache, backend, keyB)
}
