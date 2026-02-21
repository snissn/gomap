package caching

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type countingLogWriter struct {
	appendCalls int
	batchCalls  int
	flushCalls  int
	syncCalls   int
}

func (w *countingLogWriter) Append(record commitlog.Record) error {
	w.appendCalls++
	return nil
}

func (w *countingLogWriter) AppendBatch(records []commitlog.Record) error {
	w.batchCalls++
	return errors.New("append batch not expected")
}

func (w *countingLogWriter) RotateTo(_ string) error { return nil }
func (w *countingLogWriter) Size() int64             { return 0 }
func (w *countingLogWriter) Flush() error {
	w.flushCalls++
	return nil
}
func (w *countingLogWriter) Sync() error {
	w.syncCalls++
	return nil
}
func (w *countingLogWriter) Close() error { return nil }

type errMemtable struct {
	memtable.Table
	deleteErr error
}

func (m *errMemtable) DeleteWithCallback(_ []byte, _ func(k, v []byte) error) error {
	return m.deleteErr
}

func TestCachingDB_DeleteRange_WALApplyFailurePoisonsWrites(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := db.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}

	it, err := db.Iterator(nil, nil)
	if err != nil {
		t.Fatalf("Iterator: %v", err)
	}
	if !it.Valid() {
		_ = it.Close()
		t.Fatalf("expected iterator to see keys before delete range")
	}
	_ = it.Close()

	stub := &countingLogWriter{}
	failErr := errors.New("apply delete failed")

	db.mu.Lock()
	l := &db.lanes[0]
	l.walMu.Lock()
	origWal := l.wal
	l.wal = stub
	l.walMu.Unlock()
	for i := range db.mutableShards {
		shard := &db.mutableShards[i]
		shard.mu.Lock()
		shard.mem = &errMemtable{Table: shard.mem, deleteErr: failErr}
		shard.mu.Unlock()
	}
	db.mu.Unlock()

	if err := db.DeleteRange([]byte("a"), []byte("z")); !errors.Is(err, failErr) {
		t.Fatalf("expected delete range error %v, got %v", failErr, err)
	}

	db.mu.Lock()
	l.walMu.Lock()
	l.wal = origWal
	l.walMu.Unlock()
	db.mu.Unlock()
	if stub.appendCalls == 0 {
		t.Fatalf("expected Append to be called")
	}
	if stub.batchCalls != 0 {
		t.Fatalf("unexpected AppendBatch calls: %d", stub.batchCalls)
	}

	if err := db.Set([]byte("c"), []byte("3")); !errors.Is(err, failErr) {
		t.Fatalf("expected WAL error on Set, got %v", err)
	}
}

func TestCachingDB_DeleteRange_WALFlushesAfterDeletes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Set(a): %v", err)
	}
	if err := db.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Set(b): %v", err)
	}
	if err := db.Set([]byte("z"), []byte("9")); err != nil {
		t.Fatalf("Set(z): %v", err)
	}

	stub := &countingLogWriter{}
	db.mu.Lock()
	l := &db.lanes[0]
	l.walMu.Lock()
	origWal := l.wal
	l.wal = stub
	l.walMu.Unlock()
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		l.walMu.Lock()
		l.wal = origWal
		l.walMu.Unlock()
		db.mu.Unlock()
	}()

	if err := db.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	if stub.appendCalls == 0 {
		t.Fatalf("expected WAL Append calls for range deletes")
	}
	if stub.flushCalls != 1 {
		t.Fatalf("expected single WAL Flush after range delete, got %d", stub.flushCalls)
	}
	if stub.syncCalls != 0 {
		t.Fatalf("expected no WAL Sync for non-sync delete range, got %d", stub.syncCalls)
	}
}

func TestCachingDB_DeleteRange_WALSkipsFlushWhenNoDeletes(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("m"), []byte("1")); err != nil {
		t.Fatalf("Set(m): %v", err)
	}

	stub := &countingLogWriter{}
	db.mu.Lock()
	l := &db.lanes[0]
	l.walMu.Lock()
	origWal := l.wal
	l.wal = stub
	l.walMu.Unlock()
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		l.walMu.Lock()
		l.wal = origWal
		l.walMu.Unlock()
		db.mu.Unlock()
	}()

	if err := db.DeleteRange([]byte("a"), []byte("b")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	if stub.appendCalls != 0 {
		t.Fatalf("expected no WAL Append calls when range has no keys, got %d", stub.appendCalls)
	}
	if stub.flushCalls != 0 {
		t.Fatalf("expected no WAL Flush when range has no keys, got %d", stub.flushCalls)
	}
}
