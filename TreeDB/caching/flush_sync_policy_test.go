package caching

import (
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

type countingValueWriter struct {
	inner valueWriter

	flushCalls atomic.Int64
	syncCalls  atomic.Int64
}

func (w *countingValueWriter) Append(dictID uint64, dict []byte, rid uint64, value []byte) (page.ValuePtr, error) {
	return w.inner.Append(dictID, dict, rid, value)
}

func (w *countingValueWriter) AppendFrame(dictID uint64, dict []byte, records []valuelog.Record) ([]page.ValuePtr, error) {
	// Should not be called in this test; forward anyway.
	return w.inner.AppendFrame(dictID, dict, records)
}

func (w *countingValueWriter) RotateTo(path string, fileID uint32) error {
	return w.inner.RotateTo(path, fileID)
}
func (w *countingValueWriter) Size() int64 { return w.inner.Size() }

func (w *countingValueWriter) Flush() error {
	w.flushCalls.Add(1)
	// Avoid doing real IO in this regression test; we're only checking policy.
	return nil
}

func (w *countingValueWriter) Sync() error {
	w.syncCalls.Add(1)
	// Avoid fsync in tests; we're only checking whether sync is requested.
	return nil
}

func (w *countingValueWriter) Close() error { return w.inner.Close() }

func TestCachingDB_FlushDoesNotSyncWhenJournalDisabledButValueLogEnabled(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		DisableJournal:           true,
		RelaxedSync:              false,
		AllowUnsafe:              true,
		SplitValueLog:            true,
		MemtableValueLogPointers: true,
		FlushThreshold:           1 << 60,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if got := db.flushSyncRequested(false); got {
		t.Fatalf("flushSyncRequested(false)=true; want false when only journal is disabled")
	}

	// Replace the underlying value-log writer with a counting wrapper so the
	// test can assert whether flushAll(false) attempts to sync.
	l := &db.lanes[0]
	l.vlogMu.Lock()
	if l.vlog == nil {
		l.vlogMu.Unlock()
		t.Fatalf("expected value-log writer to be initialized")
	}
	cw := &countingValueWriter{inner: l.vlog}
	l.vlog = cw
	l.vlogMu.Unlock()

	// Enqueue a memtable so flushAll(false) does real work.
	db.mu.Lock()
	setMutable(db, []byte("k"), []byte("v"))
	if err := db.rotateMemtableLocked(false); err != nil {
		db.mu.Unlock()
		t.Fatalf("rotateMemtableLocked: %v", err)
	}
	db.mu.Unlock()

	db.flushAll(false)

	if got := cw.flushCalls.Load(); got == 0 {
		t.Fatalf("expected value-log Flush to be called during flush")
	}
	if got := cw.syncCalls.Load(); got != 0 {
		t.Fatalf("expected value-log Sync NOT to be called during flushAll(false); got %d", got)
	}
}

func TestCachingDB_FlushStillSyncsInDisableWALLegacyMode(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		DisableWAL:  true,
		RelaxedSync: false,
		AllowUnsafe: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if got := db.flushSyncRequested(false); !got {
		t.Fatalf("flushSyncRequested(false)=false; want true for legacy DisableWAL mode")
	}
}
