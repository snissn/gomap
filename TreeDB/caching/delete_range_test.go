package caching

import (
	"errors"
	"testing"
	"time"

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
func (w *countingLogWriter) RotateToWithSync(_ string, _ bool) error {
	return nil
}
func (w *countingLogWriter) Size() int64 { return 0 }
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

func TestCachingDB_DeleteRange_WALFlushesOnlySelectedLane(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold: 1 << 30,
		JournalLanes:   4,
		MemtableShards: 4,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for i := 0; i < 64; i++ {
		key := []byte{byte('a' + (i % 26)), byte('0' + byte(i%10)), byte('x'), byte('0' + byte((i/10)%10))}
		if err := db.Set(key, []byte("v")); err != nil {
			t.Fatalf("Set(%q): %v", key, err)
		}
	}

	stubs := make([]*countingLogWriter, len(db.lanes))
	db.mu.Lock()
	origWALs := make([]commitWriter, len(db.lanes))
	for i := range db.lanes {
		stub := &countingLogWriter{}
		stubs[i] = stub
		l := &db.lanes[i]
		l.walMu.Lock()
		origWALs[i] = l.wal
		l.wal = stub
		l.walMu.Unlock()
	}
	db.mu.Unlock()
	defer func() {
		db.mu.Lock()
		for i := range db.lanes {
			l := &db.lanes[i]
			l.walMu.Lock()
			l.wal = origWALs[i]
			l.walMu.Unlock()
		}
		db.mu.Unlock()
	}()

	if err := db.DeleteRange([]byte("a"), []byte("z")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}

	totalAppends := 0
	totalFlushes := 0
	totalSyncs := 0
	activeLaneCount := 0
	for i, stub := range stubs {
		totalAppends += stub.appendCalls
		totalFlushes += stub.flushCalls
		totalSyncs += stub.syncCalls
		if stub.appendCalls > 0 || stub.flushCalls > 0 || stub.syncCalls > 0 {
			activeLaneCount++
		}
		if stub.flushCalls > 1 {
			t.Fatalf("lane %d flush calls=%d want <= 1", i, stub.flushCalls)
		}
	}
	if totalAppends == 0 {
		t.Fatalf("expected WAL appends for range deletes")
	}
	if totalFlushes != 1 {
		t.Fatalf("expected exactly one lane flush per DeleteRange call, got total flushes=%d", totalFlushes)
	}
	if activeLaneCount != 1 {
		t.Fatalf("expected DeleteRange to use exactly one WAL lane, active lanes=%d", activeLaneCount)
	}
	if totalSyncs != 0 {
		t.Fatalf("expected no WAL sync calls for non-sync DeleteRange, got %d", totalSyncs)
	}
}

func TestCachingBatchDeleteRangeSerializesScanAndPublish(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	backend.Set([]byte("b"), []byte("old"))
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	backend.iteratorStartedCh = make(chan struct{})
	backend.iteratorBlockCh = make(chan struct{})

	rangeBatch := db.NewBatch()
	if err := rangeBatch.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	rangeDone := make(chan error, 1)
	go func() {
		rangeDone <- rangeBatch.Write()
		_ = rangeBatch.Close()
	}()

	select {
	case <-backend.iteratorStartedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("range batch did not start backend iterator")
	}

	writerDone := make(chan error, 1)
	go func() {
		b := db.NewBatch()
		defer b.Close()
		if err := b.Set([]byte("b"), []byte("updated")); err != nil {
			writerDone <- err
			return
		}
		if err := b.Set([]byte("c"), []byte("new")); err != nil {
			writerDone <- err
			return
		}
		writerDone <- b.Write()
	}()

	select {
	case err := <-writerDone:
		t.Fatalf("concurrent writer completed before blocked range scan published: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(backend.iteratorBlockCh)
	select {
	case err := <-rangeDone:
		if err != nil {
			t.Fatalf("range batch Write: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for range batch")
	}
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatalf("concurrent writer: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for concurrent writer")
	}

	if val, err := db.Get([]byte("b")); err != nil || string(val) != "updated" {
		t.Fatalf("b=(%q,%v), want updated", val, err)
	}
	if val, err := db.Get([]byte("c")); err != nil || string(val) != "new" {
		t.Fatalf("c=(%q,%v), want new", val, err)
	}
}

func TestCachingBatchDeleteRangeMaterializationCapFailsClosed(t *testing.T) {
	oldEntries := cachedBatchDeleteRangeMaterializeMaxEntries
	oldBytes := cachedBatchDeleteRangeMaterializeMaxKeyBytes
	cachedBatchDeleteRangeMaterializeMaxEntries = 2
	cachedBatchDeleteRangeMaterializeMaxKeyBytes = defaultCachedBatchDeleteRangeMaterializeMaxKeyBytes
	defer func() {
		cachedBatchDeleteRangeMaterializeMaxEntries = oldEntries
		cachedBatchDeleteRangeMaterializeMaxKeyBytes = oldBytes
	}()

	dir := t.TempDir()
	backend := NewMockBackend()
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		backend.Set([]byte(kv.k), []byte(kv.v))
	}
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	b := db.NewBatch()
	defer b.Close()
	if err := b.DeleteRange([]byte("a"), []byte("z")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := b.Write(); !errors.Is(err, ErrBatchDeleteRangeTooLarge) {
		t.Fatalf("Write err=%v, want ErrBatchDeleteRangeTooLarge", err)
	}
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		val, err := db.Get([]byte(kv.k))
		if err != nil || string(val) != kv.v {
			t.Fatalf("%s=(%q,%v), want %q", kv.k, val, err, kv.v)
		}
	}
}

func TestCachingBatchDeleteRangeMixedOrder(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()
	db, err := Open(dir, backend, Options{FlushThreshold: 1 << 20, JournalLanes: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	for _, kv := range []struct{ k, v string }{{"a", "va"}, {"b", "vb"}, {"c", "vc"}, {"d", "vd"}} {
		if err := db.Set([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Set %s: %v", kv.k, err)
		}
	}

	b := db.NewBatch()
	if err := b.Set([]byte("b"), []byte("shadowed")); err != nil {
		t.Fatalf("batch Set shadowed: %v", err)
	}
	if err := b.DeleteRange([]byte("a"), []byte("d")); err != nil {
		t.Fatalf("batch DeleteRange: %v", err)
	}
	if err := b.Set([]byte("c"), []byte("after")); err != nil {
		t.Fatalf("batch Set after: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("batch Write: %v", err)
	}
	_ = b.Close()

	if val, err := db.Get([]byte("a")); err != nil || val != nil {
		t.Fatalf("a=(%q,%v), want missing", val, err)
	}
	if val, err := db.Get([]byte("b")); err != nil || val != nil {
		t.Fatalf("b=(%q,%v), want missing", val, err)
	}
	if val, err := db.Get([]byte("c")); err != nil || string(val) != "after" {
		t.Fatalf("c=(%q,%v), want after", val, err)
	}
	if val, err := db.Get([]byte("d")); err != nil || string(val) != "vd" {
		t.Fatalf("d=(%q,%v), want vd", val, err)
	}
}
