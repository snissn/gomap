package caching

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

type failingBatchWriter struct {
	err          error
	batchCalls   int
	appendCalls  int
	rotateCalls  int
	flushCalls   int
	syncCalls    int
	closeCalls   int
	latestFileID uint32
}

func (w *failingBatchWriter) Append(op byte, key, value []byte) (page.ValuePtr, error) {
	w.appendCalls++
	return page.ValuePtr{}, nil
}

func (w *failingBatchWriter) AppendBatch(records []logRecord) ([]page.ValuePtr, error) {
	w.batchCalls++
	return nil, w.err
}

func (w *failingBatchWriter) RotateTo(_ string, fileID uint32) error {
	w.rotateCalls++
	w.latestFileID = fileID
	return nil
}

func (w *failingBatchWriter) Size() int64 { return 0 }

func (w *failingBatchWriter) Flush() error {
	w.flushCalls++
	return nil
}

func (w *failingBatchWriter) Sync() error {
	w.syncCalls++
	return nil
}

func (w *failingBatchWriter) Close() error {
	w.closeCalls++
	return nil
}

func TestCachingDB_DeleteRange_WALAppendBatchFailure(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{
		FlushThreshold:  1 << 30,
		DisableValueLog: true,
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

	failErr := errors.New("append batch failed")
	stub := &failingBatchWriter{err: failErr}
	db.mu.Lock()
	db.wal = stub
	db.mu.Unlock()

	if err := db.DeleteRange([]byte("a"), []byte("z")); !errors.Is(err, failErr) {
		t.Fatalf("expected delete range error %v, got %v", failErr, err)
	}
	if stub.batchCalls == 0 {
		t.Fatalf("expected AppendBatch to be called")
	}

	if err := db.Set([]byte("c"), []byte("3")); !errors.Is(err, failErr) {
		t.Fatalf("expected WAL error on Set, got %v", err)
	}
}
