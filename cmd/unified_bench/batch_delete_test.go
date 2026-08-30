package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/kvstore"
)

func registerHiddenDBForTest(t *testing.T, name string, factory DBFactory) {
	t.Helper()
	prev, hadPrev := dbFactories[name]
	RegisterHiddenDB(name, factory)
	t.Cleanup(func() {
		if hadPrev {
			dbFactories[name] = prev
			return
		}
		delete(dbFactories, name)
	})
}

type batchDeleteFastPathDB struct {
	newBatchCalls         int
	newBatchWithSizeCalls int
	setCalls              int
	batches               []*batchDeleteFastPathBatch
}

func (d *batchDeleteFastPathDB) Name() string                   { return "BatchDeleteFastPath" }
func (d *batchDeleteFastPathDB) Close() error                   { return nil }
func (d *batchDeleteFastPathDB) Get(key []byte) ([]byte, error) { return nil, nil }
func (d *batchDeleteFastPathDB) Delete(key []byte) error        { return nil }
func (d *batchDeleteFastPathDB) Set(key, value []byte) error    { d.setCalls++; return nil }
func (d *batchDeleteFastPathDB) NewBatch() (kvstore.Batch, error) {
	d.newBatchCalls++
	return d.newTrackedBatch(), nil
}
func (d *batchDeleteFastPathDB) newTrackedBatch() *batchDeleteFastPathBatch {
	b := &batchDeleteFastPathBatch{}
	d.batches = append(d.batches, b)
	return b
}

func (d *batchDeleteFastPathDB) NewBatchWithSize(size int) (kvstore.Batch, error) {
	d.newBatchWithSizeCalls++
	b := d.newTrackedBatch()
	b.sizeHint = size
	return b, nil
}

type batchDeleteFastPathBatch struct {
	sizeHint        int
	deleteCalls     int
	deleteViewCalls int
	commitCalls     int
	resetCalls      int
	closeCalls      int
}

func (b *batchDeleteFastPathBatch) Set(key, value []byte) error { return nil }
func (b *batchDeleteFastPathBatch) Delete(key []byte) error {
	b.deleteCalls++
	return nil
}
func (b *batchDeleteFastPathBatch) DeleteView(key []byte) error {
	b.deleteViewCalls++
	return nil
}
func (b *batchDeleteFastPathBatch) Commit() error {
	b.commitCalls++
	return nil
}
func (b *batchDeleteFastPathBatch) CommitSync() error { return b.Commit() }
func (b *batchDeleteFastPathBatch) Close() error {
	b.closeCalls++
	return nil
}
func (b *batchDeleteFastPathBatch) Reset() { b.resetCalls++ }

type batchDeleteFallbackDB struct {
	newBatchWithSizeCalls int
	setCalls              int
	batches               []*batchDeleteFallbackBatch
}

func (d *batchDeleteFallbackDB) Name() string                   { return "BatchDeleteFallback" }
func (d *batchDeleteFallbackDB) Close() error                   { return nil }
func (d *batchDeleteFallbackDB) Get(key []byte) ([]byte, error) { return nil, nil }
func (d *batchDeleteFallbackDB) Delete(key []byte) error        { return nil }
func (d *batchDeleteFallbackDB) Set(key, value []byte) error    { d.setCalls++; return nil }
func (d *batchDeleteFallbackDB) NewBatch() (kvstore.Batch, error) {
	return d.NewBatchWithSize(0)
}
func (d *batchDeleteFallbackDB) NewBatchWithSize(size int) (kvstore.Batch, error) {
	d.newBatchWithSizeCalls++
	b := &batchDeleteFallbackBatch{sizeHint: size}
	d.batches = append(d.batches, b)
	return b, nil
}

type batchDeleteFallbackBatch struct {
	sizeHint    int
	deleteCalls int
	commitCalls int
	closeCalls  int
}

func (b *batchDeleteFallbackBatch) Set(key, value []byte) error { return nil }
func (b *batchDeleteFallbackBatch) Delete(key []byte) error {
	b.deleteCalls++
	return nil
}
func (b *batchDeleteFallbackBatch) Commit() error {
	b.commitCalls++
	return nil
}
func (b *batchDeleteFallbackBatch) CommitSync() error { return b.Commit() }
func (b *batchDeleteFallbackBatch) Close() error {
	b.closeCalls++
	return nil
}

type batchDeleteErrorDB struct {
	batch *batchDeleteErrorBatch
}

func (d *batchDeleteErrorDB) Name() string                   { return "BatchDeleteError" }
func (d *batchDeleteErrorDB) Close() error                   { return nil }
func (d *batchDeleteErrorDB) Get(key []byte) ([]byte, error) { return nil, nil }
func (d *batchDeleteErrorDB) Delete(key []byte) error        { return nil }
func (d *batchDeleteErrorDB) Set(key, value []byte) error    { return nil }
func (d *batchDeleteErrorDB) NewBatch() (kvstore.Batch, error) {
	return d.NewBatchWithSize(0)
}
func (d *batchDeleteErrorDB) NewBatchWithSize(size int) (kvstore.Batch, error) {
	d.batch = &batchDeleteErrorBatch{}
	return d.batch, nil
}

type batchDeleteErrorBatch struct {
	deleteCalls int
	closeCalls  int
}

func (b *batchDeleteErrorBatch) Set(key, value []byte) error { return nil }
func (b *batchDeleteErrorBatch) Delete(key []byte) error {
	b.deleteCalls++
	return errors.New("delete failed")
}
func (b *batchDeleteErrorBatch) Commit() error     { return nil }
func (b *batchDeleteErrorBatch) CommitSync() error { return b.Commit() }
func (b *batchDeleteErrorBatch) Close() error {
	b.closeCalls++
	return nil
}

type batchDeleteNilBatchDB struct{}

func (d *batchDeleteNilBatchDB) Name() string                   { return "BatchDeleteNilBatch" }
func (d *batchDeleteNilBatchDB) Close() error                   { return nil }
func (d *batchDeleteNilBatchDB) Get(key []byte) ([]byte, error) { return nil, nil }
func (d *batchDeleteNilBatchDB) Delete(key []byte) error        { return nil }
func (d *batchDeleteNilBatchDB) Set(key, value []byte) error    { return nil }
func (d *batchDeleteNilBatchDB) NewBatch() (kvstore.Batch, error) {
	return nil, nil
}

func TestRunBenchmark_BatchDelete_ReusesResettableDeleteViewBatch(t *testing.T) {
	const dbName = "batch_delete_fast_path_mock"
	var db *batchDeleteFastPathDB
	registerHiddenDBForTest(t, dbName, func(_ string) (kvstore.DB, error) {
		db = &batchDeleteFastPathDB{}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         5,
		ValueSize:    16,
		BatchSize:    2,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "batch_delete",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if got := run.Results["batch_delete"]["BatchDeleteFastPath"]; got <= 0 {
		t.Fatalf("batch_delete result=%v want > 0", got)
	}
	if db == nil {
		t.Fatal("expected db to be initialized")
	}
	if got, want := db.setCalls, 5; got != want {
		t.Fatalf("preload Set calls=%d want=%d", got, want)
	}
	if got, want := db.newBatchCalls, 0; got != want {
		t.Fatalf("NewBatch calls=%d want=%d", got, want)
	}
	if got, want := db.newBatchWithSizeCalls, 1; got != want {
		t.Fatalf("NewBatchWithSize calls=%d want=%d", got, want)
	}
	if got, want := len(db.batches), 1; got != want {
		t.Fatalf("batch instances=%d want=%d", got, want)
	}
	b := db.batches[0]
	if got, want := b.sizeHint, 2; got != want {
		t.Fatalf("size hint=%d want=%d", got, want)
	}
	if got, want := b.deleteViewCalls, 5; got != want {
		t.Fatalf("DeleteView calls=%d want=%d", got, want)
	}
	if got, want := b.deleteCalls, 0; got != want {
		t.Fatalf("Delete calls=%d want=%d", got, want)
	}
	if got, want := b.commitCalls, 3; got != want {
		t.Fatalf("Commit calls=%d want=%d", got, want)
	}
	if got, want := b.resetCalls, 2; got != want {
		t.Fatalf("Reset calls=%d want=%d", got, want)
	}
	if got, want := b.closeCalls, 1; got != want {
		t.Fatalf("Close calls=%d want=%d", got, want)
	}
}

func TestRunBenchmark_BatchDelete_ClosesOnceOnDeleteError(t *testing.T) {
	const dbName = "batch_delete_error_mock"
	var db *batchDeleteErrorDB
	registerHiddenDBForTest(t, dbName, func(_ string) (kvstore.DB, error) {
		db = &batchDeleteErrorDB{}
		return db, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:         1,
		ValueSize:    16,
		BatchSize:    1,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "batch_delete",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err == nil {
		t.Fatal("expected batch_delete error")
	}
	if db == nil || db.batch == nil {
		t.Fatal("expected failing batch to be created")
	}
	if got, want := db.batch.deleteCalls, 1; got != want {
		t.Fatalf("Delete calls=%d want=%d", got, want)
	}
	if got, want := db.batch.closeCalls, 1; got != want {
		t.Fatalf("Close calls=%d want=%d", got, want)
	}
}

func TestRunBenchmark_BatchDelete_FallsBackWithoutDeleteViewOrReset(t *testing.T) {
	const dbName = "batch_delete_fallback_mock"
	var db *batchDeleteFallbackDB
	registerHiddenDBForTest(t, dbName, func(_ string) (kvstore.DB, error) {
		db = &batchDeleteFallbackDB{}
		return db, nil
	})

	run, err := runBenchmark(BenchConfig{
		Keys:         5,
		ValueSize:    16,
		BatchSize:    2,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "batch_delete",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark: %v", err)
	}
	if got := run.Results["batch_delete"]["BatchDeleteFallback"]; got <= 0 {
		t.Fatalf("batch_delete result=%v want > 0", got)
	}
	if db == nil {
		t.Fatal("expected db to be initialized")
	}
	if got, want := db.setCalls, 5; got != want {
		t.Fatalf("preload Set calls=%d want=%d", got, want)
	}
	if got, want := db.newBatchWithSizeCalls, 3; got != want {
		t.Fatalf("NewBatchWithSize calls=%d want=%d", got, want)
	}
	if got, want := len(db.batches), 3; got != want {
		t.Fatalf("batch instances=%d want=%d", got, want)
	}
	for i, b := range db.batches {
		if got, want := b.sizeHint, 2; got != want {
			t.Fatalf("batch %d size hint=%d want=%d", i, got, want)
		}
		if got := b.deleteCalls; got == 0 {
			t.Fatalf("batch %d Delete calls=%d want > 0", i, got)
		}
		if got, want := b.commitCalls, 1; got != want {
			t.Fatalf("batch %d Commit calls=%d want=%d", i, got, want)
		}
		if got, want := b.closeCalls, 1; got != want {
			t.Fatalf("batch %d Close calls=%d want=%d", i, got, want)
		}
	}
}

func TestRunBenchmark_BatchDelete_NilBatchReturnsError(t *testing.T) {
	const dbName = "batch_delete_nil_batch_mock"
	registerHiddenDBForTest(t, dbName, func(_ string) (kvstore.DB, error) {
		return &batchDeleteNilBatchDB{}, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:         1,
		ValueSize:    16,
		BatchSize:    1,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "batch_delete",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err == nil {
		t.Fatal("expected nil batch error")
	}
	if !strings.Contains(err.Error(), "new batch returned nil") {
		t.Fatalf("error=%q want nil batch message", err)
	}
}
