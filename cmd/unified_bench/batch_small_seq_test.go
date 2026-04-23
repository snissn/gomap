package main

import (
	"sync/atomic"
	"testing"

	"github.com/snissn/gomap/kvstore"
)

type batchSmallSeqTrackingDB struct {
	batch *batchSmallSeqTrackingBatch
}

func (d *batchSmallSeqTrackingDB) Name() string { return "BatchSmallSeqTracking" }
func (d *batchSmallSeqTrackingDB) Close() error { return nil }
func (d *batchSmallSeqTrackingDB) Get(key []byte) ([]byte, error) {
	return nil, nil
}
func (d *batchSmallSeqTrackingDB) Set(key, value []byte) error { return nil }
func (d *batchSmallSeqTrackingDB) Delete(key []byte) error     { return nil }
func (d *batchSmallSeqTrackingDB) Checkpoint() error           { return nil }

func (d *batchSmallSeqTrackingDB) NewBatch() (kvstore.Batch, error) {
	return d.NewBatchWithSize(0)
}

func (d *batchSmallSeqTrackingDB) NewBatchWithSize(size int) (kvstore.Batch, error) {
	if d.batch == nil {
		d.batch = &batchSmallSeqTrackingBatch{}
	} else {
		d.batch.Reset()
	}
	return d.batch, nil
}

type batchSmallSeqTrackingBatch struct {
	setCalls          atomic.Int64
	setViewCalls      atomic.Int64
	setStealViewCalls atomic.Int64
	commitCalls       atomic.Int64
}

func (b *batchSmallSeqTrackingBatch) Set(key, value []byte) error {
	b.setCalls.Add(1)
	return nil
}

func (b *batchSmallSeqTrackingBatch) SetView(key, value []byte) error {
	b.setViewCalls.Add(1)
	return nil
}

func (b *batchSmallSeqTrackingBatch) SetStealView(key, value []byte) error {
	b.setStealViewCalls.Add(1)
	return nil
}

func (b *batchSmallSeqTrackingBatch) Delete(key []byte) error { return nil }

func (b *batchSmallSeqTrackingBatch) Commit() error {
	b.commitCalls.Add(1)
	return nil
}

func (b *batchSmallSeqTrackingBatch) CommitSync() error {
	return b.Commit()
}

func (b *batchSmallSeqTrackingBatch) Close() error { return nil }

func (b *batchSmallSeqTrackingBatch) Reset() {}

func TestBatchSmallSeqPrefersSetStealView(t *testing.T) {
	const dbName = "batch_small_seq_tracking"
	var dbRef atomic.Pointer[batchSmallSeqTrackingDB]
	RegisterHiddenDB(dbName, func(dir string) (kvstore.DB, error) {
		db := &batchSmallSeqTrackingDB{}
		dbRef.Store(db)
		return db, nil
	})

	_, err := runBenchmark(BenchConfig{
		Keys:         64,
		ValueSize:    16,
		BatchSize:    8,
		RangeQueries: 0,
		RangeSpan:    0,
		DBsArg:       dbName,
		TestsArg:     "batch_small_seq",
		KeepDir:      false,
		Progress:     false,
		SeedUsed:     1,
	})
	if err != nil {
		t.Fatalf("runBenchmark(batch_small_seq): %v", err)
	}

	db := dbRef.Load()
	if db == nil || db.batch == nil {
		t.Fatal("tracking batch was not created")
	}
	if got := db.batch.setStealViewCalls.Load(); got != 64 {
		t.Fatalf("batch_small_seq used SetStealView %d times; want 64", got)
	}
	if got := db.batch.setViewCalls.Load(); got != 0 {
		t.Fatalf("batch_small_seq used SetView %d times; want 0", got)
	}
	if got := db.batch.setCalls.Load(); got != 0 {
		t.Fatalf("batch_small_seq used Set %d times; want 0", got)
	}
	if got := db.batch.commitCalls.Load(); got == 0 {
		t.Fatal("batch_small_seq did not commit any batches")
	}
}
