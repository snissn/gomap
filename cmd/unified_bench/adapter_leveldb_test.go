package main

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/kvstore"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func newLevelDBBatchForTest(t *testing.T, db kvstore.DB) *LevelDBBatch {
	t.Helper()
	batcher, ok := db.(kvstore.Batcher)
	if !ok {
		t.Fatalf("LevelDB wrapper missing Batcher")
	}
	batch, err := batcher.NewBatch()
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	lb, ok := batch.(*LevelDBBatch)
	if !ok {
		_ = batch.Close()
		t.Fatalf("NewBatch returned %T, want *LevelDBBatch", batch)
	}
	t.Cleanup(func() { _ = lb.Close() })
	return lb
}

func TestLevelDBBatchPointOpsUseDirectBatchPath(t *testing.T) {
	db, err := NewLevelDB(t.TempDir())
	if err != nil {
		t.Fatalf("NewLevelDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	batch := newLevelDBBatchForTest(t, db)
	if err := batch.Set([]byte("set"), []byte("value")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := batch.Delete([]byte("delete")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if len(batch.ranges) != 0 {
		t.Fatalf("point-only batch should not buffer range ops, got %+v", batch.ranges)
	}
	if got := batch.batch.Len(); got != 2 {
		t.Fatalf("direct leveldb.Batch len=%d, want 2", got)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	val, err := db.Get([]byte("set"))
	if err != nil {
		t.Fatalf("get set key: %v", err)
	}
	if string(val) != "value" {
		t.Fatalf("set key value=%q want value", val)
	}
}

func TestLevelDBBatchDeleteRangeRangeOnlyAndRejectsMixedOps(t *testing.T) {
	db, err := NewLevelDB(t.TempDir())
	if err != nil {
		t.Fatalf("NewLevelDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, key := range [][]byte{[]byte("k0"), []byte("k1"), []byte("z0")} {
		if err := db.Set(key, []byte("committed")); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
	}

	batch := newLevelDBBatchForTest(t, db)
	if err := batch.DeleteRange([]byte("k"), []byte("l")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	for _, key := range [][]byte{[]byte("k0"), []byte("k1")} {
		if val, err := db.Get(key); !errors.Is(err, leveldb.ErrNotFound) {
			t.Fatalf("%q should be deleted by range, got value=%q err=%v", key, val, err)
		}
	}
	if val, err := db.Get([]byte("z0")); err != nil || string(val) != "committed" {
		t.Fatalf("z0 should survive, value=%q err=%v", val, err)
	}

	pointThenRange := newLevelDBBatchForTest(t, db)
	if err := pointThenRange.Set([]byte("m0"), []byte("point")); err != nil {
		t.Fatalf("Set before mixed range: %v", err)
	}
	if err := pointThenRange.DeleteRange([]byte("m"), []byte("n")); err == nil {
		t.Fatalf("DeleteRange after point op should fail closed")
	}

	rangeThenPoint := newLevelDBBatchForTest(t, db)
	if err := rangeThenPoint.DeleteRange([]byte("m"), []byte("n")); err != nil {
		t.Fatalf("range setup: %v", err)
	}
	if err := rangeThenPoint.Set([]byte("m0"), []byte("point")); err == nil {
		t.Fatalf("Set after DeleteRange should fail closed")
	}
	if err := rangeThenPoint.Delete([]byte("m1")); err == nil {
		t.Fatalf("Delete after DeleteRange should fail closed")
	}
}

func TestLevelDBBatchDeleteRangePreservesEmptyEndBound(t *testing.T) {
	db, err := NewLevelDB(t.TempDir())
	if err != nil {
		t.Fatalf("NewLevelDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	seed := func(key, value []byte) {
		t.Helper()
		if err := db.Set(key, value); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
	}
	assertPresent := func(key []byte) {
		t.Helper()
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("key %q should be present, got err=%v", key, err)
		}
		if string(val) != string(key)+"-value" {
			t.Fatalf("key %q value=%q", key, val)
		}
	}
	commitRange := func(start, end []byte) {
		t.Helper()
		batcher, ok := db.(kvstore.Batcher)
		if !ok {
			t.Fatalf("LevelDB wrapper missing Batcher")
		}
		batch, err := batcher.NewBatch()
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		defer func() { _ = batch.Close() }()
		deleter, ok := batch.(kvstore.BatchRangeDeleter)
		if !ok {
			t.Fatalf("LevelDB batch missing BatchRangeDeleter")
		}
		if err := deleter.DeleteRange(start, end); err != nil {
			t.Fatalf("DeleteRange: %v", err)
		}
		if err := batch.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}

	keys := [][]byte{[]byte(""), []byte("a")}
	for _, key := range keys {
		seed(key, append(append([]byte(nil), key...), []byte("-value")...))
	}

	commitRange(nil, []byte{})
	for _, key := range keys {
		assertPresent(key)
	}

	commitRange(nil, nil)
	for _, key := range keys {
		if val, err := db.Get(key); !errors.Is(err, leveldb.ErrNotFound) {
			t.Fatalf("key %q should be deleted by unbounded range, got value=%q err=%v", key, val, err)
		}
	}
}

func TestLevelDBBenchOptions_BlockSizeFlag(t *testing.T) {
	prev := *leveldbBlockSize
	defer func() { *leveldbBlockSize = prev }()

	*leveldbBlockSize = 8 << 10
	opts := leveldbBenchOptions(opt.NoCompression)
	if got := opts.GetBlockSize(); got != 8<<10 {
		t.Fatalf("GetBlockSize()=%d, want %d", got, 8<<10)
	}

	*leveldbBlockSize = -1
	opts = leveldbBenchOptions(opt.NoCompression)
	if got := opts.GetBlockSize(); got != 4096 {
		t.Fatalf("GetBlockSize()=%d, want default %d", got, 4096)
	}
}
