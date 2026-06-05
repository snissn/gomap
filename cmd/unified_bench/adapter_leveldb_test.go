package main

import (
	"errors"
	"testing"

	"github.com/snissn/gomap/kvstore"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestLevelDBBatchDeleteRangeMixedOpsOrderedSemantics(t *testing.T) {
	db, err := NewLevelDB(t.TempDir())
	if err != nil {
		t.Fatalf("NewLevelDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Set([]byte("k0"), []byte("committed")); err != nil {
		t.Fatalf("seed committed key: %v", err)
	}
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

	if err := batch.Set([]byte("k1"), []byte("before-range")); err != nil {
		t.Fatalf("batch set before range: %v", err)
	}
	if err := deleter.DeleteRange([]byte("k"), []byte("l")); err != nil {
		t.Fatalf("DeleteRange: %v", err)
	}
	if err := batch.Set([]byte("k2"), []byte("after-range")); err != nil {
		t.Fatalf("batch set after range: %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	for _, key := range [][]byte{[]byte("k0"), []byte("k1")} {
		if val, err := db.Get(key); !errors.Is(err, leveldb.ErrNotFound) {
			t.Fatalf("%q should be deleted by range, got value=%q err=%v", key, val, err)
		}
	}
	val, err := db.Get([]byte("k2"))
	if err != nil {
		t.Fatalf("k2 should survive after-range set: %v", err)
	}
	if string(val) != "after-range" {
		t.Fatalf("k2 value=%q want after-range", val)
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
