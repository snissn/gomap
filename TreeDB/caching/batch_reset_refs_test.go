package caching

import (
	"bytes"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
)

func TestBatchReset_ClearsReferenceBearingScratch(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64)
	entryBacking := make([]batchpkg.Entry, 2, 4)
	walBacking := make([]logRecord, 2, 4)
	shardBacking := make([]batchpkg.Entry, 2, 4)

	entryBacking[0] = batchpkg.Entry{Type: batchpkg.OpPut, Key: payload[:8], Value: payload[8:16]}
	entryBacking[1] = batchpkg.Entry{Type: batchpkg.OpPut, Key: payload[16:24], Value: payload[24:32]}
	walBacking[0] = logRecord{Op: logOpSetInline, Key: payload[:8], Value: payload[8:16]}
	walBacking[1] = logRecord{Op: logOpDelete, Key: payload[16:24]}
	shardBacking[0] = batchpkg.Entry{Type: batchpkg.OpPut, Key: payload[32:40], Value: payload[40:48]}
	shardBacking[1] = batchpkg.Entry{Type: batchpkg.OpPut, Key: payload[48:56], Value: payload[56:64]}

	b := &Batch{
		entries:      entryBacking[:2],
		walBuf:       walBacking[:2],
		shardEntries: [][]batchpkg.Entry{shardBacking[:2]},
	}

	b.Reset()

	if got := len(b.entries); got != 0 {
		t.Fatalf("entries len=%d want=0", got)
	}
	if got := len(b.walBuf); got != 0 {
		t.Fatalf("wal len=%d want=0", got)
	}
	if got := len(b.shardEntries); got != 0 {
		t.Fatalf("shard entries len=%d want=0", got)
	}

	fullEntries := b.entries[:cap(b.entries)]
	for i := range entryBacking[:2] {
		if fullEntries[i].Key != nil || fullEntries[i].Value != nil {
			t.Fatalf("entry %d retained key/value refs after reset", i)
		}
	}

	fullWAL := b.walBuf[:cap(b.walBuf)]
	for i := range walBacking[:2] {
		if fullWAL[i].Key != nil || fullWAL[i].Value != nil {
			t.Fatalf("wal record %d retained key/value refs after reset", i)
		}
	}

	fullShardEntries := b.shardEntries[:cap(b.shardEntries)]
	if len(fullShardEntries[0]) != 0 {
		t.Fatalf("inner shard entry slice len=%d want=0", len(fullShardEntries[0]))
	}
	fullShardBacking := fullShardEntries[0][:cap(fullShardEntries[0])]
	for i := range shardBacking[:2] {
		if fullShardBacking[i].Key != nil || fullShardBacking[i].Value != nil {
			t.Fatalf("shard entry %d retained key/value refs after reset", i)
		}
	}
}

func TestBatchWrite_ClearsResetScratchRefs(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, NewMockBackend(), Options{
		AllowUnsafe:    true,
		MemtableMode:   "hash_sorted",
		MemtableShards: 2,
		FlushThreshold: 1 << 30,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	firstKey, secondKey := keysOnDistinctMutableShards(t, db)
	firstVal := bytes.Repeat([]byte("a"), 128)
	secondVal := bytes.Repeat([]byte("b"), 128)

	b := db.NewBatchWithSize(2)
	defer func() { _ = b.Close() }()
	if err := b.Set(firstKey, firstVal); err != nil {
		t.Fatalf("set first: %v", err)
	}
	if err := b.Set(secondKey, secondVal); err != nil {
		t.Fatalf("set second: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}

	if cap(b.walBuf) == 0 {
		t.Fatal("expected WAL scratch to be allocated")
	}
	fullWAL := b.walBuf[:cap(b.walBuf)]
	for i := range fullWAL {
		if fullWAL[i].Key != nil || fullWAL[i].Value != nil {
			t.Fatalf("wal scratch retained refs at index %d", i)
		}
	}

	if cap(b.shardEntries) == 0 {
		t.Fatal("expected shard scratch to be allocated")
	}
	fullShardEntries := b.shardEntries[:cap(b.shardEntries)]
	foundInner := false
	for i := range fullShardEntries {
		if cap(fullShardEntries[i]) == 0 {
			continue
		}
		foundInner = true
		fullInner := fullShardEntries[i][:cap(fullShardEntries[i])]
		for j := range fullInner {
			if fullInner[j].Key != nil || fullInner[j].Value != nil {
				t.Fatalf("shard scratch retained refs at [%d][%d]", i, j)
			}
		}
	}
	if !foundInner {
		t.Fatal("expected at least one populated shard scratch slice")
	}
}
