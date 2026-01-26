package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestDB_DeferValueLogOps_DoesNotDedupDuplicates(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		AllowUnsafe:    true,
		DisableJournal: true,
		SplitValueLog:  true,
		FlushThreshold: 1 << 30,
		MemtableMode:   "skiplist",
		MemtableShards: 1,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	v1 := bytes.Repeat([]byte{1}, db.valueLogThreshold+1)
	v2 := bytes.Repeat([]byte{2}, db.valueLogThreshold+1)

	ops := []batch.Entry{
		{Type: batch.OpPut, Key: []byte("k"), Value: v1},
		{Type: batch.OpPut, Key: []byte("k"), Value: v2},
	}
	out, err := db.deferValueLogOps(ops, false)
	if err != nil {
		t.Fatalf("deferValueLogOps: %v", err)
	}
	if got, want := len(out), len(ops); got != want {
		t.Fatalf("expected deferValueLogOps to preserve op count (no dedup); got %d want %d", got, want)
	}
	for i := range out {
		if out[i].IsPtr {
			t.Fatalf("op %d expected inline value (copy-on-flush); got IsPtr=true", i)
		}
		if !bytes.Equal(out[i].Value, ops[i].Value) {
			t.Fatalf("op %d value mismatch after deferValueLogOps", i)
		}
	}
}
