package treedbadapter

import (
	"bytes"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestAdapterAcquireReadSnapshot_IncludesCachedWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{
		Dir:            dir,
		FlushThreshold: 1 << 30,
		MemtableMode:   "append_only",
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	adapter := Wrap(db)
	key := []byte("k1")
	v1 := bytes.Repeat([]byte("a"), 64)
	v2 := bytes.Repeat([]byte("b"), 64)

	if err := adapter.Set(key, v1); err != nil {
		t.Fatalf("set v1: %v", err)
	}

	snap, err := adapter.AcquireReadSnapshot()
	if err != nil {
		t.Fatalf("AcquireReadSnapshot: %v", err)
	}
	t.Cleanup(func() { _ = snap.Close() })

	got, err := snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get v1: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("snapshot get v1 mismatch: got=%q want=%q", got, v1)
	}

	if err := adapter.Set(key, v2); err != nil {
		t.Fatalf("set v2: %v", err)
	}

	got, err = snap.Get(key)
	if err != nil {
		t.Fatalf("snapshot get after overwrite: %v", err)
	}
	if !bytes.Equal(got, v1) {
		t.Fatalf("snapshot isolation mismatch after overwrite: got=%q want=%q", got, v1)
	}
}
