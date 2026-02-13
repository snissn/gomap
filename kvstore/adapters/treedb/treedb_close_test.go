package treedbadapter

import (
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestAdapterGetAfterCloseDoesNotError(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	adapter := Wrap(db)
	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got, err := adapter.Get([]byte("missing")); err != nil {
		t.Fatalf("get after close err=%v got=%q", err, got)
	}
	if got, err := adapter.GetUnsafe([]byte("missing")); err != nil {
		t.Fatalf("get unsafe after close err=%v got=%q", err, got)
	}
	if got, err := adapter.GetAppend([]byte("missing"), []byte("x")); err != nil {
		t.Fatalf("get append after close err=%v got=%q", err, got)
	}
	if ok, err := adapter.Has([]byte("missing")); err != nil || ok {
		t.Fatalf("has after close err=%v ok=%v", err, ok)
	}
}

func TestAdapterReadBatch_IgnoresMissingAndDuplicates(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set k2: %v", err)
	}

	adapter := WrapNamed(db, "TreeDB")
	adapter.SetReadWorkers(8)
	err = adapter.ReadBatch([][]byte{
		[]byte("k1"),
		[]byte("missing"),
		[]byte("k1"),
		[]byte("k2"),
		[]byte("missing2"),
	})
	if err != nil {
		t.Fatalf("readbatch: %v", err)
	}
}

func TestAdapterReadBatch_ClampsWorkerCount(t *testing.T) {
	dir := t.TempDir()
	db, err := treedb.Open(treedb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("set k: %v", err)
	}

	adapter := WrapNamed(db, "TreeDB")
	adapter.SetReadWorkers(-1)
	if got := int(adapter.readWorkers.Load()); got != 1 {
		t.Fatalf("expected resolved readWorkers=%d got=%d", 1, got)
	}

	if err := adapter.ReadBatch([][]byte{[]byte("k"), []byte("missing")}); err != nil {
		t.Fatalf("readbatch: %v", err)
	}
}
