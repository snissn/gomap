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
