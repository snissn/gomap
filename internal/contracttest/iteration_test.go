package contracttest

import (
	"bytes"
	"testing"
)

func TestContract_TreeDBIteratorBounds(t *testing.T) {
	dir := t.TempDir()
	db, err := openEngine("treedb-cached", dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tdb, ok := db.(treedbCached)
	if !ok {
		t.Fatalf("unexpected treedb engine type")
	}

	if err := tdb.PutSync([]byte("a"), []byte("va")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := tdb.PutSync([]byte("b"), []byte("vb")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := tdb.PutSync([]byte("c"), []byte("vc")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := tdb.PutSync([]byte("d"), []byte("vd")); err != nil {
		t.Fatalf("put: %v", err)
	}

	it, err := tdb.db.Iterator([]byte("b"), []byte("d"))
	if err != nil {
		t.Fatalf("iterator: %v", err)
	}
	defer it.Close()

	var gotKeys [][]byte
	for it.Valid() {
		gotKeys = append(gotKeys, append([]byte(nil), it.Key()...))
		it.Next()
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}

	wantKeys := [][]byte{[]byte("b"), []byte("c")}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("keys mismatch: got %v, want %v", gotKeys, wantKeys)
	}
	for i := range wantKeys {
		if !bytes.Equal(gotKeys[i], wantKeys[i]) {
			t.Fatalf("key[%d] mismatch: got %q, want %q", i, string(gotKeys[i]), string(wantKeys[i]))
		}
	}
}
