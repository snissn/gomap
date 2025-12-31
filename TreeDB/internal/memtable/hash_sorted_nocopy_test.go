package memtable

import (
	"testing"
	"unsafe"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
)

func TestHashSortedNoCopyValues(t *testing.T) {
	m := NewHashSorted()

	value := []byte("val1")
	entries := []batchpkg.Entry{
		{Type: batchpkg.OpPut, Key: []byte("k1"), Value: value},
	}

	m.ApplyStealBatchIndicesNoCopyValues(entries, []int{0}, nil)

	got, deleted, ok := m.Get([]byte("k1"))
	if !ok || deleted || string(got) != "val1" {
		t.Fatalf("Get: ok=%v deleted=%v got=%q", ok, deleted, got)
	}
	if unsafe.SliceData(got) != unsafe.SliceData(value) {
		t.Fatalf("expected no-copy value pointer")
	}

	it := m.NewIterator(nil, nil)
	it.Seek(nil)
	if !it.Valid() || string(it.UnsafeKey()) != "k1" {
		t.Fatalf("iterator seek: valid=%v key=%q", it.Valid(), it.UnsafeKey())
	}
	gotIter := it.UnsafeValue()
	if string(gotIter) != "val1" {
		t.Fatalf("iterator value: %q", gotIter)
	}
	if unsafe.SliceData(gotIter) != unsafe.SliceData(value) {
		t.Fatalf("expected iterator no-copy value pointer")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	// Verify that safe overwrite clears the external reference.
	m.Set([]byte("k1"), []byte("val2"))
	got2, deleted2, ok2 := m.Get([]byte("k1"))
	if !ok2 || deleted2 || string(got2) != "val2" {
		t.Fatalf("overwrite Get: ok=%v deleted=%v got=%q", ok2, deleted2, got2)
	}
	if unsafe.SliceData(got2) == unsafe.SliceData(value) {
		t.Fatalf("expected overwrite to replace external value reference")
	}
}
