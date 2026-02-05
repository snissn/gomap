package tree

import (
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestIterator(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Build Tree
	// Root (0) -> Internal
	// Child 1 (Left) -> "A", "C"
	// Child 2 (Right) -> "E", "G"

	p.Alloc(3) // 0, 1, 2

	// Leaf 1
	d1, _ := p.Get(1)
	n1 := node.NewNode(d1)
	n1.SetPageID(1)
	n1.SetType(page.PageTypeLeaf)
	n1.AddLeafEntry([]byte("A"), []byte("valA"), node.FlagInline, page.ValuePtr{})
	n1.AddLeafEntry([]byte("C"), []byte("valC"), node.FlagInline, page.ValuePtr{})
	n1.UpdateChecksum()

	// Leaf 2
	d2, _ := p.Get(2)
	n2 := node.NewNode(d2)
	n2.SetPageID(2)
	n2.SetType(page.PageTypeLeaf)
	n2.AddLeafEntry([]byte("E"), []byte("valE"), node.FlagInline, page.ValuePtr{})
	n2.AddLeafEntry([]byte("G"), []byte("valG"), node.FlagInline, page.ValuePtr{})
	n2.UpdateChecksum()

	// Root
	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeInternal)
	n0.AddInternalChild([]byte(""), 1)  // Min key
	n0.AddInternalChild([]byte("D"), 2) // Split key
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)

	// 1. Full Forward
	t.Run("Forward", func(t *testing.T) {
		it := tr.Iterator(nil, nil)
		expected := []string{"A", "C", "E", "G"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	// 2. Full Reverse
	t.Run("Reverse", func(t *testing.T) {
		it := tr.ReverseIterator(nil, nil)
		expected := []string{"G", "E", "C", "A"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	// 3. Range Forward [B, F) -> C, E
	t.Run("RangeForward", func(t *testing.T) {
		it := tr.Iterator([]byte("B"), []byte("F"))
		expected := []string{"C", "E"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	// 4. Range Reverse [B, F) -> E, C
	t.Run("RangeReverse", func(t *testing.T) {
		it := tr.ReverseIterator([]byte("B"), []byte("F"))
		expected := []string{"E", "C"}
		i := 0
		for ; it.Valid(); it.Next() {
			k := string(it.Key())
			if k != expected[i] {
				t.Errorf("Idx %d: expected %s, got %s", i, expected[i], k)
			}
			i++
		}
		if i != len(expected) {
			t.Errorf("Expected %d items, got %d", len(expected), i)
		}
		it.Close()
	})

	t.Run("ViewSemantics", func(t *testing.T) {
		it := tr.Iterator(nil, nil)
		if !it.Valid() {
			t.Fatalf("expected iterator to be valid")
		}

		k := it.Key()
		uk := it.UnsafeKey()
		if len(k) == 0 || len(uk) == 0 {
			t.Fatalf("expected non-empty key views")
		}
		if unsafe.Pointer(&k[0]) != unsafe.Pointer(&uk[0]) {
			t.Fatalf("expected Key() to be a view (same backing) as UnsafeKey()")
		}

		v := it.Value()
		uv := it.UnsafeValue()
		if len(v) == 0 || len(uv) == 0 {
			t.Fatalf("expected non-empty value views")
		}
		if unsafe.Pointer(&v[0]) != unsafe.Pointer(&uv[0]) {
			t.Fatalf("expected Value() to be a view (same backing) as UnsafeValue()")
		}

		kc := it.KeyCopy(nil)
		vc := it.ValueCopy(nil)
		if len(kc) == 0 || len(vc) == 0 {
			t.Fatalf("expected KeyCopy/ValueCopy to return bytes")
		}
		if unsafe.Pointer(&kc[0]) == unsafe.Pointer(&k[0]) {
			t.Fatalf("expected KeyCopy to allocate a distinct buffer")
		}
		if unsafe.Pointer(&vc[0]) == unsafe.Pointer(&v[0]) {
			t.Fatalf("expected ValueCopy to allocate a distinct buffer")
		}
		it.Close()
	})
}

func TestIterator_SkipsTombstones(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // 0

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), []byte("valA"), node.FlagInline, page.ValuePtr{})
	n0.AddLeafEntry([]byte("B"), nil, node.FlagTombstone, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), []byte("valC"), node.FlagInline, page.ValuePtr{})
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)

	it := tr.Iterator(nil, nil)
	got := make([]string, 0, 2)
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	_ = it.Close()

	if len(got) != 2 || got[0] != "A" || got[1] != "C" {
		t.Fatalf("expected [A C], got %v", got)
	}
}

func TestIterator_PointerKeysOnly_DoesNotReadValues(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	p.Alloc(1) // 0

	d0, _ := p.Get(0)
	n0 := node.NewNode(d0)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeLeaf)
	n0.AddLeafEntry([]byte("A"), nil, node.FlagPointer, page.ValuePtr{Offset: 1, Length: 3, FileID: 1})
	n0.AddLeafEntry([]byte("B"), nil, node.FlagTombstone, page.ValuePtr{})
	n0.AddLeafEntry([]byte("C"), nil, node.FlagPointer, page.ValuePtr{Offset: 5, Length: 3, FileID: 1})
	n0.UpdateChecksum()

	tr := New(p, panicValueReader{}, 0)
	it := tr.Iterator(nil, nil)
	defer it.Close()

	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if len(keys) != 2 || keys[0] != "A" || keys[1] != "C" {
		t.Fatalf("expected [A C], got %v", keys)
	}
}
