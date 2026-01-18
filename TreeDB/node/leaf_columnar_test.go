package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafColumnarRoundTrip(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafColumnar: true})
	b.SetPageID(1)

	if err := b.AddLeafEntry([]byte("a"), []byte("value"), FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("add inline entry: %v", err)
	}

	ptr := page.ValuePtr{Offset: 42, Length: 99, FileID: 7}
	if err := b.AddLeafEntry([]byte("b"), nil, FlagPointer, ptr); err != nil {
		t.Fatalf("add pointer entry: %v", err)
	}

	n := b.Finish()
	if !n.leafColumnar() {
		t.Fatalf("expected columnar leaf flag set")
	}
	if n.leafPrefixCompressed() {
		t.Fatalf("did not expect prefix compression on columnar leaf")
	}

	idx, found, err := n.SearchLeaf([]byte("a"))
	if err != nil || !found {
		t.Fatalf("search leaf a: found=%v err=%v", found, err)
	}
	k, v, vp, flags, err := n.GetLeafEntryView(idx)
	if err != nil {
		t.Fatalf("get leaf a: %v", err)
	}
	if flags != FlagInline || !bytes.Equal(k, []byte("a")) || !bytes.Equal(v, []byte("value")) || vp != (page.ValuePtr{}) {
		t.Fatalf("unexpected inline entry: key=%q val=%q flags=%v ptr=%v", k, v, flags, vp)
	}

	idx, found, err = n.SearchLeaf([]byte("b"))
	if err != nil || !found {
		t.Fatalf("search leaf b: found=%v err=%v", found, err)
	}
	k, v, vp, flags, err = n.GetLeafEntryView(idx)
	if err != nil {
		t.Fatalf("get leaf b: %v", err)
	}
	if flags&FlagPointer == 0 || !bytes.Equal(k, []byte("b")) || v != nil || vp != ptr {
		t.Fatalf("unexpected pointer entry: key=%q val=%v flags=%v ptr=%v", k, v, flags, vp)
	}

	if err := n.AddLeafEntry([]byte("c"), []byte("more"), FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("add leaf c: %v", err)
	}
	idx, found, err = n.SearchLeaf([]byte("c"))
	if err != nil || !found {
		t.Fatalf("search leaf c: found=%v err=%v", found, err)
	}
	_, v, _, _, err = n.GetLeafEntryView(idx)
	if err != nil {
		t.Fatalf("get leaf c: %v", err)
	}
	if !bytes.Equal(v, []byte("more")) {
		t.Fatalf("unexpected value for c: %q", v)
	}
}
