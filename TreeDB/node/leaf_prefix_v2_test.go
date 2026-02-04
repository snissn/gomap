package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafPrefixV2_RoundTrip_PointerAndTombstone(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true})
	b.SetPageID(1)

	if err := b.AddLeafEntry([]byte("key0"), []byte("value0"), FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("add inline entry: %v", err)
	}

	ptr := page.ValuePtr{Offset: 42, Length: 99, FileID: 7}
	if err := b.AddLeafEntry([]byte("key1"), nil, FlagPointer, ptr); err != nil {
		t.Fatalf("add pointer entry: %v", err)
	}

	if err := b.AddLeafEntry([]byte("key2"), nil, FlagTombstone, page.ValuePtr{}); err != nil {
		t.Fatalf("add tombstone entry: %v", err)
	}

	n := b.Finish()
	if !n.leafPrefixCompressed() {
		t.Fatalf("expected prefix compression enabled")
	}
	if !n.leafPrefixV2() {
		t.Fatalf("expected leaf prefix v2")
	}

	idx0, found, err := n.SearchLeaf([]byte("key0"))
	if err != nil || !found {
		t.Fatalf("search leaf key0: found=%v err=%v", found, err)
	}
	k, v, vp, flags, err := n.GetLeafEntryView(idx0)
	if err != nil {
		t.Fatalf("get leaf key0: %v", err)
	}
	if flags != FlagInline || !bytes.Equal(k, []byte("key0")) || !bytes.Equal(v, []byte("value0")) || vp != (page.ValuePtr{}) {
		t.Fatalf("unexpected inline entry: key=%q val=%q flags=%v ptr=%v", k, v, flags, vp)
	}
	_, layout0, _, err := n.leafEntryKeyAt(idx0)
	if err != nil {
		t.Fatalf("leafEntryKeyAt key0: %v", err)
	}
	if layout0.headerSize <= leafPrefixV2HeaderBaseSize {
		t.Fatalf("expected inline v2 entry to include ValueLen varint, headerSize=%d", layout0.headerSize)
	}

	idx1, found, err := n.SearchLeaf([]byte("key1"))
	if err != nil || !found {
		t.Fatalf("search leaf key1: found=%v err=%v", found, err)
	}
	k, v, vp, flags, err = n.GetLeafEntryView(idx1)
	if err != nil {
		t.Fatalf("get leaf key1: %v", err)
	}
	if flags&FlagPointer == 0 || !bytes.Equal(k, []byte("key1")) || v != nil || vp != ptr {
		t.Fatalf("unexpected pointer entry: key=%q val=%v flags=%v ptr=%v", k, v, flags, vp)
	}
	_, layout1, _, err := n.leafEntryKeyAt(idx1)
	if err != nil {
		t.Fatalf("leafEntryKeyAt key1: %v", err)
	}
	if layout1.prefixLen == 0 {
		t.Fatalf("expected pointer entry to be prefix-coded (prefixLen>0)")
	}
	if layout1.headerSize != leafPrefixV2HeaderBaseSize {
		t.Fatalf("expected pointer v2 entry to omit ValueLen varint, headerSize=%d", layout1.headerSize)
	}

	idx2, found, err := n.SearchLeaf([]byte("key2"))
	if err != nil || !found {
		t.Fatalf("search leaf key2: found=%v err=%v", found, err)
	}
	k, v, vp, flags, err = n.GetLeafEntryView(idx2)
	if err != nil {
		t.Fatalf("get leaf key2: %v", err)
	}
	if flags&FlagTombstone == 0 || !bytes.Equal(k, []byte("key2")) || v != nil || vp != (page.ValuePtr{}) {
		t.Fatalf("unexpected tombstone entry: key=%q val=%v flags=%v ptr=%v", k, v, flags, vp)
	}
	_, layout2, _, err := n.leafEntryKeyAt(idx2)
	if err != nil {
		t.Fatalf("leafEntryKeyAt key2: %v", err)
	}
	if layout2.prefixLen == 0 {
		t.Fatalf("expected tombstone entry to be prefix-coded (prefixLen>0)")
	}
	if layout2.headerSize != leafPrefixV2HeaderBaseSize {
		t.Fatalf("expected tombstone v2 entry to omit ValueLen varint, headerSize=%d", layout2.headerSize)
	}

	// Validate pointer rewrite works under the v2 layout (valOff correctness).
	newPtr := page.ValuePtr{Offset: 111, Length: 222, FileID: 7}
	updated, err := n.UpdateLeafValuePtr(idx1, ptr, newPtr)
	if err != nil {
		t.Fatalf("UpdateLeafValuePtr: %v", err)
	}
	if !updated {
		t.Fatalf("expected UpdateLeafValuePtr to rewrite pointer entry")
	}
	_, _, vp, flags, err = n.GetLeafEntryView(idx1)
	if err != nil {
		t.Fatalf("get leaf key1 after rewrite: %v", err)
	}
	if flags&FlagPointer == 0 || vp != newPtr {
		t.Fatalf("unexpected pointer after rewrite: flags=%v ptr=%v", flags, vp)
	}
}
