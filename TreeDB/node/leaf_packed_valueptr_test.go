package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafPackedValuePtr_RoundTripPointerEntries(t *testing.T) {
	type variant struct {
		name string
		opts BuilderOptions
	}
	variants := []variant{
		{name: "plain", opts: BuilderOptions{PackedValuePtr: true}},
		{name: "prefix_v2", opts: BuilderOptions{LeafPrefixCompression: true, PackedValuePtr: true}},
		{name: "columnar", opts: BuilderOptions{LeafColumnar: true, PackedValuePtr: true}},
		{name: "columnar_prefix_v2", opts: BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, PackedValuePtr: true}},
	}

	keys := makeBenchKeys(128, 16)
	ptrs := make([]page.ValuePtr, len(keys))
	for i := range keys {
		ptrs[i] = page.ValuePtr{Offset: uint64(i + 1), Length: 123, FileID: 7}
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			buf := make([]byte, page.PageSize)
			b := NewBuilderWithOptions(buf, page.PageTypeLeaf, v.opts)
			b.SetPageID(1)

			inserted := 0
			for i := range keys {
				if err := b.AddLeafEntry(keys[i], nil, FlagPointer, ptrs[i]); err != nil {
					if err == ErrNodeFull {
						break
					}
					t.Fatalf("AddLeafEntry: %v", err)
				}
				inserted++
			}
			n := b.Finish()

			if inserted == 0 {
				t.Fatalf("expected at least one inserted entry")
			}
			if !n.leafPackedValuePtr() {
				t.Fatalf("expected leafPackedValuePtr flag set")
			}

			for i := uint16(0); i < uint16(inserted); i++ {
				k, val, gotPtr, flags, err := n.GetLeafEntryView(i)
				if err != nil {
					t.Fatalf("GetLeafEntryView(%d): %v", i, err)
				}
				if flags&FlagPointer == 0 {
					t.Fatalf("expected pointer flag for idx=%d", i)
				}
				if val != nil {
					t.Fatalf("expected nil inline value for pointer entry idx=%d", i)
				}
				if !bytes.Equal(k, keys[int(i)]) {
					t.Fatalf("key mismatch idx=%d", i)
				}
				if gotPtr != ptrs[int(i)] {
					t.Fatalf("ptr mismatch idx=%d got=%+v want=%+v", i, gotPtr, ptrs[int(i)])
				}
			}
		})
	}
}

func TestLeafPackedValuePtr_UpdateLeafValuePtr(t *testing.T) {
	buf := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(buf, page.PageTypeLeaf, BuilderOptions{
		LeafPrefixCompression: true,
		PackedValuePtr:        true,
	})
	b.SetPageID(1)

	key := bytes.Repeat([]byte("k"), 32)
	oldPtr := page.ValuePtr{Offset: 1, Length: 123, FileID: 7}
	newPtr := page.ValuePtr{Offset: 2, Length: 123, FileID: 7}

	if err := b.AddLeafEntry(key, nil, FlagPointer, oldPtr); err != nil {
		t.Fatalf("AddLeafEntry: %v", err)
	}
	n := b.Finish()

	updated, err := n.UpdateLeafValuePtr(0, oldPtr, newPtr)
	if err != nil {
		t.Fatalf("UpdateLeafValuePtr: %v", err)
	}
	if !updated {
		t.Fatalf("expected UpdateLeafValuePtr to rewrite pointer entry")
	}

	_, _, got, flags, err := n.GetLeafEntryView(0)
	if err != nil {
		t.Fatalf("GetLeafEntryView: %v", err)
	}
	if flags&FlagPointer == 0 {
		t.Fatalf("expected pointer entry after rewrite")
	}
	if got != newPtr {
		t.Fatalf("ptr mismatch after rewrite got=%+v want=%+v", got, newPtr)
	}
}
