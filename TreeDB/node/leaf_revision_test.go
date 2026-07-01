package node

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestLeafEntryRevisionRoundTripAcrossEncodings(t *testing.T) {
	tests := []struct {
		name string
		opts BuilderOptions
	}{
		{name: "plain", opts: BuilderOptions{EntryRevisions: true}},
		{name: "prefix_v2", opts: BuilderOptions{LeafPrefixCompression: true, EntryRevisions: true}},
		{name: "columnar_v2", opts: BuilderOptions{LeafColumnar: true, EntryRevisions: true}},
		{name: "columnar_prefix_v2", opts: BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, EntryRevisions: true}},
		{name: "packed_pointer", opts: BuilderOptions{PackedValuePtr: true, EntryRevisions: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, page.PageSize)
			b := NewBuilderWithOptions(data, page.PageTypeLeaf, tt.opts)
			b.SetPageID(7)

			ptr := page.ValuePtr{Offset: 42, Length: 99, FileID: 3}
			if err := b.AddLeafEntryWithRevision([]byte("a"), []byte("inline"), FlagInline, page.ValuePtr{}, 11); err != nil {
				t.Fatalf("add inline: %v", err)
			}
			if err := b.AddLeafEntryWithRevision([]byte("b"), nil, FlagPointer, ptr, 12); err != nil {
				t.Fatalf("add pointer: %v", err)
			}
			if err := b.AddLeafEntryWithRevision([]byte("c"), nil, FlagTombstone, page.ValuePtr{}, 13); err != nil {
				t.Fatalf("add tombstone: %v", err)
			}

			n := b.Finish()
			if !n.leafEntryRevisions() {
				t.Fatalf("expected revision leaf flag")
			}

			assertRevisionEntry(t, n, "a", []byte("inline"), page.ValuePtr{}, FlagInline, 11)
			assertRevisionEntry(t, n, "b", nil, ptr, FlagPointer, 12)
			assertRevisionEntry(t, n, "c", nil, page.ValuePtr{}, FlagTombstone, 13)
		})
	}
}

func TestLeafEntryRevisionPreservedBySplitAndCompact(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafPrefixCompression: true, LeafColumnar: true, EntryRevisions: true})
	b.SetPageID(1)

	for i, key := range []string{"a", "b", "c", "d"} {
		if err := b.AddLeafEntryWithRevision([]byte(key), []byte("value-"+key), FlagInline, page.ValuePtr{}, page.EntryRevision(100+i)); err != nil {
			t.Fatalf("add %s: %v", key, err)
		}
	}
	left := b.Finish()

	right := NewNode(make([]byte, page.PageSize))
	right.SetPageID(2)
	pivot, err := left.Split(right)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	if !bytes.Equal(pivot, []byte("c")) {
		t.Fatalf("pivot=%q, want c", pivot)
	}

	assertRevisionEntry(t, left, "a", []byte("value-a"), page.ValuePtr{}, FlagInline, 100)
	assertRevisionEntry(t, left, "b", []byte("value-b"), page.ValuePtr{}, FlagInline, 101)
	assertRevisionEntry(t, right, "c", []byte("value-c"), page.ValuePtr{}, FlagInline, 102)
	assertRevisionEntry(t, right, "d", []byte("value-d"), page.ValuePtr{}, FlagInline, 103)

	if err := left.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	assertRevisionEntry(t, left, "a", []byte("value-a"), page.ValuePtr{}, FlagInline, 100)
	assertRevisionEntry(t, left, "b", []byte("value-b"), page.ValuePtr{}, FlagInline, 101)
}

func TestLeafEntryRevisionLegacyPagesReturnZero(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{})
	b.SetPageID(1)
	if err := b.AddLeafEntry([]byte("a"), []byte("value"), FlagInline, page.ValuePtr{}); err != nil {
		t.Fatalf("add: %v", err)
	}
	n := b.Finish()

	_, value, _, flags, revision, err := n.GetLeafEntryViewWithRevision(0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if flags != FlagInline || !bytes.Equal(value, []byte("value")) || revision != page.LegacyEntryRevision {
		t.Fatalf("entry=(%q,%#x,%d), want value/inline/legacy", value, flags, revision)
	}
}

func assertRevisionEntry(t *testing.T, n *Node, key string, wantValue []byte, wantPtr page.ValuePtr, wantFlags byte, wantRevision page.EntryRevision) {
	t.Helper()
	idx, found, err := n.SearchLeaf([]byte(key))
	if err != nil || !found {
		t.Fatalf("search %q: found=%v err=%v", key, found, err)
	}
	gotKey, gotValue, gotPtr, gotFlags, gotRevision, err := n.GetLeafEntryViewWithRevision(idx)
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	if !bytes.Equal(gotKey, []byte(key)) || !bytes.Equal(gotValue, wantValue) || gotPtr != wantPtr || gotFlags&wantFlags != wantFlags || gotRevision != wantRevision {
		t.Fatalf("entry %q=(key=%q value=%q ptr=%+v flags=%#x rev=%d), want value=%q ptr=%+v flags contains %#x rev=%d",
			key, gotKey, gotValue, gotPtr, gotFlags, gotRevision, wantValue, wantPtr, wantFlags, wantRevision)
	}

	entry, err := n.GetLeafEntry(idx)
	if err != nil {
		t.Fatalf("GetLeafEntry %q: %v", key, err)
	}
	if entry.Revision != wantRevision {
		t.Fatalf("safe entry %q revision=%d, want %d", key, entry.Revision, wantRevision)
	}
}
