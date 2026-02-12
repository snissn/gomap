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

func TestLeafColumnarAddLeafEntryWithPrefix(t *testing.T) {
	data := make([]byte, page.PageSize)
	b := NewBuilderWithOptions(data, page.PageTypeLeaf, BuilderOptions{LeafColumnar: true})
	b.SetPageID(1)

	entrySize, prefixLen, suffixLen := b.LeafEntrySizeWithPrefix([]byte("a"), []byte("value"), FlagInline)
	if err := b.AddLeafEntryWithPrefix([]byte("a"), []byte("value"), FlagInline, page.ValuePtr{}, entrySize, prefixLen, suffixLen); err != nil {
		t.Fatalf("add inline entry: %v", err)
	}

	ptr := page.ValuePtr{Offset: 42, Length: 99, FileID: 7}
	entrySize, prefixLen, suffixLen = b.LeafEntrySizeWithPrefix([]byte("b"), nil, FlagPointer)
	if err := b.AddLeafEntryWithPrefix([]byte("b"), nil, FlagPointer, ptr, entrySize, prefixLen, suffixLen); err != nil {
		t.Fatalf("add pointer entry: %v", err)
	}

	n := b.Finish()
	if !n.leafColumnar() {
		t.Fatalf("expected columnar leaf flag set")
	}

	idx, found, err := n.SearchLeaf([]byte("a"))
	if err != nil || !found {
		t.Fatalf("search leaf a: found=%v err=%v", found, err)
	}
	_, v, _, flags, err := n.GetLeafEntryView(idx)
	if err != nil {
		t.Fatalf("get leaf a: %v", err)
	}
	if flags != FlagInline || !bytes.Equal(v, []byte("value")) {
		t.Fatalf("unexpected inline entry: val=%q flags=%v", v, flags)
	}

	idx, found, err = n.SearchLeaf([]byte("b"))
	if err != nil || !found {
		t.Fatalf("search leaf b: found=%v err=%v", found, err)
	}
	_, v, vp, flags, err := n.GetLeafEntryView(idx)
	if err != nil {
		t.Fatalf("get leaf b: %v", err)
	}
	if flags&FlagPointer == 0 || v != nil || vp != ptr {
		t.Fatalf("unexpected pointer entry: val=%v flags=%v ptr=%v", v, flags, vp)
	}
}

func TestLeafBuilder_AdaptiveEncoding_HeuristicDeterminism(t *testing.T) {
	base := BuilderOptions{
		LeafPrefixCompression: true,
		LeafColumnar:          true,
		PackedValuePtr:        true,
	}

	pointerHeavyLowPrefix := []LeafHeuristicEntry{
		{Key: []byte("a00"), Flags: FlagPointer},
		{Key: []byte("b00"), Flags: FlagPointer},
		{Key: []byte("c00"), Flags: FlagPointer},
		{Key: []byte("d00"), Flags: FlagPointer},
		{Key: []byte("e00"), Flags: FlagInline},
	}
	wantPointerHeavy := AdaptiveLeafBuilderOptions(base, pointerHeavyLowPrefix)
	for i := 0; i < 8; i++ {
		got := AdaptiveLeafBuilderOptions(base, pointerHeavyLowPrefix)
		if got != wantPointerHeavy {
			t.Fatalf("pointer-heavy heuristic must be deterministic: got=%+v want=%+v", got, wantPointerHeavy)
		}
	}
	if !wantPointerHeavy.LeafColumnar {
		t.Fatalf("expected pointer-heavy low-prefix pages to keep columnar mode")
	}
	if wantPointerHeavy.LeafPrefixCompression {
		t.Fatalf("expected pointer-heavy low-prefix pages to disable prefix compression")
	}

	highPrefixInline := []LeafHeuristicEntry{
		{Key: []byte("orders/us/0001"), Flags: FlagInline},
		{Key: []byte("orders/us/0002"), Flags: FlagInline},
		{Key: []byte("orders/us/0003"), Flags: FlagInline},
		{Key: []byte("orders/us/0004"), Flags: FlagInline},
	}
	wantHighPrefix := AdaptiveLeafBuilderOptions(base, highPrefixInline)
	for i := 0; i < 8; i++ {
		got := AdaptiveLeafBuilderOptions(base, highPrefixInline)
		if got != wantHighPrefix {
			t.Fatalf("high-prefix heuristic must be deterministic: got=%+v want=%+v", got, wantHighPrefix)
		}
	}
	if wantHighPrefix.LeafColumnar {
		t.Fatalf("expected high-prefix inline pages to disable columnar mode")
	}
	if !wantHighPrefix.LeafPrefixCompression {
		t.Fatalf("expected high-prefix inline pages to keep prefix compression")
	}

	deleteHeavy := []LeafHeuristicEntry{
		{Key: []byte("acct/1"), Flags: FlagInline},
		{Key: []byte("acct/2"), Flags: FlagTombstone},
		{Key: []byte("acct/3"), Flags: FlagTombstone},
		{Key: []byte("acct/4"), Flags: FlagTombstone},
	}
	gotDeleteHeavy := AdaptiveLeafBuilderOptions(base, deleteHeavy)
	if gotDeleteHeavy.LeafColumnar {
		t.Fatalf("expected delete-heavy pages to disable columnar mode")
	}
}

func TestLeafHeuristicEntriesSorted_EqualKeyRespectsFlags(t *testing.T) {
	if leafHeuristicEntriesSorted([]LeafHeuristicEntry{
		{Key: []byte("dup"), Flags: FlagPointer},
		{Key: []byte("dup"), Flags: FlagInline},
	}) {
		t.Fatalf("expected duplicate-key entries out of flag order to be unsorted")
	}
	if !leafHeuristicEntriesSorted([]LeafHeuristicEntry{
		{Key: []byte("dup"), Flags: FlagInline},
		{Key: []byte("dup"), Flags: FlagPointer},
	}) {
		t.Fatalf("expected duplicate-key entries in flag order to be sorted")
	}
}
