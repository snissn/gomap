package tree

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestHasPrefixesWithStatsSortedMatchesUnsorted(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatalf("open pager: %v", err)
	}
	defer func() { _ = p.Close() }()

	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("alloc root: %v", err)
	}
	data, err := p.GetForWrite(rootID)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	b := node.NewBuilder(data, page.PageTypeLeaf)
	b.SetPageID(rootID)
	entries := []struct {
		key   string
		val   string
		flags byte
	}{
		{"a/1", "v1", node.FlagInline},
		{"b/1", "", node.FlagTombstone},
		{"b/2", "", node.FlagTombstone},
		{"c/1", "", node.FlagTombstone},
		{"c/2", "v2", node.FlagInline},
		{"e/1", "v3", node.FlagInline},
		{"m/1", "v4", node.FlagInline},
	}
	for _, e := range entries {
		if err := b.AddLeafEntry([]byte(e.key), []byte(e.val), e.flags, page.ValuePtr{}); err != nil {
			t.Fatalf("AddLeafEntry(%q): %v", e.key, err)
		}
	}
	b.FinishNoNode()

	tr := New(p, nil, rootID)

	sortedPrefixes := [][]byte{
		[]byte("0/"),
		[]byte("a/"),
		[]byte("a/"), // duplicate
		[]byte("b/"), // tombstones only -> false
		[]byte("c/"), // tombstone followed by live -> true
		[]byte("d/"), // gap -> false
		[]byte("e/"),
		[]byte("f/"), // gap -> false
		[]byte("m/"),
		[]byte("z/"), // past EOF -> false
		[]byte("zz/"),
	}
	want := []bool{false, true, true, false, true, false, true, false, true, false, false}

	gotSorted, _, err := tr.HasPrefixesWithStats(sortedPrefixes)
	if err != nil {
		t.Fatalf("HasPrefixesWithStats(sorted): %v", err)
	}
	if !slices.Equal(gotSorted, want) {
		t.Fatalf("sorted got=%v want=%v", gotSorted, want)
	}

	// Permute order so the unsorted path runs and verify exact index-mapped parity.
	perm := []int{9, 4, 0, 10, 1, 6, 3, 2, 8, 5, 7}
	unsortedPrefixes := make([][]byte, len(perm))
	wantUnsorted := make([]bool, len(perm))
	for i, srcIdx := range perm {
		unsortedPrefixes[i] = sortedPrefixes[srcIdx]
		wantUnsorted[i] = want[srcIdx]
	}
	gotUnsorted, _, err := tr.HasPrefixesWithStats(unsortedPrefixes)
	if err != nil {
		t.Fatalf("HasPrefixesWithStats(unsorted): %v", err)
	}
	if !slices.Equal(gotUnsorted, wantUnsorted) {
		t.Fatalf("unsorted got=%v want=%v", gotUnsorted, wantUnsorted)
	}
}
