package zipper

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestZipper_PackUnderfilledLeafChildren_ReducesLeafCountAndPreservesOrder(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)
	z.SetLeafPrefixCompression(true)

	buildLeaf := func(keys []string) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, node.BuilderOptions{LeafPrefixCompression: true})
		b.SetPageID(id)
		for _, k := range keys {
			if err := b.AddLeafEntry([]byte(k), []byte("v"), node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry %q: %v", k, err)
			}
		}
		b.Finish()
		return id
	}

	// Four very underfilled leaves that should pack into fewer leaves.
	l0 := buildLeaf([]string{"a0"})
	l1 := buildLeaf([]string{"b0"})
	l2 := buildLeaf([]string{"c0"})
	l3 := buildLeaf([]string{"d0"})

	entries := []internalEntry{
		{key: []byte{}, child: l0},
		{key: []byte("b0"), child: l1},
		{key: []byte("c0"), child: l2},
		{key: []byte("d0"), child: l3},
	}
	updated := []bool{true, false, false, false}

	out, outUpdated, retired, err := z.packUnderfilledLeafChildren(entries, updated, &adaptive.Metrics{})
	if err != nil {
		t.Fatalf("packUnderfilledLeafChildren: %v", err)
	}

	if len(out) >= len(entries) {
		t.Fatalf("expected packed leaf count to decrease (before=%d after=%d)", len(entries), len(out))
	}
	if len(out) != len(outUpdated) {
		t.Fatalf("updated length mismatch: entries=%d updated=%d", len(out), len(outUpdated))
	}

	// Verify retired includes all old leaf IDs when packing applies.
	for _, want := range []uint64{l0, l1, l2, l3} {
		found := false
		for _, got := range retired {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected old leaf %d to be retired", want)
		}
	}

	// Verify all keys exist and order is preserved across packed leaves.
	var gotKeys [][]byte
	for _, e := range out {
		data, err := p.Get(e.child)
		if err != nil {
			t.Fatalf("load leaf %d: %v", e.child, err)
		}
		n := node.NewNode(data)
		for i := uint16(0); i < n.Count(); i++ {
			k, _, _, _, err := n.GetLeafEntryView(i)
			if err != nil {
				t.Fatalf("leaf entry %d: %v", i, err)
			}
			gotKeys = append(gotKeys, append([]byte(nil), k...))
		}
	}

	wantKeys := [][]byte{[]byte("a0"), []byte("b0"), []byte("c0"), []byte("d0")}
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("key count mismatch: got=%d want=%d", len(gotKeys), len(wantKeys))
	}
	for i := range wantKeys {
		if !bytes.Equal(gotKeys[i], wantKeys[i]) {
			t.Fatalf("key[%d] mismatch: got=%q want=%q", i, gotKeys[i], wantKeys[i])
		}
	}

	// Separator invariant sanity: each separator key must be > maxKey(left) and <= minKey(right).
	for i := 1; i < len(out); i++ {
		leftData, _ := p.Get(out[i-1].child)
		rightData, _ := p.Get(out[i].child)
		left := node.NewNode(leftData)
		right := node.NewNode(rightData)

		leftMax, _, _, _, err := left.GetLeafEntryView(left.Count() - 1)
		if err != nil {
			t.Fatalf("left max: %v", err)
		}
		rightMin, _, _, _, err := right.GetLeafEntryView(0)
		if err != nil {
			t.Fatalf("right min: %v", err)
		}

		sep := out[i].key
		if bytes.Compare(sep, leftMax) <= 0 {
			t.Fatalf("separator not > left max: sep=%q leftMax=%q", sep, leftMax)
		}
		if bytes.Compare(sep, rightMin) > 0 {
			t.Fatalf("separator not <= right min: sep=%q rightMin=%q", sep, rightMin)
		}
	}
}
