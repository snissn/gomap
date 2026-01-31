package zipper

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestZipper_Rebalance_MergeWhenBothUnderMinAndFits(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	buildLeaf := func(keys []string, val []byte) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := z.newLeafBuilder(data)
		b.SetPageID(id)
		for _, k := range keys {
			if err := b.AddLeafEntry([]byte(k), val, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry: %v", err)
			}
		}
		b.Finish()
		return id
	}

	leftID := buildLeaf([]string{"a"}, []byte("v"))
	rightID := buildLeaf([]string{"b", "c"}, []byte("v"))

	entries := []internalEntry{
		{key: []byte{}, child: leftID},
		{key: []byte("b"), child: rightID},
	}
	updated := []bool{true, false}

	var m adaptive.Metrics
	out, retired, err := z.rebalanceUpdatedLeavesWithRightSibling(entries, updated, &m)
	if err != nil {
		t.Fatalf("rebalanceUpdatedLeavesWithRightSibling: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 entry after merge, got %d", len(out))
	}
	if len(retired) != 2 {
		t.Fatalf("expected 2 retired pages (old leaves), got %d (%v)", len(retired), retired)
	}

	data, err := p.Get(out[0].child)
	if err != nil {
		t.Fatalf("load merged leaf: %v", err)
	}
	n := node.NewNode(data)
	var got []string
	for i := uint16(0); i < n.Count(); i++ {
		k, _, _, flags, err := n.GetLeafEntryView(i)
		if err != nil {
			t.Fatalf("leaf entry view: %v", err)
		}
		if flags&node.FlagTombstone != 0 {
			t.Fatalf("unexpected tombstone in merged leaf")
		}
		got = append(got, string(k))
	}
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("merged keys = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("merged keys = %v, want %v", got, want)
		}
	}
}

func TestZipper_Rebalance_RebalanceToTargetAndKeepRightAboveMin(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	capBytes := z.leafCapBytes()
	if capBytes == 0 {
		t.Fatalf("unexpected capBytes=0")
	}
	targetBytes := (capBytes * 7) / 8 // must match zipper hysteresis ratio
	minBytes := (capBytes * 3) / 4    // must match zipper hysteresis ratio

	// Choose a value size that gives us decent control over per-entry bytes.
	val := bytes.Repeat([]byte("v"), 64)
	entryBytes := z.leafEntryBytesUpperBound([]byte{0, 0, 0}, val, page.ValuePtr{}, node.FlagInline)

	// Build a pair that cannot merge (sum > cap) but can rebalance:
	// - left is just under min
	// - right is near cap
	leftCount := (minBytes / entryBytes) - 1
	if leftCount < 1 {
		leftCount = 1
	}
	rightCount := (capBytes / entryBytes) - 1
	if rightCount < 2 {
		rightCount = 2
	}

	buildLeaf := func(prefix byte, start, count int) (uint64, []string) {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := z.newLeafBuilder(data)
		b.SetPageID(id)
		var keys []string
		for i := 0; i < count; i++ {
			k := []byte{prefix, byte((start + i) >> 8), byte(start + i)}
			keys = append(keys, string(k))
			if err := b.AddLeafEntry(k, val, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry: %v", err)
			}
		}
		b.Finish()
		return id, keys
	}

	leftID, leftKeys := buildLeaf('a', 0, leftCount)
	rightID, rightKeys := buildLeaf('b', 0, rightCount)

	entries := []internalEntry{
		{key: []byte{}, child: leftID},
		{key: []byte(rightKeys[0]), child: rightID},
	}
	updated := []bool{true, false}

	var m adaptive.Metrics
	out, retired, err := z.rebalanceUpdatedLeavesWithRightSibling(entries, updated, &m)
	if err != nil {
		t.Fatalf("rebalanceUpdatedLeavesWithRightSibling: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 entries after rebalance, got %d", len(out))
	}
	if len(retired) != 2 {
		t.Fatalf("expected 2 retired pages (old leaves), got %d (%v)", len(retired), retired)
	}

	// Validate leaf live bytes against thresholds.
	leftN, _, err := func() (*node.Node, []byte, error) {
		data, err := p.Get(out[0].child)
		if err != nil {
			return nil, nil, err
		}
		n := node.NewNode(data)
		k, _, _, _, err := n.GetLeafEntryView(0)
		if err != nil {
			return nil, nil, err
		}
		return n, k, nil
	}()
	if err != nil {
		t.Fatalf("load left leaf: %v", err)
	}
	rightN, rightFirstKey, err := func() (*node.Node, []byte, error) {
		data, err := p.Get(out[1].child)
		if err != nil {
			return nil, nil, err
		}
		n := node.NewNode(data)
		k, _, _, _, err := n.GetLeafEntryView(0)
		if err != nil {
			return nil, nil, err
		}
		return n, k, nil
	}()
	if err != nil {
		t.Fatalf("load right leaf: %v", err)
	}

	leftLive, err := z.leafLiveBytesUpperBound(leftN)
	if err != nil {
		t.Fatalf("leafLiveBytesUpperBound(left): %v", err)
	}
	rightLive, err := z.leafLiveBytesUpperBound(rightN)
	if err != nil {
		t.Fatalf("leafLiveBytesUpperBound(right): %v", err)
	}
	if leftLive < targetBytes {
		t.Fatalf("left live bytes too low: got=%d want>=%d", leftLive, targetBytes)
	}
	if rightLive < minBytes {
		t.Fatalf("right live bytes too low: got=%d want>=%d", rightLive, minBytes)
	}

	// Parent separator must be the min key of the right child.
	if !bytes.Equal(out[1].key, rightFirstKey) {
		t.Fatalf("separator key mismatch: got=%q want=%q", out[1].key, rightFirstKey)
	}

	// Build a tiny internal root and query around the boundary.
	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("alloc root: %v", err)
	}
	rootData, err := p.GetForWrite(rootID)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	ib := node.NewBuilder(rootData, page.PageTypeInternal)
	ib.SetPageID(rootID)
	if err := ib.AddInternalChild(out[0].key, out[0].child); err != nil {
		t.Fatalf("add internal child 0: %v", err)
	}
	if err := ib.AddInternalChild(out[1].key, out[1].child); err != nil {
		t.Fatalf("add internal child 1: %v", err)
	}
	ib.Finish()

	tr := tree.New(p, panicValueReader{}, rootID)

	// Key that should be in left.
	if _, err := tr.Get([]byte(leftKeys[0])); err != nil {
		t.Fatalf("Get(left key): %v", err)
	}
	// First key of right.
	if _, err := tr.Get(rightFirstKey); err != nil {
		t.Fatalf("Get(right first key): %v", err)
	}
	// A later key in right (if present).
	if rightN.Count() > 1 {
		k, _, _, _, err := rightN.GetLeafEntryView(1)
		if err != nil {
			t.Fatalf("right leaf view: %v", err)
		}
		if _, err := tr.Get(k); err != nil {
			t.Fatalf("Get(right next key): %v", err)
		}
	}
}

func TestZipper_Rebalance_NoOpWhenLeftAtOrAboveMin(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	capBytes := z.leafCapBytes()
	minBytes := (capBytes * 3) / 4 // must match zipper hysteresis ratio

	val := bytes.Repeat([]byte("v"), 64)
	entryBytes := z.leafEntryBytesUpperBound([]byte{0, 0, 0}, val, page.ValuePtr{}, node.FlagInline)
	leftCount := (minBytes / entryBytes) + 2 // ensure >= min (by our sizing function)

	buildLeaf := func(prefix byte, count int) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := z.newLeafBuilder(data)
		b.SetPageID(id)
		for i := 0; i < count; i++ {
			k := []byte{prefix, 0, byte(i)}
			if err := b.AddLeafEntry(k, val, node.FlagInline, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry: %v", err)
			}
		}
		b.Finish()
		return id
	}

	leftID := buildLeaf('a', leftCount)
	rightID := buildLeaf('b', 2)

	entries := []internalEntry{
		{key: []byte{}, child: leftID},
		{key: []byte{'b', 0, 0}, child: rightID},
	}
	updated := []bool{true, false}

	var m adaptive.Metrics
	out, retired, err := z.rebalanceUpdatedLeavesWithRightSibling(entries, updated, &m)
	if err != nil {
		t.Fatalf("rebalanceUpdatedLeavesWithRightSibling: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected no-op, entries=%d", len(out))
	}
	if len(retired) != 0 {
		t.Fatalf("expected no retirements on no-op, got %v", retired)
	}
}

func TestZipper_Rebalance_DropsTombstonesButPreservesDeleteSemantics(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	alloc := &MockAllocator{p: p}
	z := New(p, alloc)

	// Build a left leaf with tombstones (low live bytes) and a right leaf with values.
	buildLeaf := func(entries []struct {
		k     []byte
		v     []byte
		flags byte
	}) uint64 {
		id, err := p.Alloc(1)
		if err != nil {
			t.Fatalf("alloc leaf: %v", err)
		}
		data, err := p.GetForWrite(id)
		if err != nil {
			t.Fatalf("get leaf: %v", err)
		}
		b := z.newLeafBuilder(data)
		b.SetPageID(id)
		for _, e := range entries {
			if err := b.AddLeafEntry(e.k, e.v, e.flags, page.ValuePtr{}); err != nil {
				t.Fatalf("add leaf entry: %v", err)
			}
		}
		b.Finish()
		return id
	}

	leftID := buildLeaf([]struct {
		k     []byte
		v     []byte
		flags byte
	}{
		{k: []byte("a"), v: nil, flags: node.FlagTombstone},
		{k: []byte("b"), v: nil, flags: node.FlagTombstone},
		{k: []byte("c"), v: []byte("vc"), flags: node.FlagInline},
	})
	rightID := buildLeaf([]struct {
		k     []byte
		v     []byte
		flags byte
	}{
		{k: []byte("d"), v: []byte("vd"), flags: node.FlagInline},
		{k: []byte("e"), v: []byte("ve"), flags: node.FlagInline},
		{k: []byte("f"), v: []byte("vf"), flags: node.FlagInline},
	})

	entries := []internalEntry{
		{key: []byte{}, child: leftID},
		{key: []byte("d"), child: rightID},
	}
	updated := []bool{true, false}

	var m adaptive.Metrics
	out, _, err := z.rebalanceUpdatedLeavesWithRightSibling(entries, updated, &m)
	if err != nil {
		t.Fatalf("rebalanceUpdatedLeavesWithRightSibling: %v", err)
	}

	// Build internal root from resulting entries and query.
	rootID, err := p.Alloc(1)
	if err != nil {
		t.Fatalf("alloc root: %v", err)
	}
	rootData, err := p.GetForWrite(rootID)
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	ib := node.NewBuilder(rootData, page.PageTypeInternal)
	ib.SetPageID(rootID)
	for i := range out {
		if err := ib.AddInternalChild(out[i].key, out[i].child); err != nil {
			t.Fatalf("add internal child: %v", err)
		}
	}
	ib.Finish()

	tr := tree.New(p, panicValueReader{}, rootID)
	// Tombstoned keys must be not found (whether tombstones are preserved or dropped).
	if _, err := tr.Get([]byte("a")); err != tree.ErrKeyNotFound {
		t.Fatalf("expected a deleted, got %v", err)
	}
	if _, err := tr.Get([]byte("b")); err != tree.ErrKeyNotFound {
		t.Fatalf("expected b deleted, got %v", err)
	}
	// Live key must still exist.
	if v, err := tr.Get([]byte("c")); err != nil || !bytes.Equal(v, []byte("vc")) {
		t.Fatalf("expected c=vc, got v=%q err=%v", v, err)
	}
}

func stringsJoin(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		out += sep
		out += parts[i]
	}
	return out
}
