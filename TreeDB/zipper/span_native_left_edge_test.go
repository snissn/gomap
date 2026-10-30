package zipper

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// A partial stitch in the first subtree must retain its unbounded left edge.
// Base-delta separators are scratch-backed; copying an empty separator must
// preserve its distinction from an absent parent key.
func TestSpanNativeApplyBaseDeltaLeftEdgePartialStitch(t *testing.T) {
	p, z := newReadOnlyPrepareZipper(t)
	z.SetIndexInternalBaseDelta(true)
	build := func(kind page.PageType, keys []string, children []uint64) uint64 {
		id, err := z.pager.Alloc(1)
		if err != nil {
			t.Fatal(err)
		}
		data, err := z.pager.GetForWrite(id)
		if err != nil {
			t.Fatal(err)
		}
		b := node.NewBuilderWithOptions(data, kind, node.BuilderOptions{InternalBaseDelta: kind == page.PageTypeInternal})
		b.SetPageID(id)
		for i, key := range keys {
			if kind == page.PageTypeInternal {
				err = b.AddInternalChild([]byte(key), children[i])
			} else {
				err = b.AddLeafEntry([]byte(key), []byte("original-"+key), 0, page.ValuePtr{})
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		built := b.Finish()
		if kind == page.PageTypeInternal && !built.InternalBaseDeltaEnabled() {
			t.Fatal("constructed internal page is not base-delta encoded")
		}
		return id
	}
	a := build(page.PageTypeLeaf, []string{"a"}, nil)
	c := build(page.PageTypeLeaf, []string{"c"}, nil)
	m := build(page.PageTypeLeaf, []string{"m"}, nil)
	zLeaf := build(page.PageTypeLeaf, []string{"z"}, nil)
	left := build(page.PageTypeInternal, []string{"", "c"}, []uint64{a, c})
	right := build(page.PageTypeInternal, []string{"m", "z"}, []uint64{m, zLeaf})
	root := build(page.PageTypeInternal, []string{"", "m"}, []uint64{left, right})
	for _, key := range []string{"c", "a"} {
		delta := batch.New(panicValueReader{}, page.DefaultInlineThreshold)
		if err := delta.Set([]byte(key), []byte("updated-"+key)); err != nil {
			t.Fatal(err)
		}
		result, err := z.ApplyWithOptions(root, delta, ApplyOptions{SpanNativeApply: true})
		_ = delta.Close()
		if err != nil {
			t.Fatalf("partial stitch key=%s: %v", key, err)
		}
		if !result.SpanNativeUsed {
			t.Fatal("span-native path not selected")
		}
		root = result.RootID
		rootData, err := z.pager.Get(root)
		if err != nil {
			t.Fatal(err)
		}
		if !node.NewNode(rootData).InternalBaseDeltaEnabled() {
			t.Fatalf("root after partial stitch %s lost base-delta encoding", key)
		}
	}
	tr := tree.New(p, panicValueReader{}, root)
	for key, want := range map[string]string{
		"a": "updated-a", "c": "updated-c", "m": "original-m", "z": "original-z",
	} {
		got, err := tr.Get([]byte(key))
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Errorf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
}
