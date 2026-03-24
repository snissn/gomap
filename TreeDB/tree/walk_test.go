package tree

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestTreeWalkLeaves_VisitsReachableLeaves(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Pager open failed: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(3); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	leaf1, _ := p.Get(1)
	n1 := node.NewNode(leaf1)
	n1.SetPageID(1)
	n1.SetType(page.PageTypeLeaf)
	n1.AddLeafEntry([]byte("a"), []byte("va"), node.FlagInline, page.ValuePtr{})
	n1.UpdateChecksum()

	leaf2, _ := p.Get(2)
	n2 := node.NewNode(leaf2)
	n2.SetPageID(2)
	n2.SetType(page.PageTypeLeaf)
	n2.AddLeafEntry([]byte("b"), []byte("vb"), node.FlagInline, page.ValuePtr{})
	n2.UpdateChecksum()

	root, _ := p.Get(0)
	n0 := node.NewNode(root)
	n0.SetPageID(0)
	n0.SetType(page.PageTypeInternal)
	if err := n0.AddInternalChild([]byte("a"), 1); err != nil {
		t.Fatalf("AddInternalChild(1): %v", err)
	}
	if err := n0.AddInternalChild([]byte("b"), 2); err != nil {
		t.Fatalf("AddInternalChild(2): %v", err)
	}
	n0.UpdateChecksum()

	var visited []uint64
	tr := New(p, panicValueReader{}, 0)
	if err := tr.WalkLeaves(context.Background(), func(pageID uint64, n node.Node) error {
		visited = append(visited, pageID)
		return nil
	}); err != nil {
		t.Fatalf("WalkLeaves: %v", err)
	}
	sort.Slice(visited, func(i, j int) bool { return visited[i] < visited[j] })
	if len(visited) != 2 || visited[0] != 1 || visited[1] != 2 {
		t.Fatalf("visited=%v want [1 2]", visited)
	}
}
