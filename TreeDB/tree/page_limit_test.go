package tree

import (
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"testing"
)

func TestTreePageLimitResetClearsSelectedExtent(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.GrowTo(4); err != nil {
		t.Fatal(err)
	}
	image := make([]byte, page.PageSize)
	n := node.NewNode(image)
	n.SetType(page.PageTypeLeaf)
	n.SetPageID(3)
	if err := n.AddLeafEntry([]byte("key"), []byte("value"), node.FlagInline, page.ValuePtr{}); err != nil {
		t.Fatal(err)
	}
	n.UpdateChecksum()
	if err := p.Write(3, image); err != nil {
		t.Fatal(err)
	}
	tr := NewWithPageLimit(p, nil, 3, 3)
	if _, err := tr.GetAppend([]byte("key"), nil); err == nil {
		t.Fatal("page outside selected extent accepted")
	}
	tr.Reset(p, nil, 3)
	if got, err := tr.GetAppend([]byte("key"), nil); err != nil || string(got) != "value" {
		t.Fatalf("reset retained bound: %q %v", got, err)
	}
}
