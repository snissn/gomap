package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_PublicationFenceAppendsAndDefersFrees(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(5); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}
	a := New(p, 0)
	if err := a.Free(4); err != nil {
		t.Fatalf("Free head: %v", err)
	}
	if err := a.Free(3); err != nil {
		t.Fatalf("Free reusable ID: %v", err)
	}
	head := a.Head()

	if err := a.BeginPublicationFence(); err != nil {
		t.Fatalf("BeginPublicationFence: %v", err)
	}
	got, err := a.Alloc(0)
	if err != nil {
		t.Fatalf("Alloc during fence: %v", err)
	}
	if got != 5 {
		t.Fatalf("Alloc during fence=%d want appended page 5", got)
	}
	if gotHead := a.Head(); gotHead != head {
		t.Fatalf("head changed during fence: got %d want %d", gotHead, head)
	}
	if err := a.Free(5); err != nil {
		t.Fatalf("deferred Free: %v", err)
	}
	if err := a.EndPublicationFence(); err != nil {
		t.Fatalf("EndPublicationFence: %v", err)
	}

	for _, want := range []uint64{5, 3} {
		got, err := a.Alloc(0)
		if err != nil {
			t.Fatalf("Alloc after fence: %v", err)
		}
		if got != want {
			t.Fatalf("Alloc after fence=%d want reusable page %d", got, want)
		}
	}
}

func TestAllocator_RetainDrainedHeadPreservesRecoveryMetadata(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	if _, err := p.Alloc(4); err != nil {
		t.Fatalf("Alloc pages: %v", err)
	}
	a := New(p, 0)
	if err := a.Free(3); err != nil {
		t.Fatalf("Free head: %v", err)
	}
	if err := a.Free(2); err != nil {
		t.Fatalf("Free body: %v", err)
	}
	a.SetRetainDrainedHeads(true)

	got, err := a.AllocMany(2, 0)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	if len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Fatalf("AllocMany=%v want reused body 2 then appended page 4", got)
	}
	data, err := p.Get(3)
	if err != nil {
		t.Fatalf("Get retained head: %v", err)
	}
	n := node.NewNode(data)
	if n.Type() != page.PageTypeFreelist || !n.VerifyChecksum() || n.Count() != 0 {
		t.Fatalf("retained head type=%d count=%d checksum=%v", n.Type(), n.Count(), n.VerifyChecksum())
	}
}
