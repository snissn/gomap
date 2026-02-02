package freelist

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAllocator_HeadCountTracksFreeIDsInHeadPage(t *testing.T) {
	dir := t.TempDir()
	p, err := pager.Open(filepath.Join(dir, "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	// Create some pages so we can free them.
	if _, err := p.Alloc(4); err != nil {
		t.Fatalf("Alloc: %v", err)
	}

	a := New(p, 0)
	if err := a.Free(1); err != nil {
		t.Fatalf("Free(1): %v", err)
	}
	if got, err := a.HeadCount(); err != nil {
		t.Fatalf("HeadCount: %v", err)
	} else if got != 0 {
		// The head page itself is the freelist "node"; the first free creates the
		// head with Count==0 and uses NextPageID to chain.
		t.Fatalf("expected head count 0 after first free, got %d", got)
	}

	if err := a.Free(2); err != nil {
		t.Fatalf("Free(2): %v", err)
	}
	if got, err := a.HeadCount(); err != nil {
		t.Fatalf("HeadCount: %v", err)
	} else if got != 1 {
		t.Fatalf("expected head count 1 after second free, got %d", got)
	}
}
