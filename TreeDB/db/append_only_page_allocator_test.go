package db

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestAppendOnlyPageAllocatorIsScopedToOneBuild(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Alloc(12); err != nil {
		t.Fatal(err)
	}
	allocator := freelist.New(p, 0)
	base := freelist.MustNewFreelistGenerationV1(1, 12, []uint64{5}, nil)
	if err := allocator.EnableCOWV1(base, freelist.NewReservationLedger()); err != nil {
		t.Fatalf("EnableCOWV1: %v", err)
	}

	appended, err := (appendOnlyPageAllocator{alloc: allocator}).Alloc(5)
	if err != nil {
		t.Fatalf("append-only Alloc: %v", err)
	}
	if appended != 12 {
		t.Fatalf("append-only page=%d want high-water page 12", appended)
	}
	reused, err := allocator.Alloc(5)
	if err != nil {
		t.Fatalf("ordinary Alloc: %v", err)
	}
	if reused != 5 {
		t.Fatalf("ordinary page=%d want reusable page 5 after scoped append", reused)
	}
}
