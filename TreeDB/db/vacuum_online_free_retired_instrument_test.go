//go:build treedb_freelist_instrument

package db

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestVacuumFreeRetired_GetForWriteFailurePreservesSuffix(t *testing.T) {
	const suffixIDs = 7
	pageCount := page.MaxFreeIDs + suffixIDs + 2
	p, err := pager.Open(filepath.Join(t.TempDir(), "index.db"), 64*1024)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.Alloc(pageCount); err != nil {
		t.Fatalf("Alloc(%d): %v", pageCount, err)
	}
	a := freelist.New(p, 0)
	retired := make([]uint64, page.MaxFreeIDs+1+suffixIDs)
	for i := range retired {
		retired[i] = uint64(i + 1)
	}
	injectedErr := errors.New("injected second get-for-write failure")
	a.TestInjectGetForWriteFailureAfter(1, injectedErr)

	if err := freeVacuumRetired(a, retired); err != nil {
		t.Fatalf("freeVacuumRetired: %v", err)
	}

	if got := a.TestInjectedGetForWriteFailures(); got != 1 {
		t.Fatalf("injected GetForWrite failures = %d, want 1", got)
	}
	stats := a.Counters()
	if got, want := stats.ReclaimablePages(), uint64(len(retired)); got != want {
		t.Fatalf("reclaimable pages = %d, want all retired pages %d", got, want)
	}
	got, err := a.AllocMany(len(retired), 0)
	if err != nil {
		t.Fatalf("AllocMany: %v", err)
	}
	slices.Sort(got)
	if !slices.Equal(got, retired) {
		t.Fatalf("recovered retired IDs differ: got %v want %v", got, retired)
	}
}
