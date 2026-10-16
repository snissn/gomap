package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"os"
	"path/filepath"
	"testing"
)

func TestPrimaryMetadataPhysicalCloseFailureRetainsActualBacking(t *testing.T) {
	a, err := OpenCapsule(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	component := testComponent(t, a, "held", 1)
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Pager().WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err == nil {
		t.Fatal("injected physical close failure missing")
	}
	// Physical file teardown failed; Arena has deliberately retained the actual
	// RAM index/chunks. Capture refund cannot erase this residual backing charge.
	if a.slot(component.PageID) == nil || a.MetadataOwner().Bytes() == 0 {
		t.Fatal("failed close discarded custody")
	}
	enrollment.Close()
	if budget.Stats().ExternalBytes == 0 {
		t.Fatal("failed physical close erased governor")
	}
}
