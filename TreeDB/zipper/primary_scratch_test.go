package zipper

import (
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestPrimaryScratchRefusalAndExactAliasRefund(t *testing.T) {
	a, err := primaryarena.Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	limits := memtable.DefaultCOWLimits()
	capBytes := a.MetadataOwner().Bytes() + 2048
	limits.MaxTotalBytes, limits.MaxGenerationBytes, limits.MaxRetiredBytes, limits.MaxInFlightBytes = capBytes, capBytes, capBytes, capBytes
	budget, err := memtable.NewCOWBudget(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer budget.Close()
	e, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	before, governed := a.MetadataOwner().Bytes(), budget.Stats().ExternalBytes
	image, err := makePrimaryScratch[byte](a, page.PageSize, page.PageSize)
	if err == nil || image != nil || a.MetadataOwner().Bytes() != before || budget.Stats().ExternalBytes != governed {
		t.Fatal("refused scratch allocated or changed governor")
	}
	image, err = makePrimaryScratch[byte](a, 8, 32)
	if err != nil {
		t.Fatal(err)
	}
	image[0] = 1
	releasePrimaryScratch(a, &image)
	if image != nil || a.MetadataOwner().Bytes() != before || budget.Stats().ExternalBytes != governed {
		t.Fatal("scratch refund retained alias or changed baseline")
	}
	releasePrimaryScratch(a, &image)
	if a.MetadataOwner().Bytes() != before {
		t.Fatal("empty scratch refunded twice")
	}
}
