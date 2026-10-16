package rootpublication

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

type originLeaseBudgetV6 struct{ bytes, resizes, closes uint64 }
type originLeaseV6 struct {
	budget *originLeaseBudgetV6
	bytes  uint64
}

func (b *originLeaseBudgetV6) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	b.bytes += n
	return &originLeaseV6{b, n}, nil
}
func (l *originLeaseV6) Resize(n uint64) error {
	l.budget.bytes = l.budget.bytes - l.bytes + n
	l.bytes = n
	l.budget.resizes++
	return nil
}
func (l *originLeaseV6) Close() { l.budget.bytes -= l.bytes; l.bytes = 0; l.budget.closes++ }

func TestOrdinaryPrimaryCountedLeaseConstructionPromotionAbortV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	budget := &originLeaseBudgetV6{}
	enrollment, err := a.MetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := a.MetadataOwner().Bytes()
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	owner, ready, err := NewPrimaryConstructionTransactionV6(a, w)
	if !ready || err != nil || w.Records == 0 {
		t.Fatal("ordinary counted constructor", ready, err, w)
	}
	txn := owner.(*DurableRootTransaction)
	// Exercise the SAME concrete promotion allocation and original compensation;
	// no fabricated activated transaction or successful receipt is introduced.
	promotion, charge, ready, err := allocatePrimaryPromotionStorageV6(a.MetadataOwner(), &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, txn.constructionV6.nativeWorkOwned)
	if !ready || err != nil || len(promotion.Image) != PrimaryCapsuleSizeV6 {
		t.Fatal("ordinary promotion storage", ready, err)
	}
	promotion.Image = nil
	promotion = nil
	a.MetadataOwner().RemovePending(charge)
	if err = txn.Abort(); err != nil {
		t.Fatal("ordinary max-work abort", err)
	}
	if a.MetadataOwner().Bytes() != before || budget.resizes == 0 {
		t.Fatal("ordinary accounting did not return to original envelope", before, a.MetadataOwner().Bytes(), budget.resizes)
	}
	counters, bytes, resizes := a.Counters(), a.MetadataOwner().Bytes(), budget.resizes
	native, ready, err := NewNativeMVCCPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if native != nil || ready || !errors.Is(err, retainedalloc.ErrWorkRequired) || a.Counters() != counters || a.MetadataOwner().Bytes() != bytes || budget.resizes != resizes {
		t.Fatal("native constructor used ordinary lease", native, ready, err)
	}
	promotion, charge, ready, err = allocatePrimaryPromotionStorageV6(a.MetadataOwner(), &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}, true)
	if promotion != nil || charge != 0 || ready || !errors.Is(err, retainedalloc.ErrWorkRequired) || a.MetadataOwner().Bytes() != bytes || budget.resizes != resizes {
		t.Fatal("native promotion used ordinary lease", ready, err)
	}
	enrollment.Close()
	if budget.bytes != 0 || budget.closes == 0 {
		t.Fatal("original lease not closed", budget)
	}
}
