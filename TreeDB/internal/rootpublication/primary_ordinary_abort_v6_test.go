package rootpublication

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
)

func TestOrdinaryPrimaryAbortDetachesDiscoveryAndRefundsAfterUnlock(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	original, ready, err := NewPrimaryConstructionTransactionV6(a, nil)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	txn := original.(*DurableRootTransaction)
	ref := txn.PrimaryConstructionReferenceV6()
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	observed, locked := false, false
	capture, err := a.MetadataOwner().Enroll(&containingUnlockBudget{actual: budget, observe: func() {
		if txn.constructionV6 == nil {
			observed = true
			if !txn.mu.TryLock() {
				locked = true
			} else {
				txn.mu.Unlock()
			}
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	before := a.Counters().BanksFreed
	if err = txn.AbortPrivatePrimaryConstructionV6(); err != nil {
		t.Fatal(err)
	}
	if !observed || locked || txn.Owner() != ResourceOwnerReleased || a.Counters().BanksFreed != before+1 {
		t.Fatalf("original abort observed=%t locked=%t owner=%v", observed, locked, txn.Owner())
	}
	if _, ready, err = a.ReadRootConstructionOwnerV6(ref.PageID, nil); ready || err == nil {
		t.Fatal("disposed original constructor remained discoverable")
	}
	counts := a.Counters()
	if err = txn.Abort(); err != nil || a.Counters() != counts {
		t.Fatal("terminal abort replayed physical disposal", err)
	}
	capture.Close()
	budget.Close()
}

func TestOrdinaryPrimaryAbortKeepsFirstPhysicalRefusalAndOriginalDebt(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	original, ready, err := NewPrimaryConstructionTransactionV6(a, nil)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	txn := original.(*DurableRootTransaction)
	construction := txn.constructionV6
	refused := errors.New("original abort callback refused")
	calls := 0
	txn.abortComplete = func(DurableRootCallbackInput) error { calls++; return refused }
	bytes := a.MetadataOwner().Bytes()
	freed := a.Counters().BanksFreed
	first := txn.AbortPrivatePrimaryConstructionV6()
	if !errors.Is(first, refused) || txn.constructionV6 != construction || a.MetadataOwner().Bytes() != bytes || a.Counters().BanksFreed != freed {
		t.Fatal("refused disposal lost original retained constructor", first)
	}
	counts := a.Counters()
	if again := txn.Abort(); again != first || calls != 1 || a.Counters() != counts {
		t.Fatal("refused original cleanup replayed", again, calls)
	}
	if a.MetadataOwner().Bytes() != bytes || txn.Owner() != ResourceOwnerBuilder {
		t.Fatal("refused original cleanup refunded or released debt")
	}
}
