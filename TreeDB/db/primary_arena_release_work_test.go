package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestPrimaryOwnerOrdinaryActualArenaNamespaceAndContainingReceipt(t *testing.T) {
	dir := t.TempDir()
	a, err := primaryarena.Open(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	metadata := a.MetadataOwner()
	charge, err := rootpublication.StableFileMetadataCharge(uint64(len(dir)))
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	owner.namespaceParent, owner.namespaceParentCharge = parent, charge
	if err = a.Pager().WithStableResourceFile(func(file *os.File) error {
		owner.namespaceProof, err = rootpublication.NewOwnedStableNamespaceCreationProof(parent, file, "primary", metadata)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	budgets := make([]*memtable.COWBudget, 80)
	captures := make([]*retainedalloc.Enrollment, 80)
	for i := range budgets {
		budgets[i], err = memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		captures[i], err = metadata.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	held := owner
	consumed, debt, completion, err := owner.releaseOriginalWithOutcome()
	owner = nil
	if !consumed || debt || err != nil {
		t.Fatal(consumed, debt, err)
	}
	if held.arena != nil || held.namespaceParent != nil || held.namespaceProof != nil || held.metadataCharge != 0 || !metadata.PhysicalClosed() {
		t.Fatal("aliases/physical finish incomplete")
	}
	if completion.charge == 0 || metadata.Bytes() < completion.charge {
		t.Fatal("physical cleanup refunded its own containing receipt")
	}
	if !held.closeWork.mu.TryLock() {
		t.Fatal("physical cleanup returned with owner lock held")
	}
	held.closeWork.mu.Unlock()
	pending := metadata.Bytes()
	charge = completion.charge
	held = nil
	completion.complete()
	if metadata.Bytes() != pending-charge {
		t.Fatal("independent containing refund missing")
	}
	completion.complete()
	if metadata.Bytes() != pending-charge {
		t.Fatal("containing receipt replayed")
	}
	for i, capture := range captures {
		capture.Close()
		budgets[i].Close()
		if budgets[i].Stats().TotalBytes != 0 {
			t.Fatal("held capture metadata leaked")
		}
	}
	if metadata.Bytes() != 0 {
		t.Fatal("exact final containing refund incomplete")
	}
}

func TestPrimaryOwnerOrdinaryNonlastHasNoUnadmittedReceiptAllocation(t *testing.T) {
	a, err := primaryarena.Open(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := owner.release(nil); err != nil {
			t.Fatal(err)
		}
	}()
	allocations := testing.AllocsPerRun(20, func() {
		if ready, err := owner.retain(nil); !ready || err != nil {
			panic("retain failed")
		}
		if consumed, debt, err := owner.releaseWithOutcome(nil); !consumed || debt || err != nil {
			panic("ordinary nonlast release failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("ordinary owner edge allocated unadmitted receipt: %g", allocations)
	}
}

func TestPrimaryOwnerOrdinaryNonlastPreservesStickySiblingError(t *testing.T) {
	a, err := primaryarena.Open(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	sticky := errors.New("earlier sibling failed")
	owner.cleanupErr = sticky
	if ready, e := owner.retain(nil); !ready || e != nil {
		t.Fatal(e)
	}
	if consumed, debt, e := owner.releaseWithOutcome(nil); !consumed || debt || !errors.Is(e, sticky) {
		t.Fatalf("nonlast consumed=%v debt=%v err=%v", consumed, debt, e)
	}
	if owner.refs != 1 || owner.arena != a || owner.closeWork.started {
		t.Fatal("nonlast edge started physical cleanup")
	}
	// The original terminal release still reports sticky failure and retains its
	// debt; the scalar fast path must neither clear nor relocate that authority.
	consumed, debt, e := owner.releaseWithOutcome(nil)
	if !consumed || !debt || !errors.Is(e, sticky) {
		t.Fatalf("last consumed=%v debt=%v err=%v", consumed, debt, e)
	}
	if owner.arena != a || owner.metadataCharge == 0 || !owner.closeWork.done {
		t.Fatal("original sticky cleanup debt was lost")
	}
}

func TestPrimaryOwnerOriginalPhysicalFailureKeepsSameReceiptAndCharge(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	metadata, charge := a.MetadataOwner(), owner.metadataCharge
	if err = a.Pager().WithStableResourceFile(func(file *os.File) error { return file.Close() }); err != nil {
		t.Fatal(err)
	}
	consumed, debt, first := owner.releaseWithOutcome(nil)
	if !consumed || !debt || !errors.Is(first, os.ErrClosed) || owner.arena != a || owner.metadataCharge != charge || metadata.Bytes() < charge {
		t.Fatalf("physical failure custody consumed=%t debt=%t err=%v", consumed, debt, first)
	}
	bytes, counters := metadata.Bytes(), a.Counters()
	consumed, debt, again := owner.releaseWithOutcome(nil)
	if consumed || !debt || again != first || metadata.Bytes() != bytes || a.Counters() != counters {
		t.Fatal("original failed cleanup replayed or replaced its receipt", consumed, debt, again)
	}
}
