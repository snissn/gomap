package db

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
	"unsafe"
)

func TestPrimaryDependencyFailedLeaseRemainsOnActualOwner(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := db.idx.Load().primaryOwner
	metadata := owner.arena.MetadataOwner()
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDependencyLeaseV5{})))
	if err = metadata.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	if ready, e := owner.retain(nil); !ready || e != nil {
		t.Fatal(e)
	}
	// A stale exact bank capability is refused before any physical release.
	// This characterizes failed cleanup, not a replacement for a valid root.
	lease := &primaryDependencyLeaseV5{owner: owner, ref: primaryarena.Ref{PageID: primaryarena.Namespace + 2, Incarnation: ^uint64(0)}, metadataCharge: charge}
	retained := metadata.Bytes()
	err = lease.release()
	if !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("cleanup=%v", err)
	}
	if owner.failedDependencies != lease || lease.owner != owner || lease.ref.PageID == 0 || !lease.failed || metadata.Bytes() != retained {
		t.Fatal("failure lost exact lease or refunded custody")
	}
	if again := lease.release(); again != err || owner.failedDependencies.failedNext != nil {
		t.Fatal("failure repeated effects or duplicated custody")
	}
	// A sibling with no remaining bank debt consumes and refunds its OWN edge
	// while reporting the earlier error; it must not be registered as another
	// failed physical lease or retain a reference which was already decremented.
	if e := metadata.AddPending(charge); e != nil {
		t.Fatal(e)
	}
	if ready, e := owner.retain(nil); !ready || e != nil {
		t.Fatal(e)
	}
	sibling := &primaryDependencyLeaseV5{owner: owner, metadataCharge: charge}
	directory, e := rootpublication.NewOwnedLocalPrimaryDependencyDirectoryV5(owner.arena.Pager(), rootpublication.DependencyDirectoryRefV2{RootPageID: primaryarena.Namespace + 2}, owner.arena.Pager().PageCount(), metadata, sibling.releaseOutcome)
	if e != nil {
		t.Fatal(e)
	}
	sibling.directory = directory
	directory.Release()
	if !sibling.ownerReleaseConsumed || sibling.owner != nil || sibling.metadataCharge != 0 || sibling.failed || owner.failedDependencies != lease || lease.failedNext != nil || metadata.Bytes() != retained {
		t.Fatal("earlier error misclassified consumed sibling ownership")
	}
	// Constructor refusal must not return an error embedded in a lease whose
	// own physical edge and admitted allocation were successfully disposed.
	if e := metadata.AddPending(charge); e != nil {
		t.Fatal(e)
	}
	if ready, e := owner.retain(nil); !ready || e != nil {
		t.Fatal(e)
	}
	refused := &primaryDependencyLeaseV5{owner: owner, metadataCharge: charge}
	cause := errors.New("constructor refusal")
	if got := refused.constructorError(cause); got != cause || refused.metadataCharge != 0 || metadata.Bytes() != retained {
		t.Fatal("completed constructor cleanup returned refunded embedded storage")
	}
	if err = db.Close(); !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("Close hid failed remaining lease: %v", err)
	}
	if owner.failedDependencies != lease || owner.arena == nil || metadata.Bytes() == 0 {
		t.Fatal("Close discarded failed physical custody")
	}
	// The test introduced no real bank edge. Dispose its retained owner only as
	// test cleanup AFTER asserting the production error/custody boundary.
	lease.ref = primaryarena.Ref{}
	owner.mu.Lock()
	owner.failedDependencies = nil
	owner.cleanupErr = nil
	owner.mu.Unlock()
	metadata.RemovePending(charge)
	if _, err = owner.release(nil); err != nil {
		t.Fatal(err)
	}
}

func TestNilDBCloseBeforePublicationCallbackGuard(t *testing.T) {
	var db *DB
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
