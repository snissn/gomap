package db

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/tree"
)

func groupPrimaryOwnerV6(t *testing.T, g *RootPublicationBuildGroup) *rootpublication.DurableRootTransaction {
	t.Helper()
	owner, ready, err := g.idx.primary.ReadRootConstructionOwnerV6(g.currentRoot, &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)})
	if !ready || err != nil {
		t.Fatalf("original constructor: %v %v", ready, err)
	}
	txn, ok := owner.(*rootpublication.DurableRootTransaction)
	if !ok {
		t.Fatalf("owner is %T", owner)
	}
	return txn
}

func groupPrimaryWriteV6(t *testing.T, db *DB, g *RootPublicationBuildGroup, n int, final bool) {
	t.Helper()
	b := db.NewPhysicalBatch().(*Batch)
	defer b.Close()
	if err := b.Set([]byte(fmt.Sprintf("private-%d", n)), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRootPublicationBuildGroup(g, final); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
}

// These are the original production transactions returned by the ordinary
// zipper, not independently constructed stand-ins or scalar accounting probes.
func TestRootPublicationBuildGroupPrimaryV6SupersessionAndHandoff(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetSync([]byte("borrowed"), []byte("base")); err != nil {
		t.Fatal(err)
	}
	basis := db.AcquireSnapshot()
	defer basis.Close()
	g, err := db.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	var prior *rootpublication.DurableRootTransaction
	for n := 0; n < 4; n++ {
		groupPrimaryWriteV6(t, db, g, n, false)
		current := groupPrimaryOwnerV6(t, g)
		if current.Owner() != rootpublication.ResourceOwnerBuilder {
			t.Fatal("private constructor lost custody")
		}
		if prior != nil && prior.Owner() != rootpublication.ResourceOwnerReleased {
			t.Fatal("superseded original constructor retained custody")
		}
		prior = current
	}
	groupPrimaryWriteV6(t, db, g, 4, true)
	if prior.Owner() != rootpublication.ResourceOwnerReleased {
		t.Fatal("final Apply retained predecessor constructor")
	}
	if !g.Accepted() {
		t.Fatal("final group not accepted")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := basis.Get([]byte("borrowed")); err != nil || !bytes.Equal(got, []byte("base")) {
		t.Fatalf("borrowed base %q %v", got, err)
	}
	if got, err := basis.Get([]byte("private-0")); !errors.Is(err, tree.ErrKeyNotFound) || got != nil {
		t.Fatalf("basis changed %q %v", got, err)
	}
	runtime := db.rootPublication
	runtime.mu.Lock()
	member := runtime.visibleMember(db.State().CommitSeq)
	runtime.mu.Unlock()
	if member == nil || member.transaction == nil || member.transaction.Owner() != rootpublication.ResourceOwnerCoordinator {
		t.Fatal("final original transaction did not transfer")
	}
	final := member.transaction
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if final.Owner() != rootpublication.ResourceOwnerReleased {
		t.Fatal("same final transaction not finished")
	}
	for n := 0; n < 5; n++ {
		if got, err := db.Get([]byte(fmt.Sprintf("private-%d", n))); err != nil || !bytes.Equal(got, []byte("value")) {
			t.Fatalf("surviving component %d %q %v", n, got, err)
		}
	}
}

func TestRootPublicationBuildGroupPrimaryV6AbandonKeepsBorrowedReader(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.SetSync([]byte("borrowed"), []byte("base")); err != nil {
		t.Fatal(err)
	}
	basis := db.AcquireSnapshot()
	defer basis.Close()
	g, err := db.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	groupPrimaryWriteV6(t, db, g, 0, false)
	original := groupPrimaryOwnerV6(t, g)
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if original.Owner() != rootpublication.ResourceOwnerReleased {
		t.Fatal("unfinished original constructor retained custody")
	}
	if got, err := basis.Get([]byte("borrowed")); err != nil || !bytes.Equal(got, []byte("base")) {
		t.Fatalf("borrowed reader %q %v", got, err)
	}
	if got, err := db.Get([]byte("private-0")); err != nil || got != nil {
		t.Fatalf("abandoned root published %q %v", got, err)
	}
}

// A surviving bank reader owns its immutable root and component closure; it
// must not retain the released mutable transaction descriptor after Abort.
func TestPrimaryConstructionV6AbortDetachesRetainedReader(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a := db.idx.Load().primary
	id, err := db.idx.Load().zipper.CopyPrimaryRoot(db.State().RootPageID)
	if err != nil {
		t.Fatal(err)
	}
	w := &iterator.OrdinalScanWork{RecordLimit: ^uint64(0), ByteLimit: ^uint64(0)}
	owner, ready, err := a.ReadRootConstructionOwnerV6(id, w)
	if !ready || err != nil {
		t.Fatal(ready, err)
	}
	ref := owner.PrimaryConstructionReferenceV6()
	if ready, err := a.Acquire(ref, w); !ready || err != nil {
		t.Fatal(ready, err)
	}
	defer a.Drop(ref, nil)
	if err := owner.Abort(); err != nil {
		t.Fatal(err)
	}
	if retained, ready, err := a.ReadRootConstructionOwnerV6(id, w); ready || retained != nil || err != primaryarena.ErrStale {
		t.Fatal("released descriptor retained by immutable reader", ready, err)
	}
	if _, ready, err := a.BorrowReadRootCertificateV6(ref, w); !ready || err != nil {
		t.Fatal("reader root lost", ready, err)
	}
}

// Invalidating the original root's edge injects a genuine original-transaction
// Abort refusal. The failed transaction and DATA tracker stay on the SAME
// original physical owner, while Close releases serialization and teardown.
func TestRootPublicationBuildGroupPrimaryV6FailedAbortRetainsOriginalDebt(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	basis := db.AcquireSnapshot()
	g, err := db.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	b := db.NewPhysicalBatch().(*Batch)
	for n := 0; n < 64; n++ {
		if err := b.Set([]byte(fmt.Sprintf("materialized-%03d", n)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.SetRootPublicationBuildGroup(g, false); err != nil {
		t.Fatal(err)
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	original := groupPrimaryOwnerV6(t, g)
	ref := original.PrimaryConstructionReferenceV6()
	owner := g.idx.primaryOwner
	if want := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryArenaOwnerV5{}))); owner.metadataCharge != want {
		t.Fatalf("containing owner admitted %d, want %d", owner.metadataCharge, want)
	}
	pending := owner.arena.MetadataOwner().Bytes()
	pages := g.tracker.Pages()
	if len(pages) == 0 {
		t.Fatal("fault fixture did not build actual DATA debt")
	}
	// Consume the exact original root edge to force the production Abort's stale
	// reference refusal; do not substitute a fake transaction or disposal hook.
	if ready, err := owner.arena.Drop(ref, nil); !ready || err != nil {
		t.Fatal(ready, err)
	}
	err = g.Close()
	if !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
		t.Fatalf("failed Abort %v", err)
	}
	if g.primaryCurrent != original || original.PrimaryConstructionReferenceV6() != ref || original.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatal("original failed custody lost")
	}
	if owner.failedBuildGroups != g || !g.registered || g.idx == nil || g.tracker == nil {
		t.Fatal("group/pins/tracker debt lost")
	}
	if got := g.tracker.Pages(); len(got) != len(pages) {
		t.Fatal("DATA refunded after failed original Abort")
	}
	if owner.arena.MetadataOwner().Bytes() < pending {
		t.Fatal("failed constructor metadata refunded")
	}
	if again := g.Close(); again != err {
		t.Fatal("Close lost original failure receipt")
	}
	// A captured basis read and every serialization lock must be released even
	// though exact resource custody survives. This also checks shutdown ordering.
	done := make(chan error, 1)
	go func() {
		basis.Close()
		db.writeMu.Lock()
		db.writeMu.Unlock()
		db.durablePublishMu.Lock()
		db.durablePublishMu.Unlock()
		done <- db.Close()
	}()
	select {
	case closeErr := <-done:
		if !errors.Is(closeErr, rootpublication.ErrDurableRootOwnership) {
			t.Fatalf("shutdown lost debt %v", closeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed disposal deadlocked shutdown")
	}
	if owner.metadataCharge == 0 || owner.arena == nil || owner.arena.MetadataOwner().Bytes() == 0 {
		t.Fatal("physical-last-owner failure refunded debt")
	}
}

// A failing successor cannot dispose its still-needed predecessor or DATA
// closure. Both custody cells contain the SAME original production objects.
func TestRootPublicationBuildGroupPrimaryV6FailedNextKeepsPredecessor(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	g, err := db.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	first := db.NewPhysicalBatch().(*Batch)
	for n := 0; n < 64; n++ {
		if err := first.Set([]byte(fmt.Sprintf("first-%03d", n)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.SetRootPublicationBuildGroup(g, false); err != nil {
		t.Fatal(err)
	}
	if err := first.Write(); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	predecessor := groupPrimaryOwnerV6(t, g)
	pages := g.tracker.Pages()
	var successor *rootpublication.DurableRootTransaction
	g.idx.zipper.SetPrimaryConstructorV6(func(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
		output, ready, err := rootpublication.NewPrimaryConstructionTransactionV6(a, w)
		if ready && err == nil {
			successor = output.(*rootpublication.DurableRootTransaction)
			if ready, err := a.Drop(successor.PrimaryConstructionReferenceV6(), nil); !ready || err != nil {
				t.Fatal(ready, err)
			}
		}
		return output, ready, err
	})
	second := db.NewPhysicalBatch().(*Batch)
	if err := second.Set([]byte("successor"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := second.SetRootPublicationBuildGroup(g, false); err != nil {
		t.Fatal(err)
	}
	err = second.Write()
	_ = second.Close()
	err = errors.Join(err, g.Close())
	if !errors.Is(err, rootpublication.ErrDurableRootOwnership) || !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("failed successor %v", err)
	}
	if g.primaryCurrent != predecessor || g.primaryNext != successor || predecessor.Owner() != rootpublication.ResourceOwnerBuilder || successor.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatal("failed successor released an original custody cell")
	}
	if len(g.tracker.Pages()) < len(pages) || !g.registered || g.idx.primaryOwner.failedBuildGroups != g || g.idx.primaryOwner.failedTransactions != successor {
		t.Fatalf("failed successor lost original closure: pages=%d prior=%d registered=%v failedGroup=%p group=%p failedTxn=%p successor=%p", len(g.tracker.Pages()), len(pages), g.registered, g.idx.primaryOwner.failedBuildGroups, g, g.idx.primaryOwner.failedTransactions, successor)
	}
	if !db.publicationPoisoned.Load() {
		t.Fatal("failed cleanup left write admission open")
	}
	if err := g.Close(); !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
		t.Fatalf("repeat close %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case err := <-done:
		if !errors.Is(err, rootpublication.ErrDurableRootOwnership) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed next deadlocked shutdown")
	}
	if g.idx.primaryOwner.metadataCharge == 0 || predecessor.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatal("failed physical owner refunded originals")
	}
}

// The original predecessor refuses disposal after the actual successor seals.
// Close must preserve that FIRST refusal and both constructors, without trying
// either cleanup action again or refunding the group's unresolved closure.
func TestRootPublicationBuildGroupPrimaryV6SupersessionRefusalKeepsBoth(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	basis := database.AcquireSnapshot()
	g, err := database.BeginRootPublicationBuildGroupFromSnapshot(basis)
	if err != nil {
		t.Fatal(err)
	}
	groupPrimaryWriteV6(t, database, g, 0, false)
	predecessor := groupPrimaryOwnerV6(t, g)
	owner := g.idx.primaryOwner
	pages := len(g.tracker.Pages())
	var successor *rootpublication.DurableRootTransaction
	g.idx.zipper.SetPrimaryConstructorV6(func(a *primaryarena.Arena, w *iterator.OrdinalScanWork) (primaryarena.ReadRootConstructionV6, bool, error) {
		// Consume the genuine original edge. Its immutable image remains available
		// to this already-admitted zipper until ordinary retirement progresses.
		if ready, err := a.Drop(predecessor.PrimaryConstructionReferenceV6(), nil); !ready || err != nil {
			t.Fatal(ready, err)
		}
		output, ready, err := rootpublication.NewPrimaryConstructionTransactionV6(a, w)
		if ready && err == nil {
			successor = output.(*rootpublication.DurableRootTransaction)
		}
		return output, ready, err
	})
	b := database.NewPhysicalBatch().(*Batch)
	if err := b.Set([]byte("successor"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRootPublicationBuildGroup(g, false); err != nil {
		t.Fatal(err)
	}
	refusal := b.Write()
	_ = b.Close()
	if !errors.Is(refusal, rootpublication.ErrDurableRootOwnership) {
		t.Fatalf("predecessor refusal %v", refusal)
	}
	if successor == nil || successor.PrimaryConstructionCertificateV6() == nil {
		t.Fatal("actual successor did not seal before predecessor refusal")
	}
	closeErr := g.Close()
	if !errors.Is(closeErr, refusal) || !database.publicationPoisoned.Load() {
		t.Fatalf("first refusal lost: write=%v close=%v", refusal, closeErr)
	}
	if g.primaryCurrent != predecessor || g.primaryNext != successor || predecessor.Owner() != rootpublication.ResourceOwnerBuilder || successor.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatal("Close disposed original custody after predecessor refusal")
	}
	if !g.registered || g.idx == nil || g.tracker == nil || len(g.tracker.Pages()) < pages || owner.failedBuildGroups != g {
		t.Fatal("failed predecessor closure refunded")
	}
	if again := g.Close(); again != closeErr {
		t.Fatal("repeated Close replayed cleanup or changed receipt")
	}
	done := make(chan error, 1)
	go func() {
		basis.Close()
		database.writeMu.Lock()
		database.writeMu.Unlock()
		database.durablePublishMu.Lock()
		database.durablePublishMu.Unlock()
		done <- database.Close()
	}()
	select {
	case err := <-done:
		if !errors.Is(err, refusal) {
			t.Fatalf("shutdown lost first refusal %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("predecessor refusal deadlocked shutdown")
	}
	if owner.metadataCharge == 0 || successor.Owner() != rootpublication.ResourceOwnerBuilder {
		t.Fatal("shutdown refunded failed custody")
	}
}
