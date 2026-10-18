package db

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNativeResourceBootstrapExactIdentityAndPersistentSelectedImport(t *testing.T) {
	source, _ := dbSelectorFixture(t) // Deliberately generic opening metadata.
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	root := RecoverableRoot{CommitSeq: 1, UserRootPageID: 2, SystemRootPageID: 3, AppliedCommandLSN: 4, MaxEntryRevision: 5}
	set := &RecoverableRootSet{metadata: &owner}
	set.putRootResources(recoverableRootIdentity(root), source)
	if set.NativeResourceMetadataOwner() != &owner {
		t.Fatal("opening allocation authority changed")
	}
	for field := range 5 {
		wrong := root
		switch field {
		case 0:
			wrong.CommitSeq++
		case 1:
			wrong.UserRootPageID++
		case 2:
			wrong.SystemRootPageID++
		case 3:
			wrong.AppliedCommandLSN++
		case 4:
			wrong.MaxEntryRevision++
		}
		before := budget.Stats()
		if imported, err := set.ImportNativeResourcesForRoot(wrong); imported != nil || !errors.Is(err, ErrRecoverableRootSetStale) || before != budget.Stats() {
			t.Fatal("five-scalar mismatch reached import", field, imported, err)
		}
	}
	imported, err := set.ImportNativeResourcesForRoot(root)
	if err != nil || imported.MetadataOwner() != &owner || imported.Len() != 1 {
		t.Fatal("generic bootstrap did not select admitted metadata", imported, err)
	}
	set.putRootResources(recoverableRootIdentity(root), imported)
	sibling, err := set.ImportNativeResourcesForRoot(root)
	if err != nil || sibling == imported || sibling.MetadataOwner() != &owner {
		t.Fatal("selected import did not have independent admitted wrapper", sibling, err)
	}
	probe, work, err := rootpublication.CloneStableResourceSetForLogicalObligationsWithWork(sibling, rootpublication.StableLogicalObligationRequirements{})
	if err != nil || work.SourceEntriesInspected != 0 || work.PhysicalRootShares == 0 {
		t.Fatal("selected steady-state clone walked original entries", work, err)
	}
	set.released.Store(true)
	if set.NativeResourceMetadataOwner() != nil {
		t.Fatal("released capture exposed allocation authority")
	}
	if got, err := set.ImportNativeResourcesForRoot(root); got != nil || !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatal("released capture imported inventory", got, err)
	}
	for _, resources := range []*rootpublication.StableResourceSet{source, imported, sibling, probe} {
		if err := resources.ReleaseWithError(); err != nil {
			t.Fatal(err)
		}
	}
	set.rootResources = [5]recoverableRootResource{}
	set.metadata = nil
	set, source, imported, sibling, probe = nil, nil, nil, nil, nil
	enrollment.Close()
	budget.Close()
	if owner.Bytes() != 0 || budget.Stats().TotalBytes != 0 {
		t.Fatal(owner.Bytes(), budget.Stats())
	}
}

type nativeBootstrapOwnedOperations struct {
	enrollment *retainedalloc.Enrollment
	owner      *retainedalloc.Owner
	refs       atomic.Int32
	calls      atomic.Int32
	failure    error
}

func (o *nativeBootstrapOwnedOperations) MetadataOwner() *retainedalloc.Owner { return o.owner }
func (o *nativeBootstrapOwnedOperations) Retain() error                       { o.refs.Add(1); return nil }
func (o *nativeBootstrapOwnedOperations) FlushThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func (o *nativeBootstrapOwnedOperations) SyncThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func (o *nativeBootstrapOwnedOperations) Release() (bool, error) {
	o.calls.Add(1)
	if o.refs.Load() == 1 {
		return false, o.failure
	}
	o.refs.Add(-1)
	return true, nil
}

func TestNativeResourceBootstrapOriginalCaptureReleaseFailure(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, ResolvedProfile: ProfileNoWALFast, DisableBackgroundPrune: true, IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close() // Expected original cleanup failure is retained in DB custody.
	if err := database.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	opening, err := database.StableResourceMetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	set, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	owner := set.NativeResourceMetadataOwner()
	state := set.visible
	root := RecoverableRoot{CommitSeq: state.CommitSeq, UserRootPageID: state.RootPageID, SystemRootPageID: state.SystemRootPageID, AppliedCommandLSN: state.AppliedCommandLSN, MaxEntryRevision: uint64(state.MaxEntryRevision)}
	file, err := os.CreateTemp(t.TempDir(), "original-bootstrap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	failure := errors.New("original bootstrap producer refused cleanup")
	if err := owner.AddPending(retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(nativeBootstrapOwnedOperations{})))); err != nil {
		t.Fatal(err)
	}
	ops := &nativeBootstrapOwnedOperations{owner: owner, failure: failure, enrollment: opening}
	opening = nil // The original physical producer now owns this enrollment.
	registry := database.StableResourceIdentityPinRegistry()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{MetadataOwner: owner, OwnedOperations: ops, PinRegistry: registry, Kind: rootpublication.ResourceColumnAsset, LogicalLane: "bootstrap", ResourceID: "original", Generation: 1, DiagnosticPath: "original", File: file, Reachability: rootpublication.ReachabilityColumnManifest})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	failed, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.AbandonWithError(); err != nil {
		t.Fatal(err)
	}
	// Use one already admitted fixed capture slot, not a new cleanup owner.
	if len(set.resources) == cap(set.resources) {
		t.Fatal("fixture exhausted original fixed capture slots")
	}
	set.resources = append(set.resources, failed)
	failed, token, builder = nil, nil, nil
	budget.Close()
	beforeCalls := ops.calls.Load()
	err = set.ReleaseWithError()
	if !errors.Is(err, failure) || set.failure.payload != set || set.releaseErr != err || owner.Bytes() == 0 || budget.Stats().TotalBytes == 0 {
		t.Fatal("original failed capture/enrollment lost", err, budget.Stats())
	}
	if set.NativeResourceMetadataOwner() != nil {
		t.Fatal("failed released capture exposed allocation authority")
	}
	if result, err := set.ImportNativeResourcesForRoot(root); result != nil || !errors.Is(err, ErrRecoverableRootSetStale) {
		t.Fatal(result, err)
	}
	calls := ops.calls.Load()
	if calls != beforeCalls+1 || set.ReleaseWithError() != err || ops.calls.Load() != calls {
		t.Fatal("original capture cleanup retried", calls, ops.calls.Load())
	}
}
