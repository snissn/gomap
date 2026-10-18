package collections

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestNativeResourceInventorySurvivesOpeningSnapshotAndEngine(t *testing.T) {
	database, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), Durability: backenddb.DurabilityWALOffRelaxed, ResolvedProfile: backenddb.ProfileNoWALFast, DisableBackgroundPrune: true, IndexPrimaryDirectory: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Set([]byte("raw"), []byte("original")); err != nil {
		t.Fatal(err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	// The actual cache basis already enrolls this original registry before
	// capture. This backend-only fixture explicitly establishes that premise.
	opening, err := database.StableResourceMetadataOwner().Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	basis := database.AcquireSnapshot()
	inventory, err := prepareNativeResourceInventory(database, basis, budget)
	if err != nil || inventory == nil || inventory.bootstrap != nil || inventory.enrollment == nil || inventory.resources == nil {
		t.Fatal("exact bootstrap did not detach its original census", inventory, err)
	}
	engine, err := newNativeEmptyEngine(budget, budget)
	if err != nil {
		t.Fatal(err)
	}
	engine.inventory, inventory = inventory, nil // Transfer original edge.
	c := nativeTestEmptyCollection(t, budget, 1, "collection", nil)
	next, err := prepareNativeCatalogReplacement(engine, nil, c, nativeTestCatalogReplacementEffects(t, nil, c, 2))
	nativeTestReleaseCollection(t, &c)
	if err != nil || next.inventory != engine.inventory || next.inventory.refs.Load() != 2 {
		t.Fatal("successor retained an ancestor rather than inventory edge", err)
	}
	if err := basis.Close(); err != nil {
		t.Fatal(err)
	}
	basis = nil
	opening.Close()
	opening = nil
	nativeTestFinishEngine(t, &engine)
	budget.Close()
	if next.inventory.refs.Load() != 1 || next.inventory.enrollment == nil || budget.Stats().TotalBytes == 0 {
		t.Fatal("opening release detached last native inventory governor")
	}
	if err := next.inventory.resources.FlushThrough(); err != nil {
		t.Fatal("exact selected resources lost their original physical owner", err)
	}
	nativeTestFinishEngine(t, &next)
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.Stats().TotalBytes != 0 {
		t.Fatal(budget.Stats())
	}
}

type nativeInventoryOwnedOperations struct {
	owner   *retainedalloc.Owner
	refs    atomic.Int32
	calls   atomic.Int32
	failure error
	charge  uint64
}

func (o *nativeInventoryOwnedOperations) MetadataOwner() *retainedalloc.Owner { return o.owner }
func (o *nativeInventoryOwnedOperations) Retain() error                       { o.refs.Add(1); return nil }
func (o *nativeInventoryOwnedOperations) FlushThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func (o *nativeInventoryOwnedOperations) SyncThrough(*os.File, rootpublication.DurableFrontier) error {
	return nil
}
func (o *nativeInventoryOwnedOperations) Release() (bool, error) {
	o.calls.Add(1)
	if o.failure != nil && o.refs.Load() == 1 {
		return false, o.failure
	}
	if o.refs.Add(-1) == 0 {
		o.owner.RemovePending(o.charge)
	}
	return true, nil
}

func TestNativeResourceInventoryOriginalFailureKeepsSelectionAndEnrollment(t *testing.T) {
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
	file, err := os.CreateTemp(t.TempDir(), "native-inventory-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	failure := errors.New("original native physical owner refused cleanup")
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(nativeInventoryOwnedOperations{})))
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	ops := &nativeInventoryOwnedOperations{owner: &owner, failure: failure, charge: charge}
	registry := rootpublication.NewIdentityPinRegistry()
	defer registry.Close()
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	obligation := rootpublication.StableLogicalObligation{Class: "column", Kind: "chunk", Namespace: "original", Generation: 1, PartID: 1, FileID: 1, Length: 1, Reachability: rootpublication.ReachabilityColumnManifest, Digest: [32]byte{1}}
	token, err := rootpublication.NewStableResourceToken(rootpublication.StableResourceSpec{MetadataOwner: &owner, OwnedOperations: ops, PinRegistry: registry, Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "original", Generation: 1, DiagnosticPath: "original", File: file, Reachability: rootpublication.ReachabilityColumnManifest, LogicalObligations: []rootpublication.StableLogicalObligation{obligation}})
	if err != nil {
		t.Fatal(err)
	}
	builder, err := rootpublication.NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Add(token); err != nil {
		t.Fatal(err)
	}
	selected, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.AbandonWithError(); err != nil {
		t.Fatal(err)
	}
	lease, err := budget.AdmitNativeBytes(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeResourceInventory{}))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(nativeEngineRefund{}))))
	if err != nil {
		t.Fatal(err)
	}
	inventory := &nativeResourceInventory{resources: selected, metadata: &owner, enrollment: enrollment, refund: &nativeEngineRefund{lease: lease}}
	inventory.refs.Store(1)
	inventory.cleanup.Work = inventory
	selected, enrollment, token, builder = nil, nil, nil, nil
	selector := rootpublication.StableResourceSelector{Kind: rootpublication.ResourceColumnAsset, LogicalLane: "columns", ResourceID: "original", PhysicalGeneration: 1, Obligation: obligation}
	clone, err := inventory.CloneCapturedNativeResource(selector)
	if err != nil || clone.MetadataOwner() != &owner {
		t.Fatal("captured selector escaped original inventory authority", err)
	}
	if err := clone.ReleaseWithError(); err != nil {
		t.Fatal(err)
	}
	wrong := selector
	wrong.Obligation.Namespace = "outside captured inventory"
	if result, err := inventory.CloneCapturedNativeResource(wrong); result != nil || err == nil {
		t.Fatal("captured selector accepted foreign obligation")
	}
	budget.Close()
	work := inventory.release()
	if result, err := inventory.CloneCapturedNativeResource(selector); result != nil || !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatal("released inventory authorized new selection", err)
	}
	before := budget.Stats()
	beforeCalls := ops.calls.Load()
	completion, err := work.Finish()
	if !errors.Is(err, failure) || completion.Refund != nil || work.Work != inventory || inventory.resources == nil || inventory.enrollment == nil || inventory.failure == nil || inventory.refund == nil {
		t.Fatal("original failed selection or governor detached", err, completion)
	}
	calls := ops.calls.Load()
	if calls != beforeCalls+1 || budget.Stats().TotalBytes == 0 || budget.Stats().TotalBytes > before.TotalBytes {
		t.Fatal("wrong original cleanup boundary", calls, before, budget.Stats())
	}
	completion, err = work.Finish()
	if !errors.Is(err, failure) || completion.Refund != nil || ops.calls.Load() != calls || inventory.enrollment == nil {
		t.Fatal("failed original work retried or refunded", err)
	}
}
