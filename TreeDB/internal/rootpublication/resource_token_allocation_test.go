package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestOwnedResourceTokenDescriptorOutlivesPhysicalRelease(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "owned.bin", "payload")
	identity, observeErr := StableIdentityFromFile(file)
	if observeErr != nil {
		t.Fatal(observeErr)
	}
	if observeErr = registry.Observe(identity); observeErr != nil {
		t.Fatal(observeErr)
	}
	defer registry.Unobserve(identity)
	token, err := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "db/index", ResourceID: "index.db", Generation: 1, DiagnosticPath: "index.db", File: file, Frontier: DurableFrontier{Bytes: 7}, Reachability: ReachabilityIndexFile})
	if err != nil {
		t.Fatal(err)
	}
	backing, err := newResourceEntryBacking(&owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = backing.entries[0].bindOwnedToken(token); err != nil {
		t.Fatal(err)
	}
	physical := token.physicalBacking
	charge := owner.Bytes()
	clone, err := token.cloneSharedPinned("db/index", "next", "index.db", token.frontier, token.reachability, token.logicalObligations, nil)
	if err != nil {
		t.Fatal(err)
	}
	token.Release()
	if token.ResourceID() != "index.db" || backing.entries[0].token != token || physical.refs.Load() != 1 {
		t.Fatal("physical release stole descriptor or clone custody")
	}
	if _, err = clone.pinned.Stat(); err != nil {
		t.Fatal(err)
	}
	clone.Release()
	if physical.file != nil || registry.Stats().ActivePins != 0 {
		t.Fatal("last physical edge retained handle or identity pin")
	}
	if owner.Bytes() == 0 || owner.Bytes() >= charge {
		t.Fatal("descriptor alias must retain exactly its metadata after physical retirement")
	}
	if token.ResourceID() != "index.db" {
		t.Fatal("descriptor was cleared while entry alias survived")
	}
	backing.release()
	if owner.Bytes() != 0 || token.metadata != nil {
		t.Fatalf("last descriptor alias retained %d bytes", owner.Bytes())
	}
}

func TestOwnedResourceTokenRefusalKeepsInputsAndCharges(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "refusal.bin", "x")
	identity, observeErr := StableIdentityFromFile(file)
	if observeErr != nil {
		t.Fatal(observeErr)
	}
	if observeErr = registry.Observe(identity); observeErr != nil {
		t.Fatal(observeErr)
	}
	defer registry.Unobserve(identity)
	spec := StableResourceSpec{MetadataOwner: &owner, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "db/index", ResourceID: "index.db", Generation: 1, DiagnosticPath: "index.db", File: file, Frontier: DurableFrontier{Bytes: 1}, Reachability: ReachabilityIndexFile}
	token, err := NewStableResourceToken(spec)
	if err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	budget.cap = before
	if clone, e := token.cloneSharedPinned("db/index", "next", "index.db", token.frontier, token.reachability, nil, nil); clone != nil || !errors.Is(e, retainedalloc.ErrCapacity) {
		t.Fatalf("clone refusal=%v %v", clone, e)
	}
	if owner.Bytes() != before || token.physicalBacking.refs.Load() != 1 || registry.Stats().ActivePins != 1 {
		t.Fatal("refused clone consumed input or leaked custody")
	}
	budget.cap = 1 << 20
	spec.OnRelease = func() {}
	if next, e := NewStableResourceToken(spec); next != nil || !errors.Is(e, ErrResourceOwnership) {
		t.Fatalf("opaque callback accepted=%v %v", next, e)
	}
	if owner.Bytes() != before {
		t.Fatal("callback refusal allocated retained storage")
	}
	token.Release()
	if owner.Bytes() != baseline {
		t.Fatalf("token cleanup retained %d bytes above baseline %d", owner.Bytes(), baseline)
	}
	if _, err = file.Stat(); err != nil {
		t.Fatal("cleanup closed producer handle", err)
	}
}

// The operation environment is a separately admitted allocation, shared by
// genuine token clones and retained until the last immutable descriptor alias.
type ownedTokenOperationsFixture struct {
	owner    *retainedalloc.Owner
	refs     atomic.Int32
	syncs    atomic.Int32
	disposed atomic.Bool
	charge   uint64
}

func (o *ownedTokenOperationsFixture) MetadataOwner() *retainedalloc.Owner { return o.owner }
func (o *ownedTokenOperationsFixture) Retain() error                       { o.refs.Add(1); return nil }
func (o *ownedTokenOperationsFixture) Release() {
	if o.refs.Add(-1) == 0 {
		o.disposed.Store(true)
		o.owner.RemovePending(o.charge)
	}
}
func (o *ownedTokenOperationsFixture) FlushThrough(*os.File, DurableFrontier) error { return nil }
func (o *ownedTokenOperationsFixture) SyncThrough(*os.File, DurableFrontier) error {
	o.syncs.Add(1)
	return nil
}
func TestOwnedResourceTokenOperationsLastDescriptorAlias(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(ownedTokenOperationsFixture{})))
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	ops := &ownedTokenOperationsFixture{owner: &owner, charge: charge}
	ops.refs.Store(1)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "operations.bin", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	token, err := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owner, OwnedOperations: ops, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "db/index", ResourceID: "index.db", Generation: 1, DiagnosticPath: "index.db", File: file, Frontier: DurableFrontier{Bytes: 7}, Reachability: ReachabilityIndexFile})
	if err != nil {
		t.Fatal(err)
	}
	ops.Release() // Constructor edge; only actual token descriptors now own it.
	backing, err := newResourceEntryBacking(&owner, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = backing.entries[0].bindOwnedToken(token); err != nil {
		t.Fatal(err)
	}
	clone, err := token.cloneSharedPinned("db/index", "clone", "index.db", token.frontier, token.reachability, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = clone.SyncThrough(); err != nil {
		t.Fatal(err)
	}
	if ops.syncs.Load() != 1 {
		t.Fatal("clone bypassed real pager operations")
	}
	token.Release()
	clone.Release()
	if ops.refs.Load() != 1 || ops.disposed.Load() || owner.Bytes() == 0 {
		t.Fatal("physical release stole operation/descriptor custody")
	}
	backing.release()
	if ops.refs.Load() != 0 || !ops.disposed.Load() || owner.Bytes() != 0 {
		t.Fatalf("last descriptor cleanup refs=%d bytes=%d", ops.refs.Load(), owner.Bytes())
	}
}
