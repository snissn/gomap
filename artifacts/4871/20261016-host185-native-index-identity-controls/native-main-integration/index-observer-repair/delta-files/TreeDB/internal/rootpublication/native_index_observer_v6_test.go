package rootpublication

import (
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Real original token construction owns the duplicate and backing. Temporary
// private operand substitutions exercise only scalar observation; every field
// is restored before the same original Release consumer is allowed to run.
func TestNativeIndexObserverOriginalBackingV6(t *testing.T) {
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
	file := writeStableResourceFixture(t, t.TempDir(), "index.db", "payload")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	token, err := NewStableResourceToken(StableResourceSpec{MetadataOwner: &owner, OwnedOperations: ops, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: "db/index", ResourceID: "index.db", Generation: 7, DiagnosticPath: "index.db", File: file, Frontier: DurableFrontier{Bytes: 7}, Reachability: ReachabilityIndexFile})
	if err != nil {
		t.Fatal(err)
	}
	if consumed, err := ops.Release(); !consumed || err != nil {
		t.Fatal(consumed, err)
	}
	defer token.Release()
	identity.Generation = 7
	if token.pinned == file || token.pinned.Fd() == file.Fd() {
		t.Fatal("original constructor did not duplicate source handle")
	}
	before, refs, physicalRefs := owner.Bytes(), ops.refs.Load(), token.physicalBacking.refs.Load()
	observe := func(w *iterator.OrdinalScanWork) (bool, bool, bool) {
		return token.MaintenanceNativeCaptureBindingForTestV6(ops, identity, &owner, registry, w)
	}
	for _, limit := range []uint64{0, 1} {
		w := iterator.OrdinalScanWork{RecordLimit: limit, ByteLimit: 1 << 20}
		if _, _, ready := observe(&w); ready || owner.Bytes() != before || ops.refs.Load() != refs || token.physicalBacking.refs.Load() != physicalRefs {
			t.Fatal("short observer consumed custody", limit)
		}
	}
	full := func() (bool, bool, bool) {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		return observe(&w)
	}
	if sameOps, same, ready := full(); !ready || !sameOps || !same {
		t.Fatal("actual original duplicate rejected")
	}
	for _, field := range []string{"platform", "volume", "object", "generation"} {
		t.Run(field, func(t *testing.T) {
			saved := identity
			switch field {
			case "platform":
				identity.Platform += "wrong"
			case "volume":
				identity.VolumeID ^= 1
			case "object":
				identity.ObjectID[0] ^= 1
			case "generation":
				identity.Generation++
			}
			sameOps, same, ready := full()
			identity = saved
			if !ready || !sameOps || same {
				t.Fatal("wrong full identity accepted")
			}
		})
	}
	for _, shape := range []string{"missing-backing", "wrong-file", "wrong-refs", "zero-refs", "wrong-owner", "wrong-registry", "missing-pin", "wrong-pin-identity", "released"} {
		t.Run(shape, func(t *testing.T) {
			backing := token.physicalBacking
			var wrongRefs atomic.Int64
			savedFile, savedRefs, savedOwner, savedRegistry, savedPin := backing.file, token.pinnedRefs, backing.owner, backing.registry, token.identityPin
			savedIdentity := savedPin.identity
			switch shape {
			case "missing-backing":
				token.physicalBacking = nil
			case "wrong-file":
				backing.file = file
			case "wrong-refs":
				token.pinnedRefs = &wrongRefs // different counter; restored before cleanup
			case "zero-refs":
				backing.refs.Store(0)
			case "wrong-owner":
				backing.owner = nil
			case "wrong-registry":
				backing.registry = nil
			case "missing-pin":
				token.identityPin = nil
			case "wrong-pin-identity":
				savedPin.identity.Generation++
			case "released":
				token.released.Store(true)
			}
			sameOps, same, ready := full()
			token.physicalBacking = backing
			backing.file = savedFile
			token.pinnedRefs = savedRefs
			backing.refs.Store(physicalRefs)
			backing.owner = savedOwner
			backing.registry = savedRegistry
			token.identityPin = savedPin
			savedPin.identity = savedIdentity
			token.released.Store(false)
			if !ready || !sameOps || same {
				t.Fatal("wrong backing closure accepted")
			}
		})
	}
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if sameOps, _, ready := token.MaintenanceNativeCaptureBindingForTestV6(nil, identity, &owner, registry, &w); !ready || sameOps {
		t.Fatal("wrong original operations accepted")
	}
	if owner.Bytes() != before || ops.refs.Load() != refs || token.physicalBacking.refs.Load() != physicalRefs {
		t.Fatal("observer changed original charge/refcount")
	}
	if err := token.ReleaseWithError(); err != nil {
		t.Fatal("original token cleanup", err)
	}
	if owner.Bytes() != 0 || ops.refs.Load() != 0 || !ops.disposed.Load() {
		t.Fatal("original cleanup did not finish exactly once", owner.Bytes())
	}
}
