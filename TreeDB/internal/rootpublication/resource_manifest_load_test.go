package rootpublication

import (
	"bytes"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"reflect"
	"testing"
	"unsafe"
)

func TestOwnedManifestPhysicalLoaderCanonicalRebindAndRefusal(t *testing.T) {
	identity := StableIdentity{Platform: "unix", VolumeID: 9, ObjectID: [16]byte{4}, Generation: 2}
	parent := StableIdentity{Platform: "unix", VolumeID: 9, ObjectID: [16]byte{8}, Generation: 7}
	frontier := NewRIDFrontier([]uint64{2, 8})
	frontier.Bytes = 4096
	generic, err := NewDependencyManifestV1([]DependencyManifestEntryV1{{
		Kind: ResourceIndex, LogicalLane: "main", ResourceID: "index", DiagnosticPath: "index.db",
		Identity: identity, Generation: 2, Frontier: frontier,
		Reachability: []ReachabilityField{ReachabilityIndexFile},
		Namespace:    &DependencyManifestNamespaceV1{ParentIdentity: parent, Operation: NamespaceCreate, NewName: "index.db", DiagnosticPath: "value_vlog"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	store := freelist.NewMemoryPageStoreV1()
	ref, err := generic.Materialize(100, store)
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	budget.cap = baseline +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceAllocation{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(DependencyManifestV1{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(dependencyManifestEncodedEntryV1{}))) +
		retainedalloc.AllocationCharge(uint64(unsafe.Sizeof((*dependencyManifestEncodedEntryV1)(nil)))) +
		retainedalloc.AllocationCharge(ref.ByteLength) // decoded array overlaps admitted physical payload
	if got, err := LoadDependencyManifestWithMetadataV1(store, ref, &owner); !errors.Is(err, retainedalloc.ErrCapacity) || got != nil {
		t.Fatalf("refusal=%v %v", got, err)
	}
	if owner.Bytes() != baseline {
		t.Fatal("partial refusal leaked metadata")
	}
	budget.cap = 1 << 20
	loaded, err := LoadDependencyManifestWithMetadataV1(store, ref, &owner)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.DiagnosticProjectionV1() != nil || loaded.Entries() != nil {
		t.Fatal("selected diagnostics escaped")
	}
	if err = loaded.WithEntriesV1(func(values []DependencyManifestEntryV1) error {
		if !reflect.DeepEqual(values, generic.Entries()) {
			t.Fatal("canonical decoded fields differ")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	budget.cap = before
	visits := 0
	if err = loaded.RebindPhysicalIdentitiesV1(func(e DependencyManifestEntryV1) (StableIdentity, StableIdentity, error) {
		visits++
		return e.Identity, e.Namespace.ParentIdentity, nil
	}); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("rewrite refusal=%v", err)
	}
	if owner.Bytes() != before || visits != 0 {
		t.Fatal("rewrite refusal leaked overlap or invoked effects before admission")
	}
	budget.cap = 1 << 20
	identity.ObjectID[0] = 10
	parent.ObjectID[0] = 11
	if err = loaded.RebindPhysicalIdentitiesV1(func(e DependencyManifestEntryV1) (StableIdentity, StableIdentity, error) {
		return identity, parent, nil
	}); err != nil {
		t.Fatal(err)
	}
	reboundStore := freelist.NewMemoryPageStoreV1()
	rebound, err := loaded.Materialize(100, reboundStore)
	if err != nil {
		t.Fatal(err)
	}
	if rebound.Digest == ref.Digest {
		t.Fatal("identity rewrite retained old digest")
	}
	check, err := LoadDependencyManifestV1(reboundStore, rebound)
	if err != nil {
		t.Fatal(err)
	}
	entries := check.Entries()
	if entries[0].Identity != identity || entries[0].Namespace.ParentIdentity != parent || !reflect.DeepEqual(entries[0].Frontier, generic.Entries()[0].Frontier) {
		t.Fatal("rebind changed logical frontier or missed physical identity")
	}
	loaded.ReleaseOwnedMetadataV1()
	if owner.Bytes() != baseline {
		t.Fatalf("loader retained %d baseline %d", owner.Bytes(), baseline)
	}
	// Generic post-release diagnostic lifetime is independent and preserved.
	if len(generic.Entries()) != 1 || generic.DiagnosticProjectionV1() != generic {
		t.Fatal("generic diagnostics changed")
	}
	corrupt := append([]byte(nil), reboundStore.Pages[100]...)
	corrupt[200] ^= 1
	reboundStore.Pages[100] = corrupt
	if got, err := LoadDependencyManifestWithMetadataV1(reboundStore, rebound, &owner); err == nil || got != nil {
		t.Fatal("corrupt physical payload accepted")
	}
	if owner.Bytes() != baseline {
		t.Fatal("corrupt loader retained backing")
	}
}

func TestOwnedRecoveredNamespaceIndependentOriginalObservation(t *testing.T) {
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "index.db", "12345678")
	// Use the actual child parent, never a diagnostic-path replacement.
	parent, err := OpenStableParent(file.Name()[:len(file.Name())-len("index.db")])
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	generation, err := StableNamespaceParentGeneration(parent)
	if err != nil {
		t.Fatal(err)
	}
	id, err := StableIdentityFromFile(parent)
	if err != nil {
		t.Fatal(err)
	}
	id.Generation = generation
	childIdentity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(childIdentity); err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	childIdentity.Generation = 1
	manifest, err := NewDependencyManifestV1([]DependencyManifestEntryV1{{
		Kind: ResourceIndex, LogicalLane: "main", ResourceID: "index", DiagnosticPath: "index.db",
		Identity: childIdentity, Generation: 1, Frontier: DurableFrontier{Bytes: 8},
		Reachability: []ReachabilityField{ReachabilityIndexFile},
		Namespace:    &DependencyManifestNamespaceV1{ParentIdentity: id, Operation: NamespaceCreate, NewName: "index.db", DiagnosticPath: "value_vlog"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	store := freelist.NewMemoryPageStoreV1()
	ref, err := manifest.Materialize(500, store)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadDependencyManifestWithMetadataV1(store, ref, &owner)
	if err != nil {
		t.Fatal(err)
	}
	var namespace *StableNamespaceToken
	var token *StableResourceToken
	err = loaded.WithEntriesV1(func(entries []DependencyManifestEntryV1) error {
		e := entries[0]
		var err error
		namespace, err = NewRecoveredStableNamespaceTokenWithMetadata(StableNamespaceSpec{
			Parent: parent, LinkedResource: file, ParentGeneration: e.Namespace.ParentIdentity.Generation,
			Operation: e.Namespace.Operation, NewName: e.Namespace.NewName, DiagnosticPath: e.Namespace.DiagnosticPath,
		}, e.Namespace.ParentIdentity, &owner, registry)
		if err != nil {
			return err
		}
		token, err = NewStableResourceToken(StableResourceSpec{
			Kind: e.Kind, LogicalLane: e.LogicalLane, ResourceID: e.ResourceID, Generation: e.Generation,
			DiagnosticPath: e.DiagnosticPath, File: file, Frontier: e.Frontier, Reachability: e.Reachability[0],
			Namespace: namespace, ContentSynced: true, PinRegistry: registry, OriginalObservation: registry, MetadataOwner: &owner,
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	before := owner.Bytes()
	loaded.ReleaseOwnedMetadataV1()
	if owner.Bytes() == 0 || owner.Bytes() >= before || token.logicalLane != "main" || token.resourceID != "index" ||
		token.diagnosticPath != "index.db" || token.namespace.newName != "index.db" || token.namespace.diagnosticPath != "value_vlog" {
		t.Fatal("resolver output borrowed disposed manifest metadata or missed independent charge")
	}
	namespace.Release()
	token.Release()
	token.Release()
	if owner.Bytes() != 0 || registry.Stats().ActivePins != 0 || registry.ObserverCount(childIdentity) != 0 {
		t.Fatal("last selected output leaked admitted backing or physical pin")
	}
	registry.Close()
	if err = registry.CleanupError(); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedDependencyPhysicalRebindCanonicalScopeAndRefusal(t *testing.T) {
	entry := DependencyManifestEntryV1{Kind: ResourceIndex, LogicalLane: "main", ResourceID: "index", DiagnosticPath: "index.db",
		Identity: StableIdentity{Platform: "unix", ObjectID: [16]byte{1}, Generation: 3}, Generation: 3,
		Frontier: NewRIDFrontier([]uint64{0, 2, 9}), Reachability: []ReachabilityField{ReachabilityIndexFile},
		Namespace: &DependencyManifestNamespaceV1{ParentIdentity: StableIdentity{Platform: "unix", ObjectID: [16]byte{2}, Generation: 7}, Operation: NamespaceCreate, NewName: "index.db", DiagnosticPath: "indexes"}}
	entry.Frontier.Bytes = 4096
	key := DependencyPhysicalKeyV2(entry)
	value, err := EncodeDependencyPhysicalV2(entry)
	if err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	budget.cap = baseline
	visits, outputs := 0, 0
	rebind := func(e DependencyManifestEntryV1) (StableIdentity, StableIdentity, error) {
		visits++
		e.Identity.ObjectID[0] = 8
		p := e.Namespace.ParentIdentity
		p.ObjectID[0] = 9
		return e.Identity, p, nil
	}
	output := func(encoded []byte) error {
		outputs++
		actual, e := DecodeDependencyPhysicalV2(key, encoded)
		if e != nil {
			return e
		}
		if actual.Identity.ObjectID[0] != 8 || actual.Namespace.ParentIdentity.ObjectID[0] != 9 || !reflect.DeepEqual(actual.Frontier, entry.Frontier) || !bytes.Equal(key, DependencyPhysicalKeyV2(actual)) {
			t.Fatal("rebind changed logical authority")
		}
		return nil
	}
	if err = WithReboundDependencyPhysicalV2(&owner, key, value, rebind, output); !errors.Is(err, retainedalloc.ErrCapacity) || visits != 0 || outputs != 0 || owner.Bytes() != baseline {
		t.Fatalf("pre-effect refusal %d %d %d %v", visits, outputs, owner.Bytes(), err)
	}
	budget.cap = 1 << 20
	if err = WithReboundDependencyPhysicalV2(&owner, key, value, rebind, output); err != nil || visits != 1 || outputs != 1 || owner.Bytes() != baseline {
		t.Fatalf("canonical output %d %d %d %v", visits, outputs, owner.Bytes(), err)
	}
	if _, err = DecodeDependencyPhysicalV2(key, value); err != nil {
		t.Fatal("original record changed", err)
	}
	for _, pair := range [][2][]byte{{key[:len(key)-1], value}, {key, append(bytes.Clone(value), 0)}, {key, value[:len(value)-1]}} {
		if err = WithReboundDependencyPhysicalV2(&owner, pair[0], pair[1], rebind, output); err == nil || visits != 1 || outputs != 1 || owner.Bytes() != baseline {
			t.Fatalf("malformed input invoked effects: %d %d %d %v", visits, outputs, owner.Bytes(), err)
		}
	}
	fault := errors.New("private identity failure")
	if err = WithReboundDependencyPhysicalV2(&owner, key, value, func(e DependencyManifestEntryV1) (StableIdentity, StableIdentity, error) {
		return StableIdentity{}, StableIdentity{}, fault
	}, output); !errors.Is(err, fault) || outputs != 1 || owner.Bytes() != baseline {
		t.Fatalf("callback error custody: %d %d %v", outputs, owner.Bytes(), err)
	}
}
