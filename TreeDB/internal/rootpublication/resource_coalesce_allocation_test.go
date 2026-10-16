package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"os"
	"reflect"
	"testing"
)

func admittedCoalescingFixture(t *testing.T, owner *retainedalloc.Owner, registry *IdentityPinRegistry, file *os.File, lane string, frontier DurableFrontier, values []StableLogicalObligation) *StableResourceSet {
	t.Helper()
	token, err := NewStableResourceToken(StableResourceSpec{MetadataOwner: owner, PinRegistry: registry, Kind: ResourceIndex, LogicalLane: lane, ResourceID: "index.db", DiagnosticPath: "index.db", Generation: 1, File: file, Frontier: frontier, Reachability: ReachabilityIndexFile, LogicalObligations: values})
	if err != nil {
		t.Fatal(err)
	}
	backing, err := newOwnedResourceEntry(owner, token)
	if err != nil {
		token.Release()
		t.Fatal(err)
	}
	if err = token.claim(ResourceOwnerShared); err != nil {
		t.Fatal(err)
	}
	var view stableResourceKindView
	if err = appendOwnedResourceEntryToView(&view, backing, owner); err != nil {
		t.Fatal(err)
	}
	backing.release()
	kinds, err := newResourceKindBacking(owner, []resourceKindCell{{kind: token.kind, view: view}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := newStableResourceSetFromKindViews(kinds)
	if err != nil {
		kinds.release()
		t.Fatal(err)
	}
	return result
}

func TestOwnedResourceCoalescingExactFrontierAndLogicalUnion(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "same.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	one, two := appendMutationTestObligation(1), appendMutationTestObligation(2)
	one.Reachability = ReachabilityIndexFile
	two.Reachability = ReachabilityIndexFile
	a, b := NewRIDFrontier([]uint64{1, 5}), NewRIDFrontier([]uint64{3, 5})
	a.Bytes = 4
	b.Bytes = 8
	b.MaxLSN = 7
	first := admittedCoalescingFixture(t, &owner, registry, file, "z", a, []StableLogicalObligation{one})
	second := admittedCoalescingFixture(t, &owner, registry, file, "a", b, []StableLogicalObligation{one, two})
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err = builder.Merge(first); err != nil {
		t.Fatal(err)
	}
	if err = builder.Merge(second); err != nil {
		t.Fatal(err)
	}
	merged, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	view := merged.kindViews.get(ResourceIndex)
	if view.count != 1 || view.logicalObligationCount != 2 || view.logicalMembershipCount != 2 {
		t.Fatalf("coalescing output %+v", view)
	}
	var entry *stableResourceEntry
	view.root.rangeEntries(func(e *stableResourceEntry) bool { entry = e; return false })
	if entry.logicalLane != "a" || entry.token.logicalLane != "a" || entry.frontier.Bytes != 8 || entry.frontier.MaxLSN != 7 || !reflect.DeepEqual(entry.frontier.RIDs(), []uint64{1, 3, 5}) {
		t.Fatalf("incorrect canonical union: %+v", entry)
	}
	if entry.logicalObligations.count != 2 || entry.reachability.commitment(ReachabilityIndexFile).count != 2 {
		t.Fatal("logical duplicates counted twice or lost")
	}
	borrow, err := merged.borrowSelectedKindViews()
	if err != nil {
		t.Fatal(err)
	}
	merged.Release()
	first.Release()
	second.Release()
	if _, err = entry.token.pinned.Stat(); err != nil {
		t.Fatal("borrow lost physical custody", err)
	}
	if owner.Bytes() == 0 || registry.Stats().ActivePins != 1 {
		t.Fatal("borrow lost exact remaining edge")
	}
	borrow.release()
	if owner.Bytes() != 0 || registry.Stats().ActivePins != 0 {
		t.Fatalf("last borrow leak: metadata=%d pins=%d", owner.Bytes(), registry.Stats().ActivePins)
	}
}

func TestOwnedResourceCoalescingRefusalKeepsBothInputs(t *testing.T) {
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
	file := writeStableResourceFixture(t, t.TempDir(), "same.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	obligation := appendMutationTestObligation(1)
	obligation.Reachability = ReachabilityIndexFile
	first := admittedCoalescingFixture(t, &owner, registry, file, "a", DurableFrontier{Bytes: 4}, []StableLogicalObligation{obligation})
	second := admittedCoalescingFixture(t, &owner, registry, file, "b", DurableFrontier{Bytes: 8}, []StableLogicalObligation{obligation})
	builder := NewStableResourceSetBuilder()
	defer builder.Abandon()
	if err = builder.Merge(first); err != nil {
		t.Fatal(err)
	}
	current, incoming := builder.kindViews, second.kindViews
	before := owner.Bytes()
	budget.cap = before
	if err = builder.Merge(second); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("capacity refusal=%v", err)
	}
	if builder.kindViews != current || second.kindViews != incoming || ResourceOwnerState(second.owner.Load()) != ResourceOwnerBuilder || owner.Bytes() != before || registry.Stats().ActivePins != 2 {
		t.Fatal("refusal consumed input or leaked candidate")
	}
	budget.cap = 1 << 20
	if err = builder.Merge(second); err != nil {
		t.Fatal(err)
	}
	builder.Abandon()
	first.Release()
	second.Release()
	if owner.Bytes() != baseline || registry.Stats().ActivePins != 0 {
		t.Fatalf("retry leak: %d baseline%d", owner.Bytes(), baseline)
	}
}

func TestPrimaryRegistryCleanupReportsAllIntrusiveCauses(t *testing.T) {
	registry := NewIdentityPinRegistry()
	first, second, third := errors.New("first physical cleanup"), errors.New("second physical cleanup"), errors.New("namespace cleanup")
	// The source nodes already own their error/custody slots. Diagnostic access
	// must neither allocate a new join nor prefer only the first failure class.
	a, b := &resourcePinnedBacking{failure: first}, &resourcePinnedBacking{failure: second}
	namespace := &StableNamespaceToken{cleanupErr: third}
	registry.retainFailedPinnedBacking(a)
	registry.retainFailedPinnedBacking(b)
	registry.retainFailedNamespace(namespace)
	failure := registry.CleanupError()
	for _, cause := range []error{first, second, third} {
		if !errors.Is(failure, cause) {
			t.Fatalf("lost retained cleanup cause: %v", cause)
		}
	}
	if testing.AllocsPerRun(100, func() {
		if !errors.Is(registry.CleanupError(), first) {
			panic("cause lost")
		}
	}) != 0 {
		t.Fatal("diagnostic traversal allocated failure storage")
	}
}

func TestOwnedSameIdentityAppendRetainsCanonicalIndexAndCustody(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	file := writeStableResourceFixture(t, t.TempDir(), "same.bin", "12345678")
	identity, err := StableIdentityFromFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Observe(identity); err != nil {
		t.Fatal(err)
	}
	defer registry.Unobserve(identity)
	values := make([]StableLogicalObligation, 64)
	for i := range values {
		values[i] = appendMutationTestObligation(uint64(i + 1))
		values[i].Reachability = ReachabilityIndexFile
		values[i].Offset = 0
	}
	first := admittedCoalescingFixture(t, &owner, registry, file, "same", func() DurableFrontier { f := NewRIDFrontier([]uint64{1, 5}); f.Bytes = 4; return f }(), values)
	defer first.Release()
	old := first.kindViews.get(ResourceIndex)
	added := appendMutationTestObligation(65)
	added.Reachability = ReachabilityIndexFile
	added.Offset = 0
	second := admittedCoalescingFixture(t, &owner, registry, file, "same", func() DurableFrontier { f := NewRIDFrontier([]uint64{3, 5}); f.Bytes = 8; return f }(), []StableLogicalObligation{added})
	defer second.Release()
	merged, err := mergeOwnedResourceKindBackings(first.kindViews, second.kindViews)
	if err != nil {
		t.Fatal(err)
	}
	view := merged.get(ResourceIndex)
	if view.root != old.root {
		merged.release()
		t.Fatal("same physical append rebuilt historical custody rope")
	}
	entry := findStableResourceLogical(view.logical, second.kindViews.get(ResourceIndex).logical.entry.token.logicalKey())
	if entry == nil || entry.logicalObligations.count != 65 || entry.frontier.Bytes != 8 {
		t.Fatalf("incomplete appended descriptor: %+v", entry)
	}
	if !reflect.DeepEqual(entry.frontier.RIDs(), []uint64{1, 3, 5}) || !reflect.DeepEqual(old.logical.entry.frontier.RIDs(), []uint64{1, 5}) {
		t.Fatal("RID union lost immutable frontier")
	}
	if old.logical.entry.logicalObligations.count != 64 {
		t.Fatal("old immutable descriptor changed")
	}
	seen := 0
	for v := range entry.logicalObligations.selectedValues() {
		if seen < 64 && v != values[seen] || seen == 64 && v != added {
			t.Fatal("canonical index value lost")
		}
		seen++
	}
	if seen != 65 {
		t.Fatal("canonical index incomplete", seen)
	}
	encoded, err := newOwnedDependencyManifestEntryV1(entry, &owner)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded.entry.LogicalObligations) != 65 {
		t.Fatal("manifest lost inherited index values")
	}
	encoded.releaseOwned()
	// A real different governing owner must copy the exact complete history.
	set, err := newStableResourceSetFromKindViews(merged)
	if err != nil {
		t.Fatal(err)
	}
	var other retainedalloc.Owner
	other.Initialize(0)
	imported, err := ImportStableResourceSetMetadata(&other, set)
	if err != nil {
		t.Fatal(err)
	}
	importView := imported.kindViews.get(ResourceIndex)
	if importView.logicalObligationCount != 65 {
		t.Fatal("cross-owner import lost inherited history")
	}
	imported.Release()
	if other.Bytes() != 0 {
		t.Fatal("cross-owner import leaked", other.Bytes())
	}
	set.Release()
	first.Release()
	second.Release()
	if owner.Bytes() != 0 || registry.Stats().ActivePins != 0 {
		t.Fatalf("final custody metadata=%d pins=%d", owner.Bytes(), registry.Stats().ActivePins)
	}
}

func TestOwnedCanonicalHistoryBatchAndInheritedFiltering(t *testing.T) {
	var owner retainedalloc.Owner
	owner.Initialize(0)
	registry := NewIdentityPinRegistry()
	defer registry.Close()
	var original, incoming *resourceKindBacking
	var files []*os.File
	for n := 0; n < 2; n++ {
		file := writeStableResourceFixture(t, t.TempDir(), "same.bin", "12345678")
		files = append(files, file)
		identity, err := StableIdentityFromFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if err = registry.Observe(identity); err != nil {
			t.Fatal(err)
		}
		defer registry.Unobserve(identity)
		lane := []string{"a", "b"}[n]
		oldValues := []StableLogicalObligation{appendMutationTestObligation(uint64(n*10 + 1)), appendMutationTestObligation(uint64(n*10 + 2))}
		nextValue := appendMutationTestObligation(uint64(n*10 + 3))
		for i := range oldValues {
			oldValues[i].Reachability = ReachabilityIndexFile
			oldValues[i].Offset = 0
		}
		nextValue.Reachability = ReachabilityIndexFile
		nextValue.Offset = 0
		first := admittedCoalescingFixture(t, &owner, registry, file, lane, DurableFrontier{Bytes: 4}, oldValues)
		next := admittedCoalescingFixture(t, &owner, registry, file, lane, DurableFrontier{Bytes: 8}, []StableLogicalObligation{oldValues[1], nextValue})
		if original == nil {
			original, err = copyResourceKindBacking(&owner, first.kindViews.cells, nil, nil, true)
		} else {
			var previous = original
			original, err = mergeOwnedResourceKindBackings(previous, first.kindViews)
			previous.release()
		}
		if err != nil {
			t.Fatal(err)
		}
		if incoming == nil {
			incoming, err = copyResourceKindBacking(&owner, next.kindViews.cells, nil, nil, true)
		} else {
			var previous = incoming
			incoming, err = mergeOwnedResourceKindBackings(previous, next.kindViews)
			previous.release()
		}
		if err != nil {
			t.Fatal(err)
		}
		first.Release()
		next.Release()
	}
	defer func() {
		if original != nil {
			original.release()
		}
		if incoming != nil {
			incoming.release()
		}
	}()
	merged, err := mergeOwnedResourceKindBackings(original, incoming)
	if err != nil {
		t.Fatal(err)
	}
	if merged.get(ResourceIndex).root != original.get(ResourceIndex).root {
		t.Fatal("batch copied old physical custody")
	}
	set, err := newStableResourceSetFromKindViews(merged)
	if err != nil {
		t.Fatal(err)
	}
	defer set.Release()
	for _, lane := range []string{"a", "b"} {
		key := stableLogicalResourceKey{kind: ResourceIndex, lane: lane, resourceID: "index.db", generation: 1}
		old := findStableResourceLogical(original.get(ResourceIndex).logical, key)
		current := findStableResourceLogical(set.kindViews.get(ResourceIndex).logical, key)
		if old == nil || current == nil || old.logicalObligations.count != 2 || current.logicalObligations.count != 3 {
			t.Fatal("batch history or old view lost")
		}
	}
	desired := appendMutationTestObligation(1)
	desired.Reachability = ReachabilityIndexFile
	desired.Offset = 0
	filtered, _, err := CloneStableResourceSetForLogicalObligationsWithWork(set, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityIndexFile}, Obligations: []StableLogicalObligation{desired}})
	if err != nil {
		t.Fatal(err)
	}
	if got := filtered.kindViews.get(ResourceIndex).logicalObligationCount; got != 1 {
		t.Fatal("inherited obligation filter lost", got)
	}
	filtered.Release()
	set.Release()
	original.release()
	incoming.release()
	original = nil
	incoming = nil
	if owner.Bytes() != 0 || registry.Stats().ActivePins != 0 {
		t.Fatalf("batch final metadata=%d pins=%d", owner.Bytes(), registry.Stats().ActivePins)
	}
}
