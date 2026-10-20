package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"reflect"
	"slices"
	"testing"
)

type requirementStreamFixture struct {
	values []StableLogicalObligation
	err    error
}

func (s requirementStreamFixture) Walk(emit func(StableLogicalObligation) error) (StableResourceClosureWork, error) {
	for _, value := range s.values {
		if err := emit(value); err != nil {
			return StableResourceClosureWork{}, err
		}
	}
	return StableResourceClosureWork{}, s.err
}

func (s requirementStreamFixture) Lookup(key StableLogicalObligation) (StableLogicalObligation, bool, error) {
	for _, value := range s.values {
		if stableLogicalObligationKey(value) == stableLogicalObligationKey(key) {
			return value, true, s.err
		}
	}
	return StableLogicalObligation{}, false, s.err
}

func TestOwnedStreamedRequirementsExactUnionAndRefusal(t *testing.T) {
	values := []StableLogicalObligation{appendMutationTestObligation(1), appendMutationTestObligation(2), appendMutationTestObligation(3)}
	for i := range values {
		values[i].Offset = 0
	}
	token := stableTokenFixture(t, t.TempDir(), "stream", 1, 20, ReachabilityColumnManifest, "stream", func(spec *StableResourceSpec) {
		spec.Kind, spec.LogicalObligations = ResourceColumnAsset, values
	})
	b := NewStableResourceSetBuilder()
	if err := b.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	selected, err := ImportStableResourceSetMetadata(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	baseline := owner.Bytes()
	r := StableLogicalObligationRequirements{
		ScopedFields: []ReachabilityField{ReachabilityColumnManifest},
		Obligations:  values[1:2],
		Streams: []StableLogicalObligationRequirementStream{
			requirementStreamFixture{values: []StableLogicalObligation{values[2], values[0], values[0]}},
		},
	}
	r, err = MergeStableLogicalObligationRequirements(r, StableLogicalObligationRequirements{})
	if err != nil || len(r.Streams) != 1 {
		t.Fatalf("merge stream headers: %+v %v", r, err)
	}
	if err := ValidateStableResourceSetLogicalObligations(selected, r); err != nil {
		t.Fatalf("unordered streamed + ordinary exact union: %v", err)
	}
	if err := ValidateStableResourceSetLogicalObligations(source, r); !errors.Is(err, ErrResourceOwnership) {
		t.Fatalf("nonselected stream must refuse: %v", err)
	}
	// Filtering consumes the same point membership and keeps only desired values.
	r.Obligations = nil
	r.Streams = []StableLogicalObligationRequirementStream{requirementStreamFixture{values: values[:1]}}
	filtered, err := CloneStableResourceSetForLogicalObligations(selected, r)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateStableResourceSetLogicalObligations(filtered, r); err != nil {
		t.Fatalf("filtered stream closure: %v", err)
	}
	filtered.Release()
	if err := ValidateStableResourceSetLogicalObligations(selected, r); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("stale actual must refuse: %v", err)
	}
	missing := appendMutationTestObligation(4)
	r.Streams = []StableLogicalObligationRequirementStream{requirementStreamFixture{values: append(append([]StableLogicalObligation(nil), values...), missing)}}
	if err := ValidateStableResourceSetLogicalObligations(selected, r); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("missing desired must refuse: %v", err)
	}
	conflict := values[0]
	conflict.Checksum++
	r.Obligations = []StableLogicalObligation{conflict}
	r.Streams = []StableLogicalObligationRequirementStream{requirementStreamFixture{values: values}}
	if err := ValidateStableResourceSetLogicalObligations(selected, r); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("ordinary/stream integrity conflict: %v", err)
	}
	fault := errors.New("original stream read failure")
	r.Obligations = nil
	r.Streams = []StableLogicalObligationRequirementStream{requirementStreamFixture{values: values, err: fault}}
	if _, err := CloneStableResourceSetForLogicalObligations(selected, r); !errors.Is(err, fault) {
		t.Fatalf("filter must propagate source read failure: %v", err)
	}
	if owner.Bytes() != baseline {
		t.Fatalf("stream scratch leaked: %d/%d", owner.Bytes(), baseline)
	}
	selected.Release()
	if owner.Bytes() != 0 {
		t.Fatalf("original selected owner retained %d", owner.Bytes())
	}
}

func TestStableLogicalObligationEmptyRequirementsIndexAllocations(t *testing.T) {
	check := func() {
		index, err := indexStableLogicalObligationRequirements(StableLogicalObligationRequirements{})
		if err != nil || len(index.scoped) != 0 || len(index.global) != 0 || len(index.namespaces) != 0 || len(index.desired) != 0 {
			t.Fatalf("empty index=%+v err=%v", index, err)
		}
	}
	if allocs := testing.AllocsPerRun(100, check); allocs != 0 {
		t.Fatalf("empty requirements allocated unused index maps: %v allocations", allocs)
	}
	// Empty fields are not permission to ignore unscoped obligations.
	if _, err := indexStableLogicalObligationRequirements(StableLogicalObligationRequirements{Obligations: []StableLogicalObligation{appendMutationTestObligation(1)}}); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("unscoped obligation error=%v", err)
	}
	// An empty namespace declaration still has replacement semantics.
	index, err := indexStableLogicalObligationRequirements(StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: "docs/column-assets"}}})
	if err != nil || len(index.scoped) != 1 || len(index.namespaces) != 1 || len(index.desired) != 1 {
		t.Fatalf("empty namespace declaration discarded: index=%+v err=%v", index, err)
	}
}

func TestOwnedImportedRequirementsMatchGenericExactScopes(t *testing.T) {
	values := []StableLogicalObligation{appendMutationTestObligation(1), appendMutationTestObligation(2), appendMutationTestObligation(3)}
	for i := range values {
		values[i].Offset = 0
	}
	values[0].Namespace = "a"
	values[1].Namespace = "b"
	values[2].Namespace = "b"
	token := stableTokenFixture(t, t.TempDir(), "scope", 1, 20, ReachabilityColumnManifest, "scope", func(spec *StableResourceSpec) { spec.Kind = ResourceColumnAsset; spec.LogicalObligations = values })
	b := NewStableResourceSetBuilder()
	if err := b.Add(token); err != nil {
		t.Fatal(err)
	}
	source, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	imported, err := ImportStableResourceSetMetadata(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Release()
	cases := []StableLogicalObligationRequirements{
		{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: values[:1]},
		{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: "a"}}, Obligations: values[:1]},
		{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: "a"}}},
		{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}},
	}
	baseline := owner.Bytes()
	for i, r := range cases {
		old, _, err := CloneStableResourceSetForLogicalObligationsWithWork(source, r)
		if err != nil {
			t.Fatal(i, err)
		}
		fresh, _, err := CloneStableResourceSetForLogicalObligationsWithWork(imported, r)
		if err != nil {
			old.Release()
			t.Fatal(i, err)
		}
		collect := func(set *StableResourceSet) []StableLogicalObligation {
			var got []StableLogicalObligation
			if err := set.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, value StableLogicalObligation) error {
				got = append(got, value)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			slices.SortFunc(got, compareResourceObligation)
			return got
		}
		if !reflect.DeepEqual(collect(old), collect(fresh)) || old.Len() != fresh.Len() {
			t.Fatalf("case%d mismatch", i)
		}
		oldErr := ValidateStableResourceSetLogicalObligations(old, r)
		freshErr := ValidateStableResourceSetLogicalObligations(fresh, r)
		if (oldErr == nil) != (freshErr == nil) {
			t.Fatalf("validation%d old=%v selected=%v", i, oldErr, freshErr)
		}
		old.Release()
		fresh.Release()
		if owner.Bytes() != baseline {
			t.Fatalf("case%d leaked %d/%d", i, owner.Bytes(), baseline)
		}
	}
	missing := appendMutationTestObligation(9)
	if err := ValidateStableResourceSetLogicalObligations(imported, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: []StableLogicalObligation{missing}}); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("missing/stale closure err=%v", err)
	}
	if _, _, err := CloneStableResourceSetForLogicalObligationsWithWork(imported, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: "a"}}}); !errors.Is(err, ErrResourceConflict) || owner.Bytes() != baseline {
		t.Fatalf("refusal err=%v charge=%d/%d", err, owner.Bytes(), baseline)
	}
	imported.Release()
	if owner.Bytes() != 0 {
		t.Fatalf("last metadata %d", owner.Bytes())
	}
}

func TestOwnedRequirementsUnscopedObligationParity(t *testing.T) {
	r := StableLogicalObligationRequirements{Obligations: []StableLogicalObligation{appendMutationTestObligation(1)}}
	if err := validateBorrowedRequirements(r); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("selected unscoped error=%v", err)
	}
}

func TestOwnedMutationCertificationAndAppendMerge(t *testing.T) {
	values := []StableLogicalObligation{appendMutationTestObligation(1), appendMutationTestObligation(2)}
	for i := range values {
		values[i].Offset = 0
	}
	dir := t.TempDir()
	original := stableTokenFixture(t, dir, "base", 1, 20, ReachabilityColumnManifest, "base", func(spec *StableResourceSpec) { spec.Kind = ResourceColumnAsset; spec.LogicalObligations = values[:1] })
	b := NewStableResourceSetBuilder()
	if err := b.Add(original); err != nil {
		t.Fatal(err)
	}
	source, err := b.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer source.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	selected, err := ImportStableResourceSetMetadata(&owner, source)
	if err != nil {
		t.Fatal(err)
	}
	defer selected.Release()
	producerToken := stableTokenFixture(t, dir, "producer", 2, 20, ReachabilityColumnManifest, "producer", func(spec *StableResourceSpec) { spec.Kind = ResourceColumnAsset; spec.LogicalObligations = values[1:] })
	pb := NewStableResourceSetBuilder()
	if err := pb.Add(producerToken); err != nil {
		t.Fatal(err)
	}
	producer, err := pb.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Release()
	admitted, err := ImportStableResourceSetMetadata(&owner, producer)
	if err != nil {
		t.Fatal(err)
	}
	defer admitted.Release()
	mutation := StableLogicalObligationMutation{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Added: values[1:]}
	work, certified, err := CertifyStableLogicalObligationAppendMutation(selected, admitted, mutation)
	if err != nil || !certified || work.SourceObligationsInspected != 1 {
		t.Fatalf("append certificate=%v work=%+v err=%v", certified, work, err)
	}
	builder, err := NewStableResourceSetBuilderWithMetadata(&owner)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Abandon()
	inherited, _, err := CloneStableResourceSetApplyingLogicalObligationMutation(selected, mutation)
	if err != nil {
		t.Fatal(err)
	}
	if err = builder.Merge(inherited); err != nil {
		inherited.Release()
		t.Fatal(err)
	}
	copyProducer, err := CloneStableResourceSetExcludingKinds(admitted)
	if err != nil {
		t.Fatal(err)
	}
	defer copyProducer.Release()
	work, err = builder.MergeAppendOnlyLogicalObligations(copyProducer, mutation)
	if err != nil || work.AppendOnlyFastPath != 1 {
		t.Fatalf("append merge=%+v err=%v", work, err)
	}
	complete, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer complete.Release()
	requirements, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{ScopedFields: mutation.ScopedFields, Obligations: values[:1]})
	if err != nil {
		t.Fatal(err)
	}
	deletion := StableLogicalObligationMutation{ScopedFields: mutation.ScopedFields, Removed: values[1:]}
	certified, err = CertifyStableLogicalObligationMutationFinalRequirements(complete, deletion, requirements)
	if err != nil || !certified {
		t.Fatalf("deletion certificate=%v err=%v", certified, err)
	}
	trimmed, _, err := CloneStableResourceSetApplyingLogicalObligationMutation(complete, deletion)
	if err != nil {
		t.Fatal(err)
	}
	defer trimmed.Release()
	if err = ValidateStableResourceSetLogicalObligations(trimmed, requirements); err != nil {
		t.Fatal(err)
	}
	// A declaration cannot authorize an undeclared retained value or removal.
	wrong := deletion
	wrong.Removed = []StableLogicalObligation{appendMutationTestObligation(99)}
	certified, err = CertifyStableLogicalObligationMutationFinalRequirements(complete, wrong, requirements)
	if err != nil || certified {
		t.Fatalf("incomplete certificate=%v err=%v", certified, err)
	}
	if _, _, err = CloneStableResourceSetApplyingLogicalObligationMutation(complete, wrong); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("missing removal=%v", err)
	}
	trimmed.Release()
	complete.Release()
	admitted.Release()
	selected.Release()
	if owner.Bytes() != 0 {
		t.Fatalf("retained selected metadata=%d", owner.Bytes())
	}
}
