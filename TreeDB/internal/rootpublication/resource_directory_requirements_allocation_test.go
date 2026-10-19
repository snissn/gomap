package rootpublication

import (
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

func TestOwnedDirectoryCompleteRequirementsNamespaceAndRefusal(t *testing.T) {
	values := []StableLogicalObligation{appendMutationTestObligation(1), appendMutationTestObligation(2), appendMutationTestObligation(3)}
	for i := range values {
		values[i].Offset = 0
	}
	values[0].Namespace, values[1].Namespace, values[2].Namespace = "a", "b", "c"
	builder := NewStableResourceSetBuilder()
	for i, name := range []string{"shared", "other"} {
		obligations := values[:2]
		kind := ResourceColumnAsset
		if i != 0 {
			obligations, kind = values[2:], ResourceDictionary
		}
		token := stableTokenFixture(t, t.TempDir(), name, 1, 20, ReachabilityColumnManifest, name, func(spec *StableResourceSpec) {
			spec.Kind, spec.LogicalObligations = kind, obligations
		})
		if err := builder.Add(token); err != nil {
			t.Fatal(err)
		}
	}
	generic, err := builder.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer generic.Release()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 24}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	imported, err := ImportStableResourceSetMetadata(&owner, generic)
	if err != nil {
		t.Fatal(err)
	}
	directory, release := importedOwnedDirectoryLifetimeFixture(t, generic, &owner)
	bound, err := BindDependencyDirectoryV2(imported, directory)
	if err != nil {
		t.Fatal(err)
	}
	imported.Release()
	directory.Release()
	collect := func(set *StableResourceSet) []StableLogicalObligation {
		t.Helper()
		var actual []StableLogicalObligation
		if err := set.WalkLogicalObligations(func(_ StableResourcePhysicalDescriptor, o StableLogicalObligation) error {
			actual = append(actual, o)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		slices.SortFunc(actual, compareResourceObligation)
		return actual
	}
	original := collect(bound)
	for _, tc := range []struct {
		name     string
		r        StableLogicalObligationRequirements
		excluded []ResourceKind
	}{
		{"empty_namespace", StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: "b"}}}, nil},
		{"desired_and_unscoped", StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: "b"}}, Obligations: values[1:2]}, nil},
		{"drop_all", StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}}, nil},
		{"exclude_kind", StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: values[:1]}, []ResourceKind{ResourceDictionary}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := owner.Bytes()
			expected, err := CloneStableResourceSetForLogicalObligations(generic, tc.r, tc.excluded...)
			if err != nil {
				t.Fatal(err)
			}
			defer expected.Release()
			actual, err := CloneStableResourceSetForLogicalObligations(bound, tc.r, tc.excluded...)
			if err != nil {
				t.Fatal(err)
			}
			if actual.MetadataOwner() != &owner || !reflect.DeepEqual(collect(actual), collect(expected)) || actual.Len() != expected.Len() {
				t.Fatal("complete directory filter differs from ordinary exact requirements")
			}
			if err := ValidateStableResourceSetLogicalObligations(actual, tc.r); err != nil {
				t.Fatal(err)
			}
			if err := actual.ReleaseWithError(); err != nil {
				t.Fatal(err)
			}
			if owner.Bytes() != before || !reflect.DeepEqual(collect(bound), original) {
				t.Fatal("filter changed original directory or retained temporary charge")
			}
		})
	}
	stale := StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: values[:1]}
	if err := ValidateStableResourceSetLogicalObligations(bound, stale); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("stale validation: %v", err)
	}
	missing := StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: "missing"}}, Obligations: []StableLogicalObligation{values[0]}}
	missing.Obligations[0].Namespace = "missing"
	if err := ValidateStableResourceSetLogicalObligations(bound, missing); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("missing validation: %v", err)
	}
	before := owner.Bytes()
	budget.cap = budget.bytes
	if result, err := CloneStableResourceSetForLogicalObligations(bound, stale); result != nil || !errors.Is(err, retainedalloc.ErrCapacity) || owner.Bytes() != before {
		t.Fatalf("refusal result=%v bytes=%d/%d error=%v", result != nil, owner.Bytes(), before, err)
	}
	budget.cap = 1 << 24
	if !reflect.DeepEqual(collect(bound), original) {
		t.Fatal("refusal changed source")
	}
	if err := bound.ReleaseWithError(); err != nil || release.calls != 1 || owner.Bytes() != baseline {
		t.Fatalf("final original release err=%v calls=%d bytes=%d/%d", err, release.calls, owner.Bytes(), baseline)
	}
}
