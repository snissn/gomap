package rootpublication

import (
	"errors"
	"reflect"
	"testing"
)

func TestStableLogicalObligationNamespaceEmptyPreservesSharedToken(t *testing.T) {
	a, b := appendMutationTestObligation(1), appendMutationTestObligation(2)
	a.Namespace, b.Namespace = "a/column-assets", "b/column-assets"
	file := writeStableResourceFixture(t, t.TempDir(), "shared.pack", "12345678901234567890123456789012")
	source := freezeAppendMutationResources(t, appendMutationResourceToken(t, file, ResourceColumnAsset, "shared", 32, ReachabilityColumnManifest, a, b))
	defer source.Release()
	requirements := StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: ReachabilityColumnManifest, Namespace: b.Namespace}}}
	cloned, err := CloneStableResourceSetForLogicalObligations(source, requirements)
	if err != nil {
		t.Fatal(err)
	}
	defer cloned.Release()
	if err := ValidateStableResourceSetLogicalObligations(cloned, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityColumnManifest}, Obligations: []StableLogicalObligation{a}}); err != nil {
		t.Fatalf("empty B must remove B while retaining A on the same physical token: %v", err)
	}
}

func TestStableLogicalObligationNamespaceNormalizeAndValidate(t *testing.T) {
	a, b := appendMutationTestObligation(1), appendMutationTestObligation(2)
	a.Namespace, b.Namespace = "a/column-assets", "b/column-assets"
	scopeA := StableLogicalObligationNamespaceScope{Field: a.Reachability, Namespace: a.Namespace}
	scopeB := StableLogicalObligationNamespaceScope{Field: b.Reachability, Namespace: b.Namespace}
	for _, tc := range []struct {
		name         string
		requirements StableLogicalObligationRequirements
		want         error
	}{
		{"empty_namespace", StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Field: a.Reachability}}}, ErrUnresolvedResource},
		{"empty_field", StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{{Namespace: a.Namespace}}}, ErrUnresolvedResource},
		{"out_of_scope", StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{scopeB}, Obligations: []StableLogicalObligation{a}}, ErrResourceConflict},
		{"overlapping", StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{a.Reachability}, ScopedNamespaces: []StableLogicalObligationNamespaceScope{scopeA}}, ErrResourceConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NormalizeStableLogicalObligationRequirements(tc.requirements); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want %v", err, tc.want)
			}
		})
	}
	input := []StableLogicalObligationNamespaceScope{scopeB, scopeA, scopeB}
	normalized, err := NormalizeStableLogicalObligationRequirements(StableLogicalObligationRequirements{ScopedNamespaces: input, Obligations: []StableLogicalObligation{b, a, b}})
	if err != nil {
		t.Fatal(err)
	}
	input[0] = StableLogicalObligationNamespaceScope{}
	if !reflect.DeepEqual(normalized.ScopedNamespaces, []StableLogicalObligationNamespaceScope{scopeA, scopeB}) || !reflect.DeepEqual(normalized.Obligations, []StableLogicalObligation{a, b}) {
		t.Fatalf("normalized scopes/obligations=%+v", normalized)
	}
	merged, err := MergeStableLogicalObligationRequirements(StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{scopeA}}, StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{scopeB}})
	if err != nil || len(merged.ScopedNamespaces) != 2 || len(merged.Obligations) != 0 {
		t.Fatalf("empty union=%+v err=%v", merged, err)
	}
	if _, err := MergeStableLogicalObligationRequirements(merged, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{a.Reachability}}); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("overlap merge=%v", err)
	}
	// Global scope on another field remains supported alongside exact namespaces.
	if _, err := MergeStableLogicalObligationRequirements(merged, StableLogicalObligationRequirements{ScopedFields: []ReachabilityField{ReachabilityTypedColumnValue}}); err != nil {
		t.Fatal(err)
	}

	file := writeStableResourceFixture(t, t.TempDir(), "shared.pack", "12345678901234567890123456789012")
	source := freezeAppendMutationResources(t, appendMutationResourceToken(t, file, ResourceColumnAsset, "shared", 32, a.Reachability, a, b))
	defer source.Release()
	exactB := StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{scopeB}, Obligations: []StableLogicalObligation{b}}
	if err := ValidateStableResourceSetLogicalObligations(source, exactB); err != nil {
		t.Fatalf("unscoped A rejected: %v", err)
	}
	if err := ValidateStableResourceSetLogicalObligations(source, StableLogicalObligationRequirements{ScopedNamespaces: []StableLogicalObligationNamespaceScope{scopeB}}); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("stale B=%v", err)
	}
	missing := b
	missing.PartID++
	exactB.Obligations = []StableLogicalObligation{missing}
	if err := ValidateStableResourceSetLogicalObligations(nil, exactB); !errors.Is(err, ErrUnresolvedResource) {
		t.Fatalf("missing B=%v", err)
	}
	// Namespace requirements cannot use the global per-field completeness proof.
	if ok, err := CertifyStableLogicalObligationMutationFinalRequirements(source, appendMutationFor(b), normalized); err != nil || ok {
		t.Fatalf("namespace certification=%t err=%v", ok, err)
	}
	if err := ValidateStableLogicalObligationMutationFinalRequirements(StableLogicalObligationMutation{ScopedFields: []ReachabilityField{a.Reachability}, Removed: []StableLogicalObligation{a}}, exactB); !errors.Is(err, ErrResourceConflict) {
		t.Fatalf("out-of-scope removal authorized: %v", err)
	}
}
