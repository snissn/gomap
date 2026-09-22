package rootpublication

import (
	"errors"
	"testing"
)

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
