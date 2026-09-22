package db

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestCaptureDurableRootNamespaceScopeCannotBypassAppendFallback(t *testing.T) {
	database, path := openDurableRootClosureDB3928(t)
	a, oldB, added := durableRootClosureObligation3928(1), durableRootClosureObligation3928(2), durableRootClosureObligation3928(3)
	a.Namespace, added.Namespace, oldB.Namespace = "a/column-assets", "a/column-assets", "b/column-assets"
	base := durableRootClosureSet3928(t, path, a, oldB)
	defer base.Release()
	producer := durableRootClosureSet3928(t, path, added)
	defer producer.Release()
	field := rootpublication.ReachabilityColumnManifest
	calls := 0
	fallback := func() (rootpublication.StableLogicalObligationRequirements, rootpublication.StableResourceClosureWork, error) {
		calls++
		return rootpublication.StableLogicalObligationRequirements{
			ScopedNamespaces: []rootpublication.StableLogicalObligationNamespaceScope{{Field: field, Namespace: a.Namespace}},
			Obligations:      []rootpublication.StableLogicalObligation{a, added},
		}, rootpublication.StableResourceClosureWork{}, nil
	}
	var timing CommandWALPublishTiming
	candidate, err := database.captureDurableRootResourcesFromBaseV1(database.idx.Load(), database.meta, nil, base, producer,
		rootpublication.StableLogicalObligationRequirements{ScopedNamespaces: []rootpublication.StableLogicalObligationNamespaceScope{{Field: field, Namespace: oldB.Namespace}}},
		rootpublication.StableLogicalObligationMutation{}, rootpublication.StableLogicalObligationMutation{ScopedFields: []rootpublication.ReachabilityField{field}, Added: []rootpublication.StableLogicalObligation{added}},
		rootpublication.StableResourceClosureWork{}, fallback, false, &timing)
	if err != nil {
		t.Fatal(err)
	}
	defer candidate.Release()
	if calls != 1 || timing.FinalizeCandidateResourceWork.FinalRequirementProofFastPath != 0 {
		t.Fatalf("fallback calls=%d work=%+v", calls, timing.FinalizeCandidateResourceWork)
	}
	if err := rootpublication.ValidateStableResourceSetLogicalObligations(candidate, durableRootClosureRequirements3928(t, a, added)); err != nil {
		t.Fatal(err)
	}
	if got := base.Descriptors(); len(got) != 1 || !reflect.DeepEqual(got[0].LogicalObligations(), []rootpublication.StableLogicalObligation{a, oldB}) {
		t.Fatalf("base changed: %+v", got)
	}
}
