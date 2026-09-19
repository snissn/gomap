package collections

import (
	"context"
	"errors"
	"reflect"
	"testing"

	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func TestVectorPartitionRouterV3HierarchyBudgetAndDurableNodeIdentity(t *testing.T) {
	router, col, index := policyPersistedFixtureV1(t)
	n := len(router.model.Representatives)
	if n != 6 || len(router.model.Nodes) != n {
		t.Fatalf("all-level fixture: nodes=%d representatives=%d", len(router.model.Nodes), n)
	}
	anchors := make(map[uint64]bool)
	repeated, internal := false, false
	for i, rep := range router.model.Representatives {
		repeated = repeated || anchors[rep.SourceOrdinal]
		anchors[rep.SourceOrdinal] = true
		internal = internal || !router.model.Nodes[i].Leaf
		if router.manifest.Representatives[i].NodeID != rep.NodeID {
			t.Fatal("durable manifest lost represented-node identity")
		}
	}
	if !repeated || !internal {
		t.Fatal("fixture must distinguish node identity from anchor and retain internal centers")
	}
	query := []float32{1, 0}
	roots := len(router.hierarchy.rootOrdinals)
	if roots < 2 || roots >= n {
		t.Fatalf("fixture roots=%d representatives=%d", roots, n)
	}
	opts := VectorPartitionRouterSearchOptionsV3{Mode: VectorPartitionRouterModeApproxV1, ScoreBudget: n, PartitionProbes: 1}
	got, err := router.SearchWithContextV1(context.Background(), query, opts)
	if err != nil || len(got.Partitions) != 1 || got.Status.ScoreBudget != uint64(n) || got.Status.ScoreCalls != uint64(n) || got.Status.ScoreCalls != got.Status.Candidates {
		t.Fatalf("full-budget hierarchy: result=%+v err=%v", got, err)
	}
	rootOnly := opts
	rootOnly.ScoreBudget = roots + 1
	bounded, err := router.Search(query, rootOnly)
	if err != nil || bounded.Status.ScoreCalls != uint64(roots) {
		t.Fatalf("partial child group was scored: roots=%d result=%+v err=%v", roots, bounded, err)
	}
	rootOnly.ScoreBudget = roots + 2
	expanded, err := router.Search(query, rootOnly)
	if err != nil || expanded.Status.ScoreCalls != uint64(roots+2) || expanded.Status.Edges != 2 {
		t.Fatalf("exact child-group boundary was not expanded: roots=%d result=%+v err=%v", roots, expanded, err)
	}
	short := opts
	short.ScoreBudget = roots - 1
	failed, err := router.SearchWithContextV1(context.Background(), query, short)
	if !errors.Is(err, ErrVectorPartitionRouterScoreBudget) || len(failed.Partitions) != 0 || failed.Status.ScoreCalls != 0 {
		t.Fatalf("hard score exhaustion returned partial routes or exceeded C: %+v %v", failed, err)
	}
	for _, bad := range []VectorPartitionRouterSearchOptionsV3{
		{Mode: "approximate", ScoreBudget: n, PartitionProbes: 0},
		{Mode: "approximate", ScoreBudget: n, PartitionProbes: len(router.hierarchy.domainIDs) + 1},
		{Mode: "approximate", ScoreBudget: MaxVectorPartitionRouterScoreBudgetV3 + 1, PartitionProbes: 1},
	} {
		if result, err := router.Search(query, bad); err == nil || len(result.Partitions) != 0 {
			t.Fatalf("invalid routing request was clamped: %+v %v", result, err)
		}
	}
	// Opening a second pinned owner reconstructs true leaf/internal metadata and
	// permits repeated provenance anchors without inventing fake leaf identities.
	second, _, err := col.OpenVectorPartitionRouterV1(index)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if second.modelDigest != router.modelDigest || !reflect.DeepEqual(second.model, router.model) {
		t.Fatal("reopen changed hierarchy or allocation identity")
	}
	exact := opts
	exact.Mode = VectorPartitionRouterModeExactV1
	result, err := second.Search(query, exact)
	if err != nil || result.Status.ScoreCalls != uint64(n) || len(result.Partitions) != 1 {
		t.Fatalf("charged explicit exact reference: %+v %v", result, err)
	}
}

func TestVectorPartitionRouterV3ChecksCancellationAfterDomainSort(t *testing.T) {
	router, _, _ := policyPersistedFixtureV1(t)
	ctx := &vectorPartitionRouterDeadlineAfterErrContextV1{
		Context: context.Background(), deadlineAfter: 5,
	}
	result, err := router.SearchWithContextV1(ctx, []float32{1, 0}, VectorPartitionRouterSearchOptionsV3{
		Mode: VectorPartitionRouterModeApproxV1, ScoreBudget: len(router.model.Representatives), PartitionProbes: 2,
	})
	if !errors.Is(err, context.DeadlineExceeded) || result.Status.ScoreCalls != uint64(len(router.hierarchy.rootOrdinals)) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestVectorPartitionRouterV3ConcurrentPinnedSearch(t *testing.T) {
	router, _, _ := policyPersistedFixtureV1(t)
	opts := VectorPartitionRouterSearchOptionsV3{Mode: VectorPartitionRouterModeApproxV1, ScoreBudget: 6, PartitionProbes: 1}
	query := []float32{1, 0}
	want, err := router.Search(query, opts)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 8)
	for range 8 {
		go func() {
			for range 16 {
				got, err := router.SearchWithContextV1(context.Background(), query, opts)
				if err != nil {
					results <- err
					return
				}
				if !reflect.DeepEqual(got.Partitions, want.Partitions) || got.Status.ScoreCalls != want.Status.ScoreCalls {
					results <- errors.New("concurrent search changed deterministic routes or charged work")
					return
				}
			}
			results <- nil
		}()
	}
	for range 8 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestVectorPartitionRouterV3BestParentGroupOrder(t *testing.T) {
	for _, test := range []struct {
		name           string
		root0, root1   []float32
		expandedDomain uint32
		winningOrdinal int
	}{
		{name: "nearer_domain_zero", root0: []float32{.8, .6}, root1: []float32{.6, .8}, expandedDomain: 0, winningOrdinal: 1},
		{name: "nearer_domain_one", root0: []float32{.6, .8}, root1: []float32{.8, .6}, expandedDomain: 1, winningOrdinal: 4},
		{name: "stable_domain_tie", root0: []float32{.8, .6}, root1: []float32{.8, .6}, expandedDomain: 0, winningOrdinal: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			model := internalrouter.RouterModelV1{
				Dimensions: 2,
				Nodes: []internalrouter.RouterHierarchyNodeV1{
					{NodeID: 1, PartitionID: 0}, {NodeID: 2, ParentNodeID: 1, PartitionID: 0}, {NodeID: 3, ParentNodeID: 1, PartitionID: 0},
					{NodeID: 4, PartitionID: 1}, {NodeID: 5, ParentNodeID: 4, PartitionID: 1}, {NodeID: 6, ParentNodeID: 4, PartitionID: 1},
				},
				Representatives: []internalrouter.RouterRepresentativeV1{
					{NodeID: 1, PartitionID: 0, Values: test.root0}, {NodeID: 2, PartitionID: 0, Values: []float32{1, 0}}, {NodeID: 3, PartitionID: 0, Values: []float32{0, 1}},
					{NodeID: 4, PartitionID: 1, Values: test.root1}, {NodeID: 5, PartitionID: 1, Values: []float32{1, 0}}, {NodeID: 6, PartitionID: 1, Values: []float32{-1, 0}},
				},
			}
			hierarchy, err := buildVectorPartitionRouterHierarchyV3(model)
			if err != nil {
				t.Fatal(err)
			}
			router := &VectorPartitionRouterV1{model: model, view: &columnHNSWSearchPackPreparedView{}, hierarchy: hierarchy}
			router.route.New = func() any {
				return &vectorPartitionRouterRouteScratchV3{best: make([]VectorPartitionRouterPartitionScoreV1, 2)}
			}
			result, err := router.Search([]float32{1, 0}, VectorPartitionRouterSearchOptionsV3{Mode: VectorPartitionRouterModeApproxV1, ScoreBudget: 4, PartitionProbes: 2})
			if err != nil || result.Status.ScoreCalls != 4 || result.Status.Edges != 2 || len(result.Partitions) != 2 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if result.Partitions[0].PartitionID != test.expandedDomain || result.Partitions[0].WinningRepresentative != test.winningOrdinal || result.Partitions[0].Distance != 0 {
				t.Fatalf("wrong complete group expanded: %+v", result.Partitions)
			}
		})
	}
}
