package collections

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestVectorPartitionRouterV2IndependentBudgetsAndDurableNodeIdentity(t *testing.T) {
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
	opts := VectorPartitionRouterSearchOptionsV2{Mode: VectorPartitionRouterModeApproxV1, ReturnedWidth: 2, BeamWidth: n, ScoreBudget: 256, PartitionProbes: 1}
	got, err := router.SearchWithContextV1(context.Background(), query, opts)
	if err != nil || len(got.Partitions) != 1 || got.Status.ScoreBudget != 256 || got.Status.ReturnedWidth != 2 || got.Status.BeamWidth != uint64(n) || got.Status.ScoreCalls > 256 || got.Status.ScoreCalls < got.Status.Candidates {
		t.Fatalf("independent w/E/C: result=%+v err=%v", got, err)
	}
	short := opts
	short.ScoreBudget = 1
	failed, err := router.SearchWithContextV1(context.Background(), query, short)
	if !errors.Is(err, ErrVectorPartitionRouterScoreBudget) || len(failed.Partitions) != 0 || failed.Status.ScoreCalls > 1 {
		t.Fatalf("hard score exhaustion returned partial routes or exceeded C: %+v %v", failed, err)
	}
	for _, bad := range []VectorPartitionRouterSearchOptionsV2{
		{Mode: "approximate", ReturnedWidth: 0, BeamWidth: n, ScoreBudget: 256, PartitionProbes: 1},
		{Mode: "approximate", ReturnedWidth: 3, BeamWidth: 2, ScoreBudget: 256, PartitionProbes: 1},
		{Mode: "approximate", ReturnedWidth: 2, BeamWidth: n + 1, ScoreBudget: 256, PartitionProbes: 1},
		{Mode: "approximate", ReturnedWidth: 2, BeamWidth: n, ScoreBudget: MaxVectorPartitionRouterScoreBudgetV2 + 1, PartitionProbes: 1},
	} {
		if result, err := router.Search(query, bad); err == nil || len(result.Partitions) != 0 {
			t.Fatalf("invalid width/beam was clamped: %+v %v", result, err)
		}
	}
	// Opening a second pinned owner reconstructs true leaf/internal metadata and
	// permits repeated provenance anchors without inventing fake leaf identities.
	second, _, err := col.OpenVectorPartitionRouterV1(index)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if second.modelDigest != router.modelDigest || second.model.Nodes[0].Leaf || second.model.Nodes[0].Budget == 0 {
		t.Fatal("reopen changed hierarchy or allocation identity")
	}
	exact := opts
	exact.Mode = VectorPartitionRouterModeExactV1
	result, err := second.Search(query, exact)
	if err != nil || result.Status.ScoreCalls != uint64(n) || len(result.Partitions) != 1 {
		t.Fatalf("charged explicit exact reference: %+v %v", result, err)
	}
}

func TestVectorPartitionRouterV2ConcurrentPinnedSearch(t *testing.T) {
	router, _, _ := policyPersistedFixtureV1(t)
	opts := VectorPartitionRouterSearchOptionsV2{Mode: VectorPartitionRouterModeApproxV1, ReturnedWidth: 2, BeamWidth: 6, ScoreBudget: 256, PartitionProbes: 1}
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
