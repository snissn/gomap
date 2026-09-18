package collections

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func effortPreparedFixtureV1(t *testing.T) *VectorPartitionRouterV1 {
	t.Helper()
	in := testVectorPartitionLocalSearcherDisconnectedHNSWInputV1(4)
	in.Dimensions, in.MaxLayer = 2, 1
	vectors := [][]float32{{0, 1}, {.8, .6}, {1, 0}, {-1, 0}}
	for i, v := range vectors {
		copy(in.NormalizedVectors[i*4:], v)
	}
	in.Levels = []uint16{1, 0, 1, 0}
	in.AdjacencyLayers = []columnHNSWSearchPackLayerInput{
		{Offsets: []uint64{0, 1, 2, 3, 4}, Neighbors: []uint32{1, 3, 1, 0}},
		{Offsets: []uint64{0, 1, 1, 2, 2}, Neighbors: []uint32{2, 0}},
	}
	raw, err := encodeColumnHNSWSearchPack(in)
	if err != nil {
		t.Fatal(err)
	}
	view, _ := testColumnHNSWSearchPackPreparedViewFromBytes2314(t, raw, mappedresource.SourceHeapCopy, in.BaseIdentity)
	r := &VectorPartitionRouterV1{view: view, viewToModel: []int{0, 1, 2, 3}, modelDigest: strings.Repeat("a", 64),
		model:    internalrouter.RouterModelV1{Dimensions: 2, Metrics: internalrouter.RouterBuildMetricsV1{Partitions: 2}, Representatives: make([]internalrouter.RouterRepresentativeV1, 4)},
		manifest: VectorPartitionManifestV1{Generation: 11, SourceGeneration: 10}}
	for i, v := range vectors {
		r.model.Representatives[i] = internalrouter.RouterRepresentativeV1{PartitionID: uint32(i % 2), SourceOrdinal: uint64(i), Values: v}
	}
	r.scratch.New = func() any { return &columnVectorGraphNativeSearchScratch{} }
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}

func TestVectorPartitionRouterEffortLegacyEquivalence(t *testing.T) {
	persisted, _, _ := policyPersistedFixtureV1(t)
	for _, r := range []*VectorPartitionRouterV1{effortPreparedFixtureV1(t), persisted} {
		q := []float32{1, 0}
		old, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), q, VectorPartitionRouterPolicyDiagnosticOptionsV1{Mode: VectorPartitionRouterModeApproxV1, CandidateBudget: 4, ReturnedWidth: 4, PartitionProbes: 2})
		if err != nil {
			t.Fatal(err)
		}
		before := r.Status()
		got, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), q, VectorPartitionRouterEffortOptionsV1{Mode: VectorPartitionRouterEffortLegacyV1, ReturnedWidth: 4, Beam: 4, ScoreBudget: 4}, 2)
		if err != nil {
			t.Fatal(err)
		}
		after := r.Status()
		if !reflect.DeepEqual(got.Policies.Distance, old.Distance) || !reflect.DeepEqual(got.Policies.Frequency, old.Frequency) || !reflect.DeepEqual(got.Policies.Hybrid, old.Hybrid) || got.Work.Layer0Distinct != old.Candidates || got.Work.Edges != old.Edges {
			t.Fatalf("legacy mismatch: %+v / %+v", got, old)
		}
		if got.Work.UpperScoreCalls != 0 || got.Work.TotalScoreCalls != got.Work.Layer0Distinct || got.Work.UpperLayers != 0 {
			t.Fatal("legacy traversed upper layer", got.Work)
		}
		if before.Searches != after.Searches || before.Candidates != after.Candidates {
			t.Fatal("diagnostic changed serving counters")
		}
	}
}

func TestVectorPartitionRouterEffortUpperDescentIsExplicit(t *testing.T) {
	r := effortPreparedFixtureV1(t)
	legacy, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, VectorPartitionRouterEffortOptionsV1{Mode: VectorPartitionRouterEffortLegacyV1, ReturnedWidth: 1, Beam: 1, ScoreBudget: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	strict, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, VectorPartitionRouterEffortOptionsV1{Mode: VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 1, Beam: 1, ScoreBudget: 12}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.Work.Layer0EntryOrdinal != 0 || strict.Work.Layer0EntryOrdinal != 2 || strict.Work.UpperLayers != 1 || strict.Work.UpperScoreCalls != 3 {
		t.Fatalf("wrong path legacy=%+v strict=%+v", legacy.Work, strict.Work)
	}
	if legacy.Policies.Distance[0].WinningRepresentative != 0 || strict.Policies.Distance[0].WinningRepresentative != 2 {
		t.Fatal("shortcut did not change entry result")
	}
	if strict.Options.ScoreBudget <= strict.RepresentativeCount {
		t.Fatal("test must exercise legal C>N")
	}
	if strict.Work.TotalScoreCalls > 12 || strict.Work.TotalScoreCalls != strict.Work.UpperScoreCalls+strict.Work.Layer0Distinct {
		t.Fatal("unaccounted score", strict.Work)
	}
}

func TestVectorPartitionRouterEffortAtCapSemantics(t *testing.T) {
	r := effortPreparedFixtureV1(t)
	for _, cap := range []int{1, 2, 3, 4} {
		got, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, VectorPartitionRouterEffortOptionsV1{Mode: VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 1, Beam: 1, ScoreBudget: cap}, 1)
		if !errors.Is(err, ErrVectorPartitionRouterEffortBudgetV1) || !got.Work.BudgetExhausted || got.Policies != nil || got.Work.TotalScoreCalls > uint64(cap) || got.ModelSHA256 == "" || got.QuerySHA256 == "" {
			t.Fatalf("cap=%d result=%+v err=%v", cap, got, err)
		}
	}
}

func TestVectorPartitionRouterEffortRequestedEqualsEffectiveCoordinates(t *testing.T) {
	r := effortPreparedFixtureV1(t)
	valid := VectorPartitionRouterEffortOptionsV1{Mode: VectorPartitionRouterEffortLegacyV1, ReturnedWidth: 2, Beam: 3, ScoreBudget: 4}
	for _, bad := range []VectorPartitionRouterEffortOptionsV1{
		{Mode: "unknown", ReturnedWidth: 2, Beam: 3, ScoreBudget: 4},
		{Mode: valid.Mode, ReturnedWidth: 0, Beam: 3, ScoreBudget: 4},
		{Mode: valid.Mode, ReturnedWidth: 4, Beam: 3, ScoreBudget: 4},
		{Mode: valid.Mode, ReturnedWidth: 2, Beam: 5, ScoreBudget: 6},
		{Mode: valid.Mode, ReturnedWidth: 2, Beam: 3, ScoreBudget: 2},
		{Mode: valid.Mode, ReturnedWidth: 2, Beam: 3, ScoreBudget: 5},
		{Mode: VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 2, Beam: 3, ScoreBudget: math.MaxInt},
	} {
		if _, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, bad, 1); err == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
	got, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, valid, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Options != valid {
		t.Fatal("silent clamp", got.Options)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(ctx, []float32{1, 0}, valid, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// Observe actual scalar/tiled kernel invocations independently of budget counters.
func TestVectorPartitionRouterEffortBudgetIndependentOfStatsMode(t *testing.T) {
	r := effortPreparedFixtureV1(t)
	for _, cap := range []int{2, 4, 12} {
		var ref []columnVectorGraphNativeSearchResult
		var refCalls uint64
		var refBudget bool
		for i, mode := range []columnVectorGraphNativeSearchStatsMode{columnVectorGraphNativeSearchStatsModeMinimal, columnVectorGraphNativeSearchStatsModeFullDiagnostics, columnVectorGraphNativeSearchStatsModeWorkAccounting} {
			var observed []int
			scratch := &columnVectorGraphNativeSearchScratch{hnswScoreObserver: func(id int) { observed = append(observed, id) }}
			got, stats, err := r.view.searchCosineWithContext(t.Context(), []float32{1, 0}, columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 1, CandidateLimit: cap, StrictScoreBudget: true, OmitResultMaterialization: true, StatsMode: mode}, scratch)
			if len(observed) != int(stats.PreparedScoreCalls) || len(observed) > cap {
				t.Fatalf("observer=%v stats=%+v C=%d", observed, stats, cap)
			}
			if cap == 12 && (len(observed) < 4 || observed[0] != 0 || observed[1] != 2 || observed[2] != 0 || observed[3] != 2) {
				t.Fatal("repeated upper/seed scores absent", observed)
			}
			budget := errors.Is(err, errTypedGraphSearchBudget)
			if err != nil && !budget {
				t.Fatal(err)
			}
			if budget && len(got) != 0 {
				t.Fatal("partial strict result")
			}
			if i == 0 {
				ref = append([]columnVectorGraphNativeSearchResult(nil), got...)
				refCalls = stats.PreparedScoreCalls
				refBudget = budget
			} else if (!reflect.DeepEqual(ref, got) && !(len(ref) == 0 && len(got) == 0)) || refCalls != stats.PreparedScoreCalls || refBudget != budget {
				t.Fatal("stats mode changed behavior")
			}
		}
	}
}

func TestVectorPartitionRouterEffortTraceFastParity(t *testing.T) {
	r := effortPreparedFixtureV1(t)
	for _, strict := range []bool{false, true} {
		for _, cap := range []int{2, 4, 12} {
			opts := columnVectorGraphNativeSearchOptions{TopK: 1, EfSearch: 2, CandidateLimit: cap, StrictScoreBudget: strict, OmitResultMaterialization: true}
			fast, fs, fe := r.view.searchCosineWithContext(t.Context(), []float32{1, 0}, opts, &columnVectorGraphNativeSearchScratch{})
			trace, ts, te := r.view.searchCosineWithContextTrace(t.Context(), []float32{1, 0}, opts, &columnVectorGraphNativeSearchScratch{}, &columnHNSWSearchPackAttributionTrace{})
			if !reflect.DeepEqual(fast, trace) || fs.PreparedScoreCalls != ts.PreparedScoreCalls || fs.Candidates != ts.Candidates || errors.Is(fe, errTypedGraphSearchBudget) != errors.Is(te, errTypedGraphSearchBudget) {
				t.Fatalf("strict=%v C=%d fast=%v/%v trace=%v/%v", strict, cap, fast, fe, trace, te)
			}
		}
	}
}

func TestVectorPartitionRouterEffortIdentityAndLifetime(t *testing.T) {
	r, collection, def := policyPersistedFixtureV1(t)
	o := VectorPartitionRouterEffortOptionsV1{Mode: VectorPartitionRouterEffortHierarchicalV1, ReturnedWidth: 2, Beam: 3, ScoreBudget: 16}
	first, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, o, 1)
	if err != nil {
		t.Fatal(err)
	}
	changed := o
	changed.ScoreBudget++
	second, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, changed, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.OptionsSHA256 == second.OptionsSHA256 || first.Policies.CandidateSetSHA256 == second.Policies.CandidateSetSHA256 {
		t.Fatal("effort coordinate not bound")
	}
	saved := append([]vectorPartitionPolicyDomainV1(nil), first.Policies.Distance...)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_, _ = r.CompareRankingPoliciesWithEffortForDiagnosticsV1(context.Background(), []float32{1, 0}, o, 1)
			}
		}()
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !reflect.DeepEqual(saved, first.Policies.Distance) {
		t.Fatal("borrowed result escaped closed owner")
	}
	if _, err := r.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, o, 1); err == nil {
		t.Fatal("closed owner accepted")
	}
	reopened, _, err := collection.OpenVectorPartitionRouterV1(def)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	again, err := reopened.CompareRankingPoliciesWithEffortForDiagnosticsV1(t.Context(), []float32{1, 0}, o, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, again) {
		t.Fatal("fresh owner changed effort evidence")
	}
}
