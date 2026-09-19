package collections

import (
	"context"
	"errors"
	"math"
	"reflect"
	"sync"
	"testing"

	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func policyPersistedFixtureV1(t *testing.T) (*VectorPartitionRouterV1, *Collection, string) {
	t.Helper()
	requireVectorPartitionPersistenceV1(t)
	rows := []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, .01}}, {id: "b", vector: []float32{1, -.01}}, {id: "c", vector: []float32{.01, 1}}, {id: "d", vector: []float32{-.01, 1}}}
	_, db, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 2, 2, rows)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	manifest := vectorPartitionRouterBuildingFixtureV1(t, db, col, def, 91, rows)
	_, source, err := col.ReadVectorPartitionRouterSourceRowsV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	partitions := make([]internalrouter.RouterPartitionV1, 2)
	for i := range partitions {
		partitions[i].PartitionID = uint32(i)
	}
	for kind, members := range [][]VectorPartitionMembershipV1{manifest.Memberships, manifest.OverlapMemberships} {
		for _, m := range members {
			row := source[m.VectorOrdinal]
			partitions[m.PartitionID].Vectors = append(partitions[m.PartitionID].Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: row.Values, MembershipKind: []string{"home", "overlap"}[kind]})
		}
	}
	cfg := internalrouter.DefaultRouterConfigV1()
	cfg.BranchFactor = 2
	cfg.LeafSize = 1
	cfg.RepresentativeBudget = 6
	cfg.MaxDepth = 4
	cfg.MaxIterations = 8
	cfg.MaxScalarWork = 50_000_000_000
	if _, err := col.BuildAndPublishVectorPartitionRouterV1(t.Context(), manifest, partitions, VectorPartitionRouterBuildOptionsV1{Config: cfg, AssetFileID: 991, AssetPartID: 19, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		t.Fatal(err)
	}
	r, _, err := col.OpenVectorPartitionRouterV1(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r, col, def.Name
}

func TestVectorPartitionRouterPolicyDiagnosticMatchesOrdinaryCandidatePath(t *testing.T) {
	r, col, index := policyPersistedFixtureV1(t)
	query := []float32{1, 0}
	for _, mode := range []string{VectorPartitionRouterModeExactV1, VectorPartitionRouterModeApproxV1} {
		opts := VectorPartitionRouterPolicyDiagnosticOptionsV1{Mode: mode, CandidateBudget: 64, ReturnedWidth: 6, BeamWidth: 6, PartitionProbes: 2}
		ordinary, err := r.SearchWithContextV1(t.Context(), query, VectorPartitionRouterSearchOptionsV2{Mode: mode, ScoreBudget: 64, ReturnedWidth: 6, BeamWidth: 6, PartitionProbes: 2})
		if err != nil {
			t.Fatal(err)
		}
		before := r.Status()
		got, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), query, opts)
		if err != nil {
			t.Fatal(err)
		}
		after := r.Status()
		if before.Searches != after.Searches || before.Candidates != after.Candidates || before.Edges != after.Edges || before.SearchFailures != after.SearchFailures {
			t.Fatal("diagnostic altered serving counters")
		}
		if !got.CollectionComplete || got.Candidates != ordinary.Status.Candidates || got.Edges != ordinary.Status.Edges || got.UniqueReturned != 6 {
			t.Fatalf("work mismatch %+v / %+v", got, ordinary.Status)
		}
		for i, p := range got.Distance {
			want := ordinary.Partitions[i]
			if p.Domain != want.PartitionID || p.Distance != want.Distance || p.WinningRepresentative != want.WinningRepresentative || p.WinningSourceOrdinal != want.WinningSourceOrdinal {
				t.Fatalf("different legacy route %+v / %+v", p, want)
			}
		}
		// Re-open an independent owner of the same persisted model. Results contain
		// only owned scalars and remain valid when that owner closes.
		second, _, err := col.OpenVectorPartitionRouterV1(index)
		if err != nil {
			t.Fatal(err)
		}
		again, err := second.CompareRankingPoliciesForDiagnosticsV1(t.Context(), query, opts)
		closeErr := second.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("compare=%v close=%v", err, closeErr)
		}
		if !reflect.DeepEqual(got, again) {
			t.Fatal("persisted owner replay differs")
		}
		again.Hybrid[0].Frequency++
		third, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), query, opts)
		if err != nil || !reflect.DeepEqual(got, third) {
			t.Fatal("result retained mutable shared scratch", err)
		}
	}
}

func TestVectorPartitionRouterPolicyDiagnosticFailureAndClose(t *testing.T) {
	r, _, _ := policyPersistedFixtureV1(t)
	base := VectorPartitionRouterPolicyDiagnosticOptionsV1{Mode: "exact", CandidateBudget: 64, ReturnedWidth: 6, BeamWidth: 6, PartitionProbes: 2}
	for _, opts := range []VectorPartitionRouterPolicyDiagnosticOptionsV1{{Mode: "bad", CandidateBudget: 4, ReturnedWidth: 4, PartitionProbes: 2}, {Mode: "exact", CandidateBudget: 3, ReturnedWidth: 3, PartitionProbes: 1}, {Mode: "approximate", CandidateBudget: 5, ReturnedWidth: 4, PartitionProbes: 1}, {Mode: "exact", CandidateBudget: 4, ReturnedWidth: 0, PartitionProbes: 1}, {Mode: "exact", CandidateBudget: 4, ReturnedWidth: 5, PartitionProbes: 1}, {Mode: "exact", CandidateBudget: 4, ReturnedWidth: 4, PartitionProbes: 3}} {
		got, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), []float32{1, 0}, opts)
		if err == nil || len(got.Hybrid) > 0 {
			t.Fatalf("accepted malformed %+v: %v", opts, err)
		}
	}
	short := base
	short.ReturnedWidth = 1
	got, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), []float32{1, 0}, short)
	if !errors.Is(err, ErrVectorPartitionRouterCandidateCoverageV1) || !got.CollectionComplete || got.Candidates != 6 || len(got.Hybrid)+len(got.Distance)+len(got.Frequency) != 0 {
		t.Fatalf("shortfall not empty and attributable: %+v %v", got, err)
	}
	for _, q := range [][]float32{{1}, {0, 0}, {float32(math.NaN()), 1}} {
		if _, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), q, base); err == nil {
			t.Fatal("invalid query accepted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.CompareRankingPoliciesForDiagnosticsV1(ctx, []float32{1, 0}, base); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := r.CompareRankingPoliciesForDiagnosticsV1(t.Context(), []float32{1, 0}, base); err == nil || len(got.Hybrid) > 0 {
		t.Fatal("closed diagnostic succeeded")
	}
	var nilRouter *VectorPartitionRouterV1
	if _, err := nilRouter.CompareRankingPoliciesForDiagnosticsV1(nil, []float32{1, 0}, base); err == nil {
		t.Fatal("nil succeeded")
	}
}

func TestVectorPartitionRouterPolicyConcurrentReadersAndClose(t *testing.T) {
	r, _, _ := policyPersistedFixtureV1(t)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 12 {
				got, err := r.CompareRankingPoliciesForDiagnosticsV1(context.Background(), []float32{1, 0}, VectorPartitionRouterPolicyDiagnosticOptionsV1{Mode: "approximate", CandidateBudget: 64, ReturnedWidth: 6, BeamWidth: 6, PartitionProbes: 2})
				if err == nil && (len(got.Hybrid) != 2 || !got.CollectionComplete) {
					t.Error("partial successful diagnostic")
				}
			}
		}()
	}
	close(start)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if r.Status().ActiveHandles != 0 {
		t.Fatal("reader handle leak")
	}
}
