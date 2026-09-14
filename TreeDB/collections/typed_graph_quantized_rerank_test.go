package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestNormalizeTypedGraphQuantizedRerankPlan(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	tests := []struct {
		name                              string
		opts                              VectorIndexSearchOptions
		defaultEF, baseDomain, shadows, d int
		wantE0, wantRcap, wantC           int
		wantErr                           bool
	}{
		{
			name:      "empty_base_defaults_to_zero_widths",
			opts:      VectorIndexSearchOptions{TopK: 0},
			defaultEF: 64,
			wantE0:    0,
			wantRcap:  0,
			wantC:     0,
		},
		{
			name:       "default_rerank_uses_effective_ef",
			opts:       VectorIndexSearchOptions{TopK: 3},
			defaultEF:  8,
			baseDomain: 10,
			shadows:    1,
			wantE0:     8,
			wantRcap:   8,
			wantC:      9,
		},
		{
			name:       "explicit_cap_does_not_expand_effective_ef",
			opts:       VectorIndexSearchOptions{TopK: 4, EfSearch: 6, QuantizedRerankCandidates: 99},
			defaultEF:  8,
			baseDomain: 100,
			shadows:    20,
			wantE0:     6,
			wantRcap:   6,
			wantC:      26,
		},
		{
			name:       "top_k_lifts_default_ef_and_requested_cap_trims_only_rerank",
			opts:       VectorIndexSearchOptions{TopK: 12, EfSearch: 8, QuantizedRerankCandidates: 14},
			defaultEF:  4,
			baseDomain: 40,
			shadows:    3,
			wantE0:     12,
			wantRcap:   12,
			wantC:      15,
		},
		{
			name:       "shadow_addition_is_saturating",
			opts:       VectorIndexSearchOptions{TopK: 1, EfSearch: 1},
			defaultEF:  1,
			baseDomain: maxInt,
			shadows:    maxInt,
			wantE0:     1,
			wantRcap:   1,
			wantC:      maxInt,
		},
		{
			name:      "negative_rerank_is_rejected",
			opts:      VectorIndexSearchOptions{TopK: 1, QuantizedRerankCandidates: -1},
			defaultEF: 8,
			wantErr:   true,
		},
		{
			name:      "explicit_rerank_smaller_than_top_k_is_rejected",
			opts:      VectorIndexSearchOptions{TopK: 2, QuantizedRerankCandidates: 1},
			defaultEF: 8,
			wantErr:   true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeTypedGraphQuantizedRerankPlan(tc.opts, tc.defaultEF, tc.baseDomain, tc.shadows, tc.d)
			if tc.wantErr {
				if err == nil {
					t.Fatal("normalize succeeded")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.effectiveEF != tc.wantE0 || got.rerankCap != tc.wantRcap || got.candidateWidth != tc.wantC {
				t.Fatalf("plan=%+v want E0=%d Rcap=%d C=%d", got, tc.wantE0, tc.wantRcap, tc.wantC)
			}
		})
	}
}

func TestTypedGraphQuantizedRerankLegacyScalarU8DefinitionRejectsOutOfScopePlanes(t *testing.T) {
	legacy := QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1}
	tests := []struct {
		name    string
		metric  VectorMetric
		q       QuantizedVectorIndexDefinition
		wantErr error
	}{
		{name: "legacy_scalar_u8_v1_cosine", metric: VectorMetricCosine, q: legacy},
		{
			name:   "calibrated_scalar_u8",
			metric: VectorMetricCosine,
			q: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, Version: 1, ScalarU8Calibration: &ScalarU8CalibrationConfig{
				Mode:     ScalarU8CalibrationModePerGranuleAlpha,
				Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
				AlphaPolicy: ScalarU8AlphaPolicy{
					Name: ScalarU8AlphaPolicyMaxAbs,
				},
			}},
			wantErr: ErrHybridSearchUnsupported,
		},
		{name: "rabitq", metric: VectorMetricCosine, q: QuantizedVectorIndexDefinition{Name: "q", Codec: "rabitq_1bit", Version: 1}, wantErr: ErrHybridSearchUnsupported},
		{name: "brq", metric: VectorMetricCosine, q: QuantizedVectorIndexDefinition{Name: "q", Codec: "brq_1bit", Version: 1}, wantErr: ErrHybridSearchUnsupported},
		{name: "non_cosine", metric: VectorMetricL2, q: legacy, wantErr: ErrVectorIndexSearchUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &columnVectorGraphPhysicalRowReader{def: VectorIndexDefinition{Name: "embedding_graph", Metric: tc.metric, QuantizedIndexes: []QuantizedVectorIndexDefinition{tc.q}}}
			got, err := typedGraphQuantizedRerankLegacyScalarU8Definition(reader, tc.q.Name)
			if tc.wantErr == nil {
				if err != nil || !quantizedVectorIndexDefinitionValuesEqual(got, tc.q) {
					t.Fatalf("definition=%+v err=%v", got, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err=%v want %v", err, tc.wantErr)
			}
		})
	}
}

// The public route must keep one captured typed owner for scalar traversal,
// exact rerank, and final result IDs. This deliberately exercises the old
// exact-only rejection point with an explicitly named legacy scalar-u8 v1
// plane, then checks the selected-route work proof rather than any global
// process counter.
func TestTypedGraphPublicScalarU8QuantizedRerank(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()

	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`), []byte(`{"id":"c"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{1, 0, 0, 0, 0, 0, 0, 0}, {0.9, 0.1, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"a", "b", "c"}},
		{Name: "user", Strings: []string{"u", "u", "u"}},
		{Name: "path", Strings: []string{"p", "p", "p"}},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		t.Skip("typed shared prepared holder is unavailable on this host")
	} else if err != nil {
		t.Fatal(err)
	}

	selected := VectorIndexSearchOptions{
		IndexName:                 "embedding_graph",
		Query:                     columns[0].Float32Vectors[0],
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        "embedding.scalar_u8.legacy",
		QuantizedRerankCandidates: 3,
		TopK:                      2,
		EfSearch:                  3,
		StatsMode:                 VectorIndexSearchStatsModeMinimal,
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(selected, &buffer)
	if err != nil {
		t.Fatalf("typed quantized rerank: %v", err)
	}
	if view == nil {
		t.Fatal("typed quantized rerank returned no retained read view")
	}
	if len(response.Results) != 2 || string(response.Results[0].ID) != "a" || string(response.Results[1].ID) != "b" {
		t.Fatalf("typed quantized rerank results=%+v want [a b]", response.Results)
	}
	if response.Stats.SearchRouteQuantizedRerank != 1 || response.Stats.SearchRouteQuantizedOnly != 0 || response.Stats.QuantizedScorerActive != 1 || response.Stats.QuantizedScoreCalls == 0 {
		t.Fatalf("typed quantized rerank stats=%+v", response.Stats)
	}
	if response.Stats.QuantizedRerankCandidates == 0 || response.Stats.QuantizedRerankCandidates > 3 || response.Stats.QuantizedRerankExactScoreCalls != response.Stats.QuantizedRerankCandidates {
		t.Fatalf("typed quantized rerank shortlist stats=%+v", response.Stats)
	}
	work := response.Stats.ColumnGraphWork
	if !work.Completed || work.Route != "typed_hnsw" || work.BaseANNScored != response.Stats.QuantizedScoreCalls {
		t.Fatalf("typed quantized rerank graph work=%+v stats=%+v", work, response.Stats)
	}
	proof := work.ScorePlane
	if !proof.Available || !proof.Completed || proof.RequestedMode != VectorIndexQueryModeQuantizedRerank || proof.EffectiveMode != VectorIndexQueryModeQuantizedRerank || proof.Route != "quantized_rerank" || proof.QuantizedIndexName != "embedding.scalar_u8.legacy" {
		t.Fatalf("typed quantized rerank score-plane proof=%+v", proof)
	}
	if proof.RawCandidateWidth != 3 || proof.RerankCandidateCap != 3 || proof.ActualRerankCandidates != response.Stats.QuantizedRerankCandidates || proof.ExactBaseRerankScoreCalls != response.Stats.QuantizedRerankExactScoreCalls || proof.ExactBaseVectorBytesRead == 0 || response.Stats.VectorBytesRead == 0 || proof.ExactSuffixScoreCalls != 0 {
		t.Fatalf("typed quantized rerank score-plane counts=%+v stats=%+v", proof, response.Stats)
	}
	if err := view.Close(); err != nil {
		t.Fatalf("close first typed owner: %v", err)
	}
	// Q2 owns the coherent captured read-view seam only. The convenience
	// response APIs must not make the mutable selected score plane available
	// after immediately releasing that owner.
	if response, err := col.SearchVectorIndexWithBuffer(selected, &buffer); !errors.Is(err, ErrHybridSearchUnsupported) || len(response.Results) != 0 {
		t.Fatalf("buffer-only Q2 route response=%+v err=%v", response, err)
	}
	if response, err := col.SearchVectorIndex(selected); !errors.Is(err, ErrHybridSearchUnsupported) || len(response.Results) != 0 {
		t.Fatalf("owned-result Q2 route response=%+v err=%v", response, err)
	}
	quantizedOnly := selected
	quantizedOnly.QueryMode, quantizedOnly.QuantizedRerankCandidates = VectorIndexQueryModeQuantizedOnly, 0
	if response, rejectedView, err := col.SearchVectorIndexWithBufferReadView(quantizedOnly, &buffer); !errors.Is(err, ErrHybridSearchUnsupported) || rejectedView != nil || len(response.Results) != 0 {
		if rejectedView != nil {
			_ = rejectedView.Close()
		}
		t.Fatalf("quantized-only typed read-view response=%+v view=%v err=%v", response, rejectedView, err)
	}

	filtered, filteredView, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
		IndexName:                 "embedding_graph",
		Query:                     columns[0].Float32Vectors[0],
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        "embedding.scalar_u8.legacy",
		QuantizedRerankCandidates: 3,
		TopK:                      2,
		EfSearch:                  3,
		DeclaredScalarFilter:      &HybridScalarFilter{IndexName: "user", Value: "u"},
		StatsMode:                 VectorIndexSearchStatsModeMinimal,
	}, &buffer)
	if err != nil {
		t.Fatalf("small filtered typed quantized rerank: %v", err)
	}
	if filteredView == nil {
		t.Fatal("small filtered typed quantized rerank returned no read view")
	}
	defer filteredView.Close()
	if len(filtered.Results) != 2 || string(filtered.Results[0].ID) != "a" || string(filtered.Results[1].ID) != "b" {
		t.Fatalf("small filtered typed quantized rerank results=%+v want [a b]", filtered.Results)
	}
	filteredWork := filtered.Stats.ColumnGraphWork
	filteredProof := filteredWork.ScorePlane
	if filteredWork.Route != "typed_exact" || filteredWork.ExactBaseScored != 3 || filtered.Stats.SearchRouteQuantizedRerank != 1 || filtered.Stats.QuantizedScorerActive != 1 || filtered.Stats.QuantizedScoreCalls != 0 {
		t.Fatalf("small filtered typed quantized rerank work=%+v stats=%+v", filteredWork, filtered.Stats)
	}
	if !filteredProof.Completed || filteredProof.Route != "typed_exact" || filteredProof.ExactSmallFilterScoreCalls != 3 || filteredProof.ExactBaseVectorBytesRead == 0 || filteredProof.ExactBaseRerankScoreCalls != 0 || filteredProof.ExactSuffixScoreCalls != 0 {
		t.Fatalf("small filtered typed quantized rerank proof=%+v", filteredProof)
	}
}

func TestTypedGraphPublicScalarU8QuantizedRerankBudgetAdmission(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	setup := func(t *testing.T, searchCandidates int) (*Collection, func()) {
		t.Helper()
		meta := typedMinimaCollectionMeta()
		meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
		_, db, col := openTypedMinimaCollectionMeta(t, meta)
		ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`), []byte(`{"id":"c"}`)}
		columns := []TypedColumnBatch{
			{Name: "embedding", Float32Vectors: [][]float32{query, {0.9, 0.1, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}},
			{Name: "content", Strings: []string{"a", "b", "c"}},
			{Name: "user", Strings: []string{"u", "u", "u"}},
			{Name: "path", Strings: []string{"p", "p", "p"}},
		}
		if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		serving := typedGraphPublicTestOptions()
		serving.SearchCandidates = searchCandidates
		if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
			_ = db.Close()
			t.Skip("typed shared prepared holder is unavailable on this host")
		} else if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		return col, func() { _ = db.Close() }
	}
	search := func(t *testing.T, col *Collection, topK, rerank int, filter *HybridScalarFilter) (VectorIndexSearchResponse, *CollectionReadView, error) {
		t.Helper()
		var buffer VectorIndexSearchBuffer
		return col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
			IndexName:                 "embedding_graph",
			Query:                     query,
			QueryMode:                 VectorIndexQueryModeQuantizedRerank,
			QuantizedIndexName:        "embedding.scalar_u8.legacy",
			QuantizedRerankCandidates: rerank,
			TopK:                      topK,
			EfSearch:                  3,
			DeclaredScalarFilter:      filter,
			StatsMode:                 VectorIndexSearchStatsModeMinimal,
		}, &buffer)
	}

	t.Run("strict_ann_spare_rejects_before_scalar_work", func(t *testing.T) {
		col, close := setup(t, 5) // D=0, Rcap=2, C=3, so B-D-Rcap == C.
		defer close()
		response, view, err := search(t, col, 2, 2, nil)
		if view != nil {
			_ = view.Close()
		}
		if !errors.Is(err, errTypedGraphSearchBudget) || len(response.Results) != 0 {
			t.Fatalf("strict spare response=%+v view=%v err=%v", response, view, err)
		}
		proof := response.Stats.ColumnGraphWork.ScorePlane
		if !proof.Available || proof.Completed || proof.RawCandidateWidth != 3 || proof.RerankCandidateCap != 2 || proof.QuantizedScoreCalls != 0 || proof.ActualRerankCandidates != 0 {
			t.Fatalf("strict spare proof=%+v", proof)
		}
	})

	t.Run("admitted_q1_prefix_reports_actual_bounded_work", func(t *testing.T) {
		col, close := setup(t, 6)
		defer close()
		response, view, err := search(t, col, 2, 2, nil)
		if view != nil {
			_ = view.Close()
		}
		if !errors.Is(err, errTypedGraphSearchBudget) || len(response.Results) != 0 {
			t.Fatalf("Q1 prefix response=%+v view=%v err=%v", response, view, err)
		}
		proof := response.Stats.ColumnGraphWork.ScorePlane
		if !proof.Available || proof.Completed || proof.RawCandidateWidth != 3 || proof.RerankCandidateCap != 2 || proof.QuantizedScoreCalls == 0 || proof.QuantizedScoreCalls > 4 || proof.ActualRerankCandidates != 0 {
			t.Fatalf("Q1 prefix proof=%+v", proof)
		}
	})

	t.Run("adequate_budget_succeeds", func(t *testing.T) {
		col, close := setup(t, 32)
		defer close()
		response, view, err := search(t, col, 2, 2, nil)
		if err != nil || view == nil {
			t.Fatalf("adequate budget response=%+v view=%v err=%v", response, view, err)
		}
		defer view.Close()
		proof := response.Stats.ColumnGraphWork.ScorePlane
		if len(response.Results) != 2 || !proof.Completed || proof.RawCandidateWidth != 3 || proof.RerankCandidateCap != 2 || proof.ActualRerankCandidates == 0 || proof.ActualRerankCandidates > 2 || proof.QuantizedScoreCalls == 0 {
			t.Fatalf("adequate budget response=%+v proof=%+v", response, proof)
		}
	})

	t.Run("top_k_zero_validates_without_ann_budget_preadmission", func(t *testing.T) {
		col, close := setup(t, 1) // Existing K=0 semantics must not reserve Rcap=3.
		defer close()
		response, view, err := search(t, col, 0, 0, nil)
		if err != nil || view == nil {
			t.Fatalf("K=0 validation response=%+v view=%v err=%v", response, view, err)
		}
		defer view.Close()
		proof := response.Stats.ColumnGraphWork.ScorePlane
		if len(response.Results) != 0 || !proof.Completed || proof.Route != "typed_empty" || proof.QuantizedScoreCalls != 0 {
			t.Fatalf("K=0 validation response=%+v proof=%+v", response, proof)
		}
	})

	t.Run("small_filter_uses_actual_exact_budget", func(t *testing.T) {
		col, close := setup(t, 3) // Complete three-row exact work fits B even though Rcap is two.
		defer close()
		filter := HybridScalarFilter{IndexName: "user", Value: "u"}
		response, view, err := search(t, col, 2, 2, &filter)
		if err != nil || view == nil {
			t.Fatalf("small exact budget response=%+v view=%v err=%v", response, view, err)
		}
		defer view.Close()
		proof := response.Stats.ColumnGraphWork.ScorePlane
		if len(response.Results) != 2 || !proof.Completed || proof.Route != "typed_exact" || proof.ExactSmallFilterScoreCalls != 3 || proof.QuantizedScoreCalls != 0 {
			t.Fatalf("small exact budget response=%+v proof=%+v", response, proof)
		}
	})
}

func TestTypedGraphStableCosineScoreMatchesExactDistance(t *testing.T) {
	// Both angles are below float32 dot-product resolution at cosine ~= 1,
	// but their FP64-normalized distances are still distinguishable once the
	// distance is widened before score conversion. Different magnitudes keep
	// this from accidentally testing raw-dot rather than cosine semantics.
	query := []float32{1, 0}
	closer := []float32{1.25, 0.000001}
	farther := []float32{0.75, 0.000002}
	queryInvNorm, err := typedGraphStableCosineQueryInvNorm(query)
	if err != nil {
		t.Fatal(err)
	}
	closerScore, err := typedGraphStableCosineScore(query, queryInvNorm, closer)
	if err != nil {
		t.Fatal(err)
	}
	fartherScore, err := typedGraphStableCosineScore(query, queryInvNorm, farther)
	if err != nil {
		t.Fatal(err)
	}
	closerDistance, err := exactVectorDistance(query, closer, VectorMetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	fartherDistance, err := exactVectorDistance(query, farther, VectorMetricCosine)
	if err != nil {
		t.Fatal(err)
	}
	if closerScore != 1-float64(closerDistance) || fartherScore != 1-float64(fartherDistance) || closerScore <= fartherScore {
		t.Fatalf("stable scores closer=%0.18g farther=%0.18g exact=(%0.18g,%0.18g)", closerScore, fartherScore, closerDistance, fartherDistance)
	}
}

func TestTypedGraphPublicScalarU8QuantizedRerankZeroBaseAssetValidation(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	type fixture struct {
		col    *Collection
		close  func()
		graph  columnVectorGraphManifestSnapshot
		q      QuantizedVectorIndexDefinition
		assets columnVectorGraphQuantizedAssetSet
		cfg    ColumnStoreConfig
	}
	setup := func(t *testing.T) fixture {
		t.Helper()
		meta := typedMinimaCollectionMeta()
		meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
		_, db, col := openTypedMinimaCollectionMeta(t, meta)
		if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		state := col.collectionSchemaCoordinator().typedPublication.Load()
		if state == nil || state.servingBase == nil || state.servingBase.graph.RowCount != 0 {
			_ = db.Close()
			t.Fatalf("zero-base serving state=%+v", state)
		}
		base := state.servingBase
		def := base.view.Catalog.meta.VectorIndexes[0]
		q := def.QuantizedIndexes[0]
		assets, ok := columnVectorGraphQuantizedAssetSetsByName(base.view.VectorIndexState, def)[q.Name]
		if !ok || !assets.HasCodes || assets.Codes.RowCount != 0 {
			_ = db.Close()
			t.Fatalf("zero-base scalar-u8 assets=%+v", assets)
		}
		return fixture{col: col, close: func() { _ = db.Close() }, graph: base.graph, q: q, assets: assets, cfg: *base.view.Catalog.meta.Options.ColumnStore}
	}
	search := func(t *testing.T, col *Collection, topK int) (VectorIndexSearchResponse, *CollectionReadView, error) {
		t.Helper()
		var buffer VectorIndexSearchBuffer
		return col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
			IndexName:          "embedding_graph",
			Query:              query,
			QueryMode:          VectorIndexQueryModeQuantizedRerank,
			QuantizedIndexName: "embedding.scalar_u8.legacy",
			TopK:               topK,
			EfSearch:           8,
			StatsMode:          VectorIndexSearchStatsModeMinimal,
		}, &buffer)
	}
	assertNoZeroRowHolder := func(t *testing.T, view *CollectionReadView, where string) {
		t.Helper()
		if view == nil || view.typedGraphOwner == nil || view.typedGraphOwner.overlay == nil || view.typedGraphOwner.overlay.base == nil || view.typedGraphOwner.overlay.base.reader == nil {
			t.Fatalf("%s has no live zero-row typed owner", where)
		}
		reader := view.typedGraphOwner.overlay.base.reader
		if reader.sharedPreparedSearch != nil || len(reader.quantizedAssetStatus) != 0 {
			t.Fatalf("%s retained a zero-row holder/resource/status: shared=%v statuses=%+v", where, reader.sharedPreparedSearch != nil, reader.quantizedAssetStatus)
		}
	}
	insertSuffix := func(t *testing.T, col *Collection) {
		t.Helper()
		columns := []TypedColumnBatch{
			{Name: "embedding", Float32Vectors: [][]float32{query}},
			{Name: "content", Strings: []string{"suffix"}},
			{Name: "user", Strings: []string{"u"}},
			{Name: "path", Strings: []string{"p"}},
		}
		if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("suffix")}, [][]byte{[]byte(`{"id":"suffix"}`)}, columns); err != nil {
			t.Fatal(err)
		}
	}
	truncateCodePlane := func(t *testing.T, f fixture) {
		t.Helper()
		ref := f.assets.Codes.Ref
		if ref.Length <= 1 || f.assets.Codes.AssetBytes <= 1 {
			t.Fatalf("zero-row scalar-u8 asset too small to truncate: %+v", f.assets.Codes)
		}
		path, err := columnAssetSegmentPath(f.col.db.ColumnAssetRootDir(), ref)
		if err != nil {
			t.Fatalf("columnAssetSegmentPath: %v", err)
		}
		if err := os.Truncate(path, ref.Offset+ref.Length-1); err != nil {
			t.Fatalf("truncate zero-row scalar-u8 code plane: %v", err)
		}
		resetColumnAssetVerifiedChecksumCacheForTest(t)
	}
	assertUnavailable := func(t *testing.T, response VectorIndexSearchResponse, view *CollectionReadView, err error, where string) {
		t.Helper()
		if view != nil {
			_ = view.Close()
		}
		if err == nil || !errors.Is(err, ErrVectorIndexSearchUnavailable) || (!errors.Is(err, errColumnVectorGraphQuantizedAssetMissing) && !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid)) {
			t.Fatalf("%s response=%+v view=%v err=%v", where, response, view, err)
		}
		proof := response.Stats.ColumnGraphWork.ScorePlane
		if !proof.Available || proof.Completed || proof.Reason == "" {
			t.Fatalf("%s did not fail from the selected Q2 score plane: %+v", where, proof)
		}
	}

	t.Run("valid_empty_then_suffix", func(t *testing.T) {
		f := setup(t)
		defer f.close()
		cacheBefore := f.col.columnVectorGraphSharedPreparedSearchCacheSnapshot()
		empty, emptyView, err := search(t, f.col, 0)
		if err != nil || emptyView == nil {
			t.Fatalf("valid zero-result selected request response=%+v view=%v err=%v", empty, emptyView, err)
		}
		assertNoZeroRowHolder(t, emptyView, "valid zero-base K=0")
		if err := emptyView.Close(); err != nil {
			t.Fatal(err)
		}
		if len(empty.Results) != 0 || empty.Stats.SearchRouteQuantizedRerank != 1 || empty.Stats.ColumnGraphWork.Route != "typed_empty" || !empty.Stats.ColumnGraphWork.ScorePlane.Completed || empty.Stats.ColumnGraphWork.ScorePlane.QuantizedScoreCalls != 0 {
			t.Fatalf("valid zero-base K=0 response=%+v work=%+v", empty, empty.Stats.ColumnGraphWork)
		}
		if cacheAfter := f.col.columnVectorGraphSharedPreparedSearchCacheSnapshot(); cacheAfter != cacheBefore {
			t.Fatalf("valid zero-base K=0 changed shared prepared cache before=%+v after=%+v", cacheBefore, cacheAfter)
		}

		insertSuffix(t, f.col)
		response, view, err := search(t, f.col, 1)
		if err != nil || view == nil {
			t.Fatalf("valid suffix-only selected request response=%+v view=%v err=%v", response, view, err)
		}
		defer view.Close()
		assertNoZeroRowHolder(t, view, "valid suffix-only")
		if len(response.Results) != 1 || string(response.Results[0].ID) != "suffix" || response.Stats.SearchRouteQuantizedRerank != 1 || response.Stats.ColumnGraphWork.Route != "typed_exact" || response.Stats.ColumnGraphWork.ScorePlane.ExactSuffixScoreCalls != 1 || response.Stats.QuantizedScoreCalls != 0 {
			t.Fatalf("valid suffix-only selected response=%+v work=%+v", response, response.Stats.ColumnGraphWork)
		}
		if cacheAfter := f.col.columnVectorGraphSharedPreparedSearchCacheSnapshot(); cacheAfter != cacheBefore {
			t.Fatalf("valid suffix-only changed shared prepared cache before=%+v after=%+v", cacheBefore, cacheAfter)
		}
	})

	t.Run("corrupt_payload_fails_before_k_zero_shortcut", func(t *testing.T) {
		f := setup(t)
		defer f.close()
		raw, err := readColumnPhysicalAssetFromManager(f.col.db.ColumnAssetRootDir(), f.assets.Codes.Ref)
		if err != nil || len(raw) == 0 {
			t.Fatalf("read zero-row scalar-u8 asset bytes=%d err=%v", len(raw), err)
		}
		corrupt := append([]byte(nil), raw...)
		corrupt[len(corrupt)-1] ^= 0xff
		writeColumnVectorGraphAssetRawForTest2041(t, f.col.db.ColumnAssetRootDir(), f.assets.Codes.Ref, corrupt)
		resetColumnAssetVerifiedChecksumCacheForTest(t)

		response, view, err := search(t, f.col, 0)
		if view != nil {
			_ = view.Close()
		}
		if err == nil || !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphQuantizedAssetInvalid) {
			t.Fatalf("corrupt zero-row selected K=0 response=%+v view=%v err=%v", response, view, err)
		}
		proof := response.Stats.ColumnGraphWork.ScorePlane
		if !proof.Available || proof.Completed || proof.Reason == "" {
			t.Fatalf("corrupt zero-row selected proof=%+v", proof)
		}

		writeColumnVectorGraphAssetRawForTest2041(t, f.col.db.ColumnAssetRootDir(), f.assets.Codes.Ref, raw)
		resetColumnAssetVerifiedChecksumCacheForTest(t)
		response, view, err = search(t, f.col, 0)
		if err != nil || view == nil {
			t.Fatalf("restored zero-row selected K=0 response=%+v view=%v err=%v", response, view, err)
		}
		assertNoZeroRowHolder(t, view, "restored zero-base K=0")
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("truncated_code_plane_fails_public_k_zero", func(t *testing.T) {
		f := setup(t)
		defer f.close()
		truncateCodePlane(t, f)
		response, view, err := search(t, f.col, 0)
		assertUnavailable(t, response, view, err, "truncated zero-row selected K=0")
	})

	t.Run("corrupt_code_plane_fails_public_suffix_only", func(t *testing.T) {
		f := setup(t)
		defer f.close()
		insertSuffix(t, f.col)
		raw, err := readColumnPhysicalAssetFromManager(f.col.db.ColumnAssetRootDir(), f.assets.Codes.Ref)
		if err != nil || len(raw) == 0 {
			t.Fatalf("read zero-row scalar-u8 asset bytes=%d err=%v", len(raw), err)
		}
		corrupt := append([]byte(nil), raw...)
		corrupt[len(corrupt)-1] ^= 0xff
		writeColumnVectorGraphAssetRawForTest2041(t, f.col.db.ColumnAssetRootDir(), f.assets.Codes.Ref, corrupt)
		resetColumnAssetVerifiedChecksumCacheForTest(t)
		response, view, err := search(t, f.col, 1)
		assertUnavailable(t, response, view, err, "corrupt zero-row selected suffix-only")
	})
}

func TestTypedGraphPublicScalarU8QuantizedRerankFilterBoundary(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	const rows = typedGraphScalarExactLimit + 1
	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].M = 16
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()

	ids, retained := make([][]byte, rows), make([][]byte, rows)
	columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
	for ordinal := range rows {
		id := []byte(fmt.Sprintf("row-%05d", ordinal))
		ids[ordinal] = id
		retained[ordinal] = []byte(fmt.Sprintf(`{"id":%q}`, id))
		columns[0].Float32Vectors = append(columns[0].Float32Vectors, vectorBenchmarkEmbedding(ordinal, 8))
		columns[1].Strings = append(columns[1].Strings, "content")
		columns[2].Strings = append(columns[2].Strings, fmt.Sprintf("%05d", ordinal))
		columns[3].Strings = append(columns[3].Strings, "source")
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	serving := typedGraphPublicTestOptions()
	serving.SearchCandidates = 8192
	serving.Filter.SourceIDs = rows + 16
	serving.Filter.SourceBytes = 4 << 20
	serving.Filter.RetainedBytes = 4 << 20
	serving.Filter.MappingWork = 1 << 20
	serving.Filter.InspectedEntries = 2 * rows
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		t.Skip("typed shared prepared holder is unavailable on this host")
	} else if err != nil {
		t.Fatal(err)
	}

	search := func(filter HybridScalarFilter) VectorIndexSearchResponse {
		t.Helper()
		var buffer VectorIndexSearchBuffer
		response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
			IndexName:                 "embedding_graph",
			Query:                     columns[0].Float32Vectors[0],
			QueryMode:                 VectorIndexQueryModeQuantizedRerank,
			QuantizedIndexName:        "embedding.scalar_u8.legacy",
			QuantizedRerankCandidates: 4,
			TopK:                      2,
			EfSearch:                  8,
			DeclaredScalarFilter:      &filter,
			StatsMode:                 VectorIndexSearchStatsModeMinimal,
		}, &buffer)
		if err != nil {
			t.Fatalf("filter=%+v typed quantized rerank: %v", filter, err)
		}
		if view == nil {
			t.Fatalf("filter=%+v typed quantized rerank returned no read view", filter)
		}
		if err := view.Close(); err != nil {
			t.Fatalf("filter=%+v close read view: %v", filter, err)
		}
		return response
	}
	rangeTo := func(last int) HybridScalarFilter {
		return HybridScalarFilter{IndexName: "user", Range: &IndexRangeOptions{
			Lower: IndexRangeBound{Value: "00000", Inclusive: true},
			Upper: IndexRangeBound{Value: fmt.Sprintf("%05d", last), Inclusive: true},
		}}
	}

	empty := search(HybridScalarFilter{IndexName: "user", Value: "missing"})
	emptyWork := empty.Stats.ColumnGraphWork
	if len(empty.Results) != 0 || emptyWork.Route != "typed_empty" || empty.Stats.SearchRouteQuantizedRerank != 1 || empty.Stats.QuantizedScoreCalls != 0 || emptyWork.ExactBaseScored != 0 || emptyWork.ScorePlane.QuantizedScoreCalls != 0 || emptyWork.ScorePlane.ExactBaseRerankScoreCalls != 0 {
		t.Fatalf("empty selected filter response=%+v work=%+v", empty, emptyWork)
	}

	small := search(rangeTo(typedGraphScalarExactLimit - 1))
	smallWork := small.Stats.ColumnGraphWork
	if len(small.Results) != 2 || smallWork.Route != "typed_exact" || smallWork.ExactBaseScored != typedGraphScalarExactLimit || small.Stats.QuantizedScoreCalls != 0 || smallWork.ScorePlane.ExactSmallFilterScoreCalls != typedGraphScalarExactLimit || smallWork.ScorePlane.ExactBaseRerankScoreCalls != 0 {
		t.Fatalf("4096 selected filter response=%+v work=%+v", small, smallWork)
	}

	large := search(rangeTo(rows - 1))
	largeWork := large.Stats.ColumnGraphWork
	largeProof := largeWork.ScorePlane
	if len(large.Results) != 2 || largeWork.Route != "typed_hnsw" || large.Stats.QuantizedScoreCalls == 0 || largeWork.ExactBaseScored == 0 || largeWork.ExactBaseScored > 4 || largeProof.RawCandidateWidth != 8 || largeProof.RerankCandidateCap != 4 || largeProof.ActualRerankCandidates > 4 || largeProof.ExactSmallFilterScoreCalls != 0 {
		t.Fatalf("4097 selected filter response=%+v work=%+v proof=%+v", large, largeWork, largeProof)
	}
}

func TestTypedGraphPublicScalarU8QuantizedRerankLiveSuffixAndRebuild(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	// Keep this integration fixture materially separated so its order does not
	// depend on the deliberately near-parallel primitive regression above.
	// The replacement is exact, the second row is visibly close, and the third
	// is orthogonal; the assertions therefore isolate visibility/fold behavior.
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	closer := []float32{0.9, 0.1, 0, 0, 0, 0, 0, 0}
	farther := []float32{0, 1, 0, 0, 0, 0, 0, 0}
	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	ids := [][]byte{[]byte("closer"), []byte("farther"), []byte("replaced")}
	retained := [][]byte{[]byte(`{"id":"closer"}`), []byte(`{"id":"farther"}`), []byte(`{"id":"replaced"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{closer, farther, farther}},
		{Name: "content", Strings: []string{"closer", "farther", "old"}},
		{Name: "user", Strings: []string{"u", "u", "u"}},
		{Name: "path", Strings: []string{"p", "p", "p"}},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	serving := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		t.Skip("typed shared prepared holder is unavailable on this host")
	} else if err != nil {
		t.Fatal(err)
	}
	replacement := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{query}},
		{Name: "content", Strings: []string{"new exact"}},
		{Name: "user", Strings: []string{"u"}},
		{Name: "path", Strings: []string{"p"}},
	}
	if _, err := col.ReplaceTypedBatch(ids[2:3], retained[2:3], replacement); err != nil {
		t.Fatal(err)
	}

	search := func(filter *HybridScalarFilter) (VectorIndexSearchResponse, *CollectionReadView) {
		t.Helper()
		var buffer VectorIndexSearchBuffer
		response, view, err := col.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
			IndexName:                 "embedding_graph",
			Query:                     query,
			QueryMode:                 VectorIndexQueryModeQuantizedRerank,
			QuantizedIndexName:        "embedding.scalar_u8.legacy",
			QuantizedRerankCandidates: 3,
			TopK:                      3,
			EfSearch:                  3,
			DeclaredScalarFilter:      filter,
			StatsMode:                 VectorIndexSearchStatsModeMinimal,
		}, &buffer)
		if err != nil {
			t.Fatalf("filter=%+v typed quantized rerank: %v", filter, err)
		}
		if view == nil {
			t.Fatalf("filter=%+v typed quantized rerank returned no read view", filter)
		}
		return response, view
	}
	assertCurrent := func(stage string, response VectorIndexSearchResponse) {
		t.Helper()
		if len(response.Results) != 3 || string(response.Results[0].ID) != "replaced" || string(response.Results[1].ID) != "closer" || string(response.Results[2].ID) != "farther" {
			t.Fatalf("%s results=%+v want [replaced closer farther]", stage, response.Results)
		}
		if response.Stats.SearchRouteQuantizedRerank != 1 {
			t.Fatalf("%s did not retain selected route: %+v", stage, response.Stats)
		}
	}

	held, heldView := search(nil)
	assertCurrent("held selected owner", held)
	heldDocs, err := heldView.FetchDocumentsForVectorIndexSearchResults(held.Results, DocumentFetchOptions{})
	if err != nil || len(heldDocs.Results) != len(held.Results) {
		t.Fatalf("held selected owner fetch before rebuild: docs=%+v err=%v", heldDocs.Results, err)
	}
	heldWork := held.Stats.ColumnGraphWork
	if heldWork.BaseShadowed == 0 || heldWork.DeltaScored != 1 || heldWork.ScorePlane.ExactSuffixScoreCalls != 1 || heldWork.ScorePlane.ExactSuffixVectorBytesRead == 0 {
		t.Fatalf("held selected owner work=%+v proof=%+v", heldWork, heldWork.ScorePlane)
	}
	unfiltered, unfilteredView := search(nil)
	assertCurrent("unfiltered suffix", unfiltered)
	unfilteredWork := unfiltered.Stats.ColumnGraphWork
	if unfilteredWork.BaseShadowed == 0 || unfilteredWork.DeltaScored != 1 || unfilteredWork.ScorePlane.ExactSuffixScoreCalls != 1 || unfilteredWork.ScorePlane.ExactSuffixVectorBytesRead == 0 {
		t.Fatalf("unfiltered suffix work=%+v proof=%+v", unfilteredWork, unfilteredWork.ScorePlane)
	}
	if err := unfilteredView.Close(); err != nil {
		t.Fatal(err)
	}

	filter := HybridScalarFilter{IndexName: "user", Value: "u"}
	filtered, filteredView := search(&filter)
	assertCurrent("filtered suffix", filtered)
	filteredWork := filtered.Stats.ColumnGraphWork
	if filteredWork.DeltaScored != 1 || filteredWork.ScorePlane.ExactSuffixScoreCalls != 1 || filteredWork.ScorePlane.ExactSuffixVectorBytesRead == 0 {
		t.Fatalf("filtered suffix work=%+v proof=%+v", filteredWork, filteredWork.ScorePlane)
	}
	if err := filteredView.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	if err := col.db.VacuumIndexOnline(ctx); err != nil {
		cancel()
		t.Fatalf("selected owner vacuum: %v", err)
	}
	cancel()
	heldDocsAfter, err := heldView.FetchDocumentsForVectorIndexSearchResults(held.Results, DocumentFetchOptions{})
	if err != nil || len(heldDocsAfter.Results) != len(heldDocs.Results) {
		t.Fatalf("held selected owner fetch after rebuild/vacuum: docs=%+v err=%v", heldDocsAfter.Results, err)
	}
	for i := range heldDocs.Results {
		if !bytes.Equal(heldDocsAfter.Results[i].Document, heldDocs.Results[i].Document) {
			t.Fatalf("held selected owner document changed at %d before=%q after=%q", i, heldDocs.Results[i].Document, heldDocsAfter.Results[i].Document)
		}
	}
	if err := heldView.Close(); err != nil {
		t.Fatalf("close held selected owner: %v", err)
	}
	afterRebuild, afterRebuildView := search(nil)
	defer afterRebuildView.Close()
	assertCurrent("rebuilt base", afterRebuild)
	if afterRebuild.Stats.ColumnGraphDeltaScored != 0 || afterRebuild.Stats.ColumnGraphWork.ScorePlane.ExactSuffixScoreCalls != 0 {
		t.Fatalf("rebuilt base retained stale suffix work: stats=%+v proof=%+v", afterRebuild.Stats, afterRebuild.Stats.ColumnGraphWork.ScorePlane)
	}
}

func TestTypedGraphPublicScalarU8QuantizedRerankReopen(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	dir, db, col := openTypedMinimaCollectionMeta(t, func() CollectionMeta {
		meta := typedMinimaCollectionMeta()
		meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
		return meta
	}())
	query := []float32{1, 0, 0, 0, 0, 0, 0, 0}
	ids := [][]byte{[]byte("a"), []byte("b")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{query, {0, 1, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"before", "other"}},
		{Name: "user", Strings: []string{"u", "u"}},
		{Name: "path", Strings: []string{"p", "p"}},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	serving := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		db.Close()
		t.Skip("typed shared prepared holder is unavailable on this host")
	} else if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	// Leave this replacement after the checkpoint so reopening exercises the
	// durable command-WAL/current-suffix path rather than only the base files.
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{query}},
		{Name: "content", Strings: []string{"after-reopen"}},
		{Name: "user", Strings: []string{"u"}},
		{Name: "path", Strings: []string{"p"}},
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openTypedMinimaDB(t, dir)
	defer reopened.Close()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedCol.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); err != nil {
		t.Fatal(err)
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := reopenedCol.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
		IndexName:                 "embedding_graph",
		Query:                     query,
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        "embedding.scalar_u8.legacy",
		QuantizedRerankCandidates: 2,
		TopK:                      1,
		EfSearch:                  2,
		StatsMode:                 VectorIndexSearchStatsModeMinimal,
	}, &buffer)
	if err != nil || view == nil {
		t.Fatalf("reopened selected search response=%+v view=%v err=%v", response, view, err)
	}
	defer view.Close()
	if len(response.Results) != 1 || string(response.Results[0].ID) != "a" || response.Stats.SearchRouteQuantizedRerank != 1 || response.Stats.QuantizedScoreCalls == 0 {
		t.Fatalf("reopened selected response=%+v", response)
	}
	docs, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
	if err != nil || len(docs.Results) != 1 || !bytes.Contains(docs.Results[0].Document, []byte("after-reopen")) {
		t.Fatalf("reopened selected fetch docs=%+v err=%v", docs.Results, err)
	}
}
