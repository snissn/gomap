package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestVectorIndexSearchStatsModePublicMapping2126(t *testing.T) {
	tests := []struct {
		name string
		mode VectorIndexSearchStatsMode
		want columnVectorGraphNativeSearchStatsMode
	}{
		{name: "default_preserves_full_diagnostics", mode: VectorIndexSearchStatsModeDefault, want: columnVectorGraphNativeSearchStatsModeFullDiagnostics},
		{name: "full_diagnostics", mode: VectorIndexSearchStatsModeFullDiagnostics, want: columnVectorGraphNativeSearchStatsModeFullDiagnostics},
		{name: "minimal", mode: VectorIndexSearchStatsModeMinimal, want: columnVectorGraphNativeSearchStatsModeMinimal},
		{name: "production_alias", mode: VectorIndexSearchStatsModeProduction, want: columnVectorGraphNativeSearchStatsModeMinimal},
		{name: "benchmark_debug", mode: VectorIndexSearchStatsModeBenchmarkDebug, want: columnVectorGraphNativeSearchStatsModeBenchmarkDebug},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := columnVectorGraphNativeSearchStatsModeFromPublic(tt.mode)
			if err != nil {
				t.Fatalf("columnVectorGraphNativeSearchStatsModeFromPublic(%q): %v", tt.mode, err)
			}
			if got != tt.want {
				t.Fatalf("columnVectorGraphNativeSearchStatsModeFromPublic(%q)=%s want %s", tt.mode, got, tt.want)
			}
		})
	}
	if got, err := columnVectorGraphNativeSearchStatsModeFromPublic(VectorIndexSearchStatsMode("debug_everything")); err == nil || got != columnVectorGraphNativeSearchStatsModeDefault {
		t.Fatalf("unsupported mode got=(%s,%v) want default mode with error", got, err)
	}
}

func TestVectorIndexQuantizedDefinitionNormalization1926(t *testing.T) {
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 3,
		Strategy:   VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedVectorIndexDefinition{{
			Name: "embedding.scalar_u8.fast",
		}},
	})
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	if got := def.QuantizedIndexes; len(got) != 1 || got[0].Name != "embedding.scalar_u8.fast" || got[0].Codec != QuantizedVectorCodecScalarU8 || got[0].Version != 1 || got[0].ScalarU8Calibration != nil {
		t.Fatalf("QuantizedIndexes=%+v want normalized legacy scalar_u8 v1 without explicit calibration config", got)
	}
	codecConfig, codecConfigHash, err := scalarU8CalibrationCodecConfig(def.QuantizedIndexes[0])
	if err != nil {
		t.Fatalf("legacy scalar_u8 codec config identity: %v", err)
	}
	if codecConfigHash != 0 || len(codecConfig) != 0 {
		t.Fatalf("legacy scalar_u8 codec config hash/bytes=(%d,%q) want zero/empty", codecConfigHash, codecConfig)
	}
	if _, ok := findQuantizedVectorIndex(def, "embedding.scalar_u8.fast"); !ok {
		t.Fatalf("findQuantizedVectorIndex did not find normalized quantized index")
	}

	rejects := []struct {
		name string
		def  VectorIndexDefinition
		want string
	}{
		{
			name: "non_column_graph",
			def: VectorIndexDefinition{
				Name:       "embedding_graph",
				Field:      "embedding",
				Metric:     VectorMetricCosine,
				Dimensions: 3,
				QuantizedIndexes: []QuantizedVectorIndexDefinition{{
					Name: "q",
				}},
			},
			want: "require strategy",
		},
		{
			name: "duplicate_name",
			def: VectorIndexDefinition{
				Name:       "embedding_graph",
				Field:      "embedding",
				Metric:     VectorMetricCosine,
				Dimensions: 3,
				Strategy:   VectorIndexStrategyColumnGraph,
				QuantizedIndexes: []QuantizedVectorIndexDefinition{
					{Name: "q"},
					{Name: "q"},
				},
			},
			want: "duplicate quantized index",
		},
		{
			name: "unsupported_codec",
			def: VectorIndexDefinition{
				Name:       "embedding_graph",
				Field:      "embedding",
				Metric:     VectorMetricCosine,
				Dimensions: 3,
				Strategy:   VectorIndexStrategyColumnGraph,
				QuantizedIndexes: []QuantizedVectorIndexDefinition{{
					Name:  "q",
					Codec: "unsupported_quantizer",
				}},
			},
			want: "unsupported",
		},
	}
	for _, tt := range rejects {
		t.Run(tt.name, func(t *testing.T) {
			_, err := normalizeVectorIndexDefinition(tt.def)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("normalizeVectorIndexDefinition err=%v want containing %q", err, tt.want)
			}
		})
	}
}

func TestScalarU8CalibrationDefinitionNormalization2842(t *testing.T) {
	base := VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 3,
		Strategy:   VectorIndexStrategyColumnGraph,
	}

	explicitLegacy := base
	explicitLegacy.QuantizedIndexes = []QuantizedVectorIndexDefinition{{
		Name:                "embedding.scalar_u8.legacy",
		Codec:               QuantizedVectorCodecScalarU8,
		ScalarU8Calibration: &ScalarU8CalibrationConfig{},
	}}
	normalizedLegacy, err := normalizeVectorIndexDefinition(explicitLegacy)
	if err != nil {
		t.Fatalf("normalize explicit legacy scalar_u8: %v", err)
	}
	legacyCfg := normalizedLegacy.QuantizedIndexes[0].ScalarU8Calibration
	if legacyCfg == nil || legacyCfg.Mode != ScalarU8CalibrationModeLegacy || legacyCfg.Grouping != "" || !scalarU8AlphaPolicyZero(legacyCfg.AlphaPolicy) {
		t.Fatalf("explicit legacy scalar_u8 config=%+v want normalized legacy mode only", legacyCfg)
	}
	legacyBytes, legacyHash, err := scalarU8CalibrationCodecConfig(normalizedLegacy.QuantizedIndexes[0])
	if err != nil {
		t.Fatalf("explicit legacy scalar_u8 config identity: %v", err)
	}
	if legacyHash != 0 || len(legacyBytes) != 0 {
		t.Fatalf("explicit legacy config identity hash/bytes=(%d,%q) want zero/empty legacy identity", legacyHash, legacyBytes)
	}

	perGranule := base
	perGranule.QuantizedIndexes = []QuantizedVectorIndexDefinition{{
		Name:  "embedding.scalar_u8.alpha",
		Codec: QuantizedVectorCodecScalarU8,
		ScalarU8Calibration: &ScalarU8CalibrationConfig{
			Mode:     ScalarU8CalibrationModePerGranuleAlpha,
			Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
			AlphaPolicy: ScalarU8AlphaPolicy{
				Name: ScalarU8AlphaPolicyMaxAbs,
			},
		},
	}}
	normalizedAlpha, err := normalizeVectorIndexDefinition(perGranule)
	if err != nil {
		t.Fatalf("normalize per-granule scalar_u8 alpha: %v", err)
	}
	alphaCfg := normalizedAlpha.QuantizedIndexes[0].ScalarU8Calibration
	if alphaCfg == nil || alphaCfg.Mode != ScalarU8CalibrationModePerGranuleAlpha || alphaCfg.Grouping != ScalarU8CalibrationGroupingStorageLayoutGranule || alphaCfg.AlphaPolicy.Name != ScalarU8AlphaPolicyMaxAbs {
		t.Fatalf("per-granule scalar_u8 config=%+v want normalized alpha config", alphaCfg)
	}
	alphaBytes, alphaHash, err := scalarU8CalibrationCodecConfig(normalizedAlpha.QuantizedIndexes[0])
	if err != nil {
		t.Fatalf("per-granule scalar_u8 config identity: %v", err)
	}
	if alphaHash == 0 || !strings.Contains(string(alphaBytes), "calibration_mode=per_granule_alpha") || !strings.Contains(string(alphaBytes), "grouping=storage_layout_granule") || !strings.Contains(string(alphaBytes), "alpha_policy=max_abs") {
		t.Fatalf("per-granule scalar_u8 identity hash=%d bytes=%q missing alpha contract", alphaHash, alphaBytes)
	}
	if got := columnVectorGraphQuantizedAssetID(normalizedAlpha.QuantizedIndexes[0]); !strings.Contains(got, fmt.Sprintf("/%016x/codes", alphaHash)) {
		t.Fatalf("per-granule scalar_u8 asset_id=%q want config hash %#x", got, alphaHash)
	}

	quantile := perGranule
	quantile.QuantizedIndexes[0].ScalarU8Calibration = &ScalarU8CalibrationConfig{
		Mode:     ScalarU8CalibrationModePerGranuleAlpha,
		Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
		AlphaPolicy: ScalarU8AlphaPolicy{
			Name:        ScalarU8AlphaPolicyAbsQuantile,
			QuantilePPM: ScalarU8AlphaPolicyAbsQuantilePPM999,
		},
	}
	normalizedQuantile, err := normalizeVectorIndexDefinition(quantile)
	if err != nil {
		t.Fatalf("normalize per-granule scalar_u8 quantile alpha: %v", err)
	}
	quantileBytes, quantileHash, err := scalarU8CalibrationCodecConfig(normalizedQuantile.QuantizedIndexes[0])
	if err != nil {
		t.Fatalf("quantile scalar_u8 config identity: %v", err)
	}
	if quantileHash == 0 || quantileHash == alphaHash || string(quantileBytes) == string(alphaBytes) || !strings.Contains(string(quantileBytes), "alpha_quantile_ppm=999000") {
		t.Fatalf("quantile scalar_u8 identity hash/bytes=(%d,%q) should differ from max_abs hash/bytes=(%d,%q)", quantileHash, quantileBytes, alphaHash, alphaBytes)
	}
}

func TestNormalizeScalarU8CalibrationConfigDefaultsEmptyCodec2842(t *testing.T) {
	legacy, err := NormalizeScalarU8CalibrationConfig("embedding_graph", 0, QuantizedVectorIndexDefinition{
		Name:                "embedding.scalar_u8.legacy",
		ScalarU8Calibration: &ScalarU8CalibrationConfig{},
	})
	if err != nil {
		t.Fatalf("NormalizeScalarU8CalibrationConfig empty codec legacy: %v", err)
	}
	if legacy == nil || legacy.Mode != ScalarU8CalibrationModeLegacy {
		t.Fatalf("legacy config=%+v want normalized legacy mode", legacy)
	}
	alpha, err := NormalizeScalarU8CalibrationConfig("embedding_graph", 0, QuantizedVectorIndexDefinition{
		Name: "embedding.scalar_u8.alpha",
		ScalarU8Calibration: &ScalarU8CalibrationConfig{
			Mode: ScalarU8CalibrationModePerGranuleAlpha,
			AlphaPolicy: ScalarU8AlphaPolicy{
				Name: ScalarU8AlphaPolicyMaxAbs,
			},
		},
	})
	if err != nil {
		t.Fatalf("NormalizeScalarU8CalibrationConfig empty codec alpha: %v", err)
	}
	if alpha == nil || alpha.Mode != ScalarU8CalibrationModePerGranuleAlpha || alpha.Grouping != ScalarU8CalibrationGroupingStorageLayoutGranule || alpha.AlphaPolicy.Name != ScalarU8AlphaPolicyMaxAbs {
		t.Fatalf("alpha config=%+v want normalized per-granule alpha", alpha)
	}
}

func TestInvalidScalarU8CalibrationIdentityDoesNotMasqueradeAsLegacy2842(t *testing.T) {
	invalid := QuantizedVectorIndexDefinition{
		Name:    "embedding.scalar_u8.bad",
		Codec:   QuantizedVectorCodecScalarU8,
		Version: 1,
		ScalarU8Calibration: &ScalarU8CalibrationConfig{
			Mode: "per_vector_alpha",
		},
	}
	if _, _, err := scalarU8CalibrationCodecConfig(invalid); err == nil {
		t.Fatal("scalarU8CalibrationCodecConfig invalid config err=nil want error")
	}
	if hash, err := scalarU8CalibrationConfigHashForAssetID(invalid); err == nil || hash != 0 {
		t.Fatalf("scalarU8CalibrationConfigHashForAssetID hash=%d err=%v want zero with error", hash, err)
	}
	if assetID := columnVectorGraphQuantizedAssetID(invalid); assetID == "quantized/embedding.scalar_u8.bad/codes" {
		t.Fatalf("invalid scalar_u8 calibration asset id %q must not collapse to legacy", assetID)
	}
	if _, err := columnVectorGraphQuantizedAssetIDChecked(invalid); err == nil {
		t.Fatal("columnVectorGraphQuantizedAssetIDChecked invalid config err=nil want error")
	}
	base := ColumnStoreConfig{Enabled: true, AssetManager: &ColumnAssetManagerConfig{Namespace: "docs/column-assets"}}
	def := VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 3}
	if _, err := columnVectorGraphQuantizedCodesColumnStoreConfig("docs", base, def, invalid); err == nil {
		t.Fatal("columnVectorGraphQuantizedCodesColumnStoreConfig invalid config err=nil want error")
	}
}

func TestSameCollectionMetaScalarU8LegacyCalibrationIdentity2842(t *testing.T) {
	base := CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []QuantizedVectorIndexDefinition{{
				Name:  "embedding.scalar_u8.legacy",
				Codec: QuantizedVectorCodecScalarU8,
			}},
		}},
	}

	explicitEmpty := base
	explicitEmpty.VectorIndexes = copyVectorIndexDefinitions(base.VectorIndexes)
	explicitEmpty.VectorIndexes[0].QuantizedIndexes[0].ScalarU8Calibration = &ScalarU8CalibrationConfig{}

	explicitLegacy := base
	explicitLegacy.VectorIndexes = copyVectorIndexDefinitions(base.VectorIndexes)
	explicitLegacy.VectorIndexes[0].QuantizedIndexes[0].ScalarU8Calibration = &ScalarU8CalibrationConfig{Mode: ScalarU8CalibrationModeLegacy}

	for _, other := range []struct {
		name string
		meta CollectionMeta
	}{
		{name: "empty", meta: explicitEmpty},
		{name: "legacy", meta: explicitLegacy},
	} {
		t.Run(other.name, func(t *testing.T) {
			if !sameCollectionMeta(base, other.meta) || !sameCollectionMeta(other.meta, base) {
				t.Fatalf("sameCollectionMeta omitted vs explicit %s scalar_u8_calibration = false", other.name)
			}
		})
	}

	alpha := base
	alpha.VectorIndexes = copyVectorIndexDefinitions(base.VectorIndexes)
	alpha.VectorIndexes[0].QuantizedIndexes[0].ScalarU8Calibration = &ScalarU8CalibrationConfig{
		Mode:     ScalarU8CalibrationModePerGranuleAlpha,
		Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
		AlphaPolicy: ScalarU8AlphaPolicy{
			Name: ScalarU8AlphaPolicyMaxAbs,
		},
	}
	if sameCollectionMeta(base, alpha) {
		t.Fatal("sameCollectionMeta omitted vs per_granule_alpha scalar_u8_calibration = true")
	}
}

func TestScalarU8CalibrationDefinitionInvalidConfig2842(t *testing.T) {
	base := VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 3,
		Strategy:   VectorIndexStrategyColumnGraph,
	}
	tests := []struct {
		name string
		q    QuantizedVectorIndexDefinition
		want string
	}{
		{name: "unsupported_mode", q: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, ScalarU8Calibration: &ScalarU8CalibrationConfig{Mode: "per_vector_alpha"}}, want: "mode"},
		{name: "unsupported_grouping", q: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, ScalarU8Calibration: &ScalarU8CalibrationConfig{Mode: ScalarU8CalibrationModePerGranuleAlpha, Grouping: "vector_granule", AlphaPolicy: ScalarU8AlphaPolicy{Name: ScalarU8AlphaPolicyMaxAbs}}}, want: "grouping"},
		{name: "legacy_with_policy", q: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, ScalarU8Calibration: &ScalarU8CalibrationConfig{Mode: ScalarU8CalibrationModeLegacy, AlphaPolicy: ScalarU8AlphaPolicy{Name: ScalarU8AlphaPolicyMaxAbs}}}, want: "legacy"},
		{name: "max_abs_with_quantile", q: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, ScalarU8Calibration: &ScalarU8CalibrationConfig{Mode: ScalarU8CalibrationModePerGranuleAlpha, AlphaPolicy: ScalarU8AlphaPolicy{Name: ScalarU8AlphaPolicyMaxAbs, QuantilePPM: 999000}}}, want: "max_abs"},
		{name: "unsupported_quantile", q: QuantizedVectorIndexDefinition{Name: "q", Codec: QuantizedVectorCodecScalarU8, ScalarU8Calibration: &ScalarU8CalibrationConfig{Mode: ScalarU8CalibrationModePerGranuleAlpha, AlphaPolicy: ScalarU8AlphaPolicy{Name: ScalarU8AlphaPolicyAbsQuantile, QuantilePPM: 998000}}}, want: "quantile_ppm"},
		{name: "scalar_config_on_rabitq", q: QuantizedVectorIndexDefinition{Name: "q", Codec: "rabitq_1bit", ScalarU8Calibration: &ScalarU8CalibrationConfig{Mode: ScalarU8CalibrationModeLegacy}}, want: "requires codec"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := base
			def.QuantizedIndexes = []QuantizedVectorIndexDefinition{tt.q}
			_, err := normalizeVectorIndexDefinition(def)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("normalizeVectorIndexDefinition err=%v want containing %q", err, tt.want)
			}
		})
	}
}

func TestScalarU8CalibrationJSONMetadataRoundTrip2842(t *testing.T) {
	meta := CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
			QuantizedIndexes: []QuantizedVectorIndexDefinition{{
				Name:  "embedding.scalar_u8.alpha",
				Codec: QuantizedVectorCodecScalarU8,
				ScalarU8Calibration: &ScalarU8CalibrationConfig{
					Mode:     ScalarU8CalibrationModePerGranuleAlpha,
					Grouping: ScalarU8CalibrationGroupingStorageLayoutGranule,
					AlphaPolicy: ScalarU8AlphaPolicy{
						Name:        ScalarU8AlphaPolicyAbsQuantile,
						QuantilePPM: ScalarU8AlphaPolicyAbsQuantilePPM999,
					},
				},
			}},
		}},
	}
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		t.Fatalf("normalizeCollectionMeta: %v", err)
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		t.Fatalf("json.Marshal CollectionMeta: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"scalar_u8_calibration"`)) || !bytes.Contains(raw, []byte(`"per_granule_alpha"`)) || !bytes.Contains(raw, []byte(`"quantile_ppm":999000`)) {
		t.Fatalf("metadata JSON %s missing scalar_u8 alpha config", raw)
	}
	var decoded CollectionMeta
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("json.Unmarshal CollectionMeta: %v", err)
	}
	roundTrip, err := normalizeCollectionMeta(decoded)
	if err != nil {
		t.Fatalf("normalize decoded CollectionMeta: %v", err)
	}
	if !collectionMetaValuesEqual(normalized, roundTrip) {
		t.Fatalf("round-trip metadata=%+v want %+v", roundTrip, normalized)
	}
}

func TestScalarU8CalibrationDocsSpec2842(t *testing.T) {
	doc := readRepoText(t, "TreeDB/docs/spec/quantized-vector-index.md")
	requireTextContains(t, "quantized vector index scalar_u8 alpha spec", doc,
		"scalar_u8_calibration",
		"per_granule_alpha",
		"storage_layout_granule",
		"alpha_policy",
		"CodecDescriptor.Config",
		"ConfigHash",
		"Existing legacy scalar_u8 declarations with omitted calibration config keep the legacy empty codec config identity",
		"Downstream alpha asset builders must fail closed",
	)
}

func TestSearchVectorIndexQuantizedOnlyAndRerankSupported1926(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	query := []float32{1, 0, 0}
	exactOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1}
	exact, err := col.SearchVectorIndex(exactOpts)
	if err != nil {
		t.Fatalf("exact SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, exact, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, exact.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	assertExactVectorIndexSearchHasNoQuantizedRouteStats2416(t, exact.Stats)
	explicitExact, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeExact, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("explicit exact SearchVectorIndex: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, explicitExact.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	assertExactVectorIndexSearchHasNoQuantizedRouteStats2416(t, explicitExact.Stats)

	quantizedOnly, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:          def.Name,
		Query:              query,
		QueryMode:          VectorIndexQueryModeQuantizedOnly,
		QuantizedIndexName: def.QuantizedIndexes[0].Name,
		TopK:               2,
		EfSearch:           len(rows),
		MaxDecodedBlocks:   1,
	})
	if err != nil {
		t.Fatalf("quantized_only SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, quantizedOnly, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, scalarU8QuantizedTopKForTest1926(t, rows, query, 2), false)
	if quantizedOnly.Stats.QuantizedScoreCalls == 0 || quantizedOnly.Stats.QuantizedScoreCalls != quantizedOnly.Stats.CandidateFetches || quantizedOnly.Stats.QuantizedCodeBytesRead != quantizedOnly.Stats.QuantizedScoreCalls*uint64(def.Dimensions) {
		t.Fatalf("quantized stats=%+v want scalar_u8 code scoring counters", quantizedOnly.Stats)
	}
	if quantizedOnly.Stats.PreparedScoreCalls != 0 || quantizedOnly.Stats.VectorBytesRead != 0 || quantizedOnly.Stats.NormBytesRead != 0 || quantizedOnly.Stats.DocumentsFetched != 0 {
		t.Fatalf("quantized stats=%+v want no exact vector/norm scoring or document materialization", quantizedOnly.Stats)
	}
	assertQuantizedOnlyGuardrailStats2416(t, quantizedOnly.Stats, def.Dimensions)
	minimal, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction})
	if err != nil {
		t.Fatalf("production-stats quantized_only SearchVectorIndex: %v", err)
	}
	if minimal.Stats.Candidates == 0 || minimal.Stats.VisitedEdges == 0 || minimal.Stats.QuantizedScoreCalls == 0 {
		t.Fatalf("production-stats quantized stats=%+v want candidate/edge/code counters", minimal.Stats)
	}
	assertQuantizedOnlyGuardrailStats2416(t, minimal.Stats, def.Dimensions)

	rerankedNoDocs, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:                 def.Name,
		Query:                     query,
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        def.QuantizedIndexes[0].Name,
		QuantizedRerankCandidates: 3,
		TopK:                      2,
		EfSearch:                  len(rows),
		MaxDecodedBlocks:          1,
	})
	if err != nil {
		t.Fatalf("quantized_rerank no-doc SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, rerankedNoDocs, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, rerankedNoDocs.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	assertQuantizedRerankNoDocumentGuardrailStats2416(t, rerankedNoDocs.Stats, 3)

	reranked, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:                 def.Name,
		Query:                     query,
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        def.QuantizedIndexes[0].Name,
		QuantizedRerankCandidates: 2,
		TopK:                      2,
		EfSearch:                  len(rows),
		IncludeDocuments:          true,
		MaxDecodedBlocks:          1,
	})
	if err != nil {
		t.Fatalf("quantized_rerank SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, reranked, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, reranked.Results, exactColumnGraphTopKForTest(t, rows, query, 2), true)
	if reranked.Stats.QuantizedScoreCalls == 0 || reranked.Stats.QuantizedRerankCandidates != 2 || reranked.Stats.QuantizedRerankExactScoreCalls != 2 || reranked.Stats.VectorBytesRead == 0 || reranked.Stats.NormBytesRead == 0 {
		t.Fatalf("quantized_rerank stats=%+v want quantized traversal plus exact rerank", reranked.Stats)
	}
	if reranked.Stats.DocumentsFetched != uint64(len(reranked.Results)) {
		t.Fatalf("quantized_rerank stats=%+v want documents fetched after exact rerank", reranked.Stats)
	}

	missingOnly, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: "missing.scalar_u8", TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "is not declared") {
		t.Fatalf("undeclared quantized index err=%v want unavailable declared-name failure", err)
	}
	assertQuantizedUnavailableGuardrailStats2416(t, missingOnly.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, columnVectorGraphQuantizedAssetHealthMissing)
	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: "missing.scalar_u8", TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "is not declared") || len(got.Results) != 0 || got.Stats.CandidateFetches != 0 || got.Stats.QuantizedScoreCalls != 0 || got.Stats.PreparedScoreCalls != 0 {
		t.Fatalf("undeclared quantized_rerank response=%+v err=%v want fail-closed no fallback", got, err)
	}
	assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, columnVectorGraphQuantizedAssetHealthMissing)
	if _, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeExact, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}); err == nil || !strings.Contains(err.Error(), "exact") {
		t.Fatalf("exact with quantized index err=%v want validation failure", err)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	reopenedMeta := reopenedCol.Meta()
	if got := reopenedMeta.VectorIndexes[0].QuantizedIndexes; len(got) != 1 || got[0] != def.QuantizedIndexes[0] {
		t.Fatalf("reopened QuantizedIndexes=%+v want %+v", got, def.QuantizedIndexes)
	}
}

func assertExactVectorIndexSearchHasNoQuantizedRouteStats2416(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 0 || stats.QuantizedAssetUnavailable != 0 || stats.QuantizedAssetHeapCopy != 0 || stats.QuantizedAssetMmapDirect != 0 {
		tb.Fatalf("exact stats=%+v want no quantized route/scorer/asset counters", stats)
	}
}

func assertQuantizedOnlyGuardrailStats2416(tb testing.TB, stats VectorIndexSearchStats, dims int) {
	tb.Helper()
	diag := stats.Diagnostics()
	if diag.Route != VectorIndexSearchRouteQuantizedOnly || diag.HNSWSearchPackStatus != VectorIndexSearchHNSWSearchPackStatusNone || diag.ExactHNSWSearchPackNoDocRoute {
		tb.Fatalf("quantized_only diagnostics=%+v want codec-generic quantized route without exact hnsw_search_pack_v1 status", diag)
	}
	if stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 || stats.QuantizedScorerActive != 1 {
		tb.Fatalf("quantized_only route stats=%+v want quantized-only scorer route", stats)
	}
	if stats.SearchRouteHNSWSearchPack != 0 || stats.HNSWSearchPackActive != 0 || stats.HNSWSearchPackFallbacks != 0 {
		tb.Fatalf("quantized_only stats=%+v want no hnsw_search_pack_v1 route/active/fallback claim", stats)
	}
	if stats.QuantizedAssetUnavailable != 0 || stats.QuantizedAssetMissing != 0 || stats.QuantizedAssetInvalid != 0 || stats.QuantizedAssetStale != 0 || stats.QuantizedAssetClosed != 0 {
		tb.Fatalf("quantized_only asset stats=%+v want available quantized asset", stats)
	}
	if stats.QuantizedAssetHeapCopy+stats.QuantizedAssetMmapDirect != 1 || stats.QuantizedAssetHeapCopyBytes+stats.QuantizedAssetMappedBytes == 0 {
		tb.Fatalf("quantized_only asset stats=%+v want exactly one quantized asset residency counter", stats)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead != stats.QuantizedScoreCalls*uint64(dims) {
		tb.Fatalf("quantized_only score stats=%+v dims=%d want quantized code scoring bytes", stats, dims)
	}
	if stats.PreparedScoreCalls != 0 || stats.QuantizedRerankExactScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 {
		tb.Fatalf("quantized_only stats=%+v want no exact vector/norm reads or exact rerank", stats)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("quantized_only guardrails stats=%+v want no docs/fallback/scratch decode", stats)
	}
}

func assertQuantizedRerankNoDocumentGuardrailStats2416(tb testing.TB, stats VectorIndexSearchStats, shortlist int) {
	tb.Helper()
	diag := stats.Diagnostics()
	if diag.Route != VectorIndexSearchRouteQuantizedRerank || diag.HNSWSearchPackStatus != VectorIndexSearchHNSWSearchPackStatusNone || diag.ExactHNSWSearchPackNoDocRoute {
		tb.Fatalf("quantized_rerank diagnostics=%+v want codec-generic quantized route without exact hnsw_search_pack_v1 status", diag)
	}
	if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 || stats.QuantizedScorerActive != 1 {
		tb.Fatalf("quantized_rerank route stats=%+v want quantized-rerank scorer route", stats)
	}
	if stats.SearchRouteHNSWSearchPack != 0 || stats.HNSWSearchPackActive != 0 || stats.HNSWSearchPackFallbacks != 0 {
		tb.Fatalf("quantized_rerank stats=%+v want no hnsw_search_pack_v1 route/active/fallback claim", stats)
	}
	if stats.QuantizedAssetUnavailable != 0 || stats.QuantizedAssetHeapCopy+stats.QuantizedAssetMmapDirect != 1 {
		tb.Fatalf("quantized_rerank asset stats=%+v want available quantized asset", stats)
	}
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedRerankCandidates != uint64(shortlist) || stats.QuantizedRerankExactScoreCalls != uint64(shortlist) {
		tb.Fatalf("quantized_rerank stats=%+v want exact score calls equal shortlist=%d", stats, shortlist)
	}
	if stats.VectorBytesRead == 0 || stats.NormBytesRead == 0 {
		tb.Fatalf("quantized_rerank stats=%+v want exact vector/norm reads only for rerank shortlist", stats)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("quantized_rerank guardrails stats=%+v want no docs/fallback/scratch decode", stats)
	}
}

func assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(tb testing.TB, stats VectorIndexSearchStats, shortlist int, dims int) {
	tb.Helper()
	diag := stats.Diagnostics()
	if diag.Route != VectorIndexSearchRouteQuantizedRerank || diag.HNSWSearchPackStatus != VectorIndexSearchHNSWSearchPackStatusNone || diag.ExactHNSWSearchPackNoDocRoute {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank diagnostics=%+v want quantized route without exact hnsw_search_pack_v1 route claim", diag)
	}
	if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 || stats.QuantizedScorerActive != 1 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank route stats=%+v want quantized-rerank scorer route", stats)
	}
	if stats.SearchRouteHNSWSearchPack != 0 || stats.HNSWSearchPackActive != 0 || stats.HNSWSearchPackFallbacks != 0 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank stats=%+v want no public hnsw_search_pack_v1 route/active/fallback claim", stats)
	}
	if stats.QuantizedAssetUnavailable != 0 || stats.QuantizedAssetHeapCopy+stats.QuantizedAssetMmapDirect != 1 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank asset stats=%+v want available quantized asset", stats)
	}
	shortlist64 := uint64(shortlist)
	if stats.QuantizedScoreCalls == 0 || stats.QuantizedRerankCandidates != shortlist64 || stats.QuantizedRerankExactScoreCalls != shortlist64 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank stats=%+v want exact score calls equal shortlist=%d", stats, shortlist)
	}
	if stats.PreparedScoreCalls != shortlist64 || stats.VectorPreparedDirectViews != shortlist64 || stats.NormPreparedDirectViews != 0 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank stats=%+v want prepared pack exact row-ID scores=%d and no prepared norm views", stats, shortlist)
	}
	if dims > 0 && stats.VectorBytesRead != shortlist64*uint64(dims)*4 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank stats=%+v want vector bytes=%d", stats, shortlist64*uint64(dims)*4)
	}
	if stats.NormBytesRead != shortlist64*4 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank stats=%+v want logical exact norm bytes=%d", stats, shortlist64*4)
	}
	if stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("scalar_u8 pack-native quantized_rerank guardrails stats=%+v want no docs/fallback/scratch decode", stats)
	}
}

func assertQuantizedUnavailableGuardrailStats2416(tb testing.TB, stats VectorIndexSearchStats, mode columnVectorGraphNativeSearchQueryMode, health columnVectorGraphQuantizedAssetHealth) {
	tb.Helper()
	diag := stats.Diagnostics()
	if diag.HNSWSearchPackStatus != VectorIndexSearchHNSWSearchPackStatusNone || diag.ExactHNSWSearchPackNoDocRoute {
		tb.Fatalf("unavailable quantized diagnostics=%+v want no exact hnsw_search_pack_v1 route/status", diag)
	}
	if mode == columnVectorGraphNativeSearchQueryModeQuantizedOnly {
		if diag.Route != VectorIndexSearchRouteQuantizedOnly {
			tb.Fatalf("unavailable quantized_only diagnostics=%+v want quantized-only route", diag)
		}
		if stats.SearchRouteQuantizedOnly != 1 || stats.SearchRouteQuantizedRerank != 0 {
			tb.Fatalf("unavailable quantized_only stats=%+v want quantized-only route", stats)
		}
	} else if mode == columnVectorGraphNativeSearchQueryModeQuantizedRerank {
		if diag.Route != VectorIndexSearchRouteQuantizedRerank {
			tb.Fatalf("unavailable quantized_rerank diagnostics=%+v want quantized-rerank route", diag)
		}
		if stats.SearchRouteQuantizedOnly != 0 || stats.SearchRouteQuantizedRerank != 1 {
			tb.Fatalf("unavailable quantized_rerank stats=%+v want quantized-rerank route", stats)
		}
	}
	if stats.QuantizedAssetUnavailable != 1 || stats.QuantizedScorerActive != 0 || stats.QuantizedAssetHeapCopy != 0 || stats.QuantizedAssetMmapDirect != 0 {
		tb.Fatalf("unavailable quantized stats=%+v want fail-closed unavailable asset and no active scorer", stats)
	}
	switch health {
	case columnVectorGraphQuantizedAssetHealthInvalid:
		if stats.QuantizedAssetInvalid != 1 || stats.QuantizedAssetMissing != 0 || stats.QuantizedAssetStale != 0 || stats.QuantizedAssetClosed != 0 {
			tb.Fatalf("unavailable quantized stats=%+v want invalid asset", stats)
		}
	case columnVectorGraphQuantizedAssetHealthStale:
		if stats.QuantizedAssetStale != 1 || stats.QuantizedAssetMissing != 0 || stats.QuantizedAssetInvalid != 0 || stats.QuantizedAssetClosed != 0 {
			tb.Fatalf("unavailable quantized stats=%+v want stale asset", stats)
		}
	case columnVectorGraphQuantizedAssetHealthClosed:
		if stats.QuantizedAssetClosed != 1 || stats.QuantizedAssetMissing != 0 || stats.QuantizedAssetInvalid != 0 || stats.QuantizedAssetStale != 0 {
			tb.Fatalf("unavailable quantized stats=%+v want closed asset", stats)
		}
	default:
		if stats.QuantizedAssetMissing != 1 || stats.QuantizedAssetInvalid != 0 || stats.QuantizedAssetStale != 0 || stats.QuantizedAssetClosed != 0 {
			tb.Fatalf("unavailable quantized stats=%+v want missing asset", stats)
		}
	}
	if stats.SearchRouteHNSWSearchPack != 0 || stats.HNSWSearchPackActive != 0 || stats.HNSWSearchPackFallbacks != 0 || stats.PreparedScoreCalls != 0 || stats.QuantizedScoreCalls != 0 || stats.QuantizedRerankExactScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 || stats.DocumentsFetched != 0 || stats.GraphRowFallbacks != 0 || stats.TypedColumnFallbacks != 0 || stats.VectorScratchDecodes != 0 {
		tb.Fatalf("unavailable quantized stats=%+v want fail-closed with no exact/quantized scoring, docs, or fallback", stats)
	}
}

func TestVectorIndexSearcherQuantizedSearchWithBufferSuccessAndErrorReset1926(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	query := []float32{1, 0.1, 0}
	validOpts := VectorIndexSearcherSearchOptions{Query: query, TopK: 2, EfSearch: len(rows)}
	valid, err := searcher.SearchWithBuffer(validOpts, &buffer)
	if err != nil || len(valid.Results) != 2 || len(buffer.results) != 2 {
		t.Fatalf("initial SearchWithBuffer results=%d buffer=%d err=%v want 2,2,nil", len(valid.Results), len(buffer.results), err)
	}
	quantizedOpts := VectorIndexSearcherSearchOptions{
		Query:              query,
		QueryMode:          VectorIndexQueryModeQuantizedOnly,
		QuantizedIndexName: def.QuantizedIndexes[0].Name,
		TopK:               2,
		EfSearch:           len(rows),
	}
	quantized, err := searcher.SearchWithBuffer(quantizedOpts, &buffer)
	if err != nil {
		t.Fatalf("quantized SearchWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantized.Results, scalarU8QuantizedTopKForTest1926(t, rows, query, 2), false)
	if len(buffer.results) != 2 || len(buffer.idBytes) == 0 || quantized.Stats.QuantizedScoreCalls == 0 {
		t.Fatalf("quantized buffer/results stats=%+v buffer.results=%d idBytes=%d", quantized.Stats, len(buffer.results), len(buffer.idBytes))
	}
	assertQuantizedOnlyGuardrailStats2416(t, quantized.Stats, def.Dimensions)
	rerankOpts := VectorIndexSearcherSearchOptions{
		Query:                     query,
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        def.QuantizedIndexes[0].Name,
		QuantizedRerankCandidates: len(rows),
		TopK:                      2,
		EfSearch:                  len(rows),
	}
	reranked, err := searcher.SearchWithBuffer(rerankOpts, &buffer)
	if err != nil {
		t.Fatalf("quantized_rerank SearchWithBuffer: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, reranked.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	if len(buffer.results) != 2 || len(buffer.idBytes) == 0 || reranked.Stats.QuantizedScoreCalls == 0 || reranked.Stats.QuantizedRerankExactScoreCalls == 0 {
		t.Fatalf("rerank buffer/results stats=%+v buffer.results=%d idBytes=%d", reranked.Stats, len(buffer.results), len(buffer.idBytes))
	}
	assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(t, reranked.Stats, len(rows), def.Dimensions)
	quantizedAllocs := testing.AllocsPerRun(100, func() {
		got, err := searcher.SearchWithBuffer(quantizedOpts, &buffer)
		if err != nil || len(got.Results) != 2 {
			panic("unexpected quantized SearchWithBuffer allocation probe result")
		}
	})
	if quantizedAllocs != 0 {
		t.Fatalf("quantized SearchWithBuffer steady-state allocs/run=%v want 0", quantizedAllocs)
	}
	rerankAllocs := testing.AllocsPerRun(100, func() {
		got, err := searcher.SearchWithBuffer(rerankOpts, &buffer)
		if err != nil || len(got.Results) != 2 {
			panic("unexpected quantized_rerank SearchWithBuffer allocation probe result")
		}
	})
	if rerankAllocs != 0 {
		t.Fatalf("quantized_rerank SearchWithBuffer steady-state allocs/run=%v want 0", rerankAllocs)
	}
	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{
		Query:              query,
		QueryMode:          VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName: "missing.scalar_u8",
		TopK:               2,
		EfSearch:           len(rows),
	}, &buffer)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("SearchWithBuffer missing quantized index err=%v want ErrVectorIndexSearchUnavailable", err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("quantized error results=%d buffer.results=%d idBytes=%d want reset empty views", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}
	assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, columnVectorGraphQuantizedAssetHealthMissing)
	validAgain, err := searcher.SearchWithBuffer(validOpts, &buffer)
	if err != nil || len(validAgain.Results) != 2 {
		t.Fatalf("valid SearchWithBuffer after quantized error results=%d err=%v", len(validAgain.Results), err)
	}
}

func TestVectorIndexSearcherQuantizedAssetUnavailableCounters2416(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	qName := def.QuantizedIndexes[0].Name
	original := searcher.reader.quantizedAssetStatus[qName]
	query := []float32{1, 0, 0}
	for _, tc := range []struct {
		name   string
		mode   VectorIndexQueryMode
		health columnVectorGraphQuantizedAssetHealth
		mutate func()
	}{
		{
			name:   "missing",
			mode:   VectorIndexQueryModeQuantizedOnly,
			health: columnVectorGraphQuantizedAssetHealthMissing,
			mutate: func() { delete(searcher.reader.quantizedAssetStatus, qName) },
		},
		{
			name:   "invalid",
			mode:   VectorIndexQueryModeQuantizedOnly,
			health: columnVectorGraphQuantizedAssetHealthInvalid,
			mutate: func() {
				status := original
				status.Prepared = nil
				status.Health = columnVectorGraphQuantizedAssetHealthInvalid
				status.Err = fmt.Errorf("%w: checksum mismatch", errColumnVectorGraphQuantizedAssetInvalid)
				searcher.reader.quantizedAssetStatus[qName] = status
			},
		},
		{
			name:   "stale",
			mode:   VectorIndexQueryModeQuantizedRerank,
			health: columnVectorGraphQuantizedAssetHealthStale,
			mutate: func() {
				status := original
				status.Prepared = nil
				status.Health = columnVectorGraphQuantizedAssetHealthStale
				status.Err = fmt.Errorf("%w: base manifest mismatch", errColumnVectorGraphQuantizedAssetStale)
				searcher.reader.quantizedAssetStatus[qName] = status
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			searcher.reader.quantizedAssetStatus[qName] = original
			tc.mutate()
			var buffer VectorIndexSearchBuffer
			opts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: tc.mode, QuantizedIndexName: qName, TopK: 1, EfSearch: len(rows)}
			if tc.mode == VectorIndexQueryModeQuantizedRerank {
				opts.QuantizedRerankCandidates = 1
			}
			got, err := searcher.SearchWithBuffer(opts, &buffer)
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("SearchWithBuffer err=%v want ErrVectorIndexSearchUnavailable", err)
			}
			if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
				t.Fatalf("unavailable results=%d buffer.results=%d idBytes=%d want fail-closed empty", len(got.Results), len(buffer.results), len(buffer.idBytes))
			}
			internalMode := columnVectorGraphNativeSearchQueryModeQuantizedOnly
			if tc.mode == VectorIndexQueryModeQuantizedRerank {
				internalMode = columnVectorGraphNativeSearchQueryModeQuantizedRerank
			}
			assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, internalMode, tc.health)
			searcher.reader.quantizedAssetStatus[qName] = original
		})
	}
}

func TestVectorIndexSearcherQuantizedSearchAndSearchWithBufferParity2414(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	query := []float32{0.8, 0.2, 0}
	for _, tc := range []struct {
		name             string
		mode             VectorIndexQueryMode
		rerankCandidates int
		assertStats      func(testing.TB, VectorIndexSearchStats)
	}{
		{
			name: "quantized_only",
			mode: VectorIndexQueryModeQuantizedOnly,
			assertStats: func(tb testing.TB, stats VectorIndexSearchStats) {
				assertQuantizedOnlyGuardrailStats2416(tb, stats, def.Dimensions)
			},
		},
		{
			name:             "quantized_rerank",
			mode:             VectorIndexQueryModeQuantizedRerank,
			rerankCandidates: 4,
			assertStats: func(tb testing.TB, stats VectorIndexSearchStats) {
				if stats.NormBytesRead == 0 {
					assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(tb, stats, 4, def.Dimensions)
					return
				}
				assertQuantizedRerankNoDocumentGuardrailStats2416(tb, stats, 4)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := VectorIndexSearcherSearchOptions{
				Query:                     query,
				QueryMode:                 tc.mode,
				QuantizedIndexName:        def.QuantizedIndexes[0].Name,
				QuantizedRerankCandidates: tc.rerankCandidates,
				TopK:                      3,
				EfSearch:                  len(rows),
				StatsMode:                 VectorIndexSearchStatsModeProduction,
			}
			owned, err := searcher.Search(opts)
			if err != nil {
				t.Fatalf("Search: %v", err)
			}
			assertColumnGraphSearchResponseLoadedV4(t, owned, def.Name, opts.TopK)
			tc.assertStats(t, owned.Stats)

			var buffer VectorIndexSearchBuffer
			buffered, err := searcher.SearchWithBuffer(opts, &buffer)
			if err != nil {
				t.Fatalf("SearchWithBuffer: %v", err)
			}
			assertColumnGraphSearchResponseLoadedV4(t, buffered, def.Name, opts.TopK)
			assertVectorIndexSearchResponsesEquivalentNoDocs2124(t, buffered, owned)
			tc.assertStats(t, buffered.Stats)
			if len(buffer.results) != opts.TopK || len(buffer.idBytes) == 0 || &buffered.Results[0] != &buffer.results[0] {
				t.Fatalf("buffered results do not use caller-owned buffer: results=%d idBytes=%d", len(buffer.results), len(buffer.idBytes))
			}
		})
	}
}

func TestSearchVectorIndexWithBufferQuantizedOnlyAndRerank2415(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	query := []float32{0.8, 0.2, 0}
	qName := def.QuantizedIndexes[0].Name
	quantizedOnlyOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	quantizedOnly, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer quantized_only: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, quantizedOnly, def.Name, quantizedOnlyOpts.TopK)
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, scalarU8QuantizedTopKForTest1926(t, rows, query, quantizedOnlyOpts.TopK), false)
	assertQuantizedOnlyGuardrailStats2416(t, quantizedOnly.Stats, def.Dimensions)
	assertCollectionBufferedQuantizedRouteStats2415(t, quantizedOnly.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, quantizedOnlyOpts, def.Dimensions)
	if len(buffer.results) != quantizedOnlyOpts.TopK || len(buffer.idBytes) == 0 || &quantizedOnly.Results[0] != &buffer.results[0] || &quantizedOnly.Results[0].ID[0] != &buffer.idBytes[0] {
		t.Fatalf("quantized_only response does not alias caller-owned buffer: results=%d idBytes=%d", len(buffer.results), len(buffer.idBytes))
	}
	warmSnap := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if warmSnap.Entries != 1 || warmSnap.CacheBuilds != 1 || warmSnap.CacheMisses != 1 {
		t.Fatalf("cache after quantized warm=%+v want one prepared quantized entry", warmSnap)
	}

	for i := 0; i < 3; i++ {
		got, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
		if err != nil {
			t.Fatalf("cached quantized_only iteration %d: %v", i, err)
		}
		assertQuantizedOnlyGuardrailStats2416(t, got.Stats, def.Dimensions)
	}
	afterOnly := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterOnly.Entries != 1 || afterOnly.CacheBuilds != warmSnap.CacheBuilds || afterOnly.CacheHits < warmSnap.CacheHits+3 {
		t.Fatalf("cache after quantized_only reuse=%+v warm=%+v want hits without rebuild", afterOnly, warmSnap)
	}

	rerankOpts := quantizedOnlyOpts
	rerankOpts.QueryMode = VectorIndexQueryModeQuantizedRerank
	rerankOpts.QuantizedRerankCandidates = 4
	reranked, err := col.SearchVectorIndexWithBuffer(rerankOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer quantized_rerank: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, reranked, def.Name, rerankOpts.TopK)
	assertVectorIndexSearchResultsV4(t, reranked.Results, exactColumnGraphTopKForTest(t, rows, query, rerankOpts.TopK), false)
	assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(t, reranked.Stats, rerankOpts.QuantizedRerankCandidates, def.Dimensions)
	assertCollectionBufferedQuantizedRouteStats2415(t, reranked.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, rerankOpts, def.Dimensions)
	afterRerank := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterRerank.Entries != 1 || afterRerank.CacheBuilds != afterOnly.CacheBuilds || afterRerank.CacheHits <= afterOnly.CacheHits {
		t.Fatalf("cache after quantized_rerank=%+v afterOnly=%+v want shared quantized prepared entry and no rerank-candidate-key rebuild", afterRerank, afterOnly)
	}

	rerankOpts.QuantizedRerankCandidates = 5
	if _, err := col.SearchVectorIndexWithBuffer(rerankOpts, &buffer); err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer quantized_rerank candidates=5: %v", err)
	}
	afterPolicyChange := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterPolicyChange.Entries != 1 || afterPolicyChange.CacheBuilds != afterOnly.CacheBuilds || afterPolicyChange.CacheHits <= afterRerank.CacheHits {
		t.Fatalf("cache after rerank policy change=%+v afterRerank=%+v want query-policy hit without rebuild", afterPolicyChange, afterRerank)
	}

	emptyOnlyOpts := quantizedOnlyOpts
	emptyOnlyOpts.TopK = 0
	emptyOnly, err := col.SearchVectorIndexWithBuffer(emptyOnlyOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer quantized_only topK=0: %v", err)
	}
	if len(emptyOnly.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("quantized_only topK=0 results=%d bufferResults=%d idBytes=%d want empty buffered response", len(emptyOnly.Results), len(buffer.results), len(buffer.idBytes))
	}
	assertCollectionBufferedQuantizedRouteStats2415(t, emptyOnly.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, emptyOnlyOpts, def.Dimensions)
	badEmptyOnlyOpts := emptyOnlyOpts
	badEmptyOnlyOpts.Query = []float32{1, 0}
	badEmptyOnly, err := col.SearchVectorIndexWithBuffer(badEmptyOnlyOpts, &buffer)
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("SearchVectorIndexWithBuffer quantized_only topK=0 bad dims response=%+v err=%v want dimension mismatch", badEmptyOnly, err)
	}
	emptyRerankOpts := rerankOpts
	emptyRerankOpts.TopK = 0
	emptyRerankOpts.QuantizedRerankCandidates = 0
	emptyRerank, err := col.SearchVectorIndexWithBuffer(emptyRerankOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer quantized_rerank topK=0: %v", err)
	}
	if len(emptyRerank.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("quantized_rerank topK=0 results=%d bufferResults=%d idBytes=%d want empty buffered response", len(emptyRerank.Results), len(buffer.results), len(buffer.idBytes))
	}
	assertCollectionBufferedQuantizedRouteStats2415(t, emptyRerank.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, emptyRerankOpts, def.Dimensions)

	if !collectionsRaceEnabled {
		quantizedOnlyAllocs := testing.AllocsPerRun(100, func() {
			got, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
			if err != nil || len(got.Results) != quantizedOnlyOpts.TopK {
				panic("unexpected quantized_only SearchVectorIndexWithBuffer allocation probe result")
			}
		})
		if quantizedOnlyAllocs != 0 {
			t.Fatalf("quantized_only SearchVectorIndexWithBuffer steady-state allocs/run=%v want 0", quantizedOnlyAllocs)
		}
		rerankAllocs := testing.AllocsPerRun(100, func() {
			got, err := col.SearchVectorIndexWithBuffer(rerankOpts, &buffer)
			if err != nil || len(got.Results) != rerankOpts.TopK {
				panic("unexpected quantized_rerank SearchVectorIndexWithBuffer allocation probe result")
			}
		})
		if rerankAllocs != 0 {
			t.Fatalf("quantized_rerank SearchVectorIndexWithBuffer steady-state allocs/run=%v want 0", rerankAllocs)
		}
	}

	exactOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeExact, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	exact, err := col.SearchVectorIndexWithBuffer(exactOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer exact after quantized: %v", err)
	}
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, exact.Stats)
	afterExact := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterExact.Entries != 2 || afterExact.CacheBuilds != afterPolicyChange.CacheBuilds+1 {
		t.Fatalf("cache after exact=%+v afterQuantized=%+v want distinct exact and quantized prepared entries", afterExact, afterPolicyChange)
	}
}

func TestSearchVectorIndexWithBufferScalarU8PreparedReadersShareQuantizedAsset2621(t *testing.T) {
	for _, tc := range []struct {
		name      string
		forceHeap bool
	}{
		{name: "default_mmap_direct_when_supported"},
		{name: "forced_heap_fallback", forceHeap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.forceHeap {
				columnVectorGraphQuantizedAssetForceReadAtFallbackForTest.Store(true)
				defer columnVectorGraphQuantizedAssetForceReadAtFallbackForTest.Store(false)
			}
			rows := []columnGraphRebuildInputRowV2A{
				{id: "doc-a", vector: []float32{1, 0, 0}},
				{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
				{id: "doc-c", vector: []float32{0, 1, 0}},
				{id: "doc-d", vector: []float32{0, 0, 1}},
				{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
			}
			_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
			defer func() { _ = d.Close() }()
			if _, err := col.RebuildVectorIndex(def.Name); err != nil {
				t.Fatalf("RebuildVectorIndex: %v", err)
			}

			qName := def.QuantizedIndexes[0].Name
			opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0.8, 0.2, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
			var buffer VectorIndexSearchBuffer
			warm, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if err != nil {
				t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
			}
			assertCollectionBufferedQuantizedRouteStats2415(t, warm.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, opts, def.Dimensions)
			expectMmapDirect := !tc.forceHeap && columnVectorGraphQuantizedAssetMmapExpectedForTest2621()
			if expectMmapDirect {
				if warm.Stats.QuantizedAssetMmapDirect != 1 || warm.Stats.QuantizedAssetHeapCopy != 0 || warm.Stats.QuantizedAssetMappedBytes == 0 || warm.Stats.QuantizedAssetHeapCopyBytes != 0 {
					t.Fatalf("warm scalar_u8 stats=%+v want mmap/direct quantized asset", warm.Stats)
				}
			} else if warm.Stats.QuantizedAssetHeapCopy != 1 || warm.Stats.QuantizedAssetMmapDirect != 0 || warm.Stats.QuantizedAssetHeapCopyBytes == 0 {
				t.Fatalf("warm scalar_u8 stats=%+v want one heap-copy fallback quantized asset", warm.Stats)
			}

			queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
			if err != nil {
				t.Fatalf("normalize query mode: %v", err)
			}
			prepared, _, _, err := col.acquireCollectionVectorIndexPreparedSearch(opts)
			if err != nil {
				t.Fatalf("acquire prepared search: %v", err)
			}
			checkedOut := make([]int, 0, 8)
			defer func() {
				for _, idx := range checkedOut {
					prepared.returnCollectionVectorIndexPreparedQuantizedReader(idx)
				}
				if err := col.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
					t.Fatalf("close prepared cache: %v", err)
				}
				if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 || snap.ActiveHeapCopyBytes != 0 || snap.ActiveMappedBytes != 0 {
					t.Fatalf("cache after close=%+v want released quantized/shared handles", snap)
				}
			}()

			for i := 0; i < 8; i++ {
				reader, idx, stats, err := prepared.checkoutCollectionVectorIndexPreparedQuantizedReader(opts, queryMode)
				if err != nil {
					t.Fatalf("checkout reader %d stats=%+v err=%v", i, stats, err)
				}
				if reader == nil {
					t.Fatalf("checkout reader %d returned nil", i)
				}
				checkedOut = append(checkedOut, idx)
			}
			if got := len(prepared.quantizedReaders); got != 8 {
				t.Fatalf("prepared quantized readers=%d want 8", got)
			}

			var shared *columnVectorGraphQuantizedAssetResource
			var sharedStatus columnVectorGraphQuantizedAssetLoadStatus
			var assetBytes int64
			for i, reader := range prepared.quantizedReaders {
				status, ok := reader.quantizedAssetStatus[qName]
				if !ok || status.Prepared == nil || status.Err != nil || status.resource == nil {
					t.Fatalf("reader %d scalar_u8 status=%+v ok=%v want shared healthy resource", i, status, ok)
				}
				if status.ownsResource {
					t.Fatalf("reader %d owns shared scalar_u8 resource; prepared cache owner should hold the resource", i)
				}
				if shared == nil {
					shared = status.resource
					sharedStatus = status
				} else if status.resource != shared {
					t.Fatalf("reader %d scalar_u8 resource=%p want shared %p", i, status.resource, shared)
				}
				if status.Asset.AssetBytes > assetBytes {
					assetBytes = status.Asset.AssetBytes
				}
			}
			if shared == nil || shared.manager == nil {
				t.Fatalf("shared scalar_u8 resource=%p want resource manager", shared)
			}
			if assetBytes <= 0 {
				t.Fatalf("scalar_u8 asset bytes=%d want positive", assetBytes)
			}
			snap := col.collectionVectorIndexPreparedSearchCacheSnapshot()
			if snap.Entries != 1 || snap.ActiveHandles == 0 {
				t.Fatalf("prepared cache snapshot=%+v want one active quantized entry", snap)
			}
			scalarStats := shared.manager.Stats()
			if scalarStats.ActiveHandles != 1 {
				t.Fatalf("shared scalar_u8 manager stats=%+v want one active scalar asset handle", scalarStats)
			}
			if expectMmapDirect {
				if sharedStatus.Health != columnVectorGraphQuantizedAssetHealthMmapDirect || sharedStatus.MappedBytes < uint64(assetBytes) || sharedStatus.HeapCopyBytes != 0 {
					t.Fatalf("shared scalar_u8 status=%+v asset_bytes=%d want mmap/direct scalar_u8 with no heap copy", sharedStatus, assetBytes)
				}
				if scalarStats.ActiveMappedBytes < assetBytes || scalarStats.ActiveHeapCopyBytes != 0 {
					t.Fatalf("shared scalar_u8 manager stats=%+v asset_bytes=%d want mmap/direct scalar_u8 with no heap copy", scalarStats, assetBytes)
				}
			} else {
				if sharedStatus.Health != columnVectorGraphQuantizedAssetHealthHeapCopy || sharedStatus.HeapCopyBytes < uint64(assetBytes) || sharedStatus.MappedBytes != 0 {
					t.Fatalf("shared scalar_u8 status=%+v asset_bytes=%d want one shared heap-copy scalar_u8 asset", sharedStatus, assetBytes)
				}
				if scalarStats.ActiveHeapCopyBytes < assetBytes || scalarStats.ActiveMappedBytes != 0 {
					t.Fatalf("shared scalar_u8 manager stats=%+v asset_bytes=%d want one shared heap-copy scalar_u8 asset", scalarStats, assetBytes)
				}
			}
		})
	}
}

func columnVectorGraphQuantizedAssetMmapExpectedForTest2621() bool {
	switch runtime.GOOS {
	case "darwin", "linux", "freebsd", "netbsd", "openbsd":
		return true
	default:
		return false
	}
}

func TestScalarU8QuantizedPreparedTraversalPackRouteWhenAdjacencyClosed2586(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if searcher.reader == nil || searcher.reader.hnswSearchPack == nil || searcher.reader.adjacencyLayerSources == nil {
		t.Fatalf("searcher missing prepared pack or adjacency state")
	}
	packStatus := searcher.reader.hnswSearchPack.fastStatus(searcher.reader.hnswSearchPackStatus)
	if packStatus != columnHNSWSearchPackPreparedStatusDirect && packStatus != columnHNSWSearchPackPreparedStatusHeap {
		t.Fatalf("searcher prepared pack status=%s", packStatus)
	}
	if err := searcher.reader.adjacencyLayerSources.Close(); err != nil {
		t.Fatalf("close adjacency sources: %v", err)
	}
	searcher.reader.preparedSearch = nil

	query := []float32{0.8, 0.2, 0}
	qName := def.QuantizedIndexes[0].Name
	var buffer VectorIndexSearchBuffer
	badTopK0, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{0.8, 0.2}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 0, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("quantized_only topK=0 bad dims response=%+v err=%v want dimension mismatch", badTopK0, err)
	}
	badRerankTopK0, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{0.8, 0.2}, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: qName, TopK: 0, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("quantized_rerank topK=0 bad dims response=%+v err=%v want dimension mismatch", badRerankTopK0, err)
	}
	quantizedOnly, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 3, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatalf("quantized_only SearchWithBuffer with closed prepared adjacency: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, scalarU8QuantizedTopKForTest1926(t, rows, query, 3), false)
	assertQuantizedOnlyGuardrailStats2416(t, quantizedOnly.Stats, def.Dimensions)
	assertScalarU8PreparedTraversalPackAdjacencyStats2586(t, quantizedOnly.Stats, packStatus, "quantized_only")

	reranked, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeQuantizedRerank, QuantizedIndexName: qName, QuantizedRerankCandidates: len(rows), TopK: 3, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatalf("quantized_rerank SearchWithBuffer with closed prepared adjacency: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, reranked.Results, exactColumnGraphTopKForTest(t, rows, query, 3), false)
	assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(t, reranked.Stats, len(rows), def.Dimensions)
	assertScalarU8PreparedTraversalPackAdjacencyStats2586(t, reranked.Stats, packStatus, "quantized_rerank")
	if reranked.Stats.QuantizedRerankExactScoreCalls != uint64(len(rows)) {
		t.Fatalf("quantized_rerank stats=%+v want exact rerank shortlist", reranked.Stats)
	}
}

func TestScalarU8QuantizedPreparedTraversalRerankPreservesEfTraversal2586(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.95, 0.05, 0}},
		{id: "doc-c", vector: []float32{0.85, 0.15, 0}},
		{id: "doc-d", vector: []float32{0.75, 0.25, 0}},
		{id: "doc-e", vector: []float32{0.65, 0.35, 0}},
		{id: "doc-f", vector: []float32{0.55, 0.45, 0}},
		{id: "doc-g", vector: []float32{0.45, 0.55, 0}},
		{id: "doc-h", vector: []float32{0.35, 0.65, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	packSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher pack: %v", err)
	}
	defer func() { _ = packSearcher.Close() }()
	fallbackSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher fallback: %v", err)
	}
	defer func() { _ = fallbackSearcher.Close() }()
	if fallbackSearcher.reader == nil || fallbackSearcher.reader.hnswSearchPack == nil {
		t.Fatalf("fallback searcher missing reader or prepared pack")
	}
	fallbackSearcher.reader.hnswSearchPack = nil

	opts := VectorIndexSearcherSearchOptions{
		Query:                     []float32{0.7, 0.3, 0},
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        def.QuantizedIndexes[0].Name,
		QuantizedRerankCandidates: 4,
		TopK:                      3,
		EfSearch:                  len(rows),
		StatsMode:                 VectorIndexSearchStatsModeProduction,
	}
	var packBuffer, fallbackBuffer VectorIndexSearchBuffer
	packResults, err := packSearcher.SearchWithBuffer(opts, &packBuffer)
	if err != nil {
		t.Fatalf("pack SearchWithBuffer: %v", err)
	}
	fallbackResults, err := fallbackSearcher.SearchWithBuffer(opts, &fallbackBuffer)
	if err != nil {
		t.Fatalf("fallback SearchWithBuffer: %v", err)
	}
	assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(t, packResults.Stats, opts.QuantizedRerankCandidates, def.Dimensions)
	assertQuantizedRerankNoDocumentGuardrailStats2416(t, fallbackResults.Stats, opts.QuantizedRerankCandidates)
	if packResults.Stats.Candidates != 0 || packResults.Stats.Edges != 0 || packResults.Stats.VisitedEdges != 0 {
		t.Fatalf("production pack stats=%+v want no traversal diagnostics", packResults.Stats)
	}
	fullOpts := opts
	fullOpts.StatsMode = VectorIndexSearchStatsModeFullDiagnostics
	packFull, err := packSearcher.SearchWithBuffer(fullOpts, &packBuffer)
	if err != nil {
		t.Fatalf("full diagnostics pack SearchWithBuffer: %v", err)
	}
	fallbackFull, err := fallbackSearcher.SearchWithBuffer(fullOpts, &fallbackBuffer)
	if err != nil {
		t.Fatalf("full diagnostics fallback SearchWithBuffer: %v", err)
	}
	if packFull.Stats.Candidates != fallbackFull.Stats.Candidates || packFull.Stats.VisitedEdges != fallbackFull.Stats.VisitedEdges || packFull.Stats.QuantizedScoreCalls != fallbackFull.Stats.QuantizedScoreCalls || packFull.Stats.QuantizedRerankExactScoreCalls != fallbackFull.Stats.QuantizedRerankExactScoreCalls {
		t.Fatalf("full pack stats=%+v fallback stats=%+v want same efSearch traversal and rerank counters", packFull.Stats, fallbackFull.Stats)
	}
	workOpts := opts
	workOpts.StatsMode = VectorIndexSearchStatsModeWorkAccounting
	packWork, err := packSearcher.SearchWithBuffer(workOpts, &packBuffer)
	if err != nil {
		t.Fatalf("work-accounting pack SearchWithBuffer: %v", err)
	}
	if packWork.Stats.Candidates == 0 || packWork.Stats.VisitedEdges == 0 || packWork.Stats.WorkAccountingSearches != 1 {
		t.Fatalf("work-accounting pack stats=%+v want traversal counters", packWork.Stats)
	}
	if len(packResults.Results) != len(fallbackResults.Results) {
		t.Fatalf("pack results=%d fallback results=%d", len(packResults.Results), len(fallbackResults.Results))
	}
	for i := range packResults.Results {
		got := packResults.Results[i]
		want := fallbackResults.Results[i]
		if string(got.ID) != string(want.ID) || got.Ordinal != want.Ordinal || math.Abs(got.Score-want.Score) > 1e-6 {
			t.Fatalf("result[%d] pack=%+v fallback=%+v", i, got, want)
		}
	}
}

func TestSearchVectorIndexWithBufferScalarU8PreparedTraversalPackRouteWhenAdjacencyClosed2586(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{0.8, 0.2, 0}
	qName := def.QuantizedIndexes[0].Name
	quantizedOnlyOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	warm, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
	if err != nil || len(warm.Results) != quantizedOnlyOpts.TopK {
		t.Fatalf("warm SearchVectorIndexWithBuffer results=%d err=%v", len(warm.Results), err)
	}
	packStatus := columnHNSWSearchPackPreparedStatusMissing
	mutateCachedCollectionQuantizedReader2586(t, col, quantizedOnlyOpts, func(reader *columnVectorGraphPhysicalRowReader) {
		if reader.hnswSearchPack == nil || reader.adjacencyLayerSources == nil {
			t.Fatalf("cached reader missing prepared pack or adjacency state")
		}
		packStatus = reader.hnswSearchPack.fastStatus(reader.hnswSearchPackStatus)
		if packStatus != columnHNSWSearchPackPreparedStatusDirect && packStatus != columnHNSWSearchPackPreparedStatusHeap {
			t.Fatalf("cached prepared pack status=%s", packStatus)
		}
		if err := reader.adjacencyLayerSources.Close(); err != nil {
			t.Fatalf("close cached adjacency sources: %v", err)
		}
		reader.preparedSearch = nil
	})

	quantizedOnly, err := col.SearchVectorIndexWithBuffer(quantizedOnlyOpts, &buffer)
	if err != nil {
		t.Fatalf("quantized_only SearchVectorIndexWithBuffer with closed prepared adjacency: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, quantizedOnly.Results, scalarU8QuantizedTopKForTest1926(t, rows, query, quantizedOnlyOpts.TopK), false)
	assertQuantizedOnlyGuardrailStats2416(t, quantizedOnly.Stats, def.Dimensions)
	assertCollectionBufferedQuantizedRouteStats2415(t, quantizedOnly.Stats, columnVectorGraphNativeSearchQueryModeQuantizedOnly, quantizedOnlyOpts, def.Dimensions)
	assertScalarU8PreparedTraversalPackAdjacencyStats2586(t, quantizedOnly.Stats, packStatus, "collection quantized_only")

	rerankOpts := quantizedOnlyOpts
	rerankOpts.QueryMode = VectorIndexQueryModeQuantizedRerank
	rerankOpts.QuantizedRerankCandidates = len(rows)
	reranked, err := col.SearchVectorIndexWithBuffer(rerankOpts, &buffer)
	if err != nil {
		t.Fatalf("quantized_rerank SearchVectorIndexWithBuffer with closed prepared adjacency: %v", err)
	}
	assertVectorIndexSearchResultsV4(t, reranked.Results, exactColumnGraphTopKForTest(t, rows, query, rerankOpts.TopK), false)
	assertScalarU8PackNativeQuantizedRerankNoDocumentGuardrailStats2657(t, reranked.Stats, rerankOpts.QuantizedRerankCandidates, def.Dimensions)
	assertCollectionBufferedQuantizedRouteStats2415(t, reranked.Stats, columnVectorGraphNativeSearchQueryModeQuantizedRerank, rerankOpts, def.Dimensions)
	assertScalarU8PreparedTraversalPackAdjacencyStats2586(t, reranked.Stats, packStatus, "collection quantized_rerank")
	if reranked.Stats.QuantizedRerankExactScoreCalls != uint64(rerankOpts.QuantizedRerankCandidates) {
		t.Fatalf("collection quantized_rerank stats=%+v want exact rerank shortlist", reranked.Stats)
	}
}

func TestSearchVectorIndexWithBufferQuantizedEmptyCollection2415(t *testing.T) {
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, nil)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	qName := def.QuantizedIndexes[0].Name
	query := []float32{1, 0, 0}
	var buffer VectorIndexSearchBuffer
	for _, tc := range []struct {
		name             string
		mode             VectorIndexQueryMode
		route            columnVectorGraphNativeSearchQueryMode
		rerankCandidates int
	}{
		{name: "quantized_only", mode: VectorIndexQueryModeQuantizedOnly, route: columnVectorGraphNativeSearchQueryModeQuantizedOnly},
		{name: "quantized_rerank", mode: VectorIndexQueryModeQuantizedRerank, route: columnVectorGraphNativeSearchQueryModeQuantizedRerank, rerankCandidates: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: tc.mode, QuantizedIndexName: qName, QuantizedRerankCandidates: tc.rerankCandidates, TopK: 3, EfSearch: 8, MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
			got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if err != nil {
				t.Fatalf("SearchVectorIndexWithBuffer empty %s: %v", tc.name, err)
			}
			if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
				t.Fatalf("empty %s results=%d bufferResults=%d idBytes=%d want empty buffered response", tc.name, len(got.Results), len(buffer.results), len(buffer.idBytes))
			}
			assertCollectionBufferedQuantizedRouteStats2415(t, got.Stats, tc.route, opts, def.Dimensions)
			badOpts := opts
			badOpts.Query = []float32{1, 0}
			bad, err := col.SearchVectorIndexWithBuffer(badOpts, &buffer)
			if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
				t.Fatalf("SearchVectorIndexWithBuffer empty bad dims response=%+v err=%v want dimension mismatch", bad, err)
			}
		})
	}
}

func TestSearchVectorIndexWithBufferQuantizedAssetUnavailableFailClosed2415(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	qName := def.QuantizedIndexes[0].Name
	base := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}

	for _, tc := range []struct {
		name   string
		mode   VectorIndexQueryMode
		health columnVectorGraphQuantizedAssetHealth
		mutate func(map[string]columnVectorGraphQuantizedAssetLoadStatus, string, columnVectorGraphQuantizedAssetLoadStatus)
	}{
		{
			name:   "missing",
			mode:   VectorIndexQueryModeQuantizedOnly,
			health: columnVectorGraphQuantizedAssetHealthMissing,
			mutate: func(status map[string]columnVectorGraphQuantizedAssetLoadStatus, qName string, original columnVectorGraphQuantizedAssetLoadStatus) {
				delete(status, qName)
			},
		},
		{
			name:   "invalid",
			mode:   VectorIndexQueryModeQuantizedOnly,
			health: columnVectorGraphQuantizedAssetHealthInvalid,
			mutate: func(status map[string]columnVectorGraphQuantizedAssetLoadStatus, qName string, original columnVectorGraphQuantizedAssetLoadStatus) {
				original.Prepared = nil
				original.Health = columnVectorGraphQuantizedAssetHealthInvalid
				original.Err = fmt.Errorf("%w: checksum mismatch", errColumnVectorGraphQuantizedAssetInvalid)
				status[qName] = original
			},
		},
		{
			name:   "stale_identity_mismatch",
			mode:   VectorIndexQueryModeQuantizedRerank,
			health: columnVectorGraphQuantizedAssetHealthStale,
			mutate: func(status map[string]columnVectorGraphQuantizedAssetLoadStatus, qName string, original columnVectorGraphQuantizedAssetLoadStatus) {
				original.Prepared = nil
				original.Health = columnVectorGraphQuantizedAssetHealthStale
				original.Err = fmt.Errorf("%w: base graph identity mismatch", errColumnVectorGraphQuantizedAssetStale)
				status[qName] = original
			},
		},
		{
			name:   "closed",
			mode:   VectorIndexQueryModeQuantizedRerank,
			health: columnVectorGraphQuantizedAssetHealthClosed,
			mutate: func(status map[string]columnVectorGraphQuantizedAssetLoadStatus, qName string, original columnVectorGraphQuantizedAssetLoadStatus) {
				original.Prepared = nil
				original.Health = columnVectorGraphQuantizedAssetHealthClosed
				original.Err = fmt.Errorf("%w: closed handle", errColumnVectorGraphQuantizedAssetClosed)
				status[qName] = original
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			opts.QueryMode = tc.mode
			if tc.mode == VectorIndexQueryModeQuantizedRerank {
				opts.QuantizedRerankCandidates = 1
			}
			var buffer VectorIndexSearchBuffer
			warm, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if err != nil {
				t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
			}
			assertCollectionBufferedQuantizedRouteStats2415(t, warm.Stats, columnVectorGraphNativeSearchQueryModeFromPublic2415(tc.mode), opts, def.Dimensions)
			mutateCachedCollectionQuantizedAssetStatus2415(t, col, opts, tc.mutate)
			got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("SearchVectorIndexWithBuffer err=%v want ErrVectorIndexSearchUnavailable", err)
			}
			if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
				t.Fatalf("unavailable response results=%d buffer.results=%d idBytes=%d want fail-closed empty", len(got.Results), len(buffer.results), len(buffer.idBytes))
			}
			assertQuantizedUnavailableGuardrailStats2416(t, got.Stats, columnVectorGraphNativeSearchQueryModeFromPublic2415(tc.mode), tc.health)
			if got.Stats.SearchRouteHNSWSearchPack != 0 ||
				got.Stats.HNSWSearchPackActive != 0 ||
				got.Stats.HNSWSearchPackMissing != 0 ||
				got.Stats.HNSWSearchPackInvalid != 0 ||
				got.Stats.HNSWSearchPackStale != 0 ||
				got.Stats.HNSWSearchPackClosed != 0 ||
				got.Stats.HNSWSearchPackFallbacks != 0 ||
				got.Stats.HNSWSearchPackMmapDirect != 0 ||
				got.Stats.HNSWSearchPackHeapCopy != 0 ||
				got.Stats.HNSWSearchPackOpenNanos != 0 ||
				got.Stats.HNSWSearchPackMappedBytes != 0 ||
				got.Stats.HNSWSearchPackHeapCopyBytes != 0 ||
				got.Stats.HNSWSearchPackActiveHandles != 0 {
				t.Fatalf("unavailable stats=%+v want no exact hnsw route/fallback telemetry", got.Stats)
			}
			afterFailure := col.collectionVectorIndexPreparedSearchCacheSnapshot()
			if afterFailure.Entries != 0 || afterFailure.ActiveHandles != 0 {
				t.Fatalf("cache after fail-closed unavailable asset=%+v want invalidated closed entry", afterFailure)
			}
			recovered, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if err != nil {
				t.Fatalf("SearchVectorIndexWithBuffer after fail-closed rebuild: %v", err)
			}
			assertCollectionBufferedQuantizedRouteStats2415(t, recovered.Stats, columnVectorGraphNativeSearchQueryModeFromPublic2415(tc.mode), opts, def.Dimensions)
		})
	}
}

func TestSearchVectorIndexWithBufferQuantizedQueryErrorsKeepPreparedState2415(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	before := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if before.Entries != 1 || before.CacheBuilds != 1 {
		t.Fatalf("cache before bad query=%+v want warmed quantized entry", before)
	}
	badOpts := opts
	badOpts.Query = []float32{1, 0}
	bad, err := col.SearchVectorIndexWithBuffer(badOpts, &buffer)
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("bad quantized query response=%+v err=%v want dimension mismatch", bad, err)
	}
	if len(bad.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("bad query left results response=%d bufferResults=%d idBytes=%d", len(bad.Results), len(buffer.results), len(buffer.idBytes))
	}
	afterBad := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterBad.Entries != 1 || afterBad.CacheBuilds != before.CacheBuilds || afterBad.Invalidations != before.Invalidations || afterBad.ActiveHandles == 0 {
		t.Fatalf("cache after bad query=%+v before=%+v want healthy prepared state retained", afterBad, before)
	}
	valid, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("valid SearchVectorIndexWithBuffer after bad query: %v", err)
	}
	assertQuantizedOnlyGuardrailStats2416(t, valid.Stats, def.Dimensions)
	afterValid := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterValid.CacheBuilds != before.CacheBuilds || afterValid.CacheHits <= afterBad.CacheHits {
		t.Fatalf("cache after valid retry=%+v afterBad=%+v want hit without rebuild", afterValid, afterBad)
	}
}

func TestSearchVectorIndexWithBufferQuantizedMaxDecodedBlocksCacheSlot2415(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	base := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: def.QuantizedIndexes[0].Name, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(base, &buffer); err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer MaxDecodedBlocks=1: %v", err)
	}
	afterFirst := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterFirst.Entries != 1 || afterFirst.CacheBuilds != 1 {
		t.Fatalf("cache after MaxDecodedBlocks=1=%+v want one built entry", afterFirst)
	}
	second := base
	second.MaxDecodedBlocks = 2
	if _, err := col.SearchVectorIndexWithBuffer(second, &buffer); err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer MaxDecodedBlocks=2: %v", err)
	}
	afterSecond := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterSecond.Entries != 2 || afterSecond.CacheBuilds != 2 {
		t.Fatalf("cache after MaxDecodedBlocks=2=%+v want distinct quantized cache entry", afterSecond)
	}
	mode, err := normalizeVectorIndexSearchQueryMode(base.QueryMode, base.QuantizedIndexName, base.QuantizedRerankCandidates, base.TopK)
	if err != nil {
		t.Fatalf("normalize query mode: %v", err)
	}
	if slotA, slotB := collectionVectorIndexPreparedSearchCacheSlotForOptions(base, mode), collectionVectorIndexPreparedSearchCacheSlotForOptions(second, mode); slotA == slotB {
		t.Fatalf("quantized cache slots are equal for MaxDecodedBlocks=1 and 2: %+v", slotA)
	}
}

func TestSearchVectorIndexWithBufferQuantizedUnsupportedShapesAndLifecycle2415(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	qName := def.QuantizedIndexes[0].Name
	base := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: qName, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	valid, err := col.SearchVectorIndexWithBuffer(base, &buffer)
	if err != nil {
		_ = d.Close()
		t.Fatalf("valid SearchVectorIndexWithBuffer: %v", err)
	}
	assertQuantizedOnlyGuardrailStats2416(t, valid.Stats, def.Dimensions)

	for _, tc := range []struct {
		name     string
		mutate   func(*VectorIndexSearchOptions)
		wantErrs []string
	}{
		{name: "include_documents", mutate: func(opts *VectorIndexSearchOptions) { opts.IncludeDocuments = true }, wantErrs: []string{"IncludeDocuments=true", "no-document"}},
		{name: "projection", mutate: func(opts *VectorIndexSearchOptions) { opts.DocumentFetchOptions.IncludePaths = []string{"did"} }, wantErrs: []string{"DocumentFetchOptions.IncludePaths", "projection"}},
		{name: "document_integrity", mutate: func(opts *VectorIndexSearchOptions) {
			opts.DocumentFetchOptions.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
		}, wantErrs: []string{"DocumentFetchOptions.ColumnAssetReadIntegrity", "materialization"}},
		{name: "filter", mutate: func(opts *VectorIndexSearchOptions) {
			opts.Filter = func(DocumentRecord) (bool, error) { return true, nil }
		}, wantErrs: []string{"Filter", "no-document"}},
		{name: "range_filter", mutate: func(opts *VectorIndexSearchOptions) {
			opts.IndexRangeFilter = &VectorIndexRangeFilter{IndexName: "kind"}
		}, wantErrs: []string{"IndexRangeFilter", "no-document"}},
		{name: "benchmark_debug", mutate: func(opts *VectorIndexSearchOptions) { opts.StatsMode = VectorIndexSearchStatsModeBenchmarkDebug }, wantErrs: []string{"StatsMode=benchmark_debug", "debug-only"}},
		{name: "missing_quantized_name", mutate: func(opts *VectorIndexSearchOptions) { opts.QuantizedIndexName = "" }, wantErrs: []string{"QueryMode/QuantizedIndexName", "quantized vector index name is required"}},
		{name: "quantized_only_rerank_candidates", mutate: func(opts *VectorIndexSearchOptions) { opts.QuantizedRerankCandidates = 2 }, wantErrs: []string{"QueryMode/QuantizedIndexName", "quantized_only", "cannot set"}},
		{name: "rerank_candidates_below_topk", mutate: func(opts *VectorIndexSearchOptions) {
			opts.QueryMode = VectorIndexQueryModeQuantizedRerank
			opts.TopK = 2
			opts.QuantizedRerankCandidates = 1
		}, wantErrs: []string{"QueryMode/QuantizedIndexName", "cannot be less than top_k"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := base
			tc.mutate(&opts)
			got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if err == nil {
				t.Fatalf("SearchVectorIndexWithBuffer err=nil want %v", tc.wantErrs)
			}
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("SearchVectorIndexWithBuffer err=%v want ErrVectorIndexSearchUnavailable", err)
			}
			for _, want := range tc.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("SearchVectorIndexWithBuffer err=%v want substring %q", err, want)
				}
			}
			if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
				t.Fatalf("unsupported shape left results response=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
			}
			if _, err := col.SearchVectorIndexWithBuffer(base, &buffer); err != nil {
				t.Fatalf("valid SearchVectorIndexWithBuffer after %s: %v", tc.name, err)
			}
		})
	}

	beforeClose := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if beforeClose.Entries != 1 || beforeClose.ActiveHandles == 0 {
		_ = d.Close()
		t.Fatalf("cache before close=%+v want active quantized entry", beforeClose)
	}
	if err := col.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		_ = d.Close()
		t.Fatalf("close collection cache: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 {
		_ = d.Close()
		t.Fatalf("cache after collection close=%+v want released", snap)
	}
	if _, err := col.SearchVectorIndexWithBuffer(base, &buffer); err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndexWithBuffer after cache close: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 1 || snap.ActiveHandles == 0 {
		_ = d.Close()
		t.Fatalf("cache after rebuild=%+v want active quantized entry", snap)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("DB Close: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 {
		t.Fatalf("cache after DB close=%+v want released by manager close hook", snap)
	}
}

func columnVectorGraphNativeSearchQueryModeFromPublic2415(mode VectorIndexQueryMode) columnVectorGraphNativeSearchQueryMode {
	if mode == VectorIndexQueryModeQuantizedRerank {
		return columnVectorGraphNativeSearchQueryModeQuantizedRerank
	}
	return columnVectorGraphNativeSearchQueryModeQuantizedOnly
}

func assertCollectionBufferedQuantizedRouteStats2415(tb testing.TB, stats VectorIndexSearchStats, mode columnVectorGraphNativeSearchQueryMode, opts VectorIndexSearchOptions, dims int) {
	tb.Helper()
	if !vectorIndexSearchStatsAreBufferedNoDocumentQuantizedRoute(stats, mode, opts, dims) {
		tb.Fatalf("collection buffered quantized stats=%+v want healthy no-document quantized route", stats)
	}
}

func assertScalarU8PreparedTraversalPackAdjacencyStats2586(tb testing.TB, stats VectorIndexSearchStats, packStatus columnHNSWSearchPackPreparedStatus, label string) {
	tb.Helper()
	if stats.AdjacencySourceFallbacks != 0 {
		tb.Fatalf("%s stats=%+v want pack traversal with no source fallback", label, stats)
	}
	switch packStatus {
	case columnHNSWSearchPackPreparedStatusDirect:
		if stats.AdjacencyPreparedCSRDirectViews == 0 {
			tb.Fatalf("%s stats=%+v want direct pack CSR adjacency", label, stats)
		}
	case columnHNSWSearchPackPreparedStatusHeap:
		if stats.AdjacencyHeapCopyTypedViews == 0 {
			tb.Fatalf("%s stats=%+v want heap-copy pack adjacency", label, stats)
		}
	default:
		tb.Fatalf("%s unexpected pack status=%s", label, packStatus)
	}
}

func mutateCachedCollectionQuantizedReader2586(tb testing.TB, col *Collection, opts VectorIndexSearchOptions, mutate func(*columnVectorGraphPhysicalRowReader)) {
	tb.Helper()
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		tb.Fatalf("normalize query mode: %v", err)
	}
	slot := collectionVectorIndexPreparedSearchCacheSlotForOptions(opts, queryMode)
	col.vectorBufferedSearchMu.Lock()
	entry := col.vectorBufferedSearch[slot]
	col.vectorBufferedSearchMu.Unlock()
	if entry == nil || entry.prepared == nil || entry.prepared.searcher == nil {
		tb.Fatalf("missing cached prepared quantized entry for slot %+v", slot)
	}
	prepared := entry.prepared
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	prepared.quantizedReadersMu.Lock()
	defer prepared.quantizedReadersMu.Unlock()
	for _, reader := range prepared.quantizedReaders {
		if reader != nil {
			mutate(reader)
			return
		}
	}
	tb.Fatalf("cached prepared quantized entry has no pooled reader for slot %+v", slot)
}

func mutateCachedCollectionQuantizedAssetStatus2415(tb testing.TB, col *Collection, opts VectorIndexSearchOptions, mutate func(map[string]columnVectorGraphQuantizedAssetLoadStatus, string, columnVectorGraphQuantizedAssetLoadStatus)) {
	tb.Helper()
	queryMode, err := normalizeVectorIndexSearchQueryMode(opts.QueryMode, opts.QuantizedIndexName, opts.QuantizedRerankCandidates, opts.TopK)
	if err != nil {
		tb.Fatalf("normalize query mode: %v", err)
	}
	slot := collectionVectorIndexPreparedSearchCacheSlotForOptions(opts, queryMode)
	col.vectorBufferedSearchMu.Lock()
	entry := col.vectorBufferedSearch[slot]
	col.vectorBufferedSearchMu.Unlock()
	if entry == nil || entry.prepared == nil || entry.prepared.searcher == nil {
		tb.Fatalf("missing cached prepared quantized entry for slot %+v", slot)
	}
	prepared := entry.prepared
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	prepared.quantizedReadersMu.Lock()
	defer prepared.quantizedReadersMu.Unlock()
	var reader *columnVectorGraphPhysicalRowReader
	for _, candidate := range prepared.quantizedReaders {
		if candidate != nil {
			reader = candidate
			break
		}
	}
	if reader == nil {
		tb.Fatalf("cached prepared quantized entry has no pooled reader for slot %+v", slot)
	}
	status := reader.quantizedAssetStatus
	if status == nil {
		tb.Fatalf("cached prepared quantized entry has nil asset status")
	}
	original, ok := status[opts.QuantizedIndexName]
	if !ok {
		tb.Fatalf("cached prepared quantized entry missing status for %q", opts.QuantizedIndexName)
	}
	mutate(status, opts.QuantizedIndexName, original)
}

func TestVectorIndexSearcherQuantizedSearchWithBufferBenchmarkRows2414(t *testing.T) {
	cases := columnGraphScalarU8QuantizedSearchWithBufferBenchCases2414()
	if len(cases) != 4 {
		t.Fatalf("benchmark cases=%d want four c=1/c=8 quantized SearchWithBuffer rows", len(cases))
	}
	seen := make(map[string]columnGraphScalarU8QuantizedSearchWithBufferBenchCase2414, len(cases))
	for _, tc := range cases {
		if tc.mode != VectorIndexQueryModeQuantizedOnly && tc.mode != VectorIndexQueryModeQuantizedRerank {
			t.Fatalf("case %+v is not an explicit quantized SearchWithBuffer row", tc)
		}
		if tc.concurrency != 1 && tc.concurrency != 8 {
			t.Fatalf("case %+v has unsupported concurrency; want c=1 or c=8", tc)
		}
		if _, ok := seen[tc.name]; ok {
			t.Fatalf("duplicate benchmark case name %q", tc.name)
		}
		seen[tc.name] = tc
	}
	for _, name := range []string{
		"route=quantized_only/c=1",
		"route=quantized_only/c=8",
		"route=quantized_rerank/candidates=32/c=1",
		"route=quantized_rerank/candidates=32/c=8",
	} {
		if _, ok := seen[name]; !ok {
			t.Fatalf("missing benchmark case %q in %+v", name, cases)
		}
	}
	if seen["route=quantized_only/c=1"].rerankCandidates != 0 || seen["route=quantized_only/c=8"].rerankCandidates != 0 {
		t.Fatalf("quantized_only benchmark rows must not configure rerank candidates: %+v", cases)
	}
	if seen["route=quantized_rerank/candidates=32/c=1"].rerankCandidates != 32 || seen["route=quantized_rerank/candidates=32/c=8"].rerankCandidates != 32 {
		t.Fatalf("quantized_rerank benchmark rows must configure candidates=32: %+v", cases)
	}
}

func TestCollectionSearchVectorIndexWithBufferQuantizedBenchmarkRows2415(t *testing.T) {
	cases := columnGraphScalarU8QuantizedCollectionWithBufferBenchCases2415()
	if len(cases) != 4 {
		t.Fatalf("benchmark cases=%d want four collection c=1/c=8 quantized SearchVectorIndexWithBuffer rows", len(cases))
	}
	seen := make(map[string]columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415, len(cases))
	for _, tc := range cases {
		if tc.mode != VectorIndexQueryModeQuantizedOnly && tc.mode != VectorIndexQueryModeQuantizedRerank {
			t.Fatalf("case %+v is not an explicit collection buffered quantized row", tc)
		}
		if tc.concurrency != 1 && tc.concurrency != 8 {
			t.Fatalf("case %+v has unsupported concurrency; want c=1 or c=8", tc)
		}
		if _, ok := seen[tc.name]; ok {
			t.Fatalf("duplicate benchmark case name %q", tc.name)
		}
		seen[tc.name] = tc
	}
	for _, name := range []string{
		"route=quantized_only/c=1",
		"route=quantized_only/c=8",
		"route=quantized_rerank/candidates=32/c=1",
		"route=quantized_rerank/candidates=32/c=8",
	} {
		if _, ok := seen[name]; !ok {
			t.Fatalf("missing collection benchmark case %q in %+v", name, cases)
		}
	}
	if seen["route=quantized_only/c=1"].rerankCandidates != 0 || seen["route=quantized_only/c=8"].rerankCandidates != 0 {
		t.Fatalf("quantized_only collection benchmark rows must not configure rerank candidates: %+v", cases)
	}
	if seen["route=quantized_rerank/candidates=32/c=1"].rerankCandidates != 32 || seen["route=quantized_rerank/candidates=32/c=8"].rerankCandidates != 32 {
		t.Fatalf("quantized_rerank collection benchmark rows must configure candidates=32: %+v", cases)
	}
}

func TestCollectionSearchVectorIndexWithBufferRabitQBenchmarkRows2452(t *testing.T) {
	cases := columnGraphRabitQQuantizedCollectionWithBufferBenchCases2452()
	if len(cases) != 4 {
		t.Fatalf("benchmark cases=%d want four collection c=1/c=8 rabitq SearchVectorIndexWithBuffer rows", len(cases))
	}
	seen := make(map[string]columnGraphScalarU8QuantizedCollectionWithBufferBenchCase2415, len(cases))
	for _, tc := range cases {
		if tc.mode != VectorIndexQueryModeQuantizedOnly && tc.mode != VectorIndexQueryModeQuantizedRerank {
			t.Fatalf("case %+v is not an explicit collection buffered rabitq quantized row", tc)
		}
		if tc.concurrency != 1 && tc.concurrency != 8 {
			t.Fatalf("case %+v has unsupported concurrency; want c=1 or c=8", tc)
		}
		if _, ok := seen[tc.name]; ok {
			t.Fatalf("duplicate benchmark case name %q", tc.name)
		}
		seen[tc.name] = tc
	}
	for _, name := range []string{
		"route=quantized_only/c=1",
		"route=quantized_only/c=8",
		"route=quantized_rerank/candidates=32/c=1",
		"route=quantized_rerank/candidates=32/c=8",
	} {
		if _, ok := seen[name]; !ok {
			t.Fatalf("missing collection rabitq benchmark case %q in %+v", name, cases)
		}
	}
	if seen["route=quantized_only/c=1"].rerankCandidates != 0 || seen["route=quantized_only/c=8"].rerankCandidates != 0 {
		t.Fatalf("quantized_only collection rabitq benchmark rows must not configure rerank candidates: %+v", cases)
	}
	if seen["route=quantized_rerank/candidates=32/c=1"].rerankCandidates != 32 || seen["route=quantized_rerank/candidates=32/c=8"].rerankCandidates != 32 {
		t.Fatalf("quantized_rerank collection rabitq benchmark rows must configure candidates=32: %+v", cases)
	}
}

func TestNativeRuntimeVectorIndexRejectsQuantizedQueryMode1926(t *testing.T) {
	idx, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding_idx", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2})
	if err != nil {
		t.Fatalf("newVectorIndex: %v", err)
	}
	_, _, err = idx.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, QueryMode: VectorIndexQueryModeQuantizedOnly, QuantizedIndexName: "q"})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("VectorIndex.Search err=%v want ErrVectorIndexSearchUnavailable", err)
	}
}

func openColumnGraphQuantizedGuardrailTestCollection1926(tb testing.TB, rows []columnGraphRebuildInputRowV2A) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dims := 3
	if len(rows) > 0 {
		dims = len(rows[0].vector)
	}
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          3,
		Strategy:   VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedVectorIndexDefinition{{
			Name: "embedding.scalar_u8.fast",
		}},
	})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	if len(rows) != 0 {
		insertColumnGraphRebuildRowsV2A(tb, col, rows)
	}
	return dir, d, col, def
}

func TestSearchVectorIndexColumnGraphNativeReaderReopenV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	query := []float32{0, 0.2, 1}
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
		StatsMode:        VectorIndexSearchStatsModeBenchmarkDebug,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, reopened, "docs")
	graphRecord, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		t.Fatalf("graph manifest record %q missing", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		t.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		t.Fatalf("healthy rebuilt graph has physical row asset %+v; want TVIS/base typed-column state only", graph.AssetRef)
	}
	if got.Stats.Candidates == 0 || got.Stats.CandidateFetches == 0 || got.Stats.ResultFetches < uint64(len(got.Results)) {
		t.Fatalf("stats=%+v want public search to expose non-zero native graph traversal/result accounting", got.Stats)
	}
	if got.Stats.CandidateRows != uint64(len(rows)) || got.Stats.VisitedNodes < got.Stats.Candidates || got.Stats.VisitedEdges != got.Stats.Edges || got.Stats.VectorBytesRead == 0 || got.Stats.AdjacencyBytesRead == 0 {
		t.Fatalf("stats=%+v want public operation-specific candidate row, non-undercounting visited graph, vector-byte, and adjacency-byte counters", got.Stats)
	}
	if got.Stats.AdjacencyPreparedCSRMmapDirectViews+got.Stats.AdjacencyTypedListMmapDirectViews+got.Stats.AdjacencyTypedListHeapCopyTypedViews+got.Stats.AdjacencyTypedListScratchDecodes == 0 || got.Stats.AdjacencyLegacyFallbacks != 0 {
		t.Fatalf("stats=%+v want public search to expose prepared/state adjacency and no legacy fallback on healthy state", got.Stats)
	}
	if got.Stats.GraphRows != 0 || got.Stats.RowFetches != 0 || got.Stats.BatchFetches != 0 || got.Stats.RowsFetched != 0 || got.Stats.PhysicalBytesRead != 0 || got.Stats.OpenPhysicalBytesRead != 0 {
		t.Fatalf("stats=%+v want zero graph row payload residency/reads on healthy current-format search", got.Stats)
	}
	if got.Stats.TypedColumnFallbacks != 0 || got.Stats.RowRefVectorSourceLegacyGraphIDs != 0 || got.Stats.ResultIDGraphFallbacks != 0 || got.Stats.NormSourceFallbacks != 0 {
		t.Fatalf("stats=%+v want TVIS/base typed-column sources without graph row fallback", got.Stats)
	}
	if columnGraphTypedColumnMmapDirectViewSupportedForTest() {
		if got.Stats.PreparedScoreCalls == 0 || got.Stats.PreparedScoreCalls != got.Stats.CandidateFetches {
			t.Fatalf("stats=%+v want prepared scoring to cover every candidate fetch", got.Stats)
		}
		if got.Stats.VectorPreparedDirectViews != got.Stats.CandidateFetches || got.Stats.NormPreparedDirectViews != got.Stats.CandidateFetches {
			t.Fatalf("stats=%+v want prepared vector/norm direct views for every scored candidate", got.Stats)
		}
		if got.Stats.VectorPreparedIdentityMappings+got.Stats.VectorPreparedRowRefMappings != got.Stats.CandidateFetches || got.Stats.ScoreFloat64Fallbacks != 0 {
			t.Fatalf("stats=%+v want prepared mapping coverage and no rare float64 score fallback", got.Stats)
		}
	}
	if got.Stats.DocumentsFetched != 0 {
		t.Fatalf("DocumentsFetched=%d want no document fetch without IncludeDocuments", got.Stats.DocumentsFetched)
	}
	if got.Results[0].Document != nil {
		t.Fatalf("document materialized without IncludeDocuments: %q", got.Results[0].Document)
	}
}

func TestSearchVectorIndexNoDocumentHighQPSContractBoundary2361(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	query := []float32{1, 0, 0}
	base := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		QueryMode:        VectorIndexQueryModeExact,
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	}
	noDocs, err := col.SearchVectorIndex(base)
	if err != nil {
		t.Fatalf("SearchVectorIndex no-doc: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, noDocs, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, noDocs.Results, exactColumnGraphTopKForTest(t, rows, query, 2), false)
	assertSearchVectorIndexNoDocumentCurrentOneShotStats2361(t, noDocs.Stats)
	firstNoDocTopID := string(noDocs.Results[0].ID)
	for i, result := range noDocs.Results {
		if len(result.ID) == 0 || len(result.Document) != 0 {
			t.Fatalf("no-doc result[%d]=%+v want ID/score only without document", i, result)
		}
	}
	var searcherBuffer VectorIndexSearchBuffer
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	searcherNoDocs, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: query, QueryMode: VectorIndexQueryModeExact, TopK: 2, EfSearch: len(rows)}, &searcherBuffer)
	if closeErr := searcher.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("SearchWithBuffer no-doc: %v", err)
	}
	if len(noDocs.Results) != len(searcherNoDocs.Results) {
		t.Fatalf("SearchVectorIndex results=%d SearchWithBuffer results=%d", len(noDocs.Results), len(searcherNoDocs.Results))
	}
	for i := range noDocs.Results {
		if !bytes.Equal(noDocs.Results[i].ID, searcherNoDocs.Results[i].ID) || noDocs.Results[i].Ordinal != searcherNoDocs.Results[i].Ordinal || math.Abs(float64(noDocs.Results[i].Score-searcherNoDocs.Results[i].Score)) > 1e-6 {
			t.Fatalf("result[%d] SearchVectorIndex=%+v SearchWithBuffer=%+v want same ID/order/score", i, noDocs.Results[i], searcherNoDocs.Results[i])
		}
	}
	noDocsAgain, err := col.SearchVectorIndex(base)
	if err != nil {
		t.Fatalf("second SearchVectorIndex no-doc: %v", err)
	}
	assertSearchVectorIndexNoDocumentCurrentOneShotStats2361(t, noDocsAgain.Stats)
	if string(noDocs.Results[0].ID) != firstNoDocTopID {
		t.Fatalf("first no-doc response ID changed after second call: got %q want %q", noDocs.Results[0].ID, firstNoDocTopID)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 1 || snap.CacheBuilds != 1 || snap.CacheHits == 0 {
		t.Fatalf("cache snapshot after SearchVectorIndex no-doc route=%+v want reused collection prepared pack", snap)
	}

	withDocsOpts := base
	withDocsOpts.IncludeDocuments = true
	withDocs, err := col.SearchVectorIndex(withDocsOpts)
	if err != nil {
		t.Fatalf("SearchVectorIndex IncludeDocuments: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, withDocs, def.Name, 2)
	assertVectorIndexSearchResultsV4(t, withDocs.Results, exactColumnGraphTopKForTest(t, rows, query, 2), true)
	assertSearchVectorIndexIncludeDocumentsStats2361(t, withDocs.Stats, len(withDocs.Results))
	for i, result := range withDocs.Results {
		if len(result.ID) == 0 || len(result.Document) == 0 {
			t.Fatalf("with-docs result[%d]=%+v want ID/score plus materialized document", i, result)
		}
		assertVectorIndexSearchDocumentDIDV4(t, result.Document, string(result.ID))
	}
}

func TestVectorIndexSearchNoDocumentSplitFetchDocuments2364(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	query := []float32{1, 0, 0}
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		QueryMode:        VectorIndexQueryModeExact,
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	}

	var buffer VectorIndexSearchBuffer
	noDocs, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer no-doc: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, noDocs, def.Name, opts.TopK)
	assertVectorIndexSearchResultsV4(t, noDocs.Results, exactColumnGraphTopKForTest(t, rows, query, opts.TopK), false)
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, noDocs.Stats)
	for i, result := range noDocs.Results {
		if len(result.Document) != 0 {
			t.Fatalf("no-doc result[%d] materialized document %q", i, result.Document)
		}
	}

	withDocsOpts := opts
	withDocsOpts.IncludeDocuments = true
	withDocs, err := col.SearchVectorIndex(withDocsOpts)
	if err != nil {
		t.Fatalf("SearchVectorIndex IncludeDocuments: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, withDocs, def.Name, opts.TopK)
	assertVectorIndexSearchResultsV4(t, withDocs.Results, exactColumnGraphTopKForTest(t, rows, query, opts.TopK), true)
	assertSearchVectorIndexIncludeDocumentsStats2361(t, withDocs.Stats, len(withDocs.Results))

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	defer func() { _ = view.Close() }()
	splitFetch, err := view.FetchDocumentsForVectorIndexSearchResults(noDocs.Results, DocumentFetchOptions{})
	if err != nil {
		t.Fatalf("FetchDocumentsForVectorIndexSearchResults: %v", err)
	}
	if splitFetch.Stats.DocumentsFetched != uint64(len(noDocs.Results)) || splitFetch.Stats.DocumentBytes == 0 || splitFetch.Stats.OutputBytes == 0 {
		t.Fatalf("split fetch stats=%+v want separate document materialization counters", splitFetch.Stats)
	}
	if noDocs.Stats.DocumentsFetched != 0 || noDocs.Stats.DocumentBytes != 0 || noDocs.Stats.DocumentOutputBytes != 0 {
		t.Fatalf("no-doc search stats changed after split fetch: %+v", noDocs.Stats)
	}
	if len(splitFetch.Results) != len(withDocs.Results) {
		t.Fatalf("split fetch results=%d with-docs=%d", len(splitFetch.Results), len(withDocs.Results))
	}
	for i := range splitFetch.Results {
		if !splitFetch.Results[i].Found {
			t.Fatalf("split fetch result[%d] missing for id %q", i, noDocs.Results[i].ID)
		}
		if !bytes.Equal(splitFetch.Results[i].ID, withDocs.Results[i].ID) || !bytes.Equal(noDocs.Results[i].ID, withDocs.Results[i].ID) {
			t.Fatalf("result[%d] IDs no-doc=%q split=%q with-docs=%q want same top-k order", i, noDocs.Results[i].ID, splitFetch.Results[i].ID, withDocs.Results[i].ID)
		}
		if !bytes.Equal(splitFetch.Results[i].Document, withDocs.Results[i].Document) {
			t.Fatalf("result[%d] split document=%q with-docs=%q want same materialized document", i, splitFetch.Results[i].Document, withDocs.Results[i].Document)
		}
		assertVectorIndexSearchDocumentDIDV4(t, splitFetch.Results[i].Document, string(noDocs.Results[i].ID))
	}
}

func TestVectorIndexSearcherSearchWithBufferHighQPSContractRejectsDocuments2361(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	valid, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{
		Query:     []float32{1, 0, 0},
		QueryMode: VectorIndexQueryModeExact,
		TopK:      1,
		EfSearch:  len(rows),
	}, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer no-doc: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, valid, def.Name, 1)
	if valid.Stats.SearchRouteHNSWSearchPack != 1 ||
		valid.Stats.HNSWSearchPackActive != 1 ||
		valid.Stats.DocumentsFetched != 0 ||
		valid.Stats.GraphRowFallbacks != 0 ||
		valid.Stats.TypedColumnFallbacks != 0 ||
		valid.Stats.VectorScratchDecodes != 0 {
		t.Fatalf("SearchWithBuffer stats=%+v want exact no-document hnsw_search_pack_v1 route without docs/fallback/decode", valid.Stats)
	}
	if len(valid.Results[0].Document) != 0 || len(buffer.results) != 1 || len(buffer.idBytes) == 0 {
		t.Fatalf("SearchWithBuffer result=%+v bufferResults=%d idBytes=%d want no-doc buffered result", valid.Results[0], len(buffer.results), len(buffer.idBytes))
	}

	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	}, &buffer)
	if err == nil || !strings.Contains(err.Error(), "IncludeDocuments") {
		t.Fatalf("SearchWithBuffer IncludeDocuments err=%v want documented fail-closed no-document boundary", err)
	}
	if len(got.Results) != 0 || got.Stats.DocumentsFetched != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("IncludeDocuments error response=%+v bufferResults=%d idBytes=%d want no results/doc counters", got, len(buffer.results), len(buffer.idBytes))
	}
}

func TestSearchVectorIndexWithBufferMatchesSearcherSearchWithBuffer2362(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	query := []float32{0.6, 0.4, 0}
	searcherOpts := VectorIndexSearcherSearchOptions{Query: query, TopK: 3, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeFullDiagnostics}
	var searcherBuffer VectorIndexSearchBuffer
	want, err := searcher.SearchWithBuffer(searcherOpts, &searcherBuffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, want, def.Name, searcherOpts.TopK)

	collectionOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, QueryMode: VectorIndexQueryModeExact, TopK: searcherOpts.TopK, EfSearch: searcherOpts.EfSearch, MaxDecodedBlocks: 1, StatsMode: searcherOpts.StatsMode}
	var collectionBuffer VectorIndexSearchBuffer
	got, err := col.SearchVectorIndexWithBuffer(collectionOpts, &collectionBuffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, collectionOpts.TopK)
	assertVectorIndexSearchResponsesEquivalentNoDocs2124(t, got, want)
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, got.Stats)
	if len(collectionBuffer.results) != len(got.Results) || len(collectionBuffer.idBytes) == 0 {
		t.Fatalf("buffer results=%d idBytes=%d want populated caller-owned result storage", len(collectionBuffer.results), len(collectionBuffer.idBytes))
	}
	if len(got.Results) == 0 || &got.Results[0] != &collectionBuffer.results[0] {
		t.Fatalf("response results do not alias caller-owned result buffer")
	}
	if len(got.Results[0].ID) == 0 || &got.Results[0].ID[0] != &collectionBuffer.idBytes[0] {
		t.Fatalf("response IDs do not alias caller-owned ID byte buffer")
	}
	for i, result := range got.Results {
		if len(result.Document) != 0 || cap(result.ID) != len(result.ID) {
			t.Fatalf("result[%d]=%+v id len/cap=%d/%d want no documents and cap-isolated ID bytes", i, result, len(result.ID), cap(result.ID))
		}
	}
}

func TestSearchVectorIndexWithBufferReuseGrowShrinkNoStaleIDs2362(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-long-a", vector: []float32{1, 0, 0}},
		{id: "b", vector: []float32{0, 1, 0}},
		{id: "cc", vector: []float32{0, 0, 1}},
		{id: "dddd", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	var buffer VectorIndexSearchBuffer
	growOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1}
	for i := 0; i < 3; i++ {
		got, err := col.SearchVectorIndexWithBuffer(growOpts, &buffer)
		if err != nil {
			t.Fatalf("SearchVectorIndexWithBuffer grow iteration %d: %v", i, err)
		}
		assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 3)
		assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, growOpts.Query, growOpts.TopK), false)
		assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, got.Stats)
		for j, result := range got.Results {
			if cap(result.ID) != len(result.ID) {
				t.Fatalf("grow iteration %d result[%d] id len/cap=%d/%d want cap isolated", i, j, len(result.ID), cap(result.ID))
			}
		}
	}

	if len(buffer.results) != 3 {
		t.Fatalf("test setup buffer results=%d want 3 before shrink", len(buffer.results))
	}
	buffer.results[1].Document = []byte("stale-document")
	buffer.results[2].Score = 99

	shrinkOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}
	shrunk, err := col.SearchVectorIndexWithBuffer(shrinkOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer shrink: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, shrunk, def.Name, 1)
	assertVectorIndexSearchResultsV4(t, shrunk.Results, exactColumnGraphTopKForTest(t, rows, shrinkOpts.Query, shrinkOpts.TopK), false)
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, shrunk.Stats)
	if gotID := string(shrunk.Results[0].ID); gotID != "b" {
		t.Fatalf("shrunk top id=%q want b without stale bytes from longer prior IDs", gotID)
	}
	if cap(shrunk.Results[0].ID) != len(shrunk.Results[0].ID) {
		t.Fatalf("shrunk id len/cap=%d/%d want cap isolated", len(shrunk.Results[0].ID), cap(shrunk.Results[0].ID))
	}
	allResults := buffer.results[:cap(buffer.results)]
	for i := len(buffer.results); i < 3 && i < len(allResults); i++ {
		if allResults[i].ID != nil || allResults[i].Ordinal != 0 || allResults[i].Score != 0 || allResults[i].Document != nil {
			t.Fatalf("shrunk stale tail result[%d]=%+v want cleared", i, allResults[i])
		}
	}

	regrown, err := col.SearchVectorIndexWithBuffer(growOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer regrow: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, regrown, def.Name, 3)
	assertVectorIndexSearchResultsV4(t, regrown.Results, exactColumnGraphTopKForTest(t, rows, growOpts.Query, growOpts.TopK), false)
}

func TestSearchVectorIndexWithBufferOpenScopedAllocationContract2362(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0.6, 0.4, 0}, QueryMode: VectorIndexQueryModeExact, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}

	var buffer VectorIndexSearchBuffer
	warm, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, warm, def.Name, opts.TopK)
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, warm.Stats)
	resultCap, idCap := cap(buffer.results), cap(buffer.idBytes)
	if resultCap < opts.TopK || idCap == 0 {
		t.Fatalf("warm buffer caps results=%d idBytes=%d want reusable capacity", resultCap, idCap)
	}

	var sink int
	bufferedAllocs := testing.AllocsPerRun(100, func() {
		got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
		if err != nil {
			panic(err)
		}
		if len(got.Results) != opts.TopK {
			panic("unexpected SearchVectorIndexWithBuffer result count")
		}
		if cap(buffer.results) != resultCap || cap(buffer.idBytes) != idCap {
			panic("SearchVectorIndexWithBuffer grew caller buffer after warmup")
		}
		sink += len(got.Results) + got.Results[0].Ordinal
	})
	ownedAllocs := testing.AllocsPerRun(100, func() {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			panic(err)
		}
		if len(got.Results) != opts.TopK {
			panic("unexpected SearchVectorIndex result count")
		}
		sink += len(got.Results) + got.Results[0].Ordinal
	})
	if sink == 0 {
		t.Fatal("allocation check did not consume results")
	}
	if bufferedAllocs != 0 {
		t.Fatalf("SearchVectorIndexWithBuffer warmed-cache allocs=%v want 0; response-owned SearchVectorIndex allocs=%v", bufferedAllocs, ownedAllocs)
	}
}

func TestSearchVectorIndexWithBufferParallelIndependentBuffers2362(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{1, 0, 0}
	topK := 2
	want := exactColumnGraphTopKForTest(t, rows, query, topK)
	const workers = 4
	const iterations = 10
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var buffer VectorIndexSearchBuffer
			opts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: topK, EfSearch: len(rows), MaxDecodedBlocks: 1}
			for i := 0; i < iterations; i++ {
				got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
				if err != nil {
					errs <- fmt.Sprintf("worker %d iteration %d SearchVectorIndexWithBuffer: %v", worker, i, err)
					return
				}
				if len(got.Results) != len(want) {
					errs <- fmt.Sprintf("worker %d iteration %d results=%d want %d", worker, i, len(got.Results), len(want))
					return
				}
				if !vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(got.Stats) {
					errs <- fmt.Sprintf("worker %d iteration %d stats=%+v want hnsw_search_pack_v1 no-document route", worker, i, got.Stats)
					return
				}
				for j := range want {
					if !bytes.Equal(got.Results[j].ID, want[j].ID) || math.Abs(got.Results[j].Score-want[j].Score) > 1e-6 {
						errs <- fmt.Sprintf("worker %d iteration %d result[%d]=%+v want id=%q score=%v", worker, i, j, got.Results[j], want[j].ID, want[j].Score)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestSearchVectorIndexWithBufferPreparedCacheReusesState2363(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	warm, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, warm, def.Name, opts.TopK)
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, warm.Stats)
	snap := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if snap.Entries != 1 || snap.BuildingEntries != 0 || snap.CacheBuilds != 1 || snap.CacheMisses != 1 || snap.ActiveHandles != 1 {
		t.Fatalf("cache snapshot after warm=%+v want one built active prepared state", snap)
	}
	for i := 0; i < 5; i++ {
		got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
		if err != nil {
			t.Fatalf("cached SearchVectorIndexWithBuffer iteration %d: %v", i, err)
		}
		assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, got.Stats)
		if got.Stats.HNSWSearchPackOpenNanos == 0 {
			t.Fatalf("cached stats=%+v want cached open-time route stats retained", got.Stats)
		}
	}
	after := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if after.Entries != 1 || after.CacheBuilds != 1 || after.CacheMisses != 1 || after.CacheHits < 5 || after.ActiveHandles != 1 {
		t.Fatalf("cache snapshot after reuse=%+v want hits without rebuild", after)
	}
}

func TestSearchVectorIndexWithBufferPreparedCacheKeepsWarmStateOnQueryError2363(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}
	var buffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	before := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if before.Entries != 1 || before.CacheBuilds != 1 || before.ActiveHandles != 1 {
		t.Fatalf("cache before bad query=%+v want one warmed entry", before)
	}

	badOpts := opts
	badOpts.Query = []float32{1, 0}
	bad, err := col.SearchVectorIndexWithBuffer(badOpts, &buffer)
	if !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) || !strings.Contains(err.Error(), "hnsw_search_pack_v1 query dims=2 want 3") {
		t.Fatalf("bad query response=%+v err=%v want hnsw_search_pack_v1 query dimension mismatch", bad, err)
	}
	if len(bad.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("bad query left results response=%d bufferResults=%d idBytes=%d", len(bad.Results), len(buffer.results), len(buffer.idBytes))
	}
	afterBad := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterBad.Entries != 1 || afterBad.CacheBuilds != before.CacheBuilds || afterBad.Invalidations != before.Invalidations || afterBad.ActiveHandles != 1 {
		t.Fatalf("cache after bad query=%+v before=%+v want warm cache retained", afterBad, before)
	}
	if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
		t.Fatalf("valid SearchVectorIndexWithBuffer after bad query: %v", err)
	}
	afterValid := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterValid.CacheBuilds != before.CacheBuilds || afterValid.CacheHits <= afterBad.CacheHits {
		t.Fatalf("cache after valid retry=%+v afterBad=%+v want hit without rebuild", afterValid, afterBad)
	}
}

func TestSearchVectorIndexWithBufferPreparedCacheInvalidatesOnMutationAndRefreshesAfterRebuild2363(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex initial: %v", err)
	}
	var buffer VectorIndexSearchBuffer
	warmOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}
	warm, err := col.SearchVectorIndexWithBuffer(warmOpts, &buffer)
	if err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, warm.Stats)
	before := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if before.Entries != 1 || before.ActiveHandles != 1 || before.CacheBuilds != 1 {
		t.Fatalf("cache before mutation=%+v want one active entry", before)
	}

	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{{id: "doc-c", vector: []float32{0, 0, 1}}})
	staleOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 0, 1}, TopK: 1, EfSearch: len(rows) + 1, MaxDecodedBlocks: 1}
	stale, err := col.SearchVectorIndexWithBuffer(staleOpts, &buffer)
	if !errors.Is(err, ErrIndexNotFound) || !strings.Contains(err.Error(), "SearchVectorIndexWithBuffer requires a declared vector index") {
		t.Fatalf("stale SearchVectorIndexWithBuffer response=%+v err=%v want fail-closed declared-index error", stale, err)
	}
	if len(stale.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("stale search left results response=%d bufferResults=%d idBytes=%d", len(stale.Results), len(buffer.results), len(buffer.idBytes))
	}
	afterStale := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterStale.ActiveHandles != 0 || afterStale.CacheBuilds < 2 || afterStale.Invalidations == 0 || afterStale.Errors == 0 {
		t.Fatalf("cache after stale mutation=%+v want old entry invalidated and failed refresh recorded", afterStale)
	}

	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex refresh: %v", err)
	}
	refreshed, err := col.SearchVectorIndexWithBuffer(staleOpts, &buffer)
	if err != nil {
		t.Fatalf("refreshed SearchVectorIndexWithBuffer: %v", err)
	}
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, refreshed.Stats)
	if len(refreshed.Results) == 0 || string(refreshed.Results[0].ID) != "doc-c" {
		t.Fatalf("refreshed top result=%+v want doc-c", refreshed.Results)
	}
	afterRefresh := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if afterRefresh.Entries != 1 || afterRefresh.ActiveHandles != 1 || afterRefresh.CacheBuilds < 3 {
		t.Fatalf("cache after rebuild refresh=%+v want one refreshed active entry", afterRefresh)
	}
}

func TestSearchVectorIndexWithBufferPreparedCacheWaitsForBuildingEntryAcrossCommitChange2363(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}, {id: "doc-b", vector: []float32{0, 1, 0}}}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}

	started := make(chan struct{})
	release := make(chan struct{})
	var hookOnce sync.Once
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
	collectionVectorIndexPreparedSearchBuildHookForTest.fn = func(indexName string) {
		if indexName != def.Name {
			return
		}
		hookOnce.Do(func() {
			close(started)
			<-release
		})
	}
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	defer func() {
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
		collectionVectorIndexPreparedSearchBuildHookForTest.fn = nil
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	}()

	runSearch := func(done chan<- error) {
		var buffer VectorIndexSearchBuffer
		got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
		if err != nil {
			done <- err
			return
		}
		if len(got.Results) != 1 || string(got.Results[0].ID) != "doc-a" {
			done <- fmt.Errorf("results=%+v want doc-a", got.Results)
			return
		}
		done <- nil
	}

	done := make(chan error, 3)
	go runSearch(done)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first cache build to start")
	}
	go runSearch(done)
	waitForCollectionVectorIndexPreparedSearchWaits2363(t, col, 1)

	if _, err := NewCollectionManager(d).CreateCollection(&CollectionMeta{Name: "other"}); err != nil {
		t.Fatalf("CreateCollection other: %v", err)
	}
	go runSearch(done)
	waitForCollectionVectorIndexPreparedSearchWaits2363(t, col, 2)
	close(release)

	for i := 0; i < 3; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("concurrent search %d: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for concurrent search %d; building cache entry may not have signaled waiters", i)
		}
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 1 || snap.BuildingEntries != 0 || snap.CacheWaits < 2 || snap.ActiveHandles != 1 {
		t.Fatalf("cache snapshot=%+v want one ready entry and two recorded waits", snap)
	}
}

func waitForCollectionVectorIndexPreparedSearchWaits2363(tb testing.TB, col *Collection, waits uint64) {
	tb.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.CacheWaits >= waits {
			return
		}
		time.Sleep(time.Millisecond)
	}
	tb.Fatalf("timed out waiting for collection prepared-search cache waits >= %d", waits)
}

func TestSearchVectorIndexWithBufferPreparedCacheDoesNotPublishDuringClose2363(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}, {id: "doc-b", vector: []float32{0, 1, 0}}}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}

	started := make(chan struct{})
	release := make(chan struct{})
	var hookOnce sync.Once
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
	collectionVectorIndexPreparedSearchBuildHookForTest.fn = func(indexName string) {
		if indexName != def.Name {
			return
		}
		hookOnce.Do(func() {
			close(started)
			<-release
		})
	}
	collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	defer func() {
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Lock()
		collectionVectorIndexPreparedSearchBuildHookForTest.fn = nil
		collectionVectorIndexPreparedSearchBuildHookForTest.mu.Unlock()
	}()

	searchDone := make(chan error, 1)
	go func() {
		var buffer VectorIndexSearchBuffer
		_, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
		searchDone <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		_ = d.Close()
		t.Fatal("timed out waiting for cache build to start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && (col.manager == nil || !col.manager.isClosing()) {
		time.Sleep(time.Millisecond)
	}
	if col.manager == nil || !col.manager.isClosing() {
		close(release)
		t.Fatal("timed out waiting for manager close to start")
	}
	close(release)
	select {
	case err := <-searchDone:
		if !errors.Is(err, backenddb.ErrClosed) {
			t.Fatalf("SearchVectorIndexWithBuffer during close err=%v want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for search during close")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("DB Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DB close")
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 {
		t.Fatalf("cache after close race=%+v want no published prepared state", snap)
	}
}

func TestSearchVectorIndexWithBufferPreparedCacheCloseReleasesHandles2363(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}, {id: "doc-b", vector: []float32{0, 1, 0}}}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}
	var buffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
		_ = d.Close()
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 1 || snap.ActiveHandles != 1 {
		_ = d.Close()
		t.Fatalf("cache before close=%+v want active handle", snap)
	}
	if err := col.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		_ = d.Close()
		t.Fatalf("close collection cache: %v", err)
	}
	if err := col.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		_ = d.Close()
		t.Fatalf("second close collection cache: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 {
		_ = d.Close()
		t.Fatalf("cache after collection close=%+v want released", snap)
	}
	if _, err := col.SearchVectorIndexWithBuffer(opts, &buffer); err != nil {
		_ = d.Close()
		t.Fatalf("SearchVectorIndexWithBuffer after collection cache close: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 1 || snap.ActiveHandles != 1 {
		_ = d.Close()
		t.Fatalf("cache before manager flush=%+v want rebuilt active handle", snap)
	}
	if col.manager == nil {
		_ = d.Close()
		t.Fatal("test setup missing collection manager")
	}
	if err := col.manager.FlushAll(); err != nil {
		_ = d.Close()
		t.Fatalf("manager FlushAll with active prepared cache: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 1 || snap.ActiveHandles != 1 {
		_ = d.Close()
		t.Fatalf("cache after manager flush=%+v want still registered active handle", snap)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("DB Close: %v", err)
	}
	if snap := col.collectionVectorIndexPreparedSearchCacheSnapshot(); snap.Entries != 0 || snap.ActiveHandles != 0 {
		t.Fatalf("cache after DB close=%+v want released by manager close hook", snap)
	}
	if err := col.closeCollectionVectorIndexPreparedSearchCache(); err != nil {
		t.Fatalf("cache close after DB close: %v", err)
	}
}

func TestSearchVectorIndexWithBufferPreparedCacheConcurrentSharedState2363(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{1, 0, 0}
	topK := 2
	want := exactColumnGraphTopKForTest(t, rows, query, topK)
	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: topK, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var warmBuffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(opts, &warmBuffer); err != nil {
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	const workers = 4
	const iterations = 20
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var buffer VectorIndexSearchBuffer
			for i := 0; i < iterations; i++ {
				got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
				if err != nil {
					errs <- fmt.Sprintf("worker %d iteration %d SearchVectorIndexWithBuffer: %v", worker, i, err)
					return
				}
				if !vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(got.Stats) {
					errs <- fmt.Sprintf("worker %d iteration %d stats=%+v want pack route", worker, i, got.Stats)
					return
				}
				if len(got.Results) != len(want) || len(buffer.results) != len(want) || len(buffer.idBytes) == 0 {
					errs <- fmt.Sprintf("worker %d iteration %d result/buffer lens got=%d/%d/%d want %d", worker, i, len(got.Results), len(buffer.results), len(buffer.idBytes), len(want))
					return
				}
				for j := range want {
					if !bytes.Equal(got.Results[j].ID, want[j].ID) || math.Abs(got.Results[j].Score-want[j].Score) > 1e-6 {
						errs <- fmt.Sprintf("worker %d iteration %d result[%d]=%+v want id=%q score=%v", worker, i, j, got.Results[j], want[j].ID, want[j].Score)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	snap := col.collectionVectorIndexPreparedSearchCacheSnapshot()
	if snap.Entries != 1 || snap.CacheBuilds != 1 || snap.CacheHits < workers*iterations || snap.ActiveHandles != 1 {
		t.Fatalf("cache snapshot after concurrent search=%+v want one shared prepared state", snap)
	}
}

func TestSearchVectorIndexWithBufferMissingIndexStateFailsClosed2408(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	base := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}
	buffer := VectorIndexSearchBuffer{
		results: []VectorIndexSearchResult{{ID: []byte("stale"), Score: 1}},
		idBytes: []byte("stale"),
	}

	got, err := col.SearchVectorIndexWithBuffer(base, &buffer)
	if !errors.Is(err, ErrIndexNotFound) || !strings.Contains(err.Error(), "SearchVectorIndexWithBuffer requires a declared vector index") || strings.Contains(err.Error(), "manifest") {
		t.Fatalf("SearchVectorIndexWithBuffer missing state response=%+v err=%v want stable fail-closed declared-index error", got, err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("missing state left results: returned=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}
}

func TestSearchVectorIndexWithBufferClosedDBFailsClosed2408(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	base := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}
	var buffer VectorIndexSearchBuffer
	if _, err := col.SearchVectorIndexWithBuffer(base, &buffer); err != nil {
		_ = d.Close()
		t.Fatalf("warm SearchVectorIndexWithBuffer: %v", err)
	}
	if len(buffer.results) == 0 || len(buffer.idBytes) == 0 {
		_ = d.Close()
		t.Fatalf("warm buffer results=%d idBytes=%d want populated before closed DB case", len(buffer.results), len(buffer.idBytes))
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := col.SearchVectorIndexWithBuffer(base, &buffer)
	if !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("SearchVectorIndexWithBuffer closed DB response=%+v err=%v want ErrClosed", got, err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("closed DB left results: returned=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}
}

func TestSearchVectorIndexWithBufferUnsupportedShapesFailClosed2362(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	base := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1}
	var buffer VectorIndexSearchBuffer
	valid, err := col.SearchVectorIndexWithBuffer(base, &buffer)
	if err != nil {
		t.Fatalf("valid SearchVectorIndexWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, valid, def.Name, 1)
	if len(buffer.results) == 0 || len(buffer.idBytes) == 0 {
		t.Fatalf("test setup buffer results=%d idBytes=%d want populated before unsupported cases", len(buffer.results), len(buffer.idBytes))
	}

	if _, err := col.SearchVectorIndexWithBuffer(base, nil); err == nil || !strings.Contains(err.Error(), "nil vector index search buffer") {
		t.Fatalf("nil buffer err=%v want fail closed", err)
	}
	var nilCollection *Collection
	gotNilCollection, err := nilCollection.SearchVectorIndexWithBuffer(base, &buffer)
	if !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil collection err=%v want errCollectionNil", err)
	}
	if len(gotNilCollection.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("nil collection left results: returned=%d bufferResults=%d idBytes=%d", len(gotNilCollection.Results), len(buffer.results), len(buffer.idBytes))
	}
	if _, err := col.SearchVectorIndexWithBuffer(base, &buffer); err != nil {
		t.Fatalf("valid SearchVectorIndexWithBuffer after nil collection: %v", err)
	}

	tests := []struct {
		name            string
		mutate          func(*VectorIndexSearchOptions)
		wantErrs        []string
		wantUnavailable bool
	}{
		{name: "include_documents", mutate: func(opts *VectorIndexSearchOptions) { opts.IncludeDocuments = true }, wantErrs: []string{"SearchVectorIndexWithBuffer", "IncludeDocuments=true", "no-document", "SearchVectorIndex with IncludeDocuments=true"}, wantUnavailable: true},
		{name: "document_include_projection", mutate: func(opts *VectorIndexSearchOptions) { opts.DocumentFetchOptions.IncludePaths = []string{"did"} }, wantErrs: []string{"SearchVectorIndexWithBuffer", "DocumentFetchOptions.IncludePaths", "projection", "IncludeDocuments=true"}, wantUnavailable: true},
		{name: "document_exclude_projection", mutate: func(opts *VectorIndexSearchOptions) { opts.DocumentFetchOptions.ExcludePaths = []string{"embedding"} }, wantErrs: []string{"SearchVectorIndexWithBuffer", "DocumentFetchOptions.ExcludePaths", "projection", "IncludeDocuments=true"}, wantUnavailable: true},
		{name: "document_format", mutate: func(opts *VectorIndexSearchOptions) { opts.DocumentFetchOptions.Format = DocumentFormatJSON }, wantErrs: []string{"SearchVectorIndexWithBuffer", "DocumentFetchOptions.Format", "materialization"}, wantUnavailable: true},
		{name: "document_integrity_option", mutate: func(opts *VectorIndexSearchOptions) {
			opts.DocumentFetchOptions.ColumnAssetReadIntegrity = ColumnAssetReadIntegritySkipChecksums
		}, wantErrs: []string{"SearchVectorIndexWithBuffer", "DocumentFetchOptions.ColumnAssetReadIntegrity", "materialization"}, wantUnavailable: true},
		{name: "filter", mutate: func(opts *VectorIndexSearchOptions) {
			opts.Filter = func(DocumentRecord) (bool, error) { return true, nil }
		}, wantErrs: []string{"SearchVectorIndexWithBuffer", "Filter", "hnsw_search_pack_v1"}, wantUnavailable: true},
		{name: "range_filter", mutate: func(opts *VectorIndexSearchOptions) {
			opts.IndexRangeFilter = &VectorIndexRangeFilter{IndexName: "kind"}
		}, wantErrs: []string{"SearchVectorIndexWithBuffer", "IndexRangeFilter", "hnsw_search_pack_v1"}, wantUnavailable: true},
		{name: "fetch_multiplier", mutate: func(opts *VectorIndexSearchOptions) { opts.FetchMultiplier = 2 }, wantErrs: []string{"SearchVectorIndexWithBuffer", "FetchMultiplier", "pack-only"}, wantUnavailable: true},
		{name: "exact_filter_max_docs", mutate: func(opts *VectorIndexSearchOptions) { opts.ExactFilterMaxDocs = 32 }, wantErrs: []string{"SearchVectorIndexWithBuffer", "ExactFilterMaxDocs", "pack-only"}, wantUnavailable: true},
		{name: "disable_exact_fallback", mutate: func(opts *VectorIndexSearchOptions) { opts.DisableExactFallback = true }, wantErrs: []string{"SearchVectorIndexWithBuffer", "DisableExactFallback", "fails closed"}, wantUnavailable: true},
		{name: "unknown_query_mode", mutate: func(opts *VectorIndexSearchOptions) { opts.QueryMode = VectorIndexQueryMode("future_mode") }, wantErrs: []string{"SearchVectorIndexWithBuffer", "QueryMode=\"future_mode\"", "exact/zero"}, wantUnavailable: true},
		{name: "quantized_index_name_option", mutate: func(opts *VectorIndexSearchOptions) { opts.QuantizedIndexName = "embedding.scalar_u8.fast" }, wantErrs: []string{"SearchVectorIndexWithBuffer", "QuantizedIndexName", "quantized"}, wantUnavailable: true},
		{name: "quantized_rerank_candidates_option", mutate: func(opts *VectorIndexSearchOptions) { opts.QuantizedRerankCandidates = 8 }, wantErrs: []string{"SearchVectorIndexWithBuffer", "QuantizedRerankCandidates", "quantized"}, wantUnavailable: true},
		{name: "benchmark_debug", mutate: func(opts *VectorIndexSearchOptions) { opts.StatsMode = VectorIndexSearchStatsModeBenchmarkDebug }, wantErrs: []string{"SearchVectorIndexWithBuffer", "StatsMode=benchmark_debug", "debug-only"}, wantUnavailable: true},
		{name: "unknown_stats_mode", mutate: func(opts *VectorIndexSearchOptions) { opts.StatsMode = VectorIndexSearchStatsMode("debug_everything") }, wantErrs: []string{"SearchVectorIndexWithBuffer", "StatsMode=\"debug_everything\"", "unsupported"}, wantUnavailable: true},
		{name: "negative_max_decoded_blocks", mutate: func(opts *VectorIndexSearchOptions) { opts.MaxDecodedBlocks = -1 }, wantErrs: []string{"max_decoded_blocks", "cannot be negative"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := base
			tt.mutate(&opts)
			got, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
			if err == nil {
				t.Fatalf("SearchVectorIndexWithBuffer err=nil want fail-closed error containing %v", tt.wantErrs)
			}
			for _, want := range tt.wantErrs {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("SearchVectorIndexWithBuffer err=%v want substring %q", err, want)
				}
			}
			if tt.wantUnavailable && !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				t.Fatalf("SearchVectorIndexWithBuffer err=%v want ErrVectorIndexSearchUnavailable", err)
			}
			if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
				t.Fatalf("unsupported shape left results: returned=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
			}
			if _, err := col.SearchVectorIndexWithBuffer(base, &buffer); err != nil {
				t.Fatalf("valid SearchVectorIndexWithBuffer after %s: %v", tt.name, err)
			}
		})
	}
}

func TestResetBufferedVectorIndexSearchResponseClearsReturnedResults2362(t *testing.T) {
	var buffer VectorIndexSearchBuffer
	buffer.results = append(buffer.results, VectorIndexSearchResult{ID: []byte("stale"), Score: 1})
	buffer.idBytes = append(buffer.idBytes, "stale"...)
	response := VectorIndexSearchResponse{Results: buffer.results}

	resetBufferedVectorIndexSearchResponse(&response, &buffer)

	if response.Results != nil {
		t.Fatalf("response results=%v want nil after buffered error invalidation", response.Results)
	}
	if len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("buffer results=%d idBytes=%d want reset view", len(buffer.results), len(buffer.idBytes))
	}
}

func TestVectorIndexSearchDiagnosticsExactPackRoute2407(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	opts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	var buffer VectorIndexSearchBuffer
	first, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("first SearchVectorIndexWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, first, def.Name, opts.TopK)
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, first.Stats)
	assertVectorIndexSearchDiagnostics2407(t, first.Diagnostics(), VectorIndexSearchRouteExactHNSWSearchPackV1, VectorIndexSearchHNSWSearchPackStatusActive, VectorIndexSearchFallbackReasonNone, true, true)
	if first.Stats.HNSWSearchPackCacheMisses != 1 || first.Stats.HNSWSearchPackCacheBuilds != 1 || first.Stats.HNSWSearchPackCacheHits != 0 || first.Stats.HNSWSearchPackCacheWaits != 0 {
		t.Fatalf("first cache stats=%+v want one exact-pack cache miss/build", first.Stats)
	}
	if first.Stats.OpenSearcherCalls != 0 || first.Stats.OpenSetupInTimedLoop != 0 || first.Stats.ResponseOwnedResultAllocs != 0 {
		t.Fatalf("first boundary stats=%+v want buffered route with no one-shot open or response-owned allocation signal", first.Stats)
	}

	second, err := col.SearchVectorIndexWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("second SearchVectorIndexWithBuffer: %v", err)
	}
	assertSearchVectorIndexWithBufferNoDocumentPackStats2362(t, second.Stats)
	if second.Stats.HNSWSearchPackCacheHits != 1 || second.Stats.HNSWSearchPackCacheMisses != 0 || second.Stats.HNSWSearchPackCacheBuilds != 0 {
		t.Fatalf("second cache stats=%+v want one exact-pack cache hit", second.Stats)
	}

	owned, err := col.SearchVectorIndex(opts)
	if err != nil {
		t.Fatalf("SearchVectorIndex no-doc convenience: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, owned, def.Name, opts.TopK)
	assertVectorIndexSearchDiagnostics2407(t, owned.Diagnostics(), VectorIndexSearchRouteExactHNSWSearchPackV1, VectorIndexSearchHNSWSearchPackStatusActive, VectorIndexSearchFallbackReasonNone, true, true)
	if owned.Stats.ResponseOwnedResultAllocs != 1 || owned.Stats.OpenSearcherCalls != 0 || owned.Stats.OpenSetupInTimedLoop != 0 || owned.Stats.HNSWSearchPackCacheHits != 1 {
		t.Fatalf("owned no-doc stats=%+v want response-owned allocation signal, no one-shot open, and exact-pack cache hit", owned.Stats)
	}

	withDocsOpts := opts
	withDocsOpts.IncludeDocuments = true
	withDocs, err := col.SearchVectorIndex(withDocsOpts)
	if err != nil {
		t.Fatalf("SearchVectorIndex with docs: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, withDocs, def.Name, opts.TopK)
	assertVectorIndexSearchColumnGraphDiagnostics2407(t, withDocs.Diagnostics(), VectorIndexSearchHNSWSearchPackStatusActive, false, VectorIndexSearchFallbackReasonNone, VectorIndexSearchFallbackReasonColumnGraphFallback)
	if withDocs.Stats.OpenSearcherCalls != 1 || withDocs.Stats.OpenSetupInTimedLoop != 1 || withDocs.Stats.ResponseOwnedResultAllocs != 1 || withDocs.Stats.DocumentsFetched != uint64(len(withDocs.Results)) {
		t.Fatalf("with-docs stats=%+v want one-shot open/setup, response-owned result allocation, and document fetch signal", withDocs.Stats)
	}
}

func TestVectorIndexSearchDiagnosticsHNSWPackMissingFallback2407(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if searcher.reader == nil {
		t.Fatal("test setup missing physical row reader")
	}
	if searcher.reader.hnswSearchPack != nil {
		if err := searcher.reader.hnswSearchPack.Close(); err != nil {
			t.Fatalf("close test hnsw pack: %v", err)
		}
		searcher.reader.hnswSearchPack = nil
	}
	searcher.reader.hnswSearchPackStatus = columnHNSWSearchPackPreparedStatusMissing
	searcher.routeStats = vectorIndexSearchRouteStatsForColumnGraphReader(searcher.reader)

	var buffer VectorIndexSearchBuffer
	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer with missing pack fallback: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	assertVectorIndexSearchColumnGraphDiagnostics2407(t, got.Diagnostics(), VectorIndexSearchHNSWSearchPackStatusMissing, true, VectorIndexSearchFallbackReasonHNSWSearchPackMissing)
	if got.Stats.SearchRouteHNSWSearchPack != 0 || got.Stats.HNSWSearchPackFallbacks != 1 || got.Stats.GraphRowFallbacks != 0 || got.Stats.TypedColumnFallbacks != 0 || got.Stats.VectorScratchDecodes != 0 {
		t.Fatalf("fallback stats=%+v want column_graph prepared fallback because exact hnsw pack is missing", got.Stats)
	}
}

func TestVectorIndexSearchDiagnosticsHelperStatuses2407(t *testing.T) {
	tests := []struct {
		name     string
		stats    VectorIndexSearchStats
		status   VectorIndexSearchHNSWSearchPackStatus
		fallback VectorIndexSearchFallbackReason
	}{
		{name: "active", stats: VectorIndexSearchStats{SearchRouteHNSWSearchPack: 1, HNSWSearchPackActive: 1}, status: VectorIndexSearchHNSWSearchPackStatusActive, fallback: VectorIndexSearchFallbackReasonNone},
		{name: "missing", stats: VectorIndexSearchStats{SearchRouteColumnGraphPrepared: 1, HNSWSearchPackMissing: 1, HNSWSearchPackFallbacks: 1}, status: VectorIndexSearchHNSWSearchPackStatusMissing, fallback: VectorIndexSearchFallbackReasonHNSWSearchPackMissing},
		{name: "invalid", stats: VectorIndexSearchStats{HNSWSearchPackInvalid: 1, HNSWSearchPackFallbacks: 1}, status: VectorIndexSearchHNSWSearchPackStatusInvalid, fallback: VectorIndexSearchFallbackReasonHNSWSearchPackInvalid},
		{name: "stale", stats: VectorIndexSearchStats{HNSWSearchPackStale: 1, HNSWSearchPackFallbacks: 1}, status: VectorIndexSearchHNSWSearchPackStatusStale, fallback: VectorIndexSearchFallbackReasonHNSWSearchPackStale},
		{name: "closed", stats: VectorIndexSearchStats{HNSWSearchPackClosed: 1, HNSWSearchPackFallbacks: 1}, status: VectorIndexSearchHNSWSearchPackStatusClosed, fallback: VectorIndexSearchFallbackReasonHNSWSearchPackClosed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diag := tt.stats.Diagnostics()
			if diag.HNSWSearchPackStatus != tt.status || diag.FallbackReason != tt.fallback {
				t.Fatalf("diagnostics=%+v want status=%q fallback=%q", diag, tt.status, tt.fallback)
			}
		})
	}

}

func TestVectorIndexSearchDiagnosticsQuantizedRouteKinds2407(t *testing.T) {
	tests := []struct {
		name  string
		stats VectorIndexSearchStats
		route VectorIndexSearchRouteKind
	}{
		{
			name:  "quantized_only_prioritizes_codec_generic_route",
			stats: VectorIndexSearchStats{SearchRouteColumnGraphPrepared: 1, SearchRouteQuantizedOnly: 1, QuantizedScoreCalls: 3, QuantizedScorerActive: 1},
			route: VectorIndexSearchRouteQuantizedOnly,
		},
		{
			name:  "quantized_rerank_prioritizes_codec_generic_route",
			stats: VectorIndexSearchStats{SearchRouteColumnGraphFallback: 1, SearchRouteQuantizedRerank: 1, QuantizedScoreCalls: 3, QuantizedRerankExactScoreCalls: 2, QuantizedScorerActive: 1},
			route: VectorIndexSearchRouteQuantizedRerank,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diag := tt.stats.Diagnostics()
			if diag.Route != tt.route || diag.ExactHNSWSearchPackNoDocRoute || diag.HNSWSearchPackStatus != VectorIndexSearchHNSWSearchPackStatusNone {
				t.Fatalf("diagnostics=%+v want route=%q without exact hnsw_search_pack_v1 route/status", diag, tt.route)
			}
		})
	}
}

func assertVectorIndexSearchDiagnostics2407(tb testing.TB, got VectorIndexSearchDiagnostics, route VectorIndexSearchRouteKind, packStatus VectorIndexSearchHNSWSearchPackStatus, fallback VectorIndexSearchFallbackReason, noDocOK bool, exactPackNoDoc bool) {
	tb.Helper()
	if got.Route != route || got.HNSWSearchPackStatus != packStatus || got.FallbackReason != fallback || got.NoDocumentGuardrailsOK != noDocOK || got.ExactHNSWSearchPackNoDocRoute != exactPackNoDoc {
		tb.Fatalf("diagnostics=%+v want route=%q pack=%q fallback=%q noDocOK=%v exactPackNoDoc=%v", got, route, packStatus, fallback, noDocOK, exactPackNoDoc)
	}
}

func assertVectorIndexSearchColumnGraphDiagnostics2407(tb testing.TB, got VectorIndexSearchDiagnostics, packStatus VectorIndexSearchHNSWSearchPackStatus, noDocOK bool, fallbackReasons ...VectorIndexSearchFallbackReason) {
	tb.Helper()
	if got.Route != VectorIndexSearchRouteColumnGraphPrepared && got.Route != VectorIndexSearchRouteColumnGraphFallback {
		tb.Fatalf("diagnostics=%+v want column_graph prepared or fallback route", got)
	}
	fallbackOK := len(fallbackReasons) == 0
	for _, want := range fallbackReasons {
		if got.FallbackReason == want {
			fallbackOK = true
			break
		}
	}
	if got.HNSWSearchPackStatus != packStatus || !fallbackOK || got.NoDocumentGuardrailsOK != noDocOK || got.ExactHNSWSearchPackNoDocRoute {
		tb.Fatalf("diagnostics=%+v want column_graph pack=%q fallback in %v noDocOK=%v exactPackNoDoc=false", got, packStatus, fallbackReasons, noDocOK)
	}
}

func assertSearchVectorIndexWithBufferNoDocumentPackStats2362(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if !vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(stats) {
		tb.Fatalf("SearchVectorIndexWithBuffer stats=%+v want exact no-document hnsw_search_pack_v1 route", stats)
	}
	if stats.DocumentBytes != 0 || stats.DocumentOutputBytes != 0 || stats.DocumentRowRefStateFetches != 0 || stats.DocumentRowRefLookupFallbacks != 0 || stats.DocumentPointRowFetches != 0 || stats.DocumentJSONReconstructionRows != 0 {
		tb.Fatalf("SearchVectorIndexWithBuffer document stats=%+v want no materialization", stats)
	}
}

func assertSearchVectorIndexNoDocumentCurrentOneShotStats2361(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.DocumentsFetched != 0 ||
		stats.DocumentBytes != 0 ||
		stats.DocumentOutputBytes != 0 ||
		stats.DocumentRowRefStateFetches != 0 ||
		stats.DocumentRowRefLookupFallbacks != 0 ||
		stats.DocumentPointRowFetches != 0 ||
		stats.DocumentJSONReconstructionRows != 0 {
		tb.Fatalf("no-doc stats=%+v want zero document materialization counters", stats)
	}
	if !vectorIndexSearchStatsAreBufferedNoDocumentPackRoute(stats) {
		tb.Fatalf("no-doc stats=%+v want Collection.SearchVectorIndex exact no-document cached hnsw_search_pack_v1 route", stats)
	}
	assertVectorIndexSearchNoFallbackStats2361(tb, stats)
}

func assertSearchVectorIndexIncludeDocumentsStats2361(tb testing.TB, stats VectorIndexSearchStats, results int) {
	tb.Helper()
	if stats.DocumentsFetched != uint64(results) || stats.DocumentBytes == 0 || stats.DocumentOutputBytes == 0 {
		tb.Fatalf("with-docs stats=%+v want document materialization counters for %d results", stats, results)
	}
	if stats.DocumentRowRefStateFetches != uint64(results) ||
		stats.DocumentRowRefLookupFallbacks != 0 ||
		stats.DocumentPointRowFetches != uint64(results) ||
		stats.DocumentPointRowDecodes != uint64(results) {
		tb.Fatalf("with-docs stats=%+v want vector-index row-ref state point fetches", stats)
	}
	assertSearchVectorIndexCurrentOneShotRoute2361(tb, "with-docs", stats)
	assertVectorIndexSearchNoFallbackStats2361(tb, stats)
}

func assertSearchVectorIndexCurrentOneShotRoute2361(tb testing.TB, label string, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.SearchRouteHNSWSearchPack != 0 {
		tb.Fatalf("%s stats=%+v want current Collection.SearchVectorIndex one-shot route, not high-QPS pack route", label, stats)
	}
	if stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 1 {
		tb.Fatalf("%s stats=%+v want exactly one current Collection.SearchVectorIndex one-shot column_graph route", label, stats)
	}
}

func assertVectorIndexSearchNoFallbackStats2361(tb testing.TB, stats VectorIndexSearchStats) {
	tb.Helper()
	if stats.GraphRows != 0 ||
		stats.GraphRowFallbacks != 0 ||
		stats.TypedColumnFallbacks != 0 ||
		stats.VectorScratchDecodes != 0 ||
		stats.RowFetches != 0 ||
		stats.BatchFetches != 0 ||
		stats.RowsFetched != 0 ||
		stats.ResultIDGraphFallbacks != 0 ||
		stats.RowRefVectorSourceLegacyGraphIDs != 0 {
		tb.Fatalf("stats=%+v want no graph-row fallback, no typed-column fallback, no vector scratch decode, and no legacy row/ID path", stats)
	}
}

func TestSearchVectorIndexColumnGraphResultIDsAreCapacityIsolatedV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{1, 0, 0},
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	if cap(got.Results) != len(got.Results) {
		t.Fatalf("results len/cap=%d/%d want cap isolated", len(got.Results), cap(got.Results))
	}
	for i, result := range got.Results {
		if cap(result.ID) != len(result.ID) {
			t.Fatalf("result[%d] id len/cap=%d/%d want cap isolated", i, len(result.ID), cap(result.ID))
		}
	}
	secondID := append([]byte(nil), got.Results[1].ID...)
	_ = append(got.Results[0].ID, '!')
	if !bytes.Equal(got.Results[1].ID, secondID) {
		t.Fatalf("second result ID changed after appending to first result ID: got %q want %q", got.Results[1].ID, secondID)
	}
}

func TestSearchVectorIndexColumnGraphNoDocumentResponseOwnedAfterReuse2404(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0.9, 0.1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	ownedOpts := VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0, 0}, QueryMode: VectorIndexQueryModeExact, TopK: 3, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}
	owned, err := col.SearchVectorIndex(ownedOpts)
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, owned, def.Name, 3)
	assertSearchVectorIndexNoDocumentCurrentOneShotStats2361(t, owned.Stats)
	if cap(owned.Results) != len(owned.Results) {
		t.Fatalf("owned results len/cap=%d/%d want cap isolated", len(owned.Results), cap(owned.Results))
	}
	ownedIDs := make([][]byte, len(owned.Results))
	ownedOrdinals := make([]int, len(owned.Results))
	ownedScores := make([]float64, len(owned.Results))
	for i, result := range owned.Results {
		if cap(result.ID) != len(result.ID) {
			t.Fatalf("owned result[%d] id len/cap=%d/%d want cap isolated", i, len(result.ID), cap(result.ID))
		}
		ownedIDs[i] = append([]byte(nil), result.ID...)
		ownedOrdinals[i] = result.Ordinal
		ownedScores[i] = result.Score
	}

	if _, err := col.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1, 0}, QueryMode: VectorIndexQueryModeExact, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}); err != nil {
		t.Fatalf("second SearchVectorIndex: %v", err)
	}
	var buffer VectorIndexSearchBuffer
	buffered, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1, 0}, QueryMode: VectorIndexQueryModeExact, TopK: 2, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer: %v", err)
	}
	if len(buffered.Results) > 0 && len(buffered.Results[0].ID) > 0 {
		buffered.Results[0].ID[0] = 'X'
	}
	if _, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 0, 1}, QueryMode: VectorIndexQueryModeExact, TopK: 1, EfSearch: len(rows), MaxDecodedBlocks: 1, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer); err != nil {
		t.Fatalf("second SearchVectorIndexWithBuffer: %v", err)
	}
	for i := range owned.Results {
		if !bytes.Equal(owned.Results[i].ID, ownedIDs[i]) || owned.Results[i].Ordinal != ownedOrdinals[i] || owned.Results[i].Score != ownedScores[i] {
			t.Fatalf("owned result[%d]=%+v changed after later searches; want ID=%q ordinal=%d score=%v", i, owned.Results[i], ownedIDs[i], ownedOrdinals[i], ownedScores[i])
		}
	}
}

func TestSearchVectorIndexByteAccountingRejectsOverflowV4(t *testing.T) {
	total, err := addVectorIndexSearchByteTotal(3, 4, 10, "document")
	if err != nil || total != 7 {
		t.Fatalf("addVectorIndexSearchByteTotal=%d, %v want 7, nil", total, err)
	}
	if _, err := addVectorIndexSearchByteTotal(math.MaxInt-1, 1, math.MaxInt, "document"); err != nil {
		t.Fatalf("max int edge add failed: %v", err)
	}
	if _, err := addVectorIndexSearchByteTotal(8, 3, 10, "document"); err == nil {
		t.Fatalf("addVectorIndexSearchByteTotal overflow err=nil want failure")
	}
	if total, err := multiplyVectorIndexSearchByteTotal(3, 4, 12, "document"); err != nil || total != 12 {
		t.Fatalf("multiplyVectorIndexSearchByteTotal=%d, %v want 12, nil", total, err)
	}
	if _, err := multiplyVectorIndexSearchByteTotal(math.MaxInt, 2, math.MaxInt, "document"); err == nil {
		t.Fatalf("multiplyVectorIndexSearchByteTotal overflow err=nil want failure")
	}
	got, err := vectorIndexSearchResultIDBytesLimit([]columnVectorGraphNativeSearchResult{
		{ID: []byte("abc")},
		{ID: []byte("de")},
	}, 5)
	if err != nil || got != 5 {
		t.Fatalf("vectorIndexSearchResultIDBytesLimit=%d, %v want 5, nil", got, err)
	}
	_, err = vectorIndexSearchResultIDBytesLimit([]columnVectorGraphNativeSearchResult{
		{ID: []byte("abc")},
		{ID: []byte("def")},
	}, 5)
	if err == nil {
		t.Fatalf("vectorIndexSearchResultIDBytesLimit overflow err=nil want failure")
	}
}

func TestSearchVectorIndexColumnGraphMaterializesDocumentsAfterTopKV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             2,
		EfSearch:         len(rows),
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	}
	got, err := col.SearchVectorIndex(opts)
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
	assertVectorIndexSearchDocumentDIDV4(t, got.Results[0].Document, "doc-c")
	if got.Stats.DocumentsFetched != uint64(len(got.Results)) {
		t.Fatalf("DocumentsFetched=%d want %d", got.Stats.DocumentsFetched, len(got.Results))
	}
	if got.Stats.DocumentRowRefStateFetches != uint64(len(got.Results)) || got.Stats.DocumentRowRefLookupFallbacks != 0 || got.Stats.DocumentRowLocatorLookups != uint64(len(got.Results)) || got.Stats.DocumentRowLocatorMisses != 0 || got.Stats.DocumentPointRowFetches != uint64(len(got.Results)) || got.Stats.DocumentPointRowDecodes != uint64(len(got.Results)) {
		t.Fatalf("stats=%+v want vector-index row refs and point row fetch per document", got.Stats)
	}
	if got.Stats.DocumentRowRefFallbackScans != 0 || got.Stats.DocumentVisibilityScans != 0 || got.Stats.DocumentVisibilityRowsScanned != 0 {
		t.Fatalf("stats=%+v want IncludeDocuments to avoid row-ref scan fallback on supported manifest", got.Stats)
	}
	documentBefore := append([]byte(nil), got.Results[0].Document...)
	if _, err := col.SearchVectorIndex(opts); err != nil {
		t.Fatalf("second SearchVectorIndex: %v", err)
	}
	if !bytes.Equal(got.Results[0].Document, documentBefore) {
		t.Fatalf("top result document changed after a later search; want response-owned bytes")
	}
}

func TestSearchVectorIndexColumnGraphProjectedDocuments1875(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	full, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             2,
		EfSearch:         len(rows),
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex full documents: %v", err)
	}
	projectedOpts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             2,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	}
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		t.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	fetchPreset.ApplyToSearchOptions(&projectedOpts)
	projected, err := col.SearchVectorIndex(projectedOpts)
	if err != nil {
		t.Fatalf("SearchVectorIndex projected documents: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, projected, def.Name, 2)
	if projected.Stats.DocumentsFetched != uint64(len(projected.Results)) || projected.Stats.DocumentFieldsSkipped == 0 || projected.Stats.DocumentFieldsReconstructed == 0 {
		t.Fatalf("projected stats=%+v want fetched docs, skipped fields, reconstructed fields", projected.Stats)
	}
	if projected.Stats.DocumentOutputBytes != projected.Stats.DocumentBytes || projected.Stats.DocumentBytes >= full.Stats.DocumentBytes {
		t.Fatalf("projected stats=%+v full stats=%+v want output bytes below full document bytes", projected.Stats, full.Stats)
	}
	for i := range projected.Results {
		var doc map[string]any
		if err := json.Unmarshal(projected.Results[i].Document, &doc); err != nil {
			t.Fatalf("projected document[%d]=%q invalid JSON: %v", i, projected.Results[i].Document, err)
		}
		if _, ok := doc["embedding"]; ok {
			t.Fatalf("projected document[%d]=%s retained embedding", i, projected.Results[i].Document)
		}
		did, didOK := doc["did"].(string)
		kind, kindOK := doc["kind"].(string)
		if !didOK || did == "" || !kindOK || kind != "vector" {
			t.Fatalf("projected document[%d]=%v want selected metadata fields", i, doc)
		}
	}
	secondBefore := append([]byte(nil), projected.Results[1].Document...)
	_ = append(projected.Results[0].Document, '!')
	if !bytes.Equal(projected.Results[1].Document, secondBefore) {
		t.Fatalf("projected response documents share capacity: second=%s want %s", projected.Results[1].Document, secondBefore)
	}
	projected.Results[0].Document[0] = 'X'
	fresh, err := col.Get(projected.Results[0].ID)
	if err != nil {
		t.Fatalf("Get after mutating projected response: %v", err)
	}
	if len(fresh) == 0 || fresh[0] == 'X' {
		t.Fatalf("mutating projected response affected stored document: %s", fresh)
	}
}

func TestVectorIndexSearcherProjectedDocumentsSnapshotBound1875(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if err := col.Delete([]byte("doc-a")); err != nil {
		t.Fatalf("Delete doc-a after opening searcher: %v", err)
	}
	searchOpts := VectorIndexSearcherSearchOptions{
		Query:    []float32{1, 0, 0},
		TopK:     1,
		EfSearch: len(rows),
	}
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		t.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	fetchPreset.ApplyToSearcherSearchOptions(&searchOpts)
	got, err := searcher.Search(searchOpts)
	if err != nil {
		t.Fatalf("Search projected after delete on bound searcher: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0].ID) != "doc-a" {
		t.Fatalf("results=%+v want snapshot-bound doc-a", got.Results)
	}
	var doc map[string]any
	if err := json.Unmarshal(got.Results[0].Document, &doc); err != nil {
		t.Fatalf("projected snapshot document invalid JSON: %v", err)
	}
	if doc["did"] != "doc-a" {
		t.Fatalf("projected snapshot doc=%v want old doc-a", doc)
	}
	if _, ok := doc["embedding"]; ok {
		t.Fatalf("projected snapshot document retained embedding: %s", got.Results[0].Document)
	}
	if got.Stats.DocumentPointRowFetches != 1 || got.Stats.DocumentFieldsSkipped == 0 {
		t.Fatalf("stats=%+v want row-ref point fetch and skipped projection field", got.Stats)
	}
}

func TestSearchVectorIndexProjectionRequiresIncludeDocuments1875(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "doc-a", vector: []float32{1, 0, 0}}}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	_, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:            def.Name,
		Query:                []float32{1, 0, 0},
		TopK:                 1,
		EfSearch:             len(rows),
		DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
	})
	if err == nil || !strings.Contains(err.Error(), "IncludeDocuments") {
		t.Fatalf("SearchVectorIndex projection without IncludeDocuments err=%v want fail closed", err)
	}
}

func TestSearchVectorIndexColumnGraphRetainedFullDocumentsFallbackV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 0)
	cfg := columnGraphRebuildColumnStoreConfigV2A(3)
	cfg.RetainedPayload = ColumnRetainedPayloadFull
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	insertColumnGraphRebuildRowsV2A(t, col, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if len(got.Results) != 1 || string(got.Results[0].ID) != "doc-a" {
		t.Fatalf("results=%+v want doc-a", got.Results)
	}
	assertVectorIndexSearchDocumentDIDV4(t, got.Results[0].Document, "doc-a")
	if got.Stats.DocumentRowRefUnsupported != 1 || got.Stats.DocumentPointRowFetches != 0 || got.Stats.DocumentVisibilityScans != 0 || got.Stats.DocumentsFetched != 1 {
		t.Fatalf("stats=%+v want retained-full fallback without row-ref point fetch", got.Stats)
	}
}

func TestSearchVectorIndexFlushesBufferedWritesBeforeSnapshotV4(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:                          DocumentFormatJSON,
			BufferedIndexedWrites:                   true,
			BufferedIndexedWriteMaxDocuments:        1024,
			DisableBufferedIndexedAsyncFlush:        true,
			BufferedIndexedAsyncFlushMaxQueuedUnits: 0,
		},
		Indexes: []IndexDefinition{{
			Name:      "kind",
			Field:     "kind",
			ValueType: IndexValueString,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("doc-a")},
		[][]byte{[]byte(`{"kind":"vector","embedding":[1,0,0]}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if got := mgr.StatsSnapshot().PendingDocuments; got == 0 {
		t.Fatalf("PendingDocuments=%d want buffered write before search", got)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: "embedding_graph",
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("SearchVectorIndex err=%v want search unavailable", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphUnavailable || got.Status.Reason != VectorIndexReasonPhysicalColumnAssetSupportMissing {
		t.Fatalf("status=%+v want column_graph unavailable response", got.Status)
	}
	if got := mgr.StatsSnapshot().PendingDocuments; got != 0 {
		t.Fatalf("PendingDocuments after SearchVectorIndex=%d want flushed before snapshot", got)
	}
}

func TestOpenVectorIndexSearcherFetchesDocumentsFromBoundSnapshotV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	if err := col.Delete([]byte("doc-a")); err != nil {
		t.Fatalf("Delete doc-a after opening searcher: %v", err)
	}
	if live, err := col.Get([]byte("doc-a")); err != nil || live != nil {
		t.Fatalf("live Get doc-a after delete=%q err=%v want missing", live, err)
	}

	got, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("Search after delete on bound searcher: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	if string(got.Results[0].ID) != "doc-a" {
		t.Fatalf("top result id=%q want doc-a from bound graph snapshot", got.Results[0].ID)
	}
	assertVectorIndexSearchDocumentDIDV4(t, got.Results[0].Document, "doc-a")
	if got.Stats.DocumentsFetched != 1 {
		t.Fatalf("DocumentsFetched=%d want 1", got.Stats.DocumentsFetched)
	}
	if got.Stats.DocumentRowRefStateFetches != 1 || got.Stats.DocumentRowRefLookupFallbacks != 0 || got.Stats.DocumentRowLocatorLookups != 1 || got.Stats.DocumentPointRowFetches != 1 || got.Stats.DocumentPointRowDecodes != 1 || got.Stats.DocumentRowRefFallbackScans != 0 || got.Stats.DocumentVisibilityScans != 0 {
		t.Fatalf("stats=%+v want prepared IncludeDocuments to use vector-index row refs and point fetches", got.Stats)
	}
	if searcher.documentView == nil || searcher.documentView.assetScopeKind != mappedresource.ScopePreparedSearch {
		t.Fatalf("document view=%+v want prepared_search scope", searcher.documentView)
	}
	again, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("second Search after delete on bound searcher: %v", err)
	}
	if !bytes.Equal(again.Results[0].Document, got.Results[0].Document) {
		t.Fatalf("second document=%s want %s", again.Results[0].Document, got.Results[0].Document)
	}
	if again.Stats.DocumentAssetFileOpens != 0 {
		t.Fatalf("second stats=%+v want prepared document read cache reuse without file opens", again.Stats)
	}
}

func TestOpenVectorIndexSearcherReusesNativeReaderV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	opts := VectorIndexSearcherSearchOptions{Query: []float32{0, 0, 1}, TopK: 2, EfSearch: len(rows)}
	first, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("first Search: %v", err)
	}
	second, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("second Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, first, def.Name, 2)
	assertColumnGraphSearchResponseLoadedV4(t, second, def.Name, 2)
	if len(first.Results) != len(second.Results) {
		t.Fatalf("second results=%d want %d", len(second.Results), len(first.Results))
	}
	for i := range first.Results {
		if !bytes.Equal(first.Results[i].ID, second.Results[i].ID) || first.Results[i].Ordinal != second.Results[i].Ordinal || first.Results[i].Score != second.Results[i].Score {
			t.Fatalf("second result[%d]=%+v want %+v", i, second.Results[i], first.Results[i])
		}
	}
	if second.Stats.OpenGranulesRead != first.Stats.OpenGranulesRead || second.Stats.OpenPhysicalBytesRead != first.Stats.OpenPhysicalBytesRead {
		t.Fatalf("open stats changed first=(%d,%d) second=(%d,%d); want stable bound-reader setup telemetry", first.Stats.OpenGranulesRead, first.Stats.OpenPhysicalBytesRead, second.Stats.OpenGranulesRead, second.Stats.OpenPhysicalBytesRead)
	}
	// Physical read/cache counters and generic row fetches may be zero when the
	// planner-backed segment/block cache is warm; logical candidate counters prove
	// the bound reader is reused without depending on cold-cache telemetry.
	if first.Stats.CandidateFetches == 0 || second.Stats.CandidateFetches == 0 {
		t.Fatalf("candidate fetch stats first=%d second=%d want non-zero per-search deltas", first.Stats.CandidateFetches, second.Stats.CandidateFetches)
	}
	if first.Stats.Candidates == 0 || second.Stats.Candidates == 0 {
		t.Fatalf("candidate stats first=%d second=%d want non-zero searches", first.Stats.Candidates, second.Stats.Candidates)
	}
}

func TestVectorIndexSearcherSearchWithBufferResultEquivalenceAndZeroAllocs2124(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	ownedSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher owned: %v", err)
	}
	defer func() { _ = ownedSearcher.Close() }()
	bufferedSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher buffered: %v", err)
	}
	defer func() { _ = bufferedSearcher.Close() }()

	opts := VectorIndexSearcherSearchOptions{Query: []float32{0.6, 0.4, 0}, TopK: 3, EfSearch: len(rows)}
	owned, err := ownedSearcher.Search(opts)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, owned, def.Name, opts.TopK)

	var buffer VectorIndexSearchBuffer
	buffered, err := bufferedSearcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, buffered, def.Name, opts.TopK)
	if buffered.Stats.SearchRouteHNSWSearchPack != 1 || buffered.Stats.HNSWSearchPackActive != 1 || buffered.Stats.HNSWSearchPackFallbacks != 0 {
		t.Fatalf("SearchWithBuffer stats=%+v want hnsw_search_pack_v1 route", buffered.Stats)
	}
	assertVectorIndexSearchResponsesEquivalentNoDocs2124(t, buffered, owned)
	assertVectorIndexSearchResultIDStatsContract2124(t, buffered.Stats, owned.Stats)

	if _, err := bufferedSearcher.SearchWithBuffer(opts, &buffer); err != nil {
		t.Fatalf("warm SearchWithBuffer for allocation check: %v", err)
	}
	if collectionsRaceEnabled {
		t.Skip("AllocsPerRun is not stable under -race")
	}
	if !enterIsolatedVectorAllocationGate(t, "search-with-buffer") {
		return
	}
	var sink int
	allocs := testing.AllocsPerRun(1000, func() {
		got, err := bufferedSearcher.SearchWithBuffer(opts, &buffer)
		if err != nil {
			panic(err)
		}
		if len(got.Results) != opts.TopK {
			panic("unexpected SearchWithBuffer result count")
		}
		sink += len(got.Results) + got.Results[0].Ordinal
	})
	if allocs != 0 {
		t.Fatalf("SearchWithBuffer steady-state allocs=%v want 0", allocs)
	}
	if sink == 0 {
		t.Fatal("allocation check did not consume results")
	}
}

func TestVectorIndexSearcherSearchWithBufferRouteStatsAndNoDocumentBoundary2311(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
		{id: "doc-e", vector: []float32{0.4, 0.6, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{
		Query:     []float32{0.6, 0.4, 0},
		TopK:      3,
		EfSearch:  len(rows),
		StatsMode: VectorIndexSearchStatsModeFullDiagnostics,
	}, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 3)
	if got.Path != VectorIndexSearchPathColumnGraphNativeReader {
		t.Fatalf("path=%q want current column_graph native-reader route", got.Path)
	}
	stats := got.Stats
	if stats.SearchRouteHNSWSearchPack != 1 || stats.SearchRouteColumnGraphPrepared+stats.SearchRouteColumnGraphFallback != 0 {
		t.Fatalf("route stats=%+v want hnsw_search_pack_v1 route", stats)
	}
	if stats.HNSWSearchPackActive != 1 || stats.HNSWSearchPackMissing != 0 || stats.HNSWSearchPackInvalid != 0 || stats.HNSWSearchPackStale != 0 || stats.HNSWSearchPackClosed != 0 || stats.HNSWSearchPackFallbacks != 0 || stats.HNSWSearchPackMmapDirect+stats.HNSWSearchPackHeapCopy != 1 || stats.HNSWSearchPackOpenNanos == 0 || stats.HNSWSearchPackActiveHandles != 1 || stats.HNSWSearchPackMappedBytes+stats.HNSWSearchPackHeapCopyBytes == 0 {
		t.Fatalf("pack stats=%+v want active hnsw_search_pack_v1 route", stats)
	}
	if stats.PreparedGraphSearchViews != 0 || stats.GraphRows != 0 || stats.GraphRowFallbacks != 0 {
		t.Fatalf("pack route stats=%+v want no column_graph prepared/row fallback", stats)
	}
	if stats.DocumentsFetched != 0 || stats.DocumentsMissing != 0 || stats.DocumentBytes != 0 || stats.DocumentOutputBytes != 0 || stats.DocumentFieldsReconstructed != 0 {
		t.Fatalf("document boundary stats=%+v want no document materialization for SearchWithBuffer", stats)
	}
	for i, result := range got.Results {
		if len(result.Document) != 0 {
			t.Fatalf("result[%d] materialized document len=%d want no-document boundary", i, len(result.Document))
		}
	}
	if stats.Candidates == 0 || stats.VisitedNodes == 0 || stats.VisitedEdges == 0 || stats.VectorBytesRead == 0 || stats.AdjacencyBytesRead == 0 {
		t.Fatalf("hot-loop counters=%+v want candidates, visited nodes/edges, vector bytes, adjacency bytes", stats)
	}
	if stats.ScoreBatchCalls == 0 || stats.ScoreBatchCandidates == 0 || stats.ScoreBatchOptimizedCalls+stats.ScoreBatchScalarFallbackCalls == 0 {
		t.Fatalf("score-batch counters=%+v want score batch mode/fallback evidence", stats)
	}
	packVectorViews := stats.VectorDirectViews + stats.VectorMmapDirectViews + stats.VectorPreparedDirectViews + stats.VectorHeapCopyTypedViews
	if packVectorViews == 0 || stats.VectorScratchDecodes != 0 || stats.TypedColumnFallbacks != 0 || stats.NormBytesRead != 0 || stats.NormScratchDecodes != 0 {
		t.Fatalf("vector source stats=%+v want pack normalized-vector dot path without norm/scratch fallback", stats)
	}
	packAdjacencyViews := stats.AdjacencyDirectViews + stats.AdjacencyMmapDirectViews + stats.AdjacencyHeapCopyTypedViews + stats.AdjacencyPreparedCSRDirectViews + stats.AdjacencyPreparedCSRMmapDirectViews
	if packAdjacencyViews == 0 || stats.AdjacencyScratchDecodes != 0 || stats.AdjacencyLegacyFallbacks != 0 || stats.AdjacencySourceFallbacks != 0 {
		t.Fatalf("adjacency source stats=%+v want pack CSR adjacency source without legacy/scratch fallback", stats)
	}
	if stats.ResultIDTypedBytesState == 0 || stats.RowRefStateResultRefs == 0 || stats.ResultIDGraphFallbacks != 0 {
		t.Fatalf("identity stats=%+v want vector-index row-ref/document-id state without graph-row fallback", stats)
	}
}

func TestVectorIndexSearcherSearchWithBufferReuseGrowShrinkNoStaleIDs1961(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-long-a", vector: []float32{1, 0, 0}},
		{id: "b", vector: []float32{0, 1, 0}},
		{id: "cc", vector: []float32{0, 0, 1}},
		{id: "dddd", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	growOpts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 3, EfSearch: len(rows)}
	for i := 0; i < 3; i++ {
		got, err := searcher.SearchWithBuffer(growOpts, &buffer)
		if err != nil {
			t.Fatalf("SearchWithBuffer grow iteration %d: %v", i, err)
		}
		assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 3)
		assertVectorIndexSearchResultsV4(t, got.Results, exactColumnGraphTopKForTest(t, rows, growOpts.Query, growOpts.TopK), false)
		for j, result := range got.Results {
			if cap(result.ID) != len(result.ID) {
				t.Fatalf("grow iteration %d result[%d] id len/cap=%d/%d want cap isolated", i, j, len(result.ID), cap(result.ID))
			}
		}
	}

	if len(buffer.results) != 3 {
		t.Fatalf("test setup buffer results=%d want 3 before shrink", len(buffer.results))
	}
	buffer.results[1].Document = []byte("stale-document")
	buffer.results[2].Score = 99

	shrinkOpts := VectorIndexSearcherSearchOptions{Query: []float32{0, 1, 0}, TopK: 1, EfSearch: len(rows)}
	shrunk, err := searcher.SearchWithBuffer(shrinkOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer shrink: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, shrunk, def.Name, 1)
	assertVectorIndexSearchResultsV4(t, shrunk.Results, exactColumnGraphTopKForTest(t, rows, shrinkOpts.Query, shrinkOpts.TopK), false)
	if gotID := string(shrunk.Results[0].ID); gotID != "b" {
		t.Fatalf("shrunk top id=%q want b without stale bytes from longer prior IDs", gotID)
	}
	if cap(shrunk.Results[0].ID) != len(shrunk.Results[0].ID) {
		t.Fatalf("shrunk id len/cap=%d/%d want cap isolated", len(shrunk.Results[0].ID), cap(shrunk.Results[0].ID))
	}
	allResults := buffer.results[:cap(buffer.results)]
	for i := len(buffer.results); i < 3 && i < len(allResults); i++ {
		if allResults[i].ID != nil || allResults[i].Ordinal != 0 || allResults[i].Score != 0 || allResults[i].Document != nil {
			t.Fatalf("shrunk stale tail result[%d]=%+v want cleared", i, allResults[i])
		}
	}

	regrown, err := searcher.SearchWithBuffer(growOpts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer regrow: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, regrown, def.Name, 3)
	assertVectorIndexSearchResultsV4(t, regrown.Results, exactColumnGraphTopKForTest(t, rows, growOpts.Query, growOpts.TopK), false)
}

func TestVectorIndexSearcherSearchWithBufferOversizedReuseClearsCurrentView2124(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(24, 3)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	wideOpts := VectorIndexSearcherSearchOptions{Query: rows[0].vector, TopK: len(rows), EfSearch: len(rows)}
	wide, err := searcher.SearchWithBuffer(wideOpts, &buffer)
	if err != nil {
		t.Fatalf("wide SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, wide, def.Name, len(rows))
	if cap(buffer.results) <= 1*2+columnVectorGraphNativeScratchOversizeSlack {
		t.Fatalf("test setup cap=%d want oversized for shrink-to-one path", cap(buffer.results))
	}
	for i := 1; i < len(buffer.results); i++ {
		buffer.results[i].Document = []byte("stale-document")
		buffer.results[i].Score = 99
	}

	shrinkOpts := VectorIndexSearcherSearchOptions{Query: rows[1].vector, TopK: 1, EfSearch: len(rows)}
	shrunk, err := searcher.SearchWithBuffer(shrinkOpts, &buffer)
	if err != nil {
		t.Fatalf("shrink SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, shrunk, def.Name, 1)
	if cap(shrunk.Results) != 1 || cap(buffer.results) != 1 {
		t.Fatalf("shrunk result cap response=%d buffer=%d want new exact backing for oversized reuse", cap(shrunk.Results), cap(buffer.results))
	}
	if shrunk.Results[0].Document != nil || shrunk.Results[0].Score == 99 {
		t.Fatalf("shrunk current result retained stale state: %+v", shrunk.Results[0])
	}
}

func TestVectorIndexSearcherSearchWithBufferDoesNotMutateSearchResponse1961(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	opts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: len(rows)}
	owned, err := searcher.Search(opts)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, owned, def.Name, 2)
	if cap(owned.Results) != len(owned.Results) {
		t.Fatalf("Search result len/cap=%d/%d want cap isolated", len(owned.Results), cap(owned.Results))
	}
	for i, result := range owned.Results {
		if cap(result.ID) != len(result.ID) {
			t.Fatalf("Search result[%d] id len/cap=%d/%d want cap isolated", i, len(result.ID), cap(result.ID))
		}
	}
	ownedID := append([]byte(nil), owned.Results[0].ID...)

	var buffer VectorIndexSearchBuffer
	buffered, err := searcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		t.Fatalf("SearchWithBuffer: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, buffered, def.Name, 2)
	buffered.Results[0].ID[0] = 'X'
	if !bytes.Equal(owned.Results[0].ID, ownedID) {
		t.Fatalf("Search response ID changed after mutating SearchWithBuffer response: got %q want %q", owned.Results[0].ID, ownedID)
	}

	if _, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{0, 1, 0}, TopK: 1, EfSearch: len(rows)}, &buffer); err != nil {
		t.Fatalf("second SearchWithBuffer: %v", err)
	}
	if !bytes.Equal(owned.Results[0].ID, ownedID) {
		t.Fatalf("Search response ID changed after reusing buffer: got %q want %q", owned.Results[0].ID, ownedID)
	}
}

func TestVectorIndexSearcherSearchWithBufferParallelIndependentBuffers1961(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.7, 0.3, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 3, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := []float32{1, 0, 0}
	topK := 2
	want := exactColumnGraphTopKForTest(t, rows, query, topK)
	const workers = 4
	const iterations = 20
	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				errs <- fmt.Sprintf("worker %d OpenVectorIndexSearcher: %v", worker, err)
				return
			}
			defer func() { _ = searcher.Close() }()
			var buffer VectorIndexSearchBuffer
			opts := VectorIndexSearcherSearchOptions{Query: query, TopK: topK, EfSearch: len(rows)}
			for i := 0; i < iterations; i++ {
				got, err := searcher.SearchWithBuffer(opts, &buffer)
				if err != nil {
					errs <- fmt.Sprintf("worker %d iteration %d SearchWithBuffer: %v", worker, i, err)
					return
				}
				if len(got.Results) != len(want) {
					errs <- fmt.Sprintf("worker %d iteration %d results=%d want %d", worker, i, len(got.Results), len(want))
					return
				}
				if got.Stats.SearchRouteHNSWSearchPack != 1 || got.Stats.HNSWSearchPackActive != 1 || got.Stats.HNSWSearchPackFallbacks != 0 {
					errs <- fmt.Sprintf("worker %d iteration %d stats=%+v want hnsw_search_pack_v1 route", worker, i, got.Stats)
					return
				}
				for j := range want {
					if !bytes.Equal(got.Results[j].ID, want[j].ID) || math.Abs(got.Results[j].Score-want[j].Score) > 1e-6 {
						errs <- fmt.Sprintf("worker %d iteration %d result[%d]=%+v want id=%q score=%v", worker, i, j, got.Results[j], want[j].ID, want[j].Score)
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestVectorIndexSearcherSearchWithBufferErrorResetsBuffer1961(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	var buffer VectorIndexSearchBuffer
	validOpts := VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 2, EfSearch: len(rows)}
	if got, err := searcher.SearchWithBuffer(validOpts, &buffer); err != nil || len(got.Results) != 2 {
		t.Fatalf("initial SearchWithBuffer results=%d err=%v want 2, nil", len(got.Results), err)
	}

	got, err := searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0, 0}, TopK: 1, EfSearch: len(rows), IncludeDocuments: true}, &buffer)
	if err == nil || !strings.Contains(err.Error(), "IncludeDocuments") {
		t.Fatalf("SearchWithBuffer IncludeDocuments err=%v want documented no-document failure", err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("IncludeDocuments error left results: returned=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}
	if got.IndexName != def.Name || got.Status.State != VectorIndexStateColumnGraphLoaded {
		t.Fatalf("error response metadata=%+v status=%+v want loaded search metadata", got, got.Status)
	}

	got, err = searcher.SearchWithBuffer(VectorIndexSearcherSearchOptions{Query: []float32{1, 0}, TopK: 1, EfSearch: len(rows)}, &buffer)
	if err == nil || !errors.Is(err, errColumnVectorGraphNativeSearchQueryDimensionMismatch) {
		t.Fatalf("SearchWithBuffer dimension err=%v want query dimension mismatch", err)
	}
	if len(got.Results) != 0 || len(buffer.results) != 0 || len(buffer.idBytes) != 0 {
		t.Fatalf("dimension error left results: returned=%d bufferResults=%d idBytes=%d", len(got.Results), len(buffer.results), len(buffer.idBytes))
	}

	got, err = searcher.SearchWithBuffer(validOpts, &buffer)
	if err != nil {
		t.Fatalf("valid SearchWithBuffer after errors: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 2)
}

func assertVectorIndexSearchDocumentDIDV4(tb testing.TB, document []byte, want string) {
	tb.Helper()
	var got struct {
		DID string `json:"did"`
	}
	if err := json.Unmarshal(document, &got); err != nil {
		tb.Fatalf("document=%q is not valid JSON: %v", document, err)
	}
	if got.DID != want {
		tb.Fatalf("document did=%q want %q in %q", got.DID, want, document)
	}
}

func TestSearchVectorIndexColumnGraphUnavailableStatusV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want rebuild-needed failure")
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded || got.Status.Loaded {
		t.Fatalf("status=%+v want rebuild-needed fail-closed status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on unavailable index", got.Path, len(got.Results))
	}
}

func TestSearchVectorIndexColumnGraphReaderOpenFailureDowngradesLoadedStatusV4(t *testing.T) {
	d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetWithManifestRowsV2B(t, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	}, 3)
	defer func() { _ = d.Close() }()

	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	if status.State != VectorIndexStateColumnGraphLoaded || !status.Loaded {
		t.Fatalf("test setup status=%+v want loaded before reader row-count validation", status)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("SearchVectorIndex err=%v want unavailable wrapping manifest mismatch", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded || got.Status.Loaded {
		t.Fatalf("status=%+v want fail-closed rebuild-needed status", got.Status)
	}
	if got.Status.Reason != VectorIndexReasonColumnGraphAssetMismatch {
		t.Fatalf("status reason=%q want %q", got.Status.Reason, VectorIndexReasonColumnGraphAssetMismatch)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on reader open failure", got.Path, len(got.Results))
	}
}

func TestColumnGraphVectorIndexStatusUsesCallerSnapshotV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()

	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()

	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	current, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus current: %v", err)
	}
	if current.State != VectorIndexStateColumnGraphLoaded || !current.Loaded {
		t.Fatalf("current status=%+v want loaded after rebuild", current)
	}

	old, err := col.columnGraphVectorIndexStatusAtSnapshot(def.Name, oldSnap)
	if err != nil {
		t.Fatalf("columnGraphVectorIndexStatusAtSnapshot: %v", err)
	}
	if old.State != VectorIndexStateColumnGraphRebuildNeeded || !old.RebuildNeeded || old.Loaded {
		t.Fatalf("old snapshot status=%+v want rebuild-needed from caller snapshot", old)
	}
}

func TestColumnGraphVectorIndexStatusRejectsNilInputsV4(t *testing.T) {
	var nilCollection *Collection
	if _, err := nilCollection.columnGraphVectorIndexStatus("embedding_graph"); !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil collection status err=%v want errCollectionNil", err)
	}
	if _, err := nilCollection.columnGraphVectorIndexStatusAtSnapshot("embedding_graph", nil); !errors.Is(err, errCollectionNil) {
		t.Fatalf("nil collection snapshot status err=%v want errCollectionNil", err)
	}
	emptyCollection := &Collection{}
	if _, err := emptyCollection.columnGraphVectorIndexStatus("embedding_graph"); !errors.Is(err, errCollectionDBNil) {
		t.Fatalf("nil db status err=%v want errCollectionDBNil", err)
	}

	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.columnGraphVectorIndexStatusAtSnapshot(def.Name, nil); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("nil snapshot status err=%v want ErrClosed", err)
	}
}

func TestSearchVectorIndexColumnGraphStaleAfterMutationV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{
		{id: "doc-c", vector: []float32{0, 0, 1}},
	})

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{0, 0, 1},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want stale/rebuild-needed failure")
	}
	if !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("SearchVectorIndex stale err=%v want manifest mismatch wrapping", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphRebuildNeeded || !got.Status.RebuildNeeded {
		t.Fatalf("status=%+v want stale graph to require rebuild", got.Status)
	}
	if got.Status.Reason != VectorIndexReasonColumnGraphUnsupportedVisibility {
		t.Fatalf("status reason=%q want %q", got.Status.Reason, VectorIndexReasonColumnGraphUnsupportedVisibility)
	}
}

func TestSearchVectorIndexColumnGraphMutationMatrixFailsClosedV5(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(testing.TB, *Collection)
		query  []float32
	}{
		{
			name: "insert_after_build",
			mutate: func(tb testing.TB, col *Collection) {
				insertColumnGraphRebuildRowsV2A(tb, col, []columnGraphRebuildInputRowV2A{
					{id: "doc-c", vector: []float32{0, 0, 1}},
				})
			},
			query: []float32{0, 0, 1},
		},
		{
			name: "vector_update",
			mutate: func(tb testing.TB, col *Collection) {
				updateColumnGraphRebuildJSONDocumentV5(tb, col, "doc-a", []float32{0, 0, 1}, "vector-updated")
			},
			query: []float32{0, 0, 1},
		},
		{
			name: "non_vector_payload_update",
			mutate: func(tb testing.TB, col *Collection) {
				updateColumnGraphRebuildJSONDocumentV5(tb, col, "doc-a", []float32{1, 0, 0}, "payload-updated")
			},
			query: []float32{1, 0, 0},
		},
		{
			name: "delete",
			mutate: func(tb testing.TB, col *Collection) {
				deleted, err := col.DeleteDocument([]byte("doc-a"))
				if err != nil {
					tb.Fatalf("DeleteDocument: %v", err)
				}
				if !deleted {
					tb.Fatalf("DeleteDocument deleted=false want true")
				}
			},
			query: []float32{1, 0, 0},
		},
		{
			name: "mixed_sequential_batch",
			mutate: func(tb testing.TB, col *Collection) {
				insertColumnGraphRebuildRowsV2A(tb, col, []columnGraphRebuildInputRowV2A{
					{id: "doc-c", vector: []float32{0, 0, 1}},
				})
				updateColumnGraphRebuildJSONDocumentV5(tb, col, "doc-b", []float32{1, 0, 0}, "vector-updated")
				deleted, err := col.DeleteDocument([]byte("doc-a"))
				if err != nil {
					tb.Fatalf("DeleteDocument: %v", err)
				}
				if !deleted {
					tb.Fatalf("DeleteDocument deleted=false want true")
				}
			},
			query: []float32{1, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := []columnGraphRebuildInputRowV2A{
				{id: "doc-a", vector: []float32{1, 0, 0}},
				{id: "doc-b", vector: []float32{0, 1, 0}},
			}
			_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
			defer func() { _ = d.Close() }()
			status, err := col.RebuildVectorIndex(def.Name)
			if err != nil {
				t.Fatalf("RebuildVectorIndex: %v", err)
			}
			assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

			tt.mutate(t, col)

			assertColumnGraphUnsupportedVisibilityStatusV5(t, col, def.Name)
			got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
				IndexName:        def.Name,
				Query:            tt.query,
				TopK:             2,
				EfSearch:         len(rows) + 1,
				IncludeDocuments: true,
				MaxDecodedBlocks: 1,
			})
			if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphManifestMismatch) {
				t.Fatalf("SearchVectorIndex err=%v want unavailable stale manifest mismatch", err)
			}
			if got.Path != "" || len(got.Results) != 0 || got.Stats.DocumentsFetched != 0 {
				t.Fatalf("response path=%q results=%d docs=%d want no search results before rebuild", got.Path, len(got.Results), got.Stats.DocumentsFetched)
			}
			assertColumnGraphUnsupportedVisibilitySearchStatusV5(t, got.Status, def.Name)
		})
	}
}

func TestSearchVectorIndexColumnGraphMutationStaleStatusSurvivesReopenV5(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	updateColumnGraphRebuildJSONDocumentV5(t, col, "doc-a", []float32{0, 0, 1}, "vector-updated")
	assertColumnGraphUnsupportedVisibilityStatusV5(t, col, def.Name)
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertColumnGraphUnsupportedVisibilityStatusV5(t, reopenedCol, def.Name)
	got, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            []float32{0, 0, 1},
		TopK:             1,
		EfSearch:         len(rows),
		MaxDecodedBlocks: 1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !errors.Is(err, errColumnVectorGraphManifestMismatch) {
		t.Fatalf("SearchVectorIndex reopen err=%v want unavailable stale manifest mismatch", err)
	}
	assertColumnGraphUnsupportedVisibilitySearchStatusV5(t, got.Status, def.Name)
}

func TestSearchVectorIndexColumnGraphSnapshotBoundSearcherSurvivesLaterMutationV5(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		t.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()

	updateColumnGraphRebuildJSONDocumentV5(t, col, "doc-a", []float32{0, 0, 1}, "vector-updated")
	assertColumnGraphUnsupportedVisibilityStatusV5(t, col, def.Name)

	got, err := searcher.Search(VectorIndexSearcherSearchOptions{
		Query:            []float32{1, 0, 0},
		TopK:             1,
		EfSearch:         len(rows),
		IncludeDocuments: true,
	})
	if err != nil {
		t.Fatalf("snapshot-bound Search: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	if string(got.Results[0].ID) != "doc-a" ||
		!bytes.Contains(got.Results[0].Document, []byte(`"did":"doc-a"`)) ||
		bytes.Contains(got.Results[0].Document, []byte(`"note":"vector-updated"`)) {
		t.Fatalf("snapshot result id=%q doc=%s want old consistent generation", got.Results[0].ID, got.Results[0].Document)
	}
}

func TestSearchVectorIndexNativeRuntimeDoesNotFallbackToColumnGraphV4(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_native",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyNativeRuntime,
		}},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: "embedding_native",
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err == nil {
		t.Fatalf("SearchVectorIndex err=nil want explicit native-runtime unsupported status")
	}
	if got.Status.State != VectorIndexStateNativeRuntime || got.Status.Reason != VectorIndexReasonNativeRuntime {
		t.Fatalf("status=%+v want native runtime status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no column graph fallback", got.Path, len(got.Results))
	}
}

func TestSearchVectorIndexWithBufferNativeRuntimeLiveRoute(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding_native",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
		Strategy:   VectorIndexStrategyNativeRuntime,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{DocumentFormat: DocumentFormatJSON},
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection fresh handle: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex empty: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	var buffer VectorIndexSearchBuffer
	search := func(query []float32) VectorIndexSearchResponse {
		t.Helper()
		got, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{
			IndexName: def.Name,
			Query:     query,
			TopK:      2,
			EfSearch:  8,
			StatsMode: VectorIndexSearchStatsModeProduction,
		}, &buffer)
		if err != nil {
			t.Fatalf("SearchVectorIndexWithBuffer: %v", err)
		}
		if got.Path != VectorIndexSearchPathNativeRuntime || got.Stats.SearchRouteNativeRuntime != 1 || got.Diagnostics().Route != VectorIndexSearchRouteNativeRuntime || !got.Diagnostics().LiveANN.Enabled || got.Diagnostics().LiveANN.FullRebuilds != 0 || !got.Diagnostics().NoDocumentGuardrailsOK || got.Stats.DocumentsFetched != 0 {
			t.Fatalf("native response=%+v diagnostics=%+v", got, got.Diagnostics())
		}
		return got
	}
	if got := search([]float32{1, 0}); len(got.Results) != 2 || string(got.Results[0].ID) != "a" {
		t.Fatalf("initial results=%+v want a first", got.Results)
	}
	if matched, err := col.Replace([]byte("a"), []byte(`{"embedding":[-1,0]}`)); err != nil || !matched {
		t.Fatalf("Replace matched=%v err=%v", matched, err)
	}
	if got := search([]float32{1, 0}); len(got.Results) != 2 || string(got.Results[0].ID) == "a" {
		t.Fatalf("updated results=%+v want replacement excluded from old-vector top hit", got.Results)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v", deleted, err)
	}
	if got := search([]float32{-1, 0}); len(got.Results) != 1 || string(got.Results[0].ID) != "b" {
		t.Fatalf("deleted results=%+v want only b", got.Results)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var concurrentBuffer VectorIndexSearchBuffer
			_, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 1}, &concurrentBuffer)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent native runtime search: %v", err)
		}
	}
	if _, err := freshCol.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex fresh handle: %v", err)
	}
	if _, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 1}, &buffer); !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("stale native runtime search err=%v want unavailable", err)
	}
}

func TestSearchVectorIndexWithBufferNativeRuntimeTombstonesDoNotReduceTopK(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, EfSearch: 2, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.99,0.01]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[-1,0]}`),
		},
	); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	for _, id := range []string{"a", "b"} {
		matched, err := col.Replace([]byte(id), []byte(`{"embedding":[-0.99,-0.01]}`))
		if err != nil || !matched {
			t.Fatalf("Replace %s matched=%v err=%v", id, matched, err)
		}
	}

	var buffer VectorIndexSearchBuffer
	got, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0},
		TopK:      2,
		EfSearch:  2,
		StatsMode: VectorIndexSearchStatsModeProduction,
	}, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer: %v", err)
	}
	if len(got.Results) != 2 {
		t.Fatalf("results=%+v want TopK live results despite closer tombstones", got.Results)
	}
	_, err = col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0},
		TopK:      2,
		EfSearch:  2,
		StatsMode: VectorIndexSearchStatsModeWorkAccounting,
	}, &buffer)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || len(buffer.results) != 0 {
		t.Fatalf("work-accounting err=%v buffered_results=%d want fail-closed reset", err, len(buffer.results))
	}
}

func TestValidateRegisteredNativeRuntimeVectorIndexMissingCollectionFailsClosed(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	c := &Collection{db: d, name: "missing"}
	_, _, err = c.validateRegisteredNativeRuntimeVectorIndexForSearch(VectorIndexDefinition{Name: "embedding_native"}, &VectorIndex{})
	if !errors.Is(err, errCollectionNotFound) {
		t.Fatalf("validate missing collection err=%v want errCollectionNotFound", err)
	}
}

func TestSearchVectorIndexWithBufferNativeRuntimeMissingRootFailsClosed(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	var buffer VectorIndexSearchBuffer
	_, err = col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 1}, &buffer)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) {
		t.Fatalf("SearchVectorIndexWithBuffer err=%v want unavailable", err)
	}
	if col.registeredVectorIndex(def.Name) != nil {
		t.Fatal("missing-root search registered or rebuilt a native graph")
	}
	start := make(chan struct{})
	errs := make(chan error, 17)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var searchBuffer VectorIndexSearchBuffer
			_, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 1}, &searchBuffer)
			if err != nil && !errors.Is(err, ErrVectorIndexSearchUnavailable) {
				errs <- err
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)})
		errs <- err
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent first insert/search: %v", err)
		}
	}
	response, err := col.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{1, 0}, TopK: 1}, &buffer)
	if err != nil {
		t.Fatalf("SearchVectorIndexWithBuffer after automatic rebuild: %v", err)
	}
	if got := response.Diagnostics().LiveANN.FullRebuilds; got != 1 {
		t.Fatalf("live ANN full rebuilds=%d want 1", got)
	}
}

func TestSearchVectorIndexColumnGraphUsesSnapshotMetadataV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutated := false
	for i := range col.meta.VectorIndexes {
		if col.meta.VectorIndexes[i].Name == def.Name {
			col.meta.VectorIndexes[i].Metric = VectorMetric(255)
			col.meta.VectorIndexes[i].Strategy = VectorIndexStrategyNativeRuntime
			mutated = true
		}
	}
	if !mutated {
		t.Fatalf("test setup missing vector index %q", def.Name)
	}

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex after handle metadata drift: %v", err)
	}
	assertColumnGraphSearchResponseLoadedV4(t, got, def.Name, 1)
	if !bytes.Equal(got.Results[0].ID, []byte("doc-a")) {
		t.Fatalf("top result id=%q want doc-a from snapshot catalog metadata", got.Results[0].ID)
	}
}

func TestSearchVectorIndexColumnGraphRejectsUnsupportedMetricV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutateCurrentSnapshotVectorIndexForTestV4(t, d, col, def.Name, func(def *VectorIndexDefinition) {
		def.Metric = VectorMetric(255)
	})

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "supports only \"cosine\"") {
		t.Fatalf("SearchVectorIndex err=%v want unsupported metric search-unavailable error", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphUnavailable || got.Status.Reason != VectorIndexReasonColumnGraphUnsupportedMetric {
		t.Fatalf("status=%+v want unsupported metric unavailable status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on unsupported metric", got.Path, len(got.Results))
	}
}

func TestSearchVectorIndexRejectsUnsupportedSnapshotStrategyV4(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	mutateCurrentSnapshotVectorIndexForTestV4(t, d, col, def.Name, func(def *VectorIndexDefinition) {
		def.Strategy = VectorIndexStrategy("decoded_graph")
	})

	got, err := col.SearchVectorIndex(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{1, 0, 0},
		TopK:      1,
	})
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || !strings.Contains(err.Error(), "unsupported strategy") {
		t.Fatalf("SearchVectorIndex err=%v want unsupported strategy search-unavailable error", err)
	}
	if got.Status.State != VectorIndexStateColumnGraphUnavailable || got.Status.Reason != VectorIndexReasonUnsupportedStrategy {
		t.Fatalf("status=%+v want unsupported strategy unavailable status", got.Status)
	}
	if got.Path != "" || len(got.Results) != 0 {
		t.Fatalf("response path=%q results=%d want no search path/results on unsupported strategy", got.Path, len(got.Results))
	}
}

func mutateCurrentSnapshotVectorIndexForTestV4(tb testing.TB, d *backenddb.DB, col *Collection, name string, mutate func(*VectorIndexDefinition)) {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		tb.Fatalf("catalogForSnapshot: %v", err)
	}
	if catalog == nil {
		tb.Fatal("catalogForSnapshot returned nil")
	}
	for i := range catalog.meta.VectorIndexes {
		if catalog.meta.VectorIndexes[i].Name == name {
			mutate(&catalog.meta.VectorIndexes[i])
			return
		}
	}
	tb.Fatalf("test setup missing vector index %q in snapshot catalog", name)
}

func assertColumnGraphSearchResponseLoadedV4(tb testing.TB, got VectorIndexSearchResponse, name string, wantResults int) {
	tb.Helper()
	if got.IndexName != name || got.Strategy != VectorIndexStrategyColumnGraph {
		tb.Fatalf("response index=%q strategy=%q want %q/%q", got.IndexName, got.Strategy, name, VectorIndexStrategyColumnGraph)
	}
	if got.Path != VectorIndexSearchPathColumnGraphNativeReader {
		tb.Fatalf("path=%q want %q", got.Path, VectorIndexSearchPathColumnGraphNativeReader)
	}
	if got.Status.State != VectorIndexStateColumnGraphLoaded || !got.Status.Loaded || got.Status.RebuildNeeded {
		tb.Fatalf("status=%+v want loaded column graph", got.Status)
	}
	if len(got.Results) != wantResults {
		tb.Fatalf("results=%d want %d", len(got.Results), wantResults)
	}
}

func assertVectorIndexSearchResultsV4(tb testing.TB, got []VectorIndexSearchResult, want []columnVectorGraphNativeSearchResult, wantDocs bool) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("results=%d want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].ID, want[i].ID) || math.Abs(got[i].Score-want[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want id=%q ordinal=%d score=%v", i, got[i], want[i].ID, want[i].Ordinal, want[i].Score)
		}
		if wantDocs && len(got[i].Document) == 0 {
			tb.Fatalf("result[%d] missing materialized document", i)
		}
	}
}

func assertVectorIndexSearchResponsesEquivalentNoDocs2124(tb testing.TB, got, want VectorIndexSearchResponse) {
	tb.Helper()
	if got.IndexName != want.IndexName || got.Strategy != want.Strategy || got.Path != want.Path || !vectorIndexStatusEquivalentForSearchResponse2124(got.Status, want.Status) {
		tb.Fatalf("response metadata got=%+v status=%+v want=%+v status=%+v", got, got.Status, want, want.Status)
	}
	if len(got.Results) != len(want.Results) {
		tb.Fatalf("results=%d want %d", len(got.Results), len(want.Results))
	}
	for i := range want.Results {
		if !bytes.Equal(got.Results[i].ID, want.Results[i].ID) || got.Results[i].Ordinal != want.Results[i].Ordinal || math.Abs(got.Results[i].Score-want.Results[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want %+v", i, got.Results[i], want.Results[i])
		}
		if len(got.Results[i].Document) != 0 {
			tb.Fatalf("result[%d] document len=%d want no-document reusable response", i, len(got.Results[i].Document))
		}
	}
}

func vectorIndexStatusEquivalentForSearchResponse2124(a, b VectorIndexStatus) bool {
	return vectorIndexDefinitionValuesEqual(a.Definition, b.Definition) &&
		a.Name == b.Name &&
		a.Strategy == b.Strategy &&
		a.State == b.State &&
		a.Reason == b.Reason &&
		a.Loaded == b.Loaded &&
		a.RootName == b.RootName &&
		a.RootID == b.RootID &&
		a.NativeRootLoaded == b.NativeRootLoaded &&
		a.NativeRootBytes == b.NativeRootBytes &&
		a.ExactFallbackReason == b.ExactFallbackReason &&
		a.Registered == b.Registered &&
		a.Stats == b.Stats &&
		a.RebuildNeeded == b.RebuildNeeded &&
		a.Duration == b.Duration
}

func assertVectorIndexSearchResultIDStatsContract2124(tb testing.TB, got, want VectorIndexSearchStats) {
	tb.Helper()
	checks := []struct {
		name string
		got  uint64
		want uint64
	}{
		{name: "candidate_rows", got: got.CandidateRows, want: want.CandidateRows},
		{name: "candidates", got: got.Candidates, want: want.Candidates},
		{name: "edges", got: got.Edges, want: want.Edges},
		{name: "visited_nodes", got: got.VisitedNodes, want: want.VisitedNodes},
		{name: "visited_edges", got: got.VisitedEdges, want: want.VisitedEdges},
		{name: "vector_bytes_read", got: got.VectorBytesRead, want: want.VectorBytesRead},
		{name: "candidate_fetches", got: got.CandidateFetches, want: want.CandidateFetches},
		{name: "expansion_fetches", got: got.ExpansionFetches, want: want.ExpansionFetches},
		{name: "result_fetches", got: got.ResultFetches, want: want.ResultFetches},
		{name: "result_id_typed_bytes_state", got: got.ResultIDTypedBytesState, want: want.ResultIDTypedBytesState},
		{name: "result_id_graph_fallbacks", got: got.ResultIDGraphFallbacks, want: want.ResultIDGraphFallbacks},
		{name: "row_ref_state_result_refs", got: got.RowRefStateResultRefs, want: want.RowRefStateResultRefs},
		{name: "graph_row_fallbacks", got: got.GraphRowFallbacks, want: want.GraphRowFallbacks},
		{name: "typed_column_fallbacks", got: got.TypedColumnFallbacks, want: want.TypedColumnFallbacks},
		{name: "vector_scratch_decodes", got: got.VectorScratchDecodes, want: want.VectorScratchDecodes},
		{name: "norm_scratch_decodes", got: got.NormScratchDecodes, want: want.NormScratchDecodes},
		{name: "adjacency_scratch_decodes", got: got.AdjacencyScratchDecodes, want: want.AdjacencyScratchDecodes},
	}
	for _, check := range checks {
		if check.got != check.want {
			tb.Fatalf("stats %s=%d want %d; got=%+v wantStats=%+v", check.name, check.got, check.want, got, want)
		}
	}
	if got.GraphRows != 0 || got.ResultIDGraphFallbacks != 0 || got.GraphRowFallbacks != 0 || got.VectorScratchDecodes != 0 || got.NormScratchDecodes != 0 || got.AdjacencyScratchDecodes != 0 || got.TypedColumnFallbacks != 0 {
		tb.Fatalf("stats=%+v want healthy vector-index result-ID path without graph-row/scratch fallbacks", got)
	}
	if got.SearchRouteHNSWSearchPack+got.SearchRouteColumnGraphPrepared+got.SearchRouteColumnGraphFallback != 1 {
		tb.Fatalf("stats=%+v want exactly one search route", got)
	}
	if got.HNSWSearchPackActive+got.HNSWSearchPackMissing+got.HNSWSearchPackInvalid+got.HNSWSearchPackStale+got.HNSWSearchPackClosed != 1 {
		tb.Fatalf("stats=%+v want exactly one hnsw_search_pack_v1 availability status", got)
	}
	if got.HNSWSearchPackActive == 1 && (got.HNSWSearchPackMmapDirect+got.HNSWSearchPackHeapCopy != 1 || got.HNSWSearchPackOpenNanos == 0 || got.HNSWSearchPackActiveHandles != 1 || got.HNSWSearchPackMappedBytes+got.HNSWSearchPackHeapCopyBytes == 0) {
		tb.Fatalf("stats=%+v want active hnsw_search_pack_v1 direct/heap resource evidence", got)
	}
	if got.ResultIDTypedBytesState == 0 || got.ResultIDPreparedBytesViews == 0 || got.RowRefStateResultRefs == 0 {
		tb.Fatalf("stats=%+v want vector-index result-ID and row-ref identity state", got)
	}
}

func assertColumnGraphUnsupportedVisibilityStatusV5(tb testing.TB, col *Collection, name string) {
	tb.Helper()
	status, err := col.VectorIndexStatus(name)
	if err != nil {
		tb.Fatalf("VectorIndexStatus: %v", err)
	}
	assertColumnGraphUnsupportedVisibilitySearchStatusV5(tb, status, name)
}

func assertColumnGraphUnsupportedVisibilitySearchStatusV5(tb testing.TB, status VectorIndexStatus, name string) {
	tb.Helper()
	if status.Name != name || status.Strategy != VectorIndexStrategyColumnGraph {
		tb.Fatalf("status=%+v want column_graph index %q", status, name)
	}
	if status.State != VectorIndexStateColumnGraphRebuildNeeded ||
		status.Reason != VectorIndexReasonColumnGraphUnsupportedVisibility ||
		!status.RebuildNeeded ||
		status.Loaded {
		tb.Fatalf("status=%+v want rebuild-needed unsupported visibility", status)
	}
}

func updateColumnGraphRebuildJSONDocumentV5(tb testing.TB, col *Collection, id string, vector []float32, note string) {
	tb.Helper()
	replacement, err := json.Marshal(map[string]any{
		"time_us":   int64(100),
		"kind":      "vector",
		"did":       id,
		"embedding": vector,
		"note":      note,
	})
	if err != nil {
		tb.Fatalf("json.Marshal replacement: %v", err)
	}
	matched, modified, err := col.Update([]byte(id), func(current []byte) ([]byte, bool, error) {
		return replacement, true, nil
	})
	if err != nil {
		tb.Fatalf("Update %q: %v", id, err)
	}
	if !matched || !modified {
		tb.Fatalf("Update %q matched=%t modified=%t want true/true", id, matched, modified)
	}
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		MaxDecodedBlocks: 1,
	}
	warm, err := col.SearchVectorIndex(opts)
	if err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	// Sample deterministic telemetry before the timed loop so reporting does not
	// add per-iteration work to the public one-shot search benchmark.
	stats := warm.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     topK,
		EfSearch: efSearch,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	// The steady-state benchmark times only Search. Metrics come from a warmed
	// pre-timer search so telemetry accumulation cannot hide throughput drift.
	stats := measuredStats.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		IncludeDocuments: true,
		MaxDecodedBlocks: 1,
	}
	warm, err := col.SearchVectorIndex(opts)
	if err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	// Document materialization remains in the timed loop; metric collection does
	// not, so allocs/op reflects the public API path instead of test bookkeeping.
	stats := warm.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsExcludeEmbedding1875(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearchOptions{
		IndexName:        def.Name,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		MaxDecodedBlocks: 1,
	}
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		b.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	fetchPreset.ApplyToSearchOptions(&opts)
	warm, err := col.SearchVectorIndex(opts)
	if err != nil {
		b.Fatalf("warm SearchVectorIndex: %v", err)
	}
	stats := warm.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := col.SearchVectorIndex(opts)
		if err != nil {
			b.Fatalf("SearchVectorIndex: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderWithDocumentsV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		IncludeDocuments: true,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderWithDocumentsExcludeEmbedding1875(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     topK,
		EfSearch: efSearch,
	}
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPreset(def)
	if err != nil {
		b.Fatalf("ProjectionOrientedVectorDocumentFetchPreset: %v", err)
	}
	fetchPreset.ApplyToSearcherSearchOptions(&opts)
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b, false, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4StatsMode2126(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderStatsModes2126(b, false, DocumentFetchOptions{})
}

func BenchmarkVectorSearchPublicSearchSerialTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithDocumentsV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b, true, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithDocumentsExcludeEmbedding1875(b *testing.B) {
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPresetForField("embedding")
	if err != nil {
		b.Fatalf("ProjectionOrientedVectorDocumentFetchPresetForField: %v", err)
	}
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b, fetchPreset.IncludeDocuments, fetchPreset.DocumentFetchOptions)
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b, includeDocuments, fetchOptions, VectorIndexSearchStatsModeDefault)
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderStatsModes2126(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	b.Helper()
	for _, tc := range []struct {
		name string
		mode VectorIndexSearchStatsMode
	}{
		{name: "stats=full_diagnostics", mode: VectorIndexSearchStatsModeFullDiagnostics},
		{name: "stats=production", mode: VectorIndexSearchStatsModeProduction},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b, includeDocuments, fetchOptions, tc.mode)
		})
	}
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithStatsModeV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions, statsMode VectorIndexSearchStatsMode) {
	b.Helper()
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:                query,
		TopK:                 topK,
		EfSearch:             efSearch,
		IncludeDocuments:     includeDocuments,
		DocumentFetchOptions: fetchOptions,
		StatsMode:            statsMode,
	}
	if _, err := searcher.Search(opts); err != nil {
		b.Fatalf("warm Search: %v", err)
	}
	measuredStats, err := searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.Search(opts)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		if includeDocuments {
			vectorSearchBenchSinkOrdinalV4 += len(got.Results[0].Document)
		} else {
			vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
		}
	}
	b.StopTimer()
	reportVectorIndexSearchStatsModeBenchMetric2126(b, statsMode)
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkVectorSearchReusableBufferSerialTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphScalarU8QuantizedModes1926(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	query := append([]float32(nil), input[37].vector...)
	exactWant := exactColumnGraphTopKForTest(b, input, query, topK)
	for _, tc := range []struct {
		name               string
		mode               VectorIndexQueryMode
		rerankCandidates   int
		wantQuantizedStats bool
		wantExactStats     bool
	}{
		{name: "mode=exact", wantExactStats: true},
		{name: "mode=quantized_only", mode: VectorIndexQueryModeQuantizedOnly, wantQuantizedStats: true},
		{name: "mode=quantized_rerank", mode: VectorIndexQueryModeQuantizedRerank, rerankCandidates: efSearch, wantQuantizedStats: true, wantExactStats: true},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			_, d, col, def := openColumnGraphQuantizedBenchmarkCollection1926(b, dims, m, input)
			defer func() { _ = d.Close() }()
			if _, err := col.RebuildVectorIndex(def.Name); err != nil {
				b.Fatalf("RebuildVectorIndex: %v", err)
			}
			searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
			if err != nil {
				b.Fatalf("OpenVectorIndexSearcher: %v", err)
			}
			defer func() { _ = searcher.Close() }()
			opts := VectorIndexSearcherSearchOptions{Query: query, QueryMode: tc.mode, TopK: topK, EfSearch: efSearch, QuantizedRerankCandidates: tc.rerankCandidates}
			if tc.wantQuantizedStats {
				opts.QuantizedIndexName = def.QuantizedIndexes[0].Name
			}
			var buffer VectorIndexSearchBuffer
			warm, err := searcher.SearchWithBuffer(opts, &buffer)
			if err != nil {
				b.Fatalf("warm SearchWithBuffer: %v", err)
			}
			if len(warm.Results) == 0 {
				b.Fatalf("warm SearchWithBuffer returned no results")
			}
			measured, err := searcher.SearchWithBuffer(opts, &buffer)
			if err != nil {
				b.Fatalf("measure SearchWithBuffer stats: %v", err)
			}
			stats := measured.Stats
			quantizedAssetBytesPerVector := 0.0
			if tc.wantQuantizedStats {
				if stats.QuantizedScoreCalls == 0 {
					b.Fatalf("quantized stats=%+v want scalar_u8 traversal scoring", stats)
				}
				if status := searcher.reader.quantizedAssetStatus[def.QuantizedIndexes[0].Name]; status.Prepared != nil {
					quantizedAssetBytesPerVector = status.Prepared.Footprint().BytesPerVector
				}
			}
			if tc.mode == VectorIndexQueryModeQuantizedOnly {
				if stats.PreparedScoreCalls != 0 || stats.VectorBytesRead != 0 || stats.NormBytesRead != 0 || stats.QuantizedRerankExactScoreCalls != 0 {
					b.Fatalf("quantized_only stats=%+v want scalar_u8 scorer without exact scoring", stats)
				}
			} else if tc.wantExactStats && stats.PreparedScoreCalls == 0 && columnGraphTypedColumnMmapDirectViewSupportedForTest() {
				b.Fatalf("exact stats=%+v want prepared exact scorer", stats)
			}
			if tc.mode == VectorIndexQueryModeQuantizedRerank {
				if stats.QuantizedRerankCandidates == 0 || stats.QuantizedRerankExactScoreCalls != stats.QuantizedRerankCandidates || stats.VectorBytesRead == 0 || stats.NormBytesRead == 0 {
					b.Fatalf("quantized_rerank stats=%+v want quantized traversal plus exact rerank", stats)
				}
			}
			recallAt10 := recallAtKVectorIndexResults1926(measured.Results, exactWant)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := searcher.SearchWithBuffer(opts, &buffer)
				if err != nil {
					b.Fatalf("SearchWithBuffer: %v", err)
				}
				vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
			}
			b.StopTimer()
			b.ReportMetric(recallAt10, "recall_at_10")
			if quantizedAssetBytesPerVector > 0 {
				b.ReportMetric(quantizedAssetBytesPerVector, "quantized_asset_B/vector")
			}
			reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
		})
	}
}

func openColumnGraphQuantizedBenchmarkCollection1926(tb testing.TB, dims, m int, rows []columnGraphRebuildInputRowV2A) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          m,
		EfSearch:   128,
		Strategy:   VectorIndexStrategyColumnGraph,
		QuantizedIndexes: []QuantizedVectorIndexDefinition{{
			Name: "embedding.scalar_u8.fast",
		}},
	})
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	if len(rows) != 0 {
		insertColumnGraphRebuildRowsV2A(tb, col, rows)
	}
	return dir, d, col, def
}

func recallAtKVectorIndexResults1926(got []VectorIndexSearchResult, exact []columnVectorGraphNativeSearchResult) float64 {
	if len(exact) == 0 {
		return 1
	}
	seen := make(map[string]struct{}, len(got))
	for _, result := range got {
		seen[string(result.ID)] = struct{}{}
	}
	var hits int
	for _, want := range exact {
		if _, ok := seen[string(want.ID)]; ok {
			hits++
		}
	}
	return float64(hits) / float64(len(exact))
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("OpenVectorIndexSearcher: %v", err)
	}
	defer func() { _ = searcher.Close() }()
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     topK,
		EfSearch: efSearch,
	}
	var buffer VectorIndexSearchBuffer
	if _, err := searcher.SearchWithBuffer(opts, &buffer); err != nil {
		b.Fatalf("warm SearchWithBuffer: %v", err)
	}
	measuredStats, err := searcher.SearchWithBuffer(opts, &buffer)
	if err != nil {
		b.Fatalf("measure SearchWithBuffer stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column reusable-buffer benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := searcher.SearchWithBuffer(opts, &buffer)
		if err != nil {
			b.Fatalf("SearchWithBuffer: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += got.Results[0].Ordinal
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b, false, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4StatsMode2126(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelStatsModes2126(b, false, DocumentFetchOptions{})
}

func BenchmarkVectorSearchPublicSearchParallelTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithDocumentsV4(b *testing.B) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b, true, DocumentFetchOptions{})
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithDocumentsExcludeEmbedding1875(b *testing.B) {
	fetchPreset, err := ProjectionOrientedVectorDocumentFetchPresetForField("embedding")
	if err != nil {
		b.Fatalf("ProjectionOrientedVectorDocumentFetchPresetForField: %v", err)
	}
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b, fetchPreset.IncludeDocuments, fetchPreset.DocumentFetchOptions)
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b, includeDocuments, fetchOptions, VectorIndexSearchStatsModeDefault)
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelStatsModes2126(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions) {
	b.Helper()
	for _, tc := range []struct {
		name string
		mode VectorIndexSearchStatsMode
	}{
		{name: "stats=full_diagnostics", mode: VectorIndexSearchStatsModeFullDiagnostics},
		{name: "stats=production", mode: VectorIndexSearchStatsModeProduction},
	} {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b, includeDocuments, fetchOptions, tc.mode)
		})
	}
}

func benchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelWithStatsModeV4(b *testing.B, includeDocuments bool, fetchOptions DocumentFetchOptions, statsMode VectorIndexSearchStatsMode) {
	b.Helper()
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 {
		workers = columnVectorGraphNativeSearchParallelBenchMaxWorkersV3
	}
	if workers < 1 {
		workers = 1
	}
	previousGOMAXPROCS := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(previousGOMAXPROCS)
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:                query,
		TopK:                 topK,
		EfSearch:             efSearch,
		IncludeDocuments:     includeDocuments,
		DocumentFetchOptions: fetchOptions,
		StatsMode:            statsMode,
	}
	type preparedWorker struct {
		searcher *VectorIndexSearcher
	}
	benchWorkers := make([]preparedWorker, workers)
	for i := range benchWorkers {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func() { _ = searcher.Close() }()
		if _, err := searcher.Search(opts); err != nil {
			b.Fatalf("warm Search worker %d: %v", i, err)
		}
		benchWorkers[i] = preparedWorker{searcher: searcher}
	}
	measuredStats, err := benchWorkers[0].searcher.Search(opts)
	if err != nil {
		b.Fatalf("measure Search stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column parallel benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	var nextWorker atomic.Uint64
	var sink atomic.Int64
	var firstErr atomic.Value
	var failed atomic.Bool
	recordParallelErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerIndex := int(nextWorker.Add(1)) - 1
		if workerIndex < 0 || workerIndex >= len(benchWorkers) {
			recordParallelErr("parallel worker requested more than %d prepared searchers", workers)
			for pb.Next() {
			}
			return
		}
		searcher := benchWorkers[workerIndex].searcher
		var localSink int64
		for pb.Next() {
			if failed.Load() {
				continue
			}
			got, err := searcher.Search(opts)
			if err != nil {
				recordParallelErr("Search: %v", err)
				continue
			}
			if len(got.Results) == 0 {
				recordParallelErr("Search returned no results")
				continue
			}
			if includeDocuments {
				localSink += int64(len(got.Results[0].Document))
			} else {
				localSink += int64(got.Results[0].Ordinal)
			}
		}
		sink.Add(localSink)
	})
	b.StopTimer()
	reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, col.columnVectorGraphSharedPreparedSearchCacheSnapshot(), workers)
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	vectorSearchBenchSinkOrdinalV4 += int(sink.Load())
	reportVectorIndexSearchStatsModeBenchMetric2126(b, statsMode)
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkVectorSearchReusableBufferParallelTypedColumn1961(b *testing.B) {
	BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferParallelV4(b)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferParallelV4(b *testing.B) {
	const (
		rows     = 1024
		dims     = 128
		m        = 16
		topK     = 10
		efSearch = 128
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	workers := runtime.GOMAXPROCS(0)
	if workers > columnVectorGraphNativeSearchParallelBenchMaxWorkersV3 {
		workers = columnVectorGraphNativeSearchParallelBenchMaxWorkersV3
	}
	if workers < 1 {
		workers = 1
	}
	previousGOMAXPROCS := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(previousGOMAXPROCS)
	query := append([]float32(nil), input[37].vector...)
	opts := VectorIndexSearcherSearchOptions{
		Query:    query,
		TopK:     topK,
		EfSearch: efSearch,
	}
	type preparedWorker struct {
		searcher *VectorIndexSearcher
		buffer   *VectorIndexSearchBuffer
	}
	type paddedBuffer struct {
		buffer VectorIndexSearchBuffer
		_      [128]byte
	}
	benchWorkers := make([]preparedWorker, workers)
	buffers := make([]paddedBuffer, workers)
	for i := range benchWorkers {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{IndexName: def.Name, MaxDecodedBlocks: 1})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher worker %d: %v", i, err)
		}
		defer func() { _ = searcher.Close() }()
		buffer := &buffers[i].buffer
		benchWorkers[i] = preparedWorker{searcher: searcher, buffer: buffer}
		if _, err := searcher.SearchWithBuffer(opts, buffer); err != nil {
			b.Fatalf("warm SearchWithBuffer worker %d: %v", i, err)
		}
	}
	measuredStats, err := benchWorkers[0].searcher.SearchWithBuffer(opts, benchWorkers[0].buffer)
	if err != nil {
		b.Fatalf("measure SearchWithBuffer stats: %v", err)
	}
	stats := measuredStats.Stats
	if stats.TypedColumnFallbacks != 0 || stats.VectorMmapDirectViews+stats.VectorHeapCopyTypedViews+stats.VectorScratchDecodes == 0 {
		b.Fatalf("typed-column reusable-buffer parallel benchmark stats=%+v want active typed-column vector source counters", stats)
	}
	var nextWorker atomic.Uint64
	var sink atomic.Int64
	var firstErr atomic.Value
	var failed atomic.Bool
	recordParallelErr := func(format string, args ...any) {
		if failed.CompareAndSwap(false, true) {
			firstErr.Store(fmt.Sprintf(format, args...))
		}
	}
	b.SetParallelism(1)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		workerIndex := int(nextWorker.Add(1)) - 1
		if workerIndex < 0 || workerIndex >= len(benchWorkers) {
			recordParallelErr("parallel worker requested more than %d prepared searchers", workers)
			for pb.Next() {
			}
			return
		}
		worker := &benchWorkers[workerIndex]
		var localSink int64
		for pb.Next() {
			if failed.Load() {
				continue
			}
			got, err := worker.searcher.SearchWithBuffer(opts, worker.buffer)
			if err != nil {
				recordParallelErr("SearchWithBuffer: %v", err)
				continue
			}
			if len(got.Results) == 0 {
				recordParallelErr("SearchWithBuffer returned no results")
				continue
			}
			localSink += int64(got.Results[0].Ordinal)
		}
		sink.Add(localSink)
	})
	b.StopTimer()
	reportColumnVectorGraphSharedPreparedSearchBenchMetrics1735(b, col.columnVectorGraphSharedPreparedSearchCacheSnapshot(), workers)
	if errValue := firstErr.Load(); errValue != nil {
		b.Fatalf("%s", errValue.(string))
	}
	vectorSearchBenchSinkOrdinalV4 += int(sink.Load())
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, false)
}

func BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6(b *testing.B) {
	const (
		rows = 1024
		dims = 128
		m    = 16
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatalf("RebuildVectorIndex: %v", err)
	}
	statsSearcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
		IndexName:        def.Name,
		MaxDecodedBlocks: 1,
	})
	if err != nil {
		b.Fatalf("stats OpenVectorIndexSearcher: %v", err)
	}
	readerStats := statsSearcher.reader.Stats()
	stats := VectorIndexSearchStats{
		GraphRows:             uint64(readerStats.Rows),
		OpenGranulesRead:      uint64(readerStats.OpenGranulesRead),
		OpenPhysicalBytesRead: readerStats.OpenPhysicalBytesRead,
		MaxResidentBytes:      readerStats.MaxResidentBytes,
	}
	if err := statsSearcher.Close(); err != nil {
		b.Fatalf("Close stats searcher: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		searcher, err := col.OpenVectorIndexSearcher(VectorIndexSearcherOptions{
			IndexName:        def.Name,
			MaxDecodedBlocks: 1,
		})
		if err != nil {
			b.Fatalf("OpenVectorIndexSearcher: %v", err)
		}
		vectorSearchBenchSinkOrdinalV4 += searcher.reader.RowCount()
		if err := searcher.Close(); err != nil {
			b.Fatalf("Close searcher: %v", err)
		}
	}
	b.StopTimer()
	reportVectorIndexSearchBenchMetricsV4(b, b.N, stats, true)
}

func reportVectorIndexSearchStatsModeBenchMetric2126(b *testing.B, mode VectorIndexSearchStatsMode) {
	b.Helper()
	switch mode {
	case VectorIndexSearchStatsModeDefault:
		b.ReportMetric(1, "stats_mode_full_diagnostics")
	case VectorIndexSearchStatsModeMinimal:
		b.ReportMetric(1, "stats_mode_minimal")
	case VectorIndexSearchStatsModeProduction:
		b.ReportMetric(1, "stats_mode_production")
	case VectorIndexSearchStatsModeFullDiagnostics:
		b.ReportMetric(1, "stats_mode_full_diagnostics")
	case VectorIndexSearchStatsModeBenchmarkDebug:
		b.ReportMetric(1, "stats_mode_benchmark_debug")
	}
}

func reportVectorIndexSearchBenchMetricsV4(b *testing.B, n int, stats VectorIndexSearchStats, includeOpenPerOp bool) {
	b.Helper()
	if n <= 0 {
		return
	}
	if elapsed := b.Elapsed(); elapsed > 0 {
		b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/sec")
	}
	// Callers pass one representative search/setup sample captured outside the
	// timer; these labels are intentionally per-search or per-open, not averaged
	// over b.N. Keep aggregation out of the hot benchmark loop.
	b.ReportMetric(float64(stats.GraphRows), "graph_rows")
	b.ReportMetric(float64(stats.CandidateRows), "candidate_rows/search")
	b.ReportMetric(float64(stats.Candidates), "candidates/search")
	b.ReportMetric(float64(stats.Edges), "edges/search")
	b.ReportMetric(float64(stats.VisitedNodes), "visited_nodes/search")
	b.ReportMetric(float64(stats.VisitedEdges), "visited_edges/search")
	b.ReportMetric(float64(stats.VectorBytesRead), "vector_B/search")
	b.ReportMetric(float64(stats.NormBytesRead), "norm_B/search")
	b.ReportMetric(float64(stats.VectorBytesRead+stats.NormBytesRead), "exact_vector_norm_B/search")
	b.ReportMetric(float64(stats.AdjacencyBytesRead), "adjacency_B/search")
	if stats.Candidates > 0 {
		b.ReportMetric(float64(stats.Edges)/float64(stats.Candidates), "edges/node")
	}
	b.ReportMetric(float64(stats.CandidateFetches), "candidate_fetches/search")
	b.ReportMetric(float64(stats.ScoreBatchCalls), "score_batch_calls/search")
	b.ReportMetric(float64(stats.ScoreBatchCandidates), "score_batch_candidates/search")
	b.ReportMetric(float64(stats.ScoreBatchMaxTileSize), "score_batch_max_tile_size")
	b.ReportMetric(float64(stats.ScoreBatchOptimizedCalls), "score_batch_optimized/search")
	b.ReportMetric(float64(stats.ScoreBatchScalarFallbackCalls), "score_batch_fallback/search")
	b.ReportMetric(float64(stats.PreparedScoreCalls), "prepared_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedScoreCalls), "quantized_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedCodeBytesRead), "quantized_code_B/search")
	b.ReportMetric(float64(stats.QuantizedRerankCandidates), "quantized_rerank_candidates/search")
	b.ReportMetric(float64(stats.QuantizedRerankExactScoreCalls), "quantized_rerank_exact_score_calls/search")
	b.ReportMetric(float64(stats.QuantizedScorerActive), "quantized_scorer_active/search")
	b.ReportMetric(float64(stats.QuantizedAssetMissing), "quantized_asset_missing/search")
	b.ReportMetric(float64(stats.QuantizedAssetInvalid), "quantized_asset_invalid/search")
	b.ReportMetric(float64(stats.QuantizedAssetStale), "quantized_asset_stale/search")
	b.ReportMetric(float64(stats.QuantizedAssetClosed), "quantized_asset_closed/search")
	b.ReportMetric(float64(stats.QuantizedAssetUnavailable), "quantized_asset_unavailable/search")
	b.ReportMetric(float64(stats.QuantizedAssetMmapDirect), "quantized_asset_mmap_direct/search")
	b.ReportMetric(float64(stats.QuantizedAssetHeapCopy), "quantized_asset_heap_copy/search")
	b.ReportMetric(float64(stats.QuantizedAssetOpenNanos), "quantized_asset_open_ns")
	b.ReportMetric(float64(stats.QuantizedAssetMappedBytes), "quantized_asset_mapped_B")
	b.ReportMetric(float64(stats.QuantizedAssetHeapCopyBytes), "quantized_asset_heap_copy_B")
	b.ReportMetric(float64(stats.QuantizedAssetActiveHandles), "quantized_asset_active_handles")
	b.ReportMetric(float64(stats.ScoreFloat64Fallbacks), "score_float64_fallbacks/search")
	if stats.ScoreBatchCalls > 0 {
		b.ReportMetric(float64(stats.ScoreBatchCandidates)/float64(stats.ScoreBatchCalls), "score_batch_avg_tile_size")
	}
	if stats.BenchmarkDebugSearches > 0 {
		reportVectorIndexSearchBenchmarkDebugMetrics2105(b, stats)
	}
	b.ReportMetric(float64(stats.ExpansionFetches), "expansion_fetches/search")
	b.ReportMetric(float64(stats.ResultFetches), "result_fetches/search")
	b.ReportMetric(float64(stats.VectorDirectViews), "vector_direct_views/search")
	b.ReportMetric(float64(stats.VectorMmapDirectViews), "vector_mmap_direct/search")
	b.ReportMetric(float64(stats.VectorHeapCopyTypedViews), "vector_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.VectorScratchDecodes), "vector_scratch_decode/search")
	b.ReportMetric(float64(stats.VectorScratchDecodes), "vector_scratch_decodes/search")
	b.ReportMetric(float64(stats.VectorPreparedDirectViews), "vector_prepared_direct/search")
	b.ReportMetric(float64(stats.VectorPreparedIdentityMappings), "vector_prepared_identity_mapping/search")
	b.ReportMetric(float64(stats.VectorPreparedRowRefMappings), "vector_prepared_row_ref_mapping/search")
	b.ReportMetric(float64(stats.VectorCertificationFailures), "vector_certification_failures/search")
	b.ReportMetric(float64(stats.VectorAbsoluteOffsetUnaligned), "vector_absolute_offset_unaligned/search")
	b.ReportMetric(float64(stats.VectorActualPointerUnaligned), "vector_actual_pointer_unaligned/search")
	b.ReportMetric(float64(stats.VectorStaleHandles), "vector_stale_handles/search")
	b.ReportMetric(float64(stats.AdjacencyDirectViews), "adjacency_direct_views/search")
	b.ReportMetric(float64(stats.AdjacencyMmapDirectViews), "adjacency_mmap_direct/search")
	b.ReportMetric(float64(stats.AdjacencyHeapCopyTypedViews), "adjacency_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.AdjacencyPreparedCSRDirectViews), "adjacency_prepared_csr_direct_views/search")
	b.ReportMetric(float64(stats.AdjacencyPreparedCSRMmapDirectViews), "adjacency_prepared_csr_mmap_direct/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListDirectViews), "adjacency_typed_list_direct_views/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListMmapDirectViews), "adjacency_typed_list_mmap_direct/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListHeapCopyTypedViews), "adjacency_typed_list_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.AdjacencyTypedListScratchDecodes), "adjacency_typed_list_scratch_decodes/search")
	b.ReportMetric(float64(stats.AdjacencyLegacyFallbacks), "adjacency_legacy_fallbacks/search")
	b.ReportMetric(float64(stats.AdjacencySourceUnavailable), "adjacency_source_unavailable/search")
	b.ReportMetric(float64(stats.AdjacencySourceFallbacks), "adjacency_source_fallbacks/search")
	b.ReportMetric(float64(stats.AdjacencyCertificationFailures), "adjacency_certification_failures/search")
	b.ReportMetric(float64(stats.AdjacencyValidationFailures), "adjacency_validation_failures/search")
	b.ReportMetric(float64(stats.AdjacencyAbsoluteOffsetUnaligned), "adjacency_absolute_offset_unaligned/search")
	b.ReportMetric(float64(stats.AdjacencyActualPointerUnaligned), "adjacency_actual_pointer_unaligned/search")
	b.ReportMetric(float64(stats.AdjacencyStaleHandles), "adjacency_stale_handles/search")
	b.ReportMetric(float64(stats.AdjacencyScratchDecodes), "adjacency_scratch_decode/search")
	b.ReportMetric(float64(stats.AdjacencyScratchDecodes), "adjacency_scratch_decodes/search")
	b.ReportMetric(float64(stats.NormDirectViews), "norm_direct_views/search")
	b.ReportMetric(float64(stats.NormMmapDirectViews), "norm_mmap_direct/search")
	b.ReportMetric(float64(stats.NormHeapCopyTypedViews), "norm_heap_copy_typed_view/search")
	b.ReportMetric(float64(stats.NormScratchDecodes), "norm_scratch_decode/search")
	b.ReportMetric(float64(stats.NormScratchDecodes), "norm_scratch_decodes/search")
	b.ReportMetric(float64(stats.NormPreparedDirectViews), "norm_prepared_direct/search")
	b.ReportMetric(float64(stats.NormSourceUnavailable), "norm_source_unavailable/search")
	b.ReportMetric(float64(stats.NormSourceFallbacks), "norm_source_fallbacks/search")
	b.ReportMetric(float64(stats.NormValidationFailures), "norm_validation_failures/search")
	b.ReportMetric(float64(stats.NormAbsoluteOffsetUnaligned), "norm_absolute_offset_unaligned/search")
	b.ReportMetric(float64(stats.NormActualPointerUnaligned), "norm_actual_pointer_unaligned/search")
	b.ReportMetric(float64(stats.NormStaleHandles), "norm_stale_handles/search")
	b.ReportMetric(float64(stats.NormMappedBytes), "norm_mapped_B")
	b.ReportMetric(float64(stats.NormHeapCopyBytes), "norm_heap_copy_B")
	b.ReportMetric(float64(stats.NormDecodedBytes), "norm_decoded_B")
	b.ReportMetric(float64(stats.NormActiveHandles), "norm_active_handles")
	b.ReportMetric(float64(stats.NormDeniedResources), "norm_denied_resources")
	if stats.VectorMmapDirectViews > 0 && stats.TypedColumnMappedBytes > 0 {
		b.ReportMetric(1, "typed_column_vector_source_mmap")
	}
	if stats.VectorHeapCopyTypedViews > 0 && stats.TypedColumnHeapCopyBytes > 0 {
		b.ReportMetric(1, "typed_column_vector_source_heap_copy")
	}
	if stats.VectorScratchDecodes > 0 && stats.TypedColumnDecodedBytes > 0 {
		b.ReportMetric(1, "typed_column_vector_source_scratch")
	}
	if stats.TypedColumnFallbacks > 0 {
		b.ReportMetric(1, "typed_column_vector_source_fallback")
	}
	b.ReportMetric(float64(stats.TypedColumnMappedBytes), "typed_column_mapped_B")
	b.ReportMetric(float64(stats.TypedColumnHeapCopyBytes), "typed_column_heap_copy_B")
	b.ReportMetric(float64(stats.TypedColumnDecodedBytes), "typed_column_decoded_derived_B")
	b.ReportMetric(float64(stats.TypedColumnActiveHandles), "typed_column_active_handles")
	b.ReportMetric(float64(stats.TypedColumnDeniedResources), "typed_column_denied_resources")
	b.ReportMetric(float64(stats.TypedColumnFallbacks), "typed_column_vector_fallbacks/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceState), "row_ref_vector_source_state/search")
	b.ReportMetric(float64(stats.RowRefVectorSourceLegacyGraphIDs), "row_ref_vector_source_legacy_graph_ids/search")
	b.ReportMetric(float64(stats.RowRefStatePreparedViews), "row_ref_state_prepared_views/search")
	b.ReportMetric(float64(stats.RowRefStateMmapDirectFields), "row_ref_state_mmap_direct_fields/search")
	b.ReportMetric(float64(stats.RowRefStateResultRefs), "row_ref_state_result_refs/search")
	b.ReportMetric(float64(stats.RowRefStateSourceUnavailable), "row_ref_state_source_unavailable/search")
	b.ReportMetric(float64(stats.RowRefStateSourceFallbacks), "row_ref_state_source_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDPreparedBytesViews), "result_id_prepared_bytes_views/search")
	b.ReportMetric(float64(stats.ResultIDTypedBytesState), "result_id_typed_bytes_state/search")
	b.ReportMetric(float64(stats.ResultIDGraphFallbacks), "result_id_graph_fallbacks/search")
	b.ReportMetric(float64(stats.ResultIDStateValidationFailures), "result_id_state_validation_failures/search")
	b.ReportMetric(float64(stats.PreparedGraphSearchViews), "prepared_graph_search_views/search")
	b.ReportMetric(float64(stats.GraphRowFallbacks), "graph_row_fallbacks/search")
	b.ReportMetric(float64(stats.SearchRouteColumnGraphPrepared), "search_route_column_graph_prepared/search")
	b.ReportMetric(float64(stats.SearchRouteColumnGraphFallback), "search_route_column_graph_fallback/search")
	b.ReportMetric(float64(stats.SearchRouteHNSWSearchPack), "search_route_hnsw_search_pack/search")
	b.ReportMetric(float64(stats.SearchRouteQuantizedOnly), "search_route_quantized_only/search")
	b.ReportMetric(float64(stats.SearchRouteQuantizedRerank), "search_route_quantized_rerank/search")
	b.ReportMetric(float64(stats.HNSWSearchPackActive), "hnsw_search_pack_active/search")
	b.ReportMetric(float64(stats.HNSWSearchPackMissing), "hnsw_search_pack_missing/search")
	b.ReportMetric(float64(stats.HNSWSearchPackInvalid), "hnsw_search_pack_invalid/search")
	b.ReportMetric(float64(stats.HNSWSearchPackStale), "hnsw_search_pack_stale/search")
	b.ReportMetric(float64(stats.HNSWSearchPackClosed), "hnsw_search_pack_closed/search")
	b.ReportMetric(float64(stats.HNSWSearchPackFallbacks), "hnsw_search_pack_fallbacks/search")
	b.ReportMetric(float64(stats.HNSWSearchPackMmapDirect), "hnsw_search_pack_mmap_direct/search")
	b.ReportMetric(float64(stats.HNSWSearchPackHeapCopy), "hnsw_search_pack_heap_copy/search")
	b.ReportMetric(float64(stats.HNSWSearchPackOpenNanos), "hnsw_search_pack_open_ns")
	b.ReportMetric(float64(stats.HNSWSearchPackMappedBytes), "hnsw_search_pack_mapped_B")
	b.ReportMetric(float64(stats.HNSWSearchPackHeapCopyBytes), "hnsw_search_pack_heap_copy_B")
	b.ReportMetric(float64(stats.HNSWSearchPackActiveHandles), "hnsw_search_pack_active_handles")
	// Collection prepared-cache hit/miss/build/wait counters are intentionally
	// omitted here: this generic reporter consumes one pre-timer sample, and some
	// collection benchmarks sample the initial miss/build while their timed loops
	// measure steady-state hits. Public per-call stats and diagnostics still
	// expose hnsw_search_pack_cache_* directly for focused tests and callers.
	b.ReportMetric(float64(stats.OpenSearcherCalls), "open_searcher_calls/search")
	b.ReportMetric(float64(stats.OpenSetupInTimedLoop), "open_setup_in_timed_loop/search")
	b.ReportMetric(float64(stats.ResponseOwnedResultAllocs), "response_owned_result_allocs/search")
	scoreBatchFallbackReasonScalar := 0.0
	if stats.ScoreBatchScalarFallbackCalls > 0 {
		scoreBatchFallbackReasonScalar = 1
	}
	b.ReportMetric(scoreBatchFallbackReasonScalar, "score_batch_fallback_reason_scalar")
	scoreBatchFallbackReasonNone := 0.0
	if stats.ScoreBatchCalls > 0 && stats.ScoreBatchScalarFallbackCalls == 0 {
		scoreBatchFallbackReasonNone = 1
	}
	b.ReportMetric(scoreBatchFallbackReasonNone, "score_batch_fallback_reason_none")
	b.ReportMetric(float64(stats.RowFetches), "row_fetches/search")
	b.ReportMetric(float64(stats.BatchFetches), "batch_fetches/search")
	b.ReportMetric(float64(stats.RowsFetched), "rows_fetched/search")
	b.ReportMetric(float64(stats.CacheHits), "cache_hits/search")
	b.ReportMetric(float64(stats.CacheMisses), "cache_misses/search")
	if cacheLookups := stats.CacheHits + stats.CacheMisses; cacheLookups > 0 {
		b.ReportMetric(float64(stats.CacheHits)/float64(cacheLookups), "cache_hit_ratio")
	}
	b.ReportMetric(float64(stats.DecodedBlocks), "decoded_blocks/search")
	b.ReportMetric(float64(stats.GranulesTouched), "granules_touched/search")
	b.ReportMetric(float64(stats.PhysicalBytesRead), "physical_B/search")
	b.ReportMetric(float64(stats.MaxResidentBytes), "max_resident_B")
	if includeOpenPerOp {
		b.ReportMetric(float64(stats.OpenGranulesRead), "open_granules/op")
		b.ReportMetric(float64(stats.OpenPhysicalBytesRead), "open_physical_B/op")
	}
	if stats.OpenGranulesRead > 0 {
		b.ReportMetric(float64(stats.OpenGranulesRead), "max_open_granules")
	}
	if stats.OpenPhysicalBytesRead > 0 {
		b.ReportMetric(float64(stats.OpenPhysicalBytesRead), "max_open_physical_B")
	}
	b.ReportMetric(float64(stats.DocumentsFetched), "docs_fetched/search")
	b.ReportMetric(float64(stats.DocumentsMissing), "docs_missing/search")
	b.ReportMetric(float64(stats.DocumentBytes), "doc_B/search")
	b.ReportMetric(float64(stats.DocumentOutputBytes), "output_B/search")
	b.ReportMetric(float64(stats.DocumentFieldsReconstructed), "fields_reconstructed/search")
	b.ReportMetric(float64(stats.DocumentFieldsSkipped), "fields_skipped/search")
	b.ReportMetric(float64(stats.DocumentFetchNanos), "doc_fetch_ns/search")
	b.ReportMetric(float64(stats.DocumentRetainedFetches), "doc_retained_fetches/search")
	b.ReportMetric(float64(stats.DocumentRetainedBytes), "doc_retained_B/search")
	b.ReportMetric(float64(stats.DocumentVisibilityScans), "doc_visibility_scans/search")
	b.ReportMetric(float64(stats.DocumentVisibilityRowsScanned), "doc_visibility_rows_scanned/search")
	b.ReportMetric(float64(stats.DocumentVisibilityRows), "doc_visibility_rows/search")
	b.ReportMetric(float64(stats.DocumentVisibilityPhysicalBytes), "doc_visibility_physical_B/search")
	b.ReportMetric(float64(stats.DocumentVisibilityNanos), "doc_visibility_ns/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnRows), "doc_typed_column_rows/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnCacheHits), "doc_typed_column_cache_hits/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnCacheMisses), "doc_typed_column_cache_misses/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnPartLoads), "doc_typed_column_part_loads/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnPartDecodes), "doc_typed_column_part_decodes/search")
	b.ReportMetric(float64(stats.DocumentTypedColumnNanos), "doc_typed_column_ns/search")
	b.ReportMetric(float64(stats.DocumentJSONReconstructionRows), "doc_json_reconstruction_rows/search")
	b.ReportMetric(float64(stats.DocumentJSONReconstructionNanos), "doc_json_reconstruction_ns/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorBuilds), "doc_row_locator_builds/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorLookups), "doc_row_locator_lookups/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorMisses), "doc_row_locator_misses/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorRowsScanned), "doc_row_locator_rows_scanned/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorPhysicalBytes), "doc_row_locator_physical_B/search")
	b.ReportMetric(float64(stats.DocumentRowLocatorNanos), "doc_row_locator_ns/search")
	b.ReportMetric(float64(stats.DocumentRowRefStateFetches), "doc_row_ref_state_fetches/search")
	b.ReportMetric(float64(stats.DocumentRowRefLookupFallbacks), "doc_row_ref_lookup_fallbacks/search")
	b.ReportMetric(float64(stats.DocumentPointRowFetches), "doc_point_row_fetches/search")
	b.ReportMetric(float64(stats.DocumentPointRowDecodes), "doc_point_row_decodes/search")
	b.ReportMetric(float64(stats.DocumentRowRefFallbackScans), "doc_row_ref_fallback_scans/search")
	b.ReportMetric(float64(stats.DocumentRowRefUnsupported), "doc_row_ref_unsupported/search")
	b.ReportMetric(float64(stats.DocumentRowRefValidationFailures), "doc_row_ref_validation_failures/search")
	b.ReportMetric(float64(stats.DocumentAssetMmapHits), "doc_asset_mmap_hits/search")
	b.ReportMetric(float64(stats.DocumentAssetReadAtFallbacks), "doc_asset_readat_fallbacks/search")
	b.ReportMetric(float64(stats.DocumentAssetFileOpens), "doc_asset_file_opens/search")
	b.ReportMetric(float64(stats.DocumentAssetFileCloses), "doc_asset_file_closes/search")
	b.ReportMetric(float64(stats.DocumentAssetActiveHandles), "doc_asset_active_handles")
}

var vectorSearchBenchSinkOrdinalV4 int
