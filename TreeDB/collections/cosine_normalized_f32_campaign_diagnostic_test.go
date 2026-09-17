package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

const (
	cosineNormalizedF32CampaignDBEnv      = "TREEDB_NORMALIZED_V4_DIAGNOSTIC_DB"
	cosineNormalizedF32CampaignDataEnv    = "TREEDB_NORMALIZED_V4_DIAGNOSTIC_DATA"
	cosineNormalizedF32CampaignServingEnv = "TREEDB_NORMALIZED_V4_DIAGNOSTIC_SERVING"
	cosineNormalizedF32CampaignOutputEnv  = "TREEDB_NORMALIZED_V4_DIAGNOSTIC_OUTPUT"
	cosineNormalizedF32CampaignCommitEnv  = "TREEDB_NORMALIZED_V4_SOURCE_COMMIT"
	cosineNormalizedF32CampaignIndexEnv   = "TREEDB_NORMALIZED_V4_INDEX"
	cosineNormalizedF32CampaignCodeEnv    = "TREEDB_NORMALIZED_V4_QUANTIZED_INDEX"
	cosineNormalizedF32CampaignRowsEnv    = "TREEDB_NORMALIZED_V4_ROWS"

	cosineNormalizedF32CampaignDimensions  = 768
	cosineNormalizedF32CampaignQueries     = 200
	cosineNormalizedF32CampaignTopK        = 10
	cosineNormalizedF32CampaignWidth       = 64
	cosineNormalizedF32CampaignRepetitions = 6
)

type cosineNormalizedF32CampaignManifest struct {
	Dataset                string `json:"dataset"`
	Rows                   int    `json:"rows"`
	Dimensions             int    `json:"dimensions"`
	QueryCount             int    `json:"query_count"`
	TopK                   int    `json:"top_k"`
	ExactTrainQueryOverlap int    `json:"exact_train_query_overlap"`
	DocumentsSHA           string `json:"documents_sha256"`
	QueriesSHA             string `json:"queries_sha256"`
	TruthSHA               string `json:"truth_sha256"`
	ExporterSHA            string `json:"exporter_sha256"`
	Filter                 string `json:"filter"`
	QuerySource            string `json:"query_source"`
	TrainSource            string `json:"train_source"`
	Truth                  string `json:"truth"`
}

type cosineNormalizedF32CampaignCandidateWork struct {
	QuantizedScoreCalls    uint64 `json:"quantized_score_calls"`
	QuantizedCodeBytesRead uint64 `json:"quantized_code_bytes_read"`
	PreparedGraphViews     uint64 `json:"prepared_graph_search_views"`
}

type cosineNormalizedF32CampaignShortlist struct {
	Query            int                                      `json:"query"`
	Ordinals         []int                                    `json:"ordinals"`
	CandidateWork    cosineNormalizedF32CampaignCandidateWork `json:"candidate_work"`
	PackedScoreCalls uint64                                   `json:"packed_score_calls"`
	PackedCandidates uint64                                   `json:"packed_score_candidates"`
	PackedBytesRead  uint64                                   `json:"packed_vector_bytes_read"`
}

type cosineNormalizedF32CampaignBenchmark struct {
	Ordinal        int     `json:"ordinal"`
	ArmOrder       int     `json:"arm_order"`
	Iterations     int     `json:"iterations"`
	ElapsedNanos   int64   `json:"elapsed_nanos"`
	NanosPerQuery  int64   `json:"nanos_per_query"`
	QPS            float64 `json:"qps"`
	BytesPerQuery  int64   `json:"bytes_per_query"`
	AllocsPerQuery int64   `json:"allocs_per_query"`
}

type cosineNormalizedF32CampaignDiagnostic struct {
	Schema                    string                                 `json:"schema"`
	SourceCommit              string                                 `json:"source_commit"`
	GoVersion                 string                                 `json:"go_version"`
	GOMAXPROCS                int                                    `json:"gomaxprocs"`
	Rows                      int                                    `json:"rows"`
	Dimensions                int                                    `json:"dimensions"`
	QueryCount                int                                    `json:"query_count"`
	TopK                      int                                    `json:"top_k"`
	EFSearch                  int                                    `json:"ef_search"`
	RerankCandidates          int                                    `json:"rerank_candidates"`
	Representation            VectorIndexRepresentation              `json:"representation"`
	Index                     string                                 `json:"index"`
	QuantizedIndex            string                                 `json:"quantized_index"`
	CollectionGeneration      uint64                                 `json:"collection_generation"`
	Dataset                   cosineNormalizedF32CampaignManifest    `json:"dataset"`
	DatasetManifestSHA256     string                                 `json:"dataset_manifest_sha256"`
	QueriesSHA256             string                                 `json:"queries_sha256"`
	ServingSHA256             string                                 `json:"serving_sha256"`
	ServingOwner              ColumnGraphServingStats                `json:"serving_owner"`
	Shortlists                []cosineNormalizedF32CampaignShortlist `json:"shortlists"`
	CandidateOnly             []cosineNormalizedF32CampaignBenchmark `json:"candidate_only"`
	PackedSameShortlist       []cosineNormalizedF32CampaignBenchmark `json:"packed_same_shortlist"`
	StableDuplicateScoreCalls uint64                                 `json:"stable_duplicate_score_calls"`
}

var (
	cosineNormalizedF32CampaignLinkedCommit string
	cosineNormalizedF32CampaignSink         int
)

func TestCosineNormalizedF32CampaignJSON(t *testing.T) {
	// Serialization-only example, deliberately not a qualifying campaign.
	report := cosineNormalizedF32CampaignDiagnostic{
		Schema:       "treedb_cosine_normalized_f32_campaign_engine/v1",
		SourceCommit: strings.Repeat("a", 40),
		Rows:         8, Dimensions: 768, QueryCount: 1, TopK: 10,
		EFSearch: 64, RerankCandidates: 64,
		Representation: VectorIndexRepresentationCosineNormalizedF32V1,
		Index:          "minima_cohere", QuantizedIndex: "minima_sq8", CollectionGeneration: 7,
		DatasetManifestSHA256: strings.Repeat("b", 64),
		QueriesSHA256:         strings.Repeat("c", 64), ServingSHA256: strings.Repeat("d", 64),
		Shortlists:          []cosineNormalizedF32CampaignShortlist{{Query: 0, Ordinals: []int{3, 1}}},
		CandidateOnly:       make([]cosineNormalizedF32CampaignBenchmark, 6),
		PackedSameShortlist: make([]cosineNormalizedF32CampaignBenchmark, 6),
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/cosine_normalized_f32_campaign.json")
	if err != nil {
		t.Fatal(err)
	}
	var decoded cosineNormalizedF32CampaignDiagnostic
	if err := json.Unmarshal(want, &decoded); err != nil {
		t.Fatal(err)
	}
	roundtrip, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, roundtrip) {
		t.Fatal("campaign serializer fields differ from the shared Python fixture")
	}
}

// TestCosineNormalizedF32V1CampaignEngineDiagnostic is an opt-in evidence seam.
// It opens an already-built campaign database through normal serving admission,
// captures the production SQ8 shortlist for every frozen query, and benchmarks
// candidate generation and packed rerank separately over those exact ordinals.
// It creates no alternate public search route and never mutates the database.
func TestCosineNormalizedF32V1CampaignEngineDiagnostic(t *testing.T) {
	dbDir := os.Getenv(cosineNormalizedF32CampaignDBEnv)
	dataDir := os.Getenv(cosineNormalizedF32CampaignDataEnv)
	servingPath := os.Getenv(cosineNormalizedF32CampaignServingEnv)
	outputPath := os.Getenv(cosineNormalizedF32CampaignOutputEnv)
	sourceCommit := os.Getenv(cosineNormalizedF32CampaignCommitEnv)
	index := os.Getenv(cosineNormalizedF32CampaignIndexEnv)
	quantizedIndex := os.Getenv(cosineNormalizedF32CampaignCodeEnv)
	rowsRaw := os.Getenv(cosineNormalizedF32CampaignRowsEnv)
	if dbDir == "" && dataDir == "" && servingPath == "" && outputPath == "" && sourceCommit == "" && index == "" && quantizedIndex == "" && rowsRaw == "" {
		t.Skip("opt-in normalized-v4 campaign engine diagnostic")
	}
	if dbDir == "" || dataDir == "" || servingPath == "" || outputPath == "" || sourceCommit == "" || index == "" || quantizedIndex == "" || rowsRaw == "" {
		t.Fatal("normalized-v4 campaign diagnostic requires every TREEDB_NORMALIZED_V4_* input")
	}
	campaignRows, err := strconv.Atoi(rowsRaw)
	if err != nil || campaignRows < 5000 {
		t.Fatalf("normalized-v4 campaign diagnostic rows=%q must be an integer >= 5000", rowsRaw)
	}
	if len(sourceCommit) != 40 {
		t.Fatal("normalized-v4 campaign diagnostic requires a full source commit")
	}
	if cosineNormalizedF32CampaignLinkedCommit != sourceCommit {
		t.Fatalf("linked source commit=%q want %q", cosineNormalizedF32CampaignLinkedCommit, sourceCommit)
	}

	manifestPath := filepath.Join(dataDir, "manifest.json")
	var manifest cosineNormalizedF32CampaignManifest
	readCosineNormalizedF32CampaignJSON(t, manifestPath, &manifest)
	if manifest.Rows < campaignRows || manifest.Dimensions != cosineNormalizedF32CampaignDimensions || manifest.QueryCount != cosineNormalizedF32CampaignQueries {
		t.Fatalf("campaign dataset shape=%+v", manifest)
	}
	queriesPath := filepath.Join(dataDir, "queries.f32")
	if got := cosineNormalizedF32CampaignFileSHA256(t, queriesPath); got != manifest.QueriesSHA {
		t.Fatalf("query file hash=%s want %s", got, manifest.QueriesSHA)
	}
	queries := readCosineNormalizedF32CampaignQueries(t, queriesPath, manifest.QueryCount, manifest.Dimensions)

	var serving ColumnGraphServingOptions
	readCosineNormalizedF32CampaignJSON(t, servingPath, &serving)
	if err := ValidateColumnGraphServingOptions(serving); err != nil {
		t.Fatal(err)
	}
	if serving.SearchCandidates <= cosineNormalizedF32CampaignWidth*2 {
		t.Fatalf("serving search-candidate budget=%d cannot preserve the production E=R=64 route", serving.SearchCandidates)
	}

	database, cleanup, _, _, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(
		treedb.OptionsFor(treedb.ProfileCommandWALDurable, dbDir),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Errorf("close campaign database: %v", err)
		}
	}()
	manager := NewCollectionManager(database)
	collection, err := manager.OpenCollection(index)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.EnsureColumnGraphServing(t.Context(), "embedding", serving); err != nil {
		t.Fatal(err)
	}
	owner, err := collection.openTypedGraphReadOwner(serving.Owners)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Close(); err != nil {
			t.Errorf("close campaign owner: %v", err)
		}
	}()
	if owner.overlay == nil || owner.overlay.base == nil || owner.overlay.base.reader == nil || owner.overlay.pack == nil {
		t.Fatal("campaign serving owner omitted its prepared graph")
	}
	definition := owner.overlay.base.reader.def
	if definition.Representation != VectorIndexRepresentationCosineNormalizedF32V1 || definition.Dimensions != manifest.Dimensions {
		t.Fatalf("campaign representation/dimensions=(%q,%d)", definition.Representation, definition.Dimensions)
	}
	if err := owner.attachTypedGraphLegacyScalarU8QuantizedAssetWithContext(t.Context(), quantizedIndex); err != nil {
		t.Fatal(err)
	}
	servingOwner, available := collection.ColumnGraphServingSnapshot()
	if !available || !servingOwner.ServingReady || !servingOwner.PublicationUnchanged || servingOwner.BaseRows != campaignRows {
		t.Fatalf("campaign serving inventory unavailable: available=%t owner=%+v", available, servingOwner)
	}
	snapshot := owner.querySnapshot()
	if snapshot.SchemaGeneration == 0 ||
		snapshot.BaseManifest.Generation != servingOwner.BaseManifest.Generation ||
		snapshot.BaseManifest.Checksum != servingOwner.BaseManifest.Checksum ||
		snapshot.CurrentManifest.Generation != servingOwner.CurrentManifest.Generation ||
		snapshot.CurrentManifest.Checksum != servingOwner.CurrentManifest.Checksum ||
		snapshot.CurrentCoverageLSN != servingOwner.CurrentCoverageLSN {
		t.Fatal("campaign serving inventory differs from the captured query owner")
	}

	canonicalQueries := make([][]float32, len(queries))
	shortlists := make([]cosineNormalizedF32CampaignShortlist, len(queries))
	var candidateScratch columnVectorGraphNativeSearchScratch
	for queryOrdinal, query := range queries {
		canonical, err := normalizeCosineNormalizedF32V1(query, manifest.Dimensions)
		if err != nil {
			t.Fatalf("normalize query %d: %v", queryOrdinal, err)
		}
		canonicalQueries[queryOrdinal] = canonical
		raw, stats, err := owner.overlay.searchScalarU8PreparedCandidatesWithContext(t.Context(), canonical, typedGraphScalarU8TraversalOptions{
			TopK: cosineNormalizedF32CampaignWidth, EfSearch: cosineNormalizedF32CampaignWidth,
			ScoreBudget:              serving.SearchCandidates - cosineNormalizedF32CampaignWidth,
			QuantizedIndexName:       quantizedIndex,
			StatsMode:                columnVectorGraphNativeSearchStatsModeMinimal,
			CanonicalNormalizedQuery: true,
		}, &candidateScratch)
		if err != nil {
			t.Fatalf("capture candidates for query %d: %v", queryOrdinal, err)
		}
		if len(raw) != cosineNormalizedF32CampaignWidth || stats.QuantizedScoreCalls == 0 || stats.QuantizedCodeBytesRead == 0 {
			t.Fatalf("query %d candidate result/work=%d/%+v", queryOrdinal, len(raw), stats)
		}
		ordinals := make([]int, len(raw))
		for i, candidate := range raw {
			ordinals[i] = candidate.Ordinal
		}
		if !cosineNormalizedF32CampaignOrdinalsValid(ordinals, campaignRows) {
			t.Fatalf("query %d produced invalid candidate ordinals", queryOrdinal)
		}
		var rerankScratch columnVectorGraphNativeSearchScratch
		fillCosineNormalizedF32CampaignShortlist(&rerankScratch, ordinals)
		var packed columnVectorGraphNativeSearchStats
		if err := owner.overlay.pack.exactRerankPreparedTraversalRowIDCandidatesWithQuery(
			canonical, true, false, cosineNormalizedF32CampaignTopK,
			cosineNormalizedF32CampaignWidth, columnVectorGraphScoreBatchModeDefault,
			&rerankScratch, &packed,
		); err != nil {
			t.Fatalf("packed rerank query %d: %v", queryOrdinal, err)
		}
		wantBytes := uint64(cosineNormalizedF32CampaignWidth * manifest.Dimensions * 4)
		if packed.PackedExactScoreCalls != 1 || packed.PackedExactScoreCandidates != cosineNormalizedF32CampaignWidth ||
			packed.PackedExactVectorBytesRead != wantBytes || packed.NormBytesRead != 0 ||
			packed.FP32ScoreCalls != cosineNormalizedF32CampaignWidth {
			t.Fatalf("query %d packed work=%+v", queryOrdinal, packed)
		}
		shortlists[queryOrdinal] = cosineNormalizedF32CampaignShortlist{
			Query: queryOrdinal, Ordinals: ordinals,
			CandidateWork: cosineNormalizedF32CampaignCandidateWork{
				QuantizedScoreCalls:    stats.QuantizedScoreCalls,
				QuantizedCodeBytesRead: stats.QuantizedCodeBytesRead,
				PreparedGraphViews:     stats.PreparedGraphSearchViews,
			},
			PackedScoreCalls: packed.PackedExactScoreCalls,
			PackedCandidates: packed.PackedExactScoreCandidates,
			PackedBytesRead:  packed.PackedExactVectorBytesRead,
		}
	}

	candidateBenchmark := func() testing.BenchmarkResult {
		var scratch columnVectorGraphNativeSearchScratch
		return testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				query := canonicalQueries[i%len(canonicalQueries)]
				raw, stats, err := owner.overlay.searchScalarU8PreparedCandidatesWithContext(context.Background(), query, typedGraphScalarU8TraversalOptions{
					TopK: cosineNormalizedF32CampaignWidth, EfSearch: cosineNormalizedF32CampaignWidth,
					ScoreBudget:              serving.SearchCandidates - cosineNormalizedF32CampaignWidth,
					QuantizedIndexName:       quantizedIndex,
					StatsMode:                columnVectorGraphNativeSearchStatsModeMinimal,
					CanonicalNormalizedQuery: true,
				}, &scratch)
				if err != nil || len(raw) != cosineNormalizedF32CampaignWidth || stats.QuantizedScoreCalls == 0 {
					b.Fatalf("candidate-only query %d: candidates=%d stats=%+v err=%v", i%len(canonicalQueries), len(raw), stats, err)
				}
				cosineNormalizedF32CampaignSink += raw[0].Ordinal
			}
		})
	}
	packedBenchmark := func() testing.BenchmarkResult {
		var scratch columnVectorGraphNativeSearchScratch
		return testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				queryOrdinal := i % len(canonicalQueries)
				fillCosineNormalizedF32CampaignShortlist(&scratch, shortlists[queryOrdinal].Ordinals)
				var stats columnVectorGraphNativeSearchStats
				if err := owner.overlay.pack.exactRerankPreparedTraversalRowIDCandidatesWithQuery(
					canonicalQueries[queryOrdinal], true, false, cosineNormalizedF32CampaignTopK,
					cosineNormalizedF32CampaignWidth, columnVectorGraphScoreBatchModeDefault,
					&scratch, &stats,
				); err != nil {
					b.Fatal(err)
				}
				if stats.PackedExactScoreCalls != 1 || stats.PackedExactScoreCandidates != cosineNormalizedF32CampaignWidth {
					b.Fatalf("packed rerank query %d work=%+v", queryOrdinal, stats)
				}
				cosineNormalizedF32CampaignSink += scratch.top[0].ordinal
			}
		})
	}
	candidateResults := make([]cosineNormalizedF32CampaignBenchmark, cosineNormalizedF32CampaignRepetitions)
	packedResults := make([]cosineNormalizedF32CampaignBenchmark, cosineNormalizedF32CampaignRepetitions)
	for repetition := range cosineNormalizedF32CampaignRepetitions {
		if repetition%2 == 0 {
			candidateResults[repetition] = cosineNormalizedF32CampaignBenchmarkResult(repetition, 0, candidateBenchmark())
			packedResults[repetition] = cosineNormalizedF32CampaignBenchmarkResult(repetition, 1, packedBenchmark())
		} else {
			packedResults[repetition] = cosineNormalizedF32CampaignBenchmarkResult(repetition, 0, packedBenchmark())
			candidateResults[repetition] = cosineNormalizedF32CampaignBenchmarkResult(repetition, 1, candidateBenchmark())
		}
	}
	report := cosineNormalizedF32CampaignDiagnostic{
		Schema:       "treedb_cosine_normalized_f32_campaign_engine/v1",
		SourceCommit: sourceCommit, GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		Rows: campaignRows, Dimensions: manifest.Dimensions, QueryCount: len(queries),
		TopK: cosineNormalizedF32CampaignTopK, EFSearch: cosineNormalizedF32CampaignWidth,
		RerankCandidates: cosineNormalizedF32CampaignWidth,
		Representation:   definition.Representation, Index: index, QuantizedIndex: quantizedIndex,
		CollectionGeneration:  snapshot.SchemaGeneration,
		Dataset:               manifest,
		DatasetManifestSHA256: cosineNormalizedF32CampaignFileSHA256(t, manifestPath),
		QueriesSHA256:         cosineNormalizedF32CampaignFileSHA256(t, queriesPath),
		ServingSHA256:         cosineNormalizedF32CampaignFileSHA256(t, servingPath),
		ServingOwner:          servingOwner, Shortlists: shortlists,
		CandidateOnly: candidateResults, PackedSameShortlist: packedResults,
		StableDuplicateScoreCalls: 0,
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	file, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(encoded); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote normalized-v4 campaign engine diagnostic %s", outputPath)
}

func readCosineNormalizedF32CampaignJSON(t *testing.T, path string, target any) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatal(err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		t.Fatal("JSON input contains trailing data")
	}
}

func readCosineNormalizedF32CampaignQueries(t *testing.T, path string, count, dimensions int) [][]float32 {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	queries := make([][]float32, count)
	for i := range queries {
		queries[i] = make([]float32, dimensions)
		if err := binary.Read(file, binary.LittleEndian, queries[i]); err != nil {
			t.Fatal(err)
		}
	}
	if extra := make([]byte, 1); func() int { n, _ := file.Read(extra); return n }() != 0 {
		t.Fatal("query file contains trailing bytes")
	}
	return queries
}

func cosineNormalizedF32CampaignFileSHA256(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func cosineNormalizedF32CampaignOrdinalsValid(ordinals []int, rows int) bool {
	if len(ordinals) != cosineNormalizedF32CampaignWidth {
		return false
	}
	copyOf := slices.Clone(ordinals)
	slices.Sort(copyOf)
	for i, ordinal := range copyOf {
		if ordinal < 0 || ordinal >= rows || (i != 0 && ordinal == copyOf[i-1]) {
			return false
		}
	}
	return true
}

func fillCosineNormalizedF32CampaignShortlist(scratch *columnVectorGraphNativeSearchScratch, ordinals []int) {
	scratch.top = resizeColumnVectorGraphNativeCandidateScratch(scratch.top, len(ordinals))[:0]
	for _, ordinal := range ordinals {
		scratch.top = append(scratch.top, columnVectorGraphSearchCandidate{ordinal: ordinal})
	}
}

func cosineNormalizedF32CampaignBenchmarkResult(ordinal, armOrder int, result testing.BenchmarkResult) cosineNormalizedF32CampaignBenchmark {
	nanos := result.NsPerOp()
	qps := 0.0
	if nanos > 0 {
		qps = 1e9 / float64(nanos)
	}
	return cosineNormalizedF32CampaignBenchmark{
		Ordinal: ordinal, ArmOrder: armOrder, Iterations: result.N,
		ElapsedNanos: result.T.Nanoseconds(), NanosPerQuery: nanos, QPS: qps,
		BytesPerQuery: result.AllocedBytesPerOp(), AllocsPerQuery: result.AllocsPerOp(),
	}
}
