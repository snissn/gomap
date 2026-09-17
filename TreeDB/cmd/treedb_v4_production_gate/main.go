// Command treedb_v4_production_gate measures the fixed Q3 production seams.
// Dataset construction and Python-client timing stay in the Minima runner; this
// helper owns only the Go native, direct service, and direct collection lanes.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/nativewire"
)

const (
	gateRepresentation = collections.VectorIndexRepresentationCosineNormalizedF32V1
	gateTopK           = 10
	gateEFSearch       = 64
	gateWarmupQueries  = 20
	gateRepetitions    = 6
)

type repetition struct {
	Ordinal        int               `json:"ordinal"`
	ArmOrder       int               `json:"arm_order"`
	WallNanos      int64             `json:"wall_nanos"`
	CPUNanos       int64             `json:"cpu_nanos"`
	CallWallNanos  []int64           `json:"call_wall_nanos"`
	CallCPUNanos   []int64           `json:"call_cpu_nanos"`
	ResultChecksum uint64            `json:"result_checksum"`
	Observations   []callObservation `json:"observations"`
}

type resultObservation struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
}

type callObservation struct {
	Query   int                 `json:"query"`
	Results []resultObservation `json:"results"`
	Route   any                 `json:"route"`
}

type measuredResult struct {
	Checksum uint64
	Results  []resultObservation
	Route    any
}

type armResult struct {
	Mode        string       `json:"mode"`
	Repetitions []repetition `json:"repetitions"`
}

type laneResult struct {
	Schema      string       `json:"schema"`
	Lane        string       `json:"lane"`
	GoVersion   string       `json:"go_version"`
	GOMAXPROCS  int          `json:"gomaxprocs"`
	QueryCount  int          `json:"query_count"`
	WarmupCount int          `json:"warmup_count"`
	TopK        int          `json:"top_k"`
	EfSearch    int          `json:"ef_search"`
	Exact       armResult    `json:"exact"`
	SQ8         armResult    `json:"sq8"`
	SubLanes    []laneResult `json:"sub_lanes,omitempty"`
}

type measuredCall func(context.Context, []float32, collections.VectorIndexQueryMode) (measuredResult, error)

func main() {
	lane := flag.String("lane", "native", "measurement lane: native or seams")
	address := flag.String("address", "", "native-wire TCP address")
	dir := flag.String("dir", "", "existing TreeDB service directory")
	dataset := flag.String("dataset", "", "directory containing queries.f32")
	servingPath := flag.String("serving", "", "column_graph serving JSON for the seams lane")
	index := flag.String("index", "q3-production-gate", "document-service index name")
	generation := flag.Uint64("generation", 0, "expected schema generation for native v4")
	quantizedIndex := flag.String("quantized-index", "minima_sq8", "scalar-u8 index name")
	flag.Parse()

	queries, err := loadQueries(*dataset)
	if err != nil {
		fatal(err)
	}
	var result laneResult
	switch *lane {
	case "native":
		if *address == "" || *generation == 0 {
			fatal(errors.New("native lane requires -address and positive -generation"))
		}
		result, err = runNative(*address, *index, *quantizedIndex, *generation, queries)
	case "seams":
		if *dir == "" || *servingPath == "" {
			fatal(errors.New("seams lane requires -dir and -serving"))
		}
		result, err = runSeams(*dir, *servingPath, *index, *quantizedIndex, queries)
	default:
		fatal(fmt.Errorf("unsupported lane %q", *lane))
	}
	if err != nil {
		fatal(err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fatal(err)
	}
}

func runNative(address, index, quantizedIndex string, generation uint64, queries [][]float32) (laneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := nativewire.DialContext(ctx, "tcp", address)
	if err != nil {
		return laneResult{}, fmt.Errorf("dial native service: %w", err)
	}
	defer client.Close()
	call := func(ctx context.Context, query []float32, mode collections.VectorIndexQueryMode) (measuredResult, error) {
		request := nativewire.DenseVectorSearchRequest{
			Index: index, Query: query, TopK: gateTopK, EfSearch: gateEFSearch,
			ExpectedGeneration: generation, VectorRepresentation: gateRepresentation, QueryMode: mode,
		}
		if mode == collections.VectorIndexQueryModeQuantizedRerank {
			request.QuantizedIndexName = quantizedIndex
			request.QuantizedRerankCandidates = gateEFSearch
		}
		response, err := client.DenseVectorSearch(ctx, request)
		if err != nil {
			return measuredResult{}, err
		}
		identity := response.RouteIdentity
		if len(response.Results) != gateTopK || identity == nil || identity.Diagnostics || response.DenseWork.Version != 0 || response.ScorePlane != nil ||
			identity.ReturnEmbedding || identity.EmbeddingVectorReads != 0 || identity.EmbeddingVectorBytes != 0 || identity.EmbeddingOutputBytes != 0 ||
			identity.QueryMode != mode || identity.ExecutionRoute != "typed_hnsw" {
			return measuredResult{}, fmt.Errorf("native %s response left the v4 production route", mode)
		}
		results := make([]resultObservation, len(response.Results))
		for i, result := range response.Results {
			results[i] = resultObservation{ID: string(result.ID), Score: result.Score}
		}
		return measuredResult{Checksum: checksumNative(response.Results), Results: results, Route: identity}, nil
	}
	return measureLane("go_native_v4", queries, call)
}

func runSeams(dir, servingPath, index, quantizedIndex string, queries [][]float32) (laneResult, error) {
	serving, err := loadServing(servingPath)
	if err != nil {
		return laneResult{}, err
	}
	database, cleanup, _, maintenance, err := treedb.OpenBackendWithCachedLeafLogStatsAndDeferredVectorBuildMaintenance(
		treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir),
	)
	if err != nil {
		return laneResult{}, fmt.Errorf("open service directory: %w", err)
	}
	manager := collections.NewCollectionManager(database)
	service := documentservice.NewWithDeferredVectorBuildMaintenance(manager, maintenance)
	defer func() {
		_ = service.Close()
		_ = cleanup()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	info, err := service.OpenIndex(ctx, index)
	if err != nil {
		return laneResult{}, fmt.Errorf("open service index: %w", err)
	}
	if info.VectorRepresentation != gateRepresentation || info.Dimension != len(queries[0]) || info.VectorIndexName == "" {
		return laneResult{}, fmt.Errorf("index representation/dimension/name mismatch: %q/%d/%q", info.VectorRepresentation, info.Dimension, info.VectorIndexName)
	}
	if _, err := service.OptimizeIndex(ctx, index, documentservice.OptimizeIndexRequest{ColumnGraphServing: &serving}); err != nil {
		return laneResult{}, fmt.Errorf("re-admit selected serving: %w", err)
	}
	collection, err := manager.OpenCollection(index)
	if err != nil {
		return laneResult{}, fmt.Errorf("open collection: %w", err)
	}
	// OpenCollection may return a handle distinct from the service's cached
	// handle. Admit and warm this handle outside the measured loop so the direct
	// collection lane represents steady-state serving rather than owner setup.
	if err := collection.EnsureColumnGraphServing(ctx, info.VectorIndexName, serving); err != nil {
		return laneResult{}, fmt.Errorf("admit collection serving handle: %w", err)
	}
	var collectionBuffer collections.VectorIndexSearchBuffer

	serviceCall := func(ctx context.Context, query []float32, mode collections.VectorIndexQueryMode) (measuredResult, error) {
		request := documentservice.DenseVectorSearchRequest{
			ExpectedGeneration: info.Generation, QueryEmbedding: query, TopK: gateTopK, EfSearch: gateEFSearch,
			Route: documentservice.RouteAnn, QueryMode: mode, VectorRepresentation: gateRepresentation,
		}
		if mode == collections.VectorIndexQueryModeQuantizedRerank {
			request.QuantizedIndexName = quantizedIndex
			request.QuantizedRerankCandidates = gateEFSearch
		}
		response, err := service.SearchDenseVector(ctx, index, request)
		if err != nil {
			return measuredResult{}, err
		}
		identity := response.RouteIdentity
		if len(response.Documents) != gateTopK || identity == nil || identity.Diagnostics || response.DenseWork != nil || response.ScorePlane != nil ||
			identity.EmbeddingVectorReads != 0 || identity.EmbeddingVectorBytes != 0 || identity.EmbeddingOutputBytes != 0 || identity.ExecutionRoute != "typed_hnsw" {
			return measuredResult{}, fmt.Errorf("service %s response left the v4 production route", mode)
		}
		results := make([]resultObservation, len(response.Documents))
		for i, result := range response.Documents {
			results[i] = resultObservation{ID: result.ID, Score: *result.Score}
		}
		return measuredResult{Checksum: checksumDocuments(response.Documents), Results: results, Route: identity}, nil
	}
	collectionCall := func(fetch bool) measuredCall {
		return func(ctx context.Context, query []float32, mode collections.VectorIndexQueryMode) (measuredResult, error) {
			options := collections.VectorIndexSearchOptions{
				Context: ctx, IndexName: info.VectorIndexName, Query: query, QueryMode: mode,
				TopK: gateTopK, EfSearch: gateEFSearch, StatsMode: collections.VectorIndexSearchStatsModeMinimal,
			}
			if mode == collections.VectorIndexQueryModeQuantizedRerank {
				options.QuantizedIndexName = quantizedIndex
				options.QuantizedRerankCandidates = gateEFSearch
			}
			response, view, err := collection.SearchVectorIndexWithBufferReadView(options, &collectionBuffer)
			if err != nil {
				return measuredResult{}, err
			}
			var fetched collections.DocumentFetchResponse
			var fetchErr error
			if fetch {
				fetched, fetchErr = view.FetchDocumentsForVectorIndexSearchResults(response.Results, collections.DocumentFetchOptions{
					Context:                  ctx,
					ExcludePaths:             []string{"embedding"},
					Format:                   collections.DocumentFormatJSON,
					ColumnAssetReadIntegrity: collections.ColumnAssetReadIntegrityCachedVerify,
				})
			}
			if err := errors.Join(fetchErr, view.Close()); err != nil {
				return measuredResult{}, err
			}
			receipt := response.Stats.ColumnGraphReceipt
			if len(response.Results) != gateTopK || (fetch && len(fetched.Results) != gateTopK) || !receipt.Available || receipt.Route != "typed_hnsw" ||
				(fetch && (fetched.Stats.EmbeddingVectorReads != 0 || fetched.Stats.EmbeddingVectorBytes != 0 || fetched.Stats.EmbeddingOutputBytes != 0)) {
				return measuredResult{}, fmt.Errorf("collection %s response left the selected production route", mode)
			}
			results := make([]resultObservation, len(response.Results))
			for i, result := range response.Results {
				results[i] = resultObservation{ID: string(result.ID), Score: result.Score}
			}
			route := struct {
				Receipt               collections.ColumnGraphRouteReceipt `json:"receipt"`
				QuantizedScoreCalls   uint64                              `json:"quantized_score_calls"`
				FP32ScoreCalls        uint64                              `json:"fp32_score_calls"`
				PackedScoreCalls      uint64                              `json:"packed_score_calls"`
				PackedScoreCandidates uint64                              `json:"packed_score_candidates"`
				PackedVectorBytesRead uint64                              `json:"packed_vector_bytes_read"`
				EmbeddingVectorReads  uint64                              `json:"embedding_vector_reads"`
				EmbeddingVectorBytes  uint64                              `json:"embedding_vector_bytes"`
				EmbeddingOutputBytes  uint64                              `json:"embedding_output_bytes"`
			}{receipt, response.Stats.QuantizedScoreCalls, response.Stats.FP32ScoreCalls,
				response.Stats.PackedExactScoreCalls, response.Stats.PackedExactScoreCandidates,
				response.Stats.PackedExactVectorBytesRead, fetched.Stats.EmbeddingVectorReads,
				fetched.Stats.EmbeddingVectorBytes, fetched.Stats.EmbeddingOutputBytes}
			checksum := checksumVectorResults(response.Results)
			if fetch {
				checksum ^= checksumFetched(fetched.Results)
			}
			return measuredResult{Checksum: checksum, Results: results, Route: route}, nil
		}
	}
	collectionSearchResult, err := measureLane("collection_search", queries, collectionCall(false))
	if err != nil {
		return laneResult{}, fmt.Errorf("collection search lane: %w", err)
	}
	collectionResult, err := measureLane("collection_fetch", queries, collectionCall(true))
	if err != nil {
		return laneResult{}, fmt.Errorf("collection fetch lane: %w", err)
	}
	serviceResult, err := measureLane("service", queries, serviceCall)
	if err != nil {
		return laneResult{}, fmt.Errorf("service lane: %w", err)
	}
	return laneResult{
		Schema: "treedb_v4_production_gate/v1", Lane: "seams", GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		QueryCount: len(queries), WarmupCount: gateWarmupQueries, TopK: gateTopK, EfSearch: gateEFSearch,
		SubLanes: []laneResult{collectionSearchResult, collectionResult, serviceResult},
	}, nil
}

func measureLane(name string, queries [][]float32, call measuredCall) (laneResult, error) {
	result := laneResult{
		Schema: "treedb_v4_production_gate/v1", Lane: name, GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		QueryCount: len(queries), WarmupCount: min(gateWarmupQueries, len(queries)), TopK: gateTopK, EfSearch: gateEFSearch,
		Exact: armResult{Mode: string(collections.VectorIndexQueryModeExact)},
		SQ8:   armResult{Mode: string(collections.VectorIndexQueryModeQuantizedRerank)},
	}
	for _, mode := range []collections.VectorIndexQueryMode{collections.VectorIndexQueryModeExact, collections.VectorIndexQueryModeQuantizedRerank} {
		for i := 0; i < result.WarmupCount; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_, err := call(ctx, queries[i], mode)
			cancel()
			if err != nil {
				return laneResult{}, fmt.Errorf("warm %s query %d: %w", mode, i, err)
			}
		}
	}
	for rep := 0; rep < gateRepetitions; rep++ {
		order := []collections.VectorIndexQueryMode{collections.VectorIndexQueryModeExact, collections.VectorIndexQueryModeQuantizedRerank}
		if rep%2 != 0 {
			order[0], order[1] = order[1], order[0]
		}
		for armOrder, mode := range order {
			measured, err := measureRepetition(rep, armOrder, queries, mode, call)
			if err != nil {
				return laneResult{}, fmt.Errorf("repetition %d %s: %w", rep, mode, err)
			}
			if mode == collections.VectorIndexQueryModeExact {
				result.Exact.Repetitions = append(result.Exact.Repetitions, measured)
			} else {
				result.SQ8.Repetitions = append(result.SQ8.Repetitions, measured)
			}
		}
	}
	return result, nil
}

func measureRepetition(rep, armOrder int, queries [][]float32, mode collections.VectorIndexQueryMode, call measuredCall) (repetition, error) {
	out := repetition{Ordinal: rep, ArmOrder: armOrder, CallWallNanos: make([]int64, len(queries)), CallCPUNanos: make([]int64, len(queries)), Observations: make([]callObservation, len(queries))}
	batchWall := time.Now()
	batchCPU, err := processCPUNanos()
	if err != nil {
		return repetition{}, err
	}
	for i, query := range queries {
		beforeCPU, err := processCPUNanos()
		if err != nil {
			return repetition{}, err
		}
		beforeWall := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		measured, err := call(ctx, query, mode)
		cancel()
		out.CallWallNanos[i] = time.Since(beforeWall).Nanoseconds()
		afterCPU, cpuErr := processCPUNanos()
		if err != nil {
			return repetition{}, err
		}
		if cpuErr != nil {
			return repetition{}, cpuErr
		}
		out.CallCPUNanos[i] = afterCPU - beforeCPU
		out.ResultChecksum ^= measured.Checksum + uint64(i+1)*0x9e3779b97f4a7c15
		out.Observations[i] = callObservation{Query: i, Results: measured.Results, Route: measured.Route}
	}
	endCPU, err := processCPUNanos()
	if err != nil {
		return repetition{}, err
	}
	out.WallNanos = time.Since(batchWall).Nanoseconds()
	out.CPUNanos = endCPU - batchCPU
	return out, nil
}

func loadQueries(dataset string) ([][]float32, error) {
	if dataset == "" {
		return nil, errors.New("-dataset is required")
	}
	raw, err := os.ReadFile(dataset + "/queries.f32")
	if err != nil {
		return nil, err
	}
	const dimensions = 768
	if len(raw) != 200*dimensions*4 {
		return nil, fmt.Errorf("queries.f32 has %d bytes, want %d", len(raw), 200*dimensions*4)
	}
	queries := make([][]float32, 200)
	for row := range queries {
		queries[row] = make([]float32, dimensions)
		for column := range dimensions {
			off := (row*dimensions + column) * 4
			queries[row][column] = math.Float32frombits(binary.LittleEndian.Uint32(raw[off:]))
		}
	}
	return queries, nil
}

func loadServing(path string) (collections.ColumnGraphServingOptions, error) {
	var serving collections.ColumnGraphServingOptions
	file, err := os.Open(path)
	if err != nil {
		return serving, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&serving); err != nil {
		return serving, err
	}
	if err := collections.ValidateColumnGraphServingOptions(serving); err != nil {
		return serving, err
	}
	return serving, nil
}

func checksumNative(results []nativewire.DenseVectorSearchResult) uint64 {
	var checksum uint64
	for _, result := range results {
		checksum ^= checksumBytes(result.ID) ^ checksumBytes(result.Document) ^ math.Float64bits(result.Score)
	}
	return checksum
}

func checksumDocuments(results []documentservice.Document) uint64 {
	var checksum uint64
	for _, result := range results {
		checksum ^= checksumBytes([]byte(result.ID)) ^ checksumBytes([]byte(result.Content))
		if result.Score != nil {
			checksum ^= math.Float64bits(*result.Score)
		}
	}
	return checksum
}

func checksumFetched(results []collections.DocumentFetchResult) uint64 {
	var checksum uint64
	for _, result := range results {
		checksum ^= checksumBytes(result.ID) ^ checksumBytes(result.Document)
	}
	return checksum
}

func checksumVectorResults(results []collections.VectorIndexSearchResult) uint64 {
	var checksum uint64
	for _, result := range results {
		checksum ^= checksumBytes(result.ID) ^ math.Float64bits(result.Score)
	}
	return checksum
}

func checksumBytes(value []byte) uint64 {
	checksum := uint64(1469598103934665603)
	for _, b := range value {
		checksum = (checksum ^ uint64(b)) * 1099511628211
	}
	return checksum
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "treedb_v4_production_gate:", err)
	os.Exit(1)
}
