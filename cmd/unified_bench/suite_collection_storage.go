package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	collectionStorageSuiteName            = "collection_storage"
	collectionStorageBenchTestName        = "collection_storage"
	collectionStorageBenchDBName          = "treedb_collection_storage"
	collectionStorageBenchDisplayName     = "TreeDB Collection Storage"
	collectionStorageDefaultRows          = 2048
	collectionStorageDefaultQueryCount    = 3
	collectionStorageDefaultPointGetCount = 128
	collectionStorageDefaultVectorTopK    = 3
	collectionStorageDefaultVectorDims    = 8
	collectionStorageDefaultPayloadSize   = 32
	collectionStorageDefaultCardinality   = 8
	collectionStorageDefaultSelectivity   = 0.01
	collectionStorageCollectionName       = "events"
	collectionStorageVectorIndexName      = "embedding_graph"
	collectionStorageProfileScope         = "collection_storage"
	collectionStorageBenchMetricPrefix    = "collection_storage_"
)

const (
	collectionStorageModeDocumentOnly            = "document_only"
	collectionStorageModeTypedRowAsset           = "typed_row_asset"
	collectionStorageModeTypedColumnPart         = "typed_column_part"
	collectionStorageModeHybridDocumentRow       = "hybrid_document_row"
	collectionStorageModeHybridDocumentColumn    = "hybrid_document_column"
	collectionStorageModeHybridRowColumn         = "hybrid_row_column"
	collectionStorageModeHybridDocumentRowColumn = "hybrid_document_row_column"
	collectionStorageModeVectorTypedColumn       = "vector_typed_column"
)

const (
	collectionStorageWorkloadInsertBatch       = "insert_batch"
	collectionStorageWorkloadPointGet          = "point_get"
	collectionStorageWorkloadPredicateScan     = "predicate_scan"
	collectionStorageWorkloadAggregate         = "aggregate"
	collectionStorageWorkloadVectorSearchSmoke = "vector_search_smoke"
	collectionStorageWorkloadMixed             = "mixed"
)

const (
	collectionStorageVectorFinalFetchNone                  = "none"
	collectionStorageVectorFinalFetchProjectionNoEmbedding = "projection_without_embedding"
	collectionStorageVectorFinalFetchFullDocument          = "full_document_embedding_echo"
)

var (
	collectionStorageAllModes = []string{
		collectionStorageModeDocumentOnly,
		collectionStorageModeTypedRowAsset,
		collectionStorageModeTypedColumnPart,
		collectionStorageModeHybridDocumentRow,
		collectionStorageModeHybridDocumentColumn,
		collectionStorageModeHybridRowColumn,
		collectionStorageModeHybridDocumentRowColumn,
		collectionStorageModeVectorTypedColumn,
	}
	collectionStorageDefaultModes = strings.Join(collectionStorageAllModes, ",")
	collectionStorageAllWorkloads = []string{
		collectionStorageWorkloadInsertBatch,
		collectionStorageWorkloadPointGet,
		collectionStorageWorkloadPredicateScan,
		collectionStorageWorkloadAggregate,
		collectionStorageWorkloadVectorSearchSmoke,
		collectionStorageWorkloadMixed,
	}
	collectionStorageDefaultWorkloads = strings.Join(collectionStorageAllWorkloads, ",")

	collectionStorageModesArg = flag.String(
		"collection-storage-modes",
		collectionStorageDefaultModes,
		"Comma-separated storage modes for -suite collection_storage (document_only,typed_row_asset,typed_column_part,hybrid_document_row,hybrid_document_column,hybrid_row_column,hybrid_document_row_column,vector_typed_column; all=default)",
	)
	collectionStorageWorkloadsArg = flag.String(
		"collection-storage-workloads",
		collectionStorageDefaultWorkloads,
		"Comma-separated workloads for -suite collection_storage (insert_batch,point_get,predicate_scan,aggregate,vector_search_smoke,mixed; all=default)",
	)
	collectionStorageQueryCountArg          = flag.Int("collection-storage-query-count", collectionStorageDefaultQueryCount, "Per-mode query repetitions for scan/aggregate/vector/mixed workloads in -suite collection_storage")
	collectionStoragePointGetCountArg       = flag.Int("collection-storage-point-get-count", collectionStorageDefaultPointGetCount, "Per-mode point-get operations for -suite collection_storage")
	collectionStorageFieldCountArg          = flag.Int("collection-storage-field-count", 3, "Logical scalar field count reported for -suite collection_storage (current comparable fixture uses time_us, kind, score; higher values add payload-only fields)")
	collectionStoragePayloadSizeArg         = flag.Int("collection-storage-payload-size", collectionStorageDefaultPayloadSize, "Payload bytes per logical document for -suite collection_storage")
	collectionStorageCardinalityArg         = flag.Int("collection-storage-cardinality", collectionStorageDefaultCardinality, "Kind/cardinality bucket count for -suite collection_storage")
	collectionStorageSelectivityArg         = flag.Float64("collection-storage-selectivity", collectionStorageDefaultSelectivity, "Target predicate selectivity for -suite collection_storage range/equality shapes (0<value<=1)")
	collectionStorageVectorDimsArg          = flag.Int("collection-storage-vector-dims", collectionStorageDefaultVectorDims, "Vector dimensions for vector_typed_column mode in -suite collection_storage")
	collectionStorageVectorTopKArg          = flag.Int("collection-storage-vector-top-k", collectionStorageDefaultVectorTopK, "TopK for vector_search_smoke in -suite collection_storage")
	collectionStorageIncludeFinalFetchArg   = flag.Bool("collection-storage-include-final-fetch", true, "Include projection-oriented final document fetch/materialization in vector_search_smoke for -suite collection_storage")
	collectionStorageVectorFullDocumentsArg = flag.Bool("collection-storage-vector-full-documents", false, "With collection-storage final fetch, return full documents including embedding instead of the preferred projection_without_embedding path")
	collectionStorageCheckpointReopenArg    = flag.Bool("collection-storage-checkpoint-reopen", true, "Checkpoint, close, and reopen each collection before read workloads in -suite collection_storage")
	collectionStorageAssetReadIntegrityArg  = flag.String("collection-storage-asset-read-integrity", string(collections.ColumnAssetReadIntegrityVerify), "Typed column asset read integrity for -suite collection_storage (verify, cached_verify, skip_checksums; relaxed modes require -treedb-allow-unsafe)")
)

type collectionStorageSuiteOptions struct {
	ProfileDir               string
	ExecutionPath            string
	ModesArg                 string
	WorkloadsArg             string
	QueryCount               int
	PointGetCount            int
	FieldCount               int
	PayloadSize              int
	Cardinality              int
	Selectivity              float64
	VectorDims               int
	VectorTopK               int
	IncludeFinalFetch        bool
	VectorFullDocuments      bool
	CheckpointReopen         bool
	ColumnAssetReadIntegrity collections.ColumnAssetReadIntegrity
	RunBenchprof             bool
}

type collectionStorageReport struct {
	GeneratedAt              string                            `json:"generated_at"`
	Suite                    string                            `json:"suite"`
	Profile                  string                            `json:"profile"`
	Rows                     int                               `json:"rows"`
	BatchSize                int                               `json:"batch_size"`
	Seed                     int64                             `json:"seed"`
	Modes                    []string                          `json:"modes"`
	Workloads                []string                          `json:"workloads"`
	QueryCount               int                               `json:"query_count"`
	PointGetCount            int                               `json:"point_get_count"`
	FieldCount               int                               `json:"field_count"`
	PayloadSize              int                               `json:"payload_size"`
	Cardinality              int                               `json:"cardinality"`
	Selectivity              float64                           `json:"selectivity"`
	VectorDims               int                               `json:"vector_dims"`
	VectorTopK               int                               `json:"vector_top_k"`
	IncludeFinalFetch        bool                              `json:"include_final_fetch"`
	VectorFinalFetchShape    string                            `json:"vector_final_fetch_shape"`
	CheckpointReopen         bool                              `json:"checkpoint_reopen"`
	ColumnAssetReadIntegrity string                            `json:"column_asset_read_integrity"`
	BenchmarkOnlyRelaxed     bool                              `json:"benchmark_only_relaxed"`
	PathLabel                string                            `json:"path_label,omitempty"`
	DataDirs                 map[string]string                 `json:"data_dirs,omitempty"`
	Semantics                collectionStorageSemantics        `json:"semantics"`
	ModeSemantics            []collectionStorageModeSemantics  `json:"mode_semantics"`
	Stages                   []collectionStorageStageMetric    `json:"stages"`
	Metrics                  []collectionStorageWorkloadMetric `json:"metrics"`
	Artifacts                collectionStorageArtifactPaths    `json:"artifacts,omitempty"`
	ProfileFinalizeError     string                            `json:"profile_finalize_error,omitempty"`
}

type collectionStorageSemantics struct {
	LogicalRows              int      `json:"logical_rows"`
	InsertedDocuments        int      `json:"inserted_documents"`
	FieldsProjected          []string `json:"fields_projected"`
	FieldsQueried            []string `json:"fields_queried"`
	ReturnedResultShape      string   `json:"returned_result_shape"`
	FinalDocumentFetch       bool     `json:"final_document_fetch"`
	VectorFinalFetchShape    string   `json:"vector_final_fetch_shape"`
	CheckpointReopenIncluded bool     `json:"checkpoint_reopen_included"`
	ReadIntegrityMode        string   `json:"read_integrity_mode"`
	SelectivityDistribution  string   `json:"selectivity_distribution"`
	MaterializationBoundary  string   `json:"materialization_boundary"`
	SemanticComparability    string   `json:"semantic_comparability"`
}

type collectionStorageModeSemantics struct {
	Mode                   string            `json:"mode"`
	RetainedPayloadPolicy  string            `json:"retained_payload_policy"`
	ReconstructionPolicy   string            `json:"reconstruction_policy,omitempty"`
	OwnerMap               map[string]string `json:"owner_map"`
	DocumentPayloadRetains string            `json:"document_payload_retains"`
	VectorIndexStrategy    string            `json:"vector_index_strategy,omitempty"`
	ComparableWorkloads    []string          `json:"comparable_workloads"`
	UnsupportedWorkloads   map[string]string `json:"unsupported_workloads,omitempty"`
}

type collectionStorageStageMetric struct {
	Mode          string  `json:"mode,omitempty"`
	Name          string  `json:"name"`
	DurationMS    float64 `json:"duration_ms"`
	Rows          int     `json:"rows,omitempty"`
	RowsPerSecond float64 `json:"rows_per_second,omitempty"`
	Bytes         int64   `json:"bytes,omitempty"`
	MiBPerSecond  float64 `json:"mib_per_second,omitempty"`
}

type collectionStorageWorkloadMetric struct {
	Mode                  string                            `json:"mode"`
	Workload              string                            `json:"workload"`
	Supported             bool                              `json:"supported"`
	UnsupportedReason     string                            `json:"unsupported_reason,omitempty"`
	SemanticEquivalent    bool                              `json:"semantic_equivalent"`
	SemanticNote          string                            `json:"semantic_note,omitempty"`
	CorrectnessValidated  bool                              `json:"correctness_validated"`
	DurationMS            float64                           `json:"duration_ms"`
	Ops                   int64                             `json:"ops"`
	Rows                  int                               `json:"rows"`
	RowsProcessed         int64                             `json:"rows_processed"`
	RowsPerSecond         float64                           `json:"rows_per_second"`
	QueriesPerSecond      float64                           `json:"queries_per_second"`
	Matches               int64                             `json:"matches"`
	MatchesPerSecond      float64                           `json:"matches_per_second"`
	NsPerOp               float64                           `json:"ns_per_op"`
	OpsPerSecond          float64                           `json:"ops_per_second"`
	BytesPerOp            float64                           `json:"bytes_per_op"`
	AllocsPerOp           float64                           `json:"allocs_per_op"`
	DBTotalBytes          uint64                            `json:"db_total_bytes"`
	DBTotalFiles          int                               `json:"db_total_files"`
	TypedRowAssetBytes    int64                             `json:"typed_row_asset_bytes"`
	TypedColumnAssetBytes int64                             `json:"typed_column_asset_bytes"`
	Counters              collectionStorageWorkloadCounters `json:"counters"`
}

type collectionStorageWorkloadCounters struct {
	MappedBytes                  uint64 `json:"mapped_bytes,omitempty"`
	HeapCopyBytes                uint64 `json:"heap_copy_bytes,omitempty"`
	DecodedBytes                 uint64 `json:"decoded_bytes,omitempty"`
	DocumentMaterializations     int64  `json:"document_materializations,omitempty"`
	DocumentReconstructions      int64  `json:"document_reconstructions,omitempty"`
	RowMaterializations          int64  `json:"row_materializations,omitempty"`
	RowLocatorDecodes            int64  `json:"row_locator_decodes,omitempty"`
	PhysicalRowAssetReads        int64  `json:"physical_row_asset_reads,omitempty"`
	PhysicalRowIDLookups         int64  `json:"physical_row_id_lookups,omitempty"`
	TypedColumnPartsConsidered   int64  `json:"typed_column_parts_considered,omitempty"`
	TypedColumnPartsPruned       int64  `json:"typed_column_parts_pruned,omitempty"`
	TypedColumnPartsDecoded      int64  `json:"typed_column_parts_decoded,omitempty"`
	TypedColumnBlocksConsidered  int64  `json:"typed_column_blocks_considered,omitempty"`
	TypedColumnBlocksPruned      int64  `json:"typed_column_blocks_pruned,omitempty"`
	TypedColumnBlocksDecoded     int64  `json:"typed_column_blocks_decoded,omitempty"`
	DirectTypedColumnAssetReads  int64  `json:"direct_typed_column_asset_reads,omitempty"`
	AssetOpenMapChecksumReads    int64  `json:"asset_open_map_checksum_reads,omitempty"`
	SegmentFileCacheHits         uint64 `json:"segment_file_cache_hits,omitempty"`
	SegmentFileCacheMisses       uint64 `json:"segment_file_cache_misses,omitempty"`
	VectorCandidates             uint64 `json:"vector_candidates,omitempty"`
	VectorEdges                  uint64 `json:"vector_edges,omitempty"`
	VectorDirectViews            uint64 `json:"vector_direct_views,omitempty"`
	VectorScratchDecodes         uint64 `json:"vector_scratch_decodes,omitempty"`
	VectorDocumentsFetched       uint64 `json:"vector_documents_fetched,omitempty"`
	VectorDocumentOutputBytes    uint64 `json:"vector_document_output_bytes,omitempty"`
	VectorDocumentFieldsSkipped  uint64 `json:"vector_document_fields_skipped,omitempty"`
	VectorDocumentRetainedBytes  uint64 `json:"vector_document_retained_bytes,omitempty"`
	VectorTypedColumnMappedBytes uint64 `json:"vector_typed_column_mapped_bytes,omitempty"`
	VectorTypedColumnHeapBytes   uint64 `json:"vector_typed_column_heap_copy_bytes,omitempty"`
	VectorTypedColumnDecoded     uint64 `json:"vector_typed_column_decoded_bytes,omitempty"`
}

type collectionStorageArtifactPaths struct {
	CollectionJSON       string `json:"collection_json,omitempty"`
	CollectionMarkdown   string `json:"collection_markdown,omitempty"`
	CollectionHTML       string `json:"collection_html,omitempty"`
	BenchprofJSON        string `json:"benchprof_json,omitempty"`
	BenchprofMarkdown    string `json:"benchprof_markdown,omitempty"`
	InsightsMarkdown     string `json:"insights_markdown,omitempty"`
	InsightsJSON         string `json:"insights_json,omitempty"`
	InsightsHTML         string `json:"insights_html,omitempty"`
	CPUProfile           string `json:"cpu_profile,omitempty"`
	AllocsProfile        string `json:"allocs_profile,omitempty"`
	CheckpointCPUProfile string `json:"checkpoint_cpu_profile,omitempty"`
	BlockProfile         string `json:"block_profile,omitempty"`
	MutexProfile         string `json:"mutex_profile,omitempty"`
	TraceProfile         string `json:"trace_profile,omitempty"`
}

// benchprofCollectionWorkload is serialized into benchprof_results.json. Keep
// this schema concrete/stable so internal/benchprof can render the same fields
// without importing cmd/unified_bench.
type benchprofCollectionWorkload struct {
	Suite                 string                            `json:"suite"`
	Mode                  string                            `json:"mode"`
	Workload              string                            `json:"workload"`
	Rows                  int                               `json:"rows"`
	SemanticEquivalent    bool                              `json:"semantic_equivalent"`
	SemanticNote          string                            `json:"semantic_note,omitempty"`
	CorrectnessValidated  bool                              `json:"correctness_validated"`
	RowsPerSecond         float64                           `json:"rows_per_second,omitempty"`
	QueriesPerSecond      float64                           `json:"queries_per_second,omitempty"`
	MatchesPerSecond      float64                           `json:"matches_per_second,omitempty"`
	OpsPerSecond          float64                           `json:"ops_per_second,omitempty"`
	NsPerOp               float64                           `json:"ns_per_op,omitempty"`
	BytesPerOp            float64                           `json:"bytes_per_op,omitempty"`
	AllocsPerOp           float64                           `json:"allocs_per_op,omitempty"`
	DBTotalBytes          uint64                            `json:"db_total_bytes,omitempty"`
	TypedRowAssetBytes    int64                             `json:"typed_row_asset_bytes,omitempty"`
	TypedColumnAssetBytes int64                             `json:"typed_column_asset_bytes,omitempty"`
	Counters              collectionStorageWorkloadCounters `json:"counters,omitempty"`
}

type collectionStorageFixtureRow struct {
	ID        []byte
	IDString  string
	TimeUS    int64
	Score     int64
	Kind      string
	Payload   string
	Embedding []float32
	Document  []byte
}

type collectionStorageDecodedDocument struct {
	TimeUS    int64     `json:"time_us"`
	Score     int64     `json:"score"`
	Kind      string    `json:"kind"`
	Payload   string    `json:"payload"`
	Embedding []float32 `json:"embedding"`
}

type collectionStorageModeRuntime struct {
	Mode                  string
	Dir                   string
	DB                    *backenddb.DB
	Collection            *collections.Collection
	Meta                  *collections.CollectionMeta
	ModeSemantics         collectionStorageModeSemantics
	InsertDuration        time.Duration
	CheckpointDuration    time.Duration
	ReopenDuration        time.Duration
	VectorRebuildDuration time.Duration
	DBTotalBytes          uint64
	DBTotalFiles          int
	TypedRowAssetBytes    int64
	TypedColumnAssetBytes int64
	TreeDBStats           map[string]string
	InsertedBytes         int64
	Closed                bool
}

type collectionStorageAllocDelta struct {
	Bytes  uint64
	Allocs uint64
}

func runCollectionStorageSuite(baseCfg BenchConfig, opts collectionStorageSuiteOptions) (string, error) {
	profile, err := collectionStorageEffectiveProfile(baseCfg.Profile)
	if err != nil {
		return "", err
	}
	if err := validateColumnStoreSuiteDBSelection(baseCfg.DBsArg, baseCfg.DBsExcludeArg); err != nil {
		return "", errors.New(strings.Replace(err.Error(), "column_store", collectionStorageSuiteName, 1))
	}
	if err := validateCollectionStorageExecutionPath(opts.ProfileDir, opts.ExecutionPath); err != nil {
		return "", err
	}
	executionPath := strings.TrimSpace(opts.ExecutionPath)
	if strings.TrimSpace(opts.ProfileDir) != "" {
		executionPath, err = normalizeBenchprofExecutionPath(executionPath)
		if err != nil {
			return "", err
		}
	}
	modes, err := collectionStorageEffectiveModes(opts.ModesArg)
	if err != nil {
		return "", err
	}
	workloads, err := collectionStorageEffectiveWorkloads(opts.WorkloadsArg)
	if err != nil {
		return "", err
	}
	if err := validateCollectionStorageSupportedSelection(modes, workloads); err != nil {
		return "", err
	}
	rows := collectionStorageEffectiveRows(baseCfg)
	if rows <= 0 {
		return "", fmt.Errorf("collection_storage: invalid row count %d", rows)
	}
	batchSize := baseCfg.BatchSize
	if batchSize <= 0 {
		return "", fmt.Errorf("collection_storage: invalid batchsize %d", batchSize)
	}
	if batchSize > rows {
		batchSize = rows
	}
	queryCount := opts.QueryCount
	if queryCount == 0 {
		queryCount = *collectionStorageQueryCountArg
	}
	if queryCount <= 0 {
		return "", fmt.Errorf("collection_storage: invalid query count %d", queryCount)
	}
	pointGetCount := opts.PointGetCount
	if pointGetCount == 0 {
		pointGetCount = *collectionStoragePointGetCountArg
	}
	if pointGetCount <= 0 {
		return "", fmt.Errorf("collection_storage: invalid point-get count %d", pointGetCount)
	}
	fieldCount := opts.FieldCount
	if fieldCount == 0 {
		fieldCount = *collectionStorageFieldCountArg
	}
	if fieldCount < 3 {
		return "", fmt.Errorf("collection_storage: invalid field count %d: fixture requires at least time_us, kind, score", fieldCount)
	}
	payloadSize := opts.PayloadSize
	if payloadSize == 0 {
		payloadSize = *collectionStoragePayloadSizeArg
	}
	if payloadSize < 0 {
		return "", fmt.Errorf("collection_storage: invalid payload size %d", payloadSize)
	}
	cardinality := opts.Cardinality
	if cardinality == 0 {
		cardinality = *collectionStorageCardinalityArg
	}
	if cardinality <= 0 {
		return "", fmt.Errorf("collection_storage: invalid cardinality %d", cardinality)
	}
	selectivity := opts.Selectivity
	if selectivity == 0 {
		selectivity = *collectionStorageSelectivityArg
	}
	if selectivity <= 0 || selectivity > 1 {
		return "", fmt.Errorf("collection_storage: invalid selectivity %.6f: expected 0 < selectivity <= 1", selectivity)
	}
	vectorDims := opts.VectorDims
	if vectorDims == 0 {
		vectorDims = *collectionStorageVectorDimsArg
	}
	if vectorDims <= 0 {
		return "", fmt.Errorf("collection_storage: invalid vector dims %d", vectorDims)
	}
	vectorTopK := opts.VectorTopK
	if vectorTopK == 0 {
		vectorTopK = *collectionStorageVectorTopKArg
	}
	if vectorTopK <= 0 {
		return "", fmt.Errorf("collection_storage: invalid vector topK %d", vectorTopK)
	}
	includeFinalFetch := opts.IncludeFinalFetch
	if !includeFinalFetch && !flagExplicit("collection-storage-include-final-fetch") {
		includeFinalFetch = *collectionStorageIncludeFinalFetchArg
	}
	vectorFullDocuments := opts.VectorFullDocuments
	if !vectorFullDocuments && !flagExplicit("collection-storage-vector-full-documents") {
		vectorFullDocuments = *collectionStorageVectorFullDocumentsArg
	}
	if vectorFullDocuments && !includeFinalFetch {
		return "", errors.New("collection_storage: -collection-storage-vector-full-documents requires -collection-storage-include-final-fetch")
	}
	vectorFinalFetchShape := collectionStorageVectorFinalFetchShape(includeFinalFetch, vectorFullDocuments)
	checkpointReopen := opts.CheckpointReopen
	if !checkpointReopen && !flagExplicit("collection-storage-checkpoint-reopen") {
		checkpointReopen = *collectionStorageCheckpointReopenArg
	}
	assetReadIntegrity, err := collectionStorageEffectiveAssetReadIntegrity(opts.ColumnAssetReadIntegrity)
	if err != nil {
		return "", err
	}
	seed := baseCfg.SeedUsed
	if seed == 0 {
		seed = 1
	}

	finishRuntimeProfiles, err := startCollectionStorageRuntimeProfiles(baseCfg)
	if err != nil {
		return "", err
	}
	runtimeProfilesActive := true
	defer func() {
		if runtimeProfilesActive {
			_ = finishRuntimeProfiles()
		}
	}()

	profileHooks := profileHooksFromConfig(baseCfg)
	var cpuFile *os.File
	cpuProfileActive := false
	defer func() {
		if cpuProfileActive {
			profileHooks.stopCPUProfile()
			_ = cpuFile.Close()
		}
	}()
	allocBasePath := ""

	fixtureStart := time.Now()
	fixture, sourceBytes := buildCollectionStorageFixture(rows, payloadSize, cardinality, vectorDims, fieldCount, seed)
	stages := []collectionStorageStageMetric{collectionStorageStage("", "fixture_generate", fixtureStart, rows, sourceBytes)}

	queryRange := collectionStorageRangeForSelectivity(fixture, selectivity)
	modeRuntimes := make([]*collectionStorageModeRuntime, 0, len(modes))
	metrics := make([]collectionStorageWorkloadMetric, 0, len(modes)*len(workloads))
	dataDirs := map[string]string{}
	cleanupDirs := make([]string, 0, len(modes))
	cleanupDBs := make([]*backenddb.DB, 0, len(modes))
	defer func() {
		for _, db := range cleanupDBs {
			_ = db.Close()
		}
		if !baseCfg.KeepDir {
			for _, dir := range cleanupDirs {
				_ = os.RemoveAll(dir)
			}
		}
	}()

	for _, mode := range modes {
		rt, modeStages, err := prepareCollectionStorageMode(baseCfg, mode, rows, batchSize, sourceBytes, vectorDims, fixture, checkpointReopen)
		stages = append(stages, modeStages...)
		if err != nil {
			return "", err
		}
		modeRuntimes = append(modeRuntimes, rt)
		cleanupDirs = append(cleanupDirs, rt.Dir)
		cleanupDBs = append(cleanupDBs, rt.DB)
		if baseCfg.KeepDir {
			dataDirs[mode] = rt.Dir
		}
		if contains(workloads, collectionStorageWorkloadVectorSearchSmoke) && mode == collectionStorageModeVectorTypedColumn {
			start := time.Now()
			status, err := rt.Collection.RebuildVectorIndex(collectionStorageVectorIndexName)
			if err != nil {
				return "", fmt.Errorf("collection_storage: mode %s vector rebuild: %w", mode, err)
			}
			rt.VectorRebuildDuration = time.Since(start)
			stages = append(stages, collectionStorageStageFromDuration(mode, "vector_rebuild", rt.VectorRebuildDuration, rows, int64(status.NativeRootBytes)))
			if err := refreshCollectionStorageModeStorage(rt); err != nil {
				return "", err
			}
		}
	}

	if shouldCPUProfile(baseCfg, collectionStorageBenchTestName) {
		path := collectionStorageCPUProfilePath(baseCfg)
		cpuFile, err = os.Create(path)
		if err != nil {
			return "", fmt.Errorf("collection_storage: cpuprofile %s: %w", path, err)
		}
		if err := profileHooks.startCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			return "", fmt.Errorf("collection_storage: cpuprofile start %s: %w", path, err)
		}
		cpuProfileActive = true
	}
	if shouldAllocsProfile(baseCfg, collectionStorageBenchTestName) {
		allocBasePath, err = profileHooks.writeAllocsSnapshotTemp("unified_bench_collection_storage_allocs_base")
		if err != nil {
			if cpuProfileActive {
				profileHooks.stopCPUProfile()
				_ = cpuFile.Close()
				cpuProfileActive = false
			}
			return "", fmt.Errorf("collection_storage: allocsprofile baseline: %w", err)
		}
	}

	for _, rt := range modeRuntimes {
		for _, workload := range workloads {
			metric, err := runCollectionStorageWorkload(rt, workload, fixture, queryRange, queryCount, pointGetCount, vectorTopK, includeFinalFetch, vectorFullDocuments, assetReadIntegrity)
			if err != nil {
				return "", err
			}
			metrics = append(metrics, metric)
		}
	}

	if cpuProfileActive {
		profileHooks.stopCPUProfile()
		_ = cpuFile.Close()
		cpuProfileActive = false
	}
	if allocBasePath != "" {
		allocAfterPath, snapErr := profileHooks.writeAllocsSnapshotTemp("unified_bench_collection_storage_allocs_after")
		if snapErr != nil {
			_ = os.Remove(allocBasePath)
			return "", fmt.Errorf("collection_storage: allocsprofile snapshot: %w", snapErr)
		}
		allocPath := collectionStorageAllocsProfilePath(baseCfg)
		deltaErr := profileHooks.writeAllocsDeltaProfile(allocBasePath, allocAfterPath, allocPath)
		_ = os.Remove(allocBasePath)
		_ = os.Remove(allocAfterPath)
		if deltaErr != nil {
			return "", fmt.Errorf("collection_storage: allocsprofile %s: %w", allocPath, deltaErr)
		}
	}
	profileFinalizeErr := finishRuntimeProfiles()
	runtimeProfilesActive = false

	modeSemantics := make([]collectionStorageModeSemantics, 0, len(modeRuntimes))
	for _, rt := range modeRuntimes {
		modeSemantics = append(modeSemantics, rt.ModeSemantics)
	}
	report := collectionStorageReport{
		GeneratedAt:              time.Now().UTC().Format(time.RFC3339),
		Suite:                    collectionStorageSuiteName,
		Profile:                  profile,
		Rows:                     rows,
		BatchSize:                batchSize,
		Seed:                     seed,
		Modes:                    cloneStringSlice(modes),
		Workloads:                cloneStringSlice(workloads),
		QueryCount:               queryCount,
		PointGetCount:            pointGetCount,
		FieldCount:               fieldCount,
		PayloadSize:              payloadSize,
		Cardinality:              cardinality,
		Selectivity:              selectivity,
		VectorDims:               vectorDims,
		VectorTopK:               vectorTopK,
		IncludeFinalFetch:        includeFinalFetch,
		VectorFinalFetchShape:    vectorFinalFetchShape,
		CheckpointReopen:         checkpointReopen,
		ColumnAssetReadIntegrity: string(assetReadIntegrity),
		BenchmarkOnlyRelaxed:     collectionStorageAssetReadIntegrityBenchmarkRelaxed(assetReadIntegrity),
		PathLabel:                executionPath,
		Semantics:                collectionStorageReportSemantics(rows, queryRange, includeFinalFetch, vectorFinalFetchShape, checkpointReopen, assetReadIntegrity),
		ModeSemantics:            modeSemantics,
		Stages:                   stages,
		Metrics:                  metrics,
	}
	if baseCfg.KeepDir {
		report.DataDirs = dataDirs
	}
	if profileFinalizeErr != nil {
		report.ProfileFinalizeError = profileFinalizeErr.Error()
	}

	md := renderCollectionStorageSuiteMarkdown(report)
	run := collectionStorageBenchRun(baseCfg, profile, report, modeRuntimes)
	if strings.TrimSpace(opts.ProfileDir) != "" {
		report.Artifacts = collectionStorageArtifactPathsForProfileDir(opts.ProfileDir, baseCfg, opts.RunBenchprof)
		md = renderCollectionStorageSuiteMarkdown(report)
		if err := writeCollectionStorageSuiteArtifacts(opts.ProfileDir, executionPath, report, md, run); err != nil {
			return "", err
		}
		if opts.RunBenchprof {
			if err := runBenchprofStrict(opts.ProfileDir); err != nil {
				return "", err
			}
		}
	}
	if profileFinalizeErr != nil {
		return "", profileFinalizeErr
	}
	return md, nil
}

func collectionStorageVectorFinalFetchShape(includeFinalFetch, fullDocuments bool) string {
	if !includeFinalFetch {
		return collectionStorageVectorFinalFetchNone
	}
	if fullDocuments {
		return collectionStorageVectorFinalFetchFullDocument
	}
	return collectionStorageVectorFinalFetchProjectionNoEmbedding
}

func collectionStorageEffectiveProfile(profile string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "", "durable", "balanced":
		return "durable", nil
	case "fast", "unsafe", "wal_on_fast":
		return "", fmt.Errorf("collection_storage: profile %q is benchmark-relaxed and unsupported for first-class collection-storage correctness runs; use -profile durable", profile)
	default:
		return "", fmt.Errorf("collection_storage: unsupported profile %q; use durable", profile)
	}
}

func collectionStorageEffectiveRows(cfg BenchConfig) int {
	if flagExplicit("keys") {
		return cfg.Keys
	}
	return collectionStorageDefaultRows
}

func collectionStorageEffectiveModes(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		value = strings.TrimSpace(*collectionStorageModesArg)
	}
	items := parseList(value)
	if len(items) == 0 || (len(items) == 1 && (items[0] == "" || strings.EqualFold(items[0], "all"))) {
		return cloneStringSlice(collectionStorageAllModes), nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		mode := normalizeCollectionStorageMode(item)
		if mode == "all" {
			for _, all := range collectionStorageAllModes {
				if _, ok := seen[all]; !ok {
					seen[all] = struct{}{}
					out = append(out, all)
				}
			}
			continue
		}
		if !collectionStorageModeKnown(mode) {
			return nil, fmt.Errorf("collection_storage: unknown mode %q; supported modes: %s", item, strings.Join(collectionStorageAllModes, ","))
		}
		if _, ok := seen[mode]; ok {
			return nil, fmt.Errorf("collection_storage: duplicate mode %q", mode)
		}
		seen[mode] = struct{}{}
		out = append(out, mode)
	}
	if len(out) == 0 {
		return nil, errors.New("collection_storage: at least one mode is required")
	}
	return out, nil
}

func normalizeCollectionStorageMode(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	v = strings.ReplaceAll(v, "-", "_")
	switch v {
	case "doc", "document", "document_only", "retained_document":
		return collectionStorageModeDocumentOnly
	case "row", "typed_row", "typed_row_asset":
		return collectionStorageModeTypedRowAsset
	case "column", "typed_column", "typed_column_part":
		return collectionStorageModeTypedColumnPart
	case "doc_row", "document_row", "hybrid_document_row":
		return collectionStorageModeHybridDocumentRow
	case "doc_column", "document_column", "hybrid_document_column":
		return collectionStorageModeHybridDocumentColumn
	case "row_column", "hybrid_row_column":
		return collectionStorageModeHybridRowColumn
	case "doc_row_column", "document_row_column", "hybrid_document_row_column":
		return collectionStorageModeHybridDocumentRowColumn
	case "vector", "vector_column", "vector_typed_column":
		return collectionStorageModeVectorTypedColumn
	default:
		return v
	}
}

func collectionStorageModeKnown(mode string) bool {
	for _, known := range collectionStorageAllModes {
		if mode == known {
			return true
		}
	}
	return false
}

func collectionStorageEffectiveWorkloads(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		value = strings.TrimSpace(*collectionStorageWorkloadsArg)
	}
	items := parseList(value)
	if len(items) == 0 || (len(items) == 1 && (items[0] == "" || strings.EqualFold(items[0], "all"))) {
		return cloneStringSlice(collectionStorageAllWorkloads), nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		workload := normalizeCollectionStorageWorkload(item)
		if workload == "all" {
			for _, all := range collectionStorageAllWorkloads {
				if _, ok := seen[all]; !ok {
					seen[all] = struct{}{}
					out = append(out, all)
				}
			}
			continue
		}
		if !collectionStorageWorkloadKnown(workload) {
			return nil, fmt.Errorf("collection_storage: unknown workload %q; supported workloads: %s", item, strings.Join(collectionStorageAllWorkloads, ","))
		}
		if _, ok := seen[workload]; ok {
			return nil, fmt.Errorf("collection_storage: duplicate workload %q", workload)
		}
		seen[workload] = struct{}{}
		out = append(out, workload)
	}
	if len(out) == 0 {
		return nil, errors.New("collection_storage: at least one workload is required")
	}
	return out, nil
}

func normalizeCollectionStorageWorkload(value string) string {
	v := strings.ToLower(strings.TrimSpace(value))
	v = strings.ReplaceAll(v, "-", "_")
	switch v {
	case "insert", "insert_batch", "batch_insert":
		return collectionStorageWorkloadInsertBatch
	case "get", "point", "point_get":
		return collectionStorageWorkloadPointGet
	case "scan", "filter", "predicate", "predicate_scan":
		return collectionStorageWorkloadPredicateScan
	case "aggregate", "agg":
		return collectionStorageWorkloadAggregate
	case "vector", "vector_search", "vector_search_smoke":
		return collectionStorageWorkloadVectorSearchSmoke
	case "mixed", "mix":
		return collectionStorageWorkloadMixed
	default:
		return v
	}
}

func validateCollectionStorageSupportedSelection(modes, workloads []string) error {
	for _, workload := range workloads {
		supported := false
		for _, mode := range modes {
			if workload == collectionStorageWorkloadVectorSearchSmoke && mode != collectionStorageModeVectorTypedColumn {
				continue
			}
			supported = true
			break
		}
		if !supported {
			return fmt.Errorf("collection_storage: workload %q has no supported selected mode; vector_search_smoke requires %q", workload, collectionStorageModeVectorTypedColumn)
		}
	}
	return nil
}

func collectionStorageWorkloadKnown(workload string) bool {
	for _, known := range collectionStorageAllWorkloads {
		if workload == known {
			return true
		}
	}
	return false
}

func collectionStorageEffectiveAssetReadIntegrity(opt collections.ColumnAssetReadIntegrity) (collections.ColumnAssetReadIntegrity, error) {
	value := strings.ToLower(strings.TrimSpace(string(opt)))
	if value == "" {
		if flagExplicit("collection-storage-asset-read-integrity") {
			value = strings.ToLower(strings.TrimSpace(*collectionStorageAssetReadIntegrityArg))
		} else if *treedbDisableReadChecksum {
			value = string(collections.ColumnAssetReadIntegritySkipChecksums)
		} else {
			value = strings.ToLower(strings.TrimSpace(*collectionStorageAssetReadIntegrityArg))
		}
	}
	switch value {
	case "", string(collections.ColumnAssetReadIntegrityVerify):
		return collections.ColumnAssetReadIntegrityVerify, nil
	case string(collections.ColumnAssetReadIntegrityCachedVerify), "cached-verify", "verify_once", "verify-once":
		if !*treedbAllowUnsafe {
			return "", errors.New("collection_storage: collection asset read integrity cached_verify requires -treedb-allow-unsafe")
		}
		return collections.ColumnAssetReadIntegrityCachedVerify, nil
	case string(collections.ColumnAssetReadIntegritySkipChecksums), "skip-checksums", "none":
		if !*treedbAllowUnsafe {
			return "", errors.New("collection_storage: collection asset read integrity skip_checksums requires -treedb-allow-unsafe")
		}
		return collections.ColumnAssetReadIntegritySkipChecksums, nil
	default:
		return "", fmt.Errorf("collection_storage: unsupported collection asset read integrity %q; use verify, cached_verify, or skip_checksums", value)
	}
}

func collectionStorageAssetReadIntegrityBenchmarkRelaxed(integrity collections.ColumnAssetReadIntegrity) bool {
	return integrity == collections.ColumnAssetReadIntegrityCachedVerify || integrity == collections.ColumnAssetReadIntegritySkipChecksums
}

func validateCollectionStorageExecutionPath(profileDir, executionPath string) error {
	if strings.TrimSpace(profileDir) == "" {
		return nil
	}
	if err := validateBenchprofExecutionPath(strings.TrimSpace(executionPath)); err != nil {
		return fmt.Errorf("collection_storage: profile-dir: %w", err)
	}
	return nil
}

func buildCollectionStorageFixture(rows, payloadSize, cardinality, vectorDims, fieldCount int, seed int64) ([]collectionStorageFixtureRow, int64) {
	out := make([]collectionStorageFixtureRow, rows)
	payload := strings.Repeat("x", payloadSize)
	var bytesTotal int64
	base := int64(1_700_000_000_000_000 + seed%997)
	for i := 0; i < rows; i++ {
		id := fmt.Sprintf("doc%09d", i)
		kind := fmt.Sprintf("kind_%02d", i%cardinality)
		timeUS := base + int64(i)
		score := int64((i*17 + int(seed%31)) % 100000)
		vec := make([]float32, vectorDims)
		for d := range vec {
			vec[d] = float32(((i+1)*(d+3))%17+1) / 17
		}
		doc := collectionStorageJSONDocument(timeUS, score, kind, payload, vec, i, max(0, fieldCount-3))
		out[i] = collectionStorageFixtureRow{ID: []byte(id), IDString: id, TimeUS: timeUS, Score: score, Kind: kind, Payload: payload, Embedding: vec, Document: doc}
		bytesTotal += int64(len(doc))
	}
	return out, bytesTotal
}

func collectionStorageJSONDocument(timeUS, score int64, kind, payload string, embedding []float32, ordinal, extraFieldCount int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"time_us":`)
	sb.WriteString(fmt.Sprintf("%d", timeUS))
	sb.WriteString(`,"score":`)
	sb.WriteString(fmt.Sprintf("%d", score))
	sb.WriteString(`,"kind":`)
	kindJSON, _ := json.Marshal(kind)
	sb.Write(kindJSON)
	sb.WriteString(`,"payload":`)
	payloadJSON, _ := json.Marshal(payload)
	sb.Write(payloadJSON)
	for i := 0; i < extraFieldCount; i++ {
		sb.WriteString(`,"extra_`)
		sb.WriteString(fmt.Sprintf("%d", i))
		sb.WriteString(`":`)
		sb.WriteString(fmt.Sprintf("%d", (ordinal+i)%11))
	}
	sb.WriteString(`,"embedding":[`)
	for i, v := range embedding {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("%.7g", v))
	}
	sb.WriteString(`]}`)
	return []byte(sb.String())
}

type collectionStoragePredicateRange struct {
	Low          int64
	High         int64
	ExpectedRows int64
	ExpectedSum  int64
}

func collectionStorageRangeForSelectivity(rows []collectionStorageFixtureRow, selectivity float64) collectionStoragePredicateRange {
	if len(rows) == 0 {
		return collectionStoragePredicateRange{}
	}
	match := int(math.Ceil(float64(len(rows)) * selectivity))
	if match < 1 {
		match = 1
	}
	if match > len(rows) {
		match = len(rows)
	}
	start := (len(rows) - match) / 2
	end := start + match - 1
	var sum int64
	for i := start; i <= end; i++ {
		sum += rows[i].TimeUS
	}
	return collectionStoragePredicateRange{Low: rows[start].TimeUS, High: rows[end].TimeUS, ExpectedRows: int64(match), ExpectedSum: sum}
}

func prepareCollectionStorageMode(baseCfg BenchConfig, mode string, rows, batchSize int, sourceBytes int64, vectorDims int, fixture []collectionStorageFixtureRow, checkpointReopen bool) (*collectionStorageModeRuntime, []collectionStorageStageMetric, error) {
	stages := make([]collectionStorageStageMetric, 0, 6)
	dataDir, err := os.MkdirTemp("", "unified-bench-collection-storage-*")
	if err != nil {
		return nil, nil, fmt.Errorf("collection_storage: create temp dir: %w", err)
	}
	if err := backenddb.SaveFormatConfig(dataDir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, nil, fmt.Errorf("collection_storage: save format config: %w", err)
	}
	start := time.Now()
	db, err := openCollectionStorageDB(dataDir)
	if err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, nil, err
	}
	meta, semantics, err := collectionStorageCollectionMeta(mode, vectorDims)
	if err != nil {
		_ = db.Close()
		_ = os.RemoveAll(dataDir)
		return nil, nil, err
	}
	mgr := collections.NewCollectionManager(db)
	if _, err := mgr.CreateCollection(meta); err != nil {
		_ = db.Close()
		_ = os.RemoveAll(dataDir)
		return nil, nil, fmt.Errorf("collection_storage: mode %s create collection: %w", mode, err)
	}
	col, err := mgr.OpenCollection(collectionStorageCollectionName)
	if err != nil {
		_ = db.Close()
		_ = os.RemoveAll(dataDir)
		return nil, nil, fmt.Errorf("collection_storage: mode %s open collection: %w", mode, err)
	}
	stages = append(stages, collectionStorageStage(mode, "open_create", start, 0, 0))

	start = time.Now()
	if err := insertCollectionStorageFixture(col, fixture, batchSize); err != nil {
		_ = db.Close()
		_ = os.RemoveAll(dataDir)
		return nil, nil, err
	}
	insertDuration := time.Since(start)
	stages = append(stages, collectionStorageStageFromDuration(mode, "ingest_insert_batch", insertDuration, rows, sourceBytes))

	var checkpointDuration time.Duration
	if checkpointReopen {
		var checkpointCPUFile *os.File
		if shouldCheckpointCPUProfile(baseCfg, collectionStorageBenchTestName) {
			checkpointCPUFile, err = startCheckpointCPUProfile(baseCfg, profileHooksFromConfig(baseCfg), collectionStorageBenchTestName, collectionStorageBenchDBName)
			if err != nil {
				_ = db.Close()
				_ = os.RemoveAll(dataDir)
				return nil, nil, fmt.Errorf("collection_storage: checkpoint profiling: %w", err)
			}
		}
		start = time.Now()
		checkpointErr := db.Checkpoint()
		if checkpointCPUFile != nil {
			profileHooksFromConfig(baseCfg).stopCPUProfile()
			_ = checkpointCPUFile.Close()
		}
		if checkpointErr != nil {
			_ = db.Close()
			_ = os.RemoveAll(dataDir)
			return nil, nil, fmt.Errorf("collection_storage: mode %s checkpoint: %w", mode, checkpointErr)
		}
		checkpointDuration = time.Since(start)
		stages = append(stages, collectionStorageStageFromDuration(mode, "checkpoint", checkpointDuration, rows, sourceBytes))
		if err := db.Close(); err != nil {
			_ = os.RemoveAll(dataDir)
			return nil, nil, fmt.Errorf("collection_storage: mode %s close before reopen: %w", mode, err)
		}
		start = time.Now()
		db, err = openCollectionStorageDB(dataDir)
		if err != nil {
			_ = os.RemoveAll(dataDir)
			return nil, nil, fmt.Errorf("collection_storage: mode %s reopen: %w", mode, err)
		}
		col, err = collections.NewCollectionManager(db).OpenCollection(collectionStorageCollectionName)
		if err != nil {
			_ = db.Close()
			_ = os.RemoveAll(dataDir)
			return nil, nil, fmt.Errorf("collection_storage: mode %s reopen collection: %w", mode, err)
		}
		reopenDuration := time.Since(start)
		stages = append(stages, collectionStorageStageFromDuration(mode, "reopen_recovery", reopenDuration, rows, sourceBytes))
	}
	totalBytes, totalFiles, err := columnStoreSuiteDirUsage(dataDir)
	if err != nil {
		_ = db.Close()
		_ = os.RemoveAll(dataDir)
		return nil, nil, fmt.Errorf("collection_storage: mode %s DB byte accounting: %w", mode, err)
	}
	rowAssetBytes, columnAssetBytes := collectionStorageAssetByteBreakdown(col)
	rt := &collectionStorageModeRuntime{
		Mode:                  mode,
		Dir:                   dataDir,
		DB:                    db,
		Collection:            col,
		Meta:                  meta,
		ModeSemantics:         semantics,
		InsertDuration:        insertDuration,
		CheckpointDuration:    checkpointDuration,
		DBTotalBytes:          uint64(totalBytes),
		DBTotalFiles:          totalFiles,
		TypedRowAssetBytes:    rowAssetBytes,
		TypedColumnAssetBytes: columnAssetBytes,
		TreeDBStats:           db.Stats(),
		InsertedBytes:         sourceBytes,
	}
	if checkpointReopen && len(stages) > 0 {
		rt.ReopenDuration = time.Duration(stages[len(stages)-1].DurationMS * float64(time.Millisecond))
	}
	return rt, stages, nil
}

func openCollectionStorageDB(dir string) (*backenddb.DB, error) {
	return backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		CommandWALStatsScan:    true,
		Durability:             backenddb.DurabilityDurable,
		DisableBackgroundPrune: true,
	})
}

func insertCollectionStorageFixture(col *collections.Collection, fixture []collectionStorageFixtureRow, batchSize int) error {
	ids := make([][]byte, len(fixture))
	docs := make([][]byte, len(fixture))
	for i := range fixture {
		ids[i] = fixture[i].ID
		docs[i] = fixture[i].Document
	}
	for start := 0; start < len(fixture); start += batchSize {
		end := start + batchSize
		if end > len(fixture) {
			end = len(fixture)
		}
		if _, err := col.InsertBatch(ids[start:end], docs[start:end]); err != nil {
			return fmt.Errorf("collection_storage: InsertBatch rows %d-%d: %w", start, end, err)
		}
	}
	return nil
}

func collectionStorageCollectionMeta(mode string, vectorDims int) (*collections.CollectionMeta, collectionStorageModeSemantics, error) {
	sem := collectionStorageModeSemantics{Mode: mode, OwnerMap: map[string]string{}, ComparableWorkloads: cloneStringSlice(collectionStorageAllWorkloads)}
	meta := &collections.CollectionMeta{
		Name: collectionStorageCollectionName,
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
		},
	}
	if mode == collectionStorageModeDocumentOnly {
		sem.RetainedPayloadPolicy = string(collections.ColumnRetainedPayloadFull)
		sem.DocumentPayloadRetains = "full logical document; no typed-storage owners"
		sem.OwnerMap = map[string]string{"*": string(collections.TypedStorageOwnerRetainedDocument)}
		sem.UnsupportedWorkloads = map[string]string{collectionStorageWorkloadVectorSearchSmoke: "document_only has no column_graph vector index; vector smoke is represented by vector_typed_column"}
		return meta, sem, nil
	}
	cfg := &collections.ColumnStoreConfig{
		Enabled: true,
		Columns: []collections.ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "score", Path: "score", ValueType: collections.ColumnStoreValueInt64},
		},
		SortKey:        []collections.ColumnSortKey{{Column: "time_us"}},
		Reconstruction: collections.ColumnReconstructionRetainedPayloadAndColumns,
		ProfileSupport: collections.ColumnStoreProfileDurableOnly,
	}
	setOwners := func(timeOwner, kindOwner, scoreOwner collections.TypedStorageFieldOwner) {
		cfg.Columns[0].Owner = timeOwner
		cfg.Columns[1].Owner = kindOwner
		cfg.Columns[2].Owner = scoreOwner
	}
	switch mode {
	case collectionStorageModeTypedRowAsset:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadNonColumn
		setOwners(collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset)
		sem.DocumentPayloadRetains = "non-declared payload fields only; queried scalar fields owned by typed_row_asset"
	case collectionStorageModeTypedColumnPart:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadNone
		setOwners(collections.TypedStorageOwnerColumnPart, collections.TypedStorageOwnerColumnPart, collections.TypedStorageOwnerColumnPart)
		sem.DocumentPayloadRetains = "none; queried scalar fields reconstructed from typed_column_part assets"
	case collectionStorageModeHybridDocumentRow:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadFull
		setOwners(collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset)
		sem.DocumentPayloadRetains = "full document retained while scalar owners are typed_row_asset compatibility duplicates"
	case collectionStorageModeHybridDocumentColumn:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadFull
		setOwners(collections.TypedStorageOwnerColumnPart, collections.TypedStorageOwnerColumnPart, collections.TypedStorageOwnerColumnPart)
		sem.DocumentPayloadRetains = "full document retained while scalar owners are typed_column_part compatibility duplicates"
	case collectionStorageModeHybridRowColumn:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadNone
		setOwners(collections.TypedStorageOwnerColumnPart, collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset)
		sem.DocumentPayloadRetains = "none; time_us owned by typed_column_part, kind/score owned by typed_row_asset"
	case collectionStorageModeHybridDocumentRowColumn:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadFull
		setOwners(collections.TypedStorageOwnerColumnPart, collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset)
		sem.DocumentPayloadRetains = "full document retained while time_us is typed_column_part and kind/score are typed_row_asset duplicates"
	case collectionStorageModeVectorTypedColumn:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadNonColumn
		setOwners(collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset, collections.TypedStorageOwnerRowAsset)
		cfg.Columns = append(cfg.Columns, collections.ColumnStoreColumn{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: vectorDims})
		meta.VectorIndexes = []collections.VectorIndexDefinition{{Name: collectionStorageVectorIndexName, Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: vectorDims, M: 4, EfSearch: 16, Strategy: collections.VectorIndexStrategyColumnGraph}}
		sem.DocumentPayloadRetains = "non-declared payload fields; vector field owned by typed_column_part dense sections"
		sem.VectorIndexStrategy = string(collections.VectorIndexStrategyColumnGraph)
	default:
		return nil, sem, fmt.Errorf("collection_storage: unknown mode %q", mode)
	}
	sem.RetainedPayloadPolicy = string(cfg.RetainedPayload)
	sem.ReconstructionPolicy = string(cfg.Reconstruction)
	for _, col := range cfg.Columns {
		owner := col.Owner
		if owner == "" {
			owner = collections.TypedStorageOwnerRowAsset
		}
		sem.OwnerMap[col.Path] = string(owner)
	}
	if mode != collectionStorageModeVectorTypedColumn {
		sem.UnsupportedWorkloads = map[string]string{collectionStorageWorkloadVectorSearchSmoke: "mode has no column_graph vector index; select vector_typed_column for typed-column vector storage"}
	}
	meta.Options.ColumnStore = cfg
	return meta, sem, nil
}

func refreshCollectionStorageModeStorage(rt *collectionStorageModeRuntime) error {
	if rt == nil {
		return nil
	}
	totalBytes, totalFiles, err := columnStoreSuiteDirUsage(rt.Dir)
	if err != nil {
		return fmt.Errorf("collection_storage: mode %s DB byte accounting refresh: %w", rt.Mode, err)
	}
	rt.DBTotalBytes = uint64(totalBytes)
	rt.DBTotalFiles = totalFiles
	rt.TypedRowAssetBytes, rt.TypedColumnAssetBytes = collectionStorageAssetByteBreakdown(rt.Collection)
	if rt.DB != nil {
		rt.TreeDBStats = rt.DB.Stats()
	}
	return nil
}

func collectionStorageAssetByteBreakdown(col *collections.Collection) (rowBytes, columnBytes int64) {
	if col == nil {
		return 0, 0
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), collections.ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		return 0, 0
	}
	for _, entry := range plan.Entries {
		switch entry.Ref.Kind {
		case collections.ColumnAssetKindTCS1PartImage:
			rowBytes += int64(entry.Ref.Length)
		case collections.ColumnAssetKindTCS1TypedColumnPart:
			columnBytes += int64(entry.Ref.Length)
		case collections.ColumnAssetKindTCS1AggregateMetadata, collections.ColumnAssetKindTCS1DictionaryCodes, collections.ColumnAssetKindTCS1Int64Values:
			columnBytes += int64(entry.Ref.Length)
		}
	}
	return rowBytes, columnBytes
}

func runCollectionStorageWorkload(rt *collectionStorageModeRuntime, workload string, fixture []collectionStorageFixtureRow, queryRange collectionStoragePredicateRange, queryCount, pointGetCount, vectorTopK int, includeFinalFetch bool, vectorFullDocuments bool, assetReadIntegrity collections.ColumnAssetReadIntegrity) (collectionStorageWorkloadMetric, error) {
	metric := collectionStorageWorkloadMetric{
		Mode:                  rt.Mode,
		Workload:              workload,
		Rows:                  len(fixture),
		SemanticEquivalent:    true,
		CorrectnessValidated:  false,
		DBTotalBytes:          rt.DBTotalBytes,
		DBTotalFiles:          rt.DBTotalFiles,
		TypedRowAssetBytes:    rt.TypedRowAssetBytes,
		TypedColumnAssetBytes: rt.TypedColumnAssetBytes,
	}
	if reason := collectionStorageUnsupportedReason(rt, workload); reason != "" {
		metric.Supported = false
		metric.UnsupportedReason = reason
		metric.SemanticEquivalent = false
		metric.SemanticNote = reason
		return metric, nil
	}
	metric.Supported = true
	switch workload {
	case collectionStorageWorkloadInsertBatch:
		metric.CorrectnessValidated = true
		metric.DurationMS = durationMS(rt.InsertDuration)
		metric.Ops = 1
		metric.RowsProcessed = int64(len(fixture))
		metric.RowsPerSecond = rate(float64(len(fixture)), rt.InsertDuration)
		metric.OpsPerSecond = rate(1, rt.InsertDuration)
		metric.NsPerOp = float64(rt.InsertDuration.Nanoseconds())
		metric.BytesPerOp = float64(rt.InsertedBytes)
		return metric, nil
	case collectionStorageWorkloadPointGet:
		return runCollectionStoragePointGet(rt, metric, fixture, pointGetCount)
	case collectionStorageWorkloadPredicateScan:
		return runCollectionStoragePredicateScan(rt, metric, queryRange, queryCount, assetReadIntegrity)
	case collectionStorageWorkloadAggregate:
		return runCollectionStorageAggregate(rt, metric, queryRange, queryCount, assetReadIntegrity)
	case collectionStorageWorkloadVectorSearchSmoke:
		return runCollectionStorageVectorSearch(rt, metric, fixture, queryCount, vectorTopK, includeFinalFetch, vectorFullDocuments)
	case collectionStorageWorkloadMixed:
		return runCollectionStorageMixed(rt, metric, fixture, queryRange, queryCount, assetReadIntegrity)
	default:
		return metric, fmt.Errorf("collection_storage: unknown workload %q", workload)
	}
}

func collectionStorageUnsupportedReason(rt *collectionStorageModeRuntime, workload string) string {
	if workload == collectionStorageWorkloadVectorSearchSmoke && rt.Mode != collectionStorageModeVectorTypedColumn {
		return "vector_search_smoke requires vector_typed_column mode so vector storage, graph search, and final-fetch semantics are explicit"
	}
	return ""
}

func runCollectionStoragePointGet(rt *collectionStorageModeRuntime, metric collectionStorageWorkloadMetric, fixture []collectionStorageFixtureRow, pointGetCount int) (collectionStorageWorkloadMetric, error) {
	ops := pointGetCount
	if ops > len(fixture) {
		ops = len(fixture)
	}
	if ops <= 0 {
		return metric, errors.New("collection_storage: point_get needs at least one fixture row")
	}
	if err := validateCollectionStoragePointGet(rt, fixture, ops); err != nil {
		return metric, err
	}
	allocStart := readCollectionStorageMemStats()
	start := time.Now()
	var decoded collectionStorageDecodedDocument
	for i := 0; i < ops; i++ {
		idx := collectionStorageSampleIndex(i, len(fixture))
		doc, err := rt.Collection.Get(fixture[idx].ID)
		if err != nil {
			return metric, fmt.Errorf("collection_storage: mode %s point_get: %w", rt.Mode, err)
		}
		if len(doc) == 0 {
			return metric, fmt.Errorf("collection_storage: mode %s point_get id %q missing", rt.Mode, fixture[idx].IDString)
		}
		if err := json.Unmarshal(doc, &decoded); err != nil {
			return metric, fmt.Errorf("collection_storage: mode %s point_get decode id %q: %w", rt.Mode, fixture[idx].IDString, err)
		}
	}
	dur := time.Since(start)
	allocDelta := collectionStorageMemDelta(allocStart, readCollectionStorageMemStats())
	metric.CorrectnessValidated = true
	metric.DurationMS = durationMS(dur)
	metric.Ops = int64(ops)
	metric.RowsProcessed = int64(ops)
	metric.QueriesPerSecond = rate(float64(ops), dur)
	metric.OpsPerSecond = rate(float64(ops), dur)
	metric.NsPerOp = nsPerOp(dur, int64(ops))
	metric.BytesPerOp = perOp(float64(allocDelta.Bytes), int64(ops))
	metric.AllocsPerOp = perOp(float64(allocDelta.Allocs), int64(ops))
	metric.Counters.DocumentReconstructions = int64(ops)
	if rt.Mode == collectionStorageModeDocumentOnly || strings.Contains(rt.ModeSemantics.DocumentPayloadRetains, "full document") {
		metric.Counters.DocumentMaterializations = int64(ops)
	} else {
		metric.Counters.RowMaterializations = int64(ops)
	}
	return metric, nil
}

func validateCollectionStoragePointGet(rt *collectionStorageModeRuntime, fixture []collectionStorageFixtureRow, samples int) error {
	for i := 0; i < samples; i++ {
		idx := collectionStorageSampleIndex(i, len(fixture))
		doc, err := rt.Collection.Get(fixture[idx].ID)
		if err != nil {
			return fmt.Errorf("collection_storage: mode %s point_get correctness: %w", rt.Mode, err)
		}
		decoded, err := decodeCollectionStorageDocument(doc)
		if err != nil {
			return fmt.Errorf("collection_storage: mode %s point_get correctness decode id %q: %w", rt.Mode, fixture[idx].IDString, err)
		}
		if decoded.TimeUS != fixture[idx].TimeUS || decoded.Score != fixture[idx].Score || decoded.Kind != fixture[idx].Kind {
			return fmt.Errorf("collection_storage: mode %s point_get correctness id %q got time=%d score=%d kind=%q want time=%d score=%d kind=%q", rt.Mode, fixture[idx].IDString, decoded.TimeUS, decoded.Score, decoded.Kind, fixture[idx].TimeUS, fixture[idx].Score, fixture[idx].Kind)
		}
	}
	return nil
}

func runCollectionStoragePredicateScan(rt *collectionStorageModeRuntime, metric collectionStorageWorkloadMetric, queryRange collectionStoragePredicateRange, queryCount int, assetReadIntegrity collections.ColumnAssetReadIntegrity) (collectionStorageWorkloadMetric, error) {
	validate, err := rt.Collection.RunTypedColumnInt64PredicateScan(collections.TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: queryRange.Low, High: queryRange.High, ColumnAssetReadIntegrity: assetReadIntegrity})
	if err != nil {
		return metric, fmt.Errorf("collection_storage: mode %s predicate_scan correctness: %w", rt.Mode, err)
	}
	if int64(len(validate.Rows)) != queryRange.ExpectedRows || int64(validate.Diagnostics.RowsMatched) != queryRange.ExpectedRows {
		return metric, fmt.Errorf("collection_storage: mode %s predicate_scan correctness matches=%d rows=%d want %d", rt.Mode, validate.Diagnostics.RowsMatched, len(validate.Rows), queryRange.ExpectedRows)
	}
	allocStart := readCollectionStorageMemStats()
	start := time.Now()
	var last collections.TypedColumnInt64PredicateScanResult
	for i := 0; i < queryCount; i++ {
		last, err = rt.Collection.RunTypedColumnInt64PredicateScan(collections.TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: queryRange.Low, High: queryRange.High, ColumnAssetReadIntegrity: assetReadIntegrity})
		if err != nil {
			return metric, fmt.Errorf("collection_storage: mode %s predicate_scan: %w", rt.Mode, err)
		}
	}
	dur := time.Since(start)
	allocDelta := collectionStorageMemDelta(allocStart, readCollectionStorageMemStats())
	metric.CorrectnessValidated = true
	metric.DurationMS = durationMS(dur)
	metric.Ops = int64(queryCount)
	metric.RowsProcessed = int64(last.Diagnostics.RowsScanned * queryCount)
	metric.RowsPerSecond = rate(float64(metric.RowsProcessed), dur)
	metric.QueriesPerSecond = rate(float64(queryCount), dur)
	metric.OpsPerSecond = metric.QueriesPerSecond
	metric.Matches = int64(last.Diagnostics.RowsMatched * queryCount)
	metric.MatchesPerSecond = rate(float64(metric.Matches), dur)
	metric.NsPerOp = nsPerOp(dur, int64(queryCount))
	metric.BytesPerOp = perOp(float64(allocDelta.Bytes), int64(queryCount))
	metric.AllocsPerOp = perOp(float64(allocDelta.Allocs), int64(queryCount))
	metric.Counters = collectionStorageCountersFromTypedColumnDiag(last.Diagnostics)
	collectionStorageScaleCounters(&metric.Counters, int64(queryCount))
	return metric, nil
}

func runCollectionStorageAggregate(rt *collectionStorageModeRuntime, metric collectionStorageWorkloadMetric, queryRange collectionStoragePredicateRange, queryCount int, assetReadIntegrity collections.ColumnAssetReadIntegrity) (collectionStorageWorkloadMetric, error) {
	validate, err := rt.Collection.RunTypedColumnInt64PredicateAggregate(collections.TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: queryRange.Low, High: queryRange.High, ColumnAssetReadIntegrity: assetReadIntegrity})
	if err != nil {
		return metric, fmt.Errorf("collection_storage: mode %s aggregate correctness: %w", rt.Mode, err)
	}
	if validate.Count != queryRange.ExpectedRows || validate.Sum != queryRange.ExpectedSum {
		return metric, fmt.Errorf("collection_storage: mode %s aggregate correctness count=%d sum=%d want count=%d sum=%d", rt.Mode, validate.Count, validate.Sum, queryRange.ExpectedRows, queryRange.ExpectedSum)
	}
	allocStart := readCollectionStorageMemStats()
	start := time.Now()
	var last collections.TypedColumnInt64PredicateAggregateResult
	for i := 0; i < queryCount; i++ {
		last, err = rt.Collection.RunTypedColumnInt64PredicateAggregate(collections.TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: queryRange.Low, High: queryRange.High, ColumnAssetReadIntegrity: assetReadIntegrity})
		if err != nil {
			return metric, fmt.Errorf("collection_storage: mode %s aggregate: %w", rt.Mode, err)
		}
	}
	dur := time.Since(start)
	allocDelta := collectionStorageMemDelta(allocStart, readCollectionStorageMemStats())
	metric.CorrectnessValidated = true
	metric.DurationMS = durationMS(dur)
	metric.Ops = int64(queryCount)
	metric.RowsProcessed = int64(last.Diagnostics.RowsScanned * queryCount)
	metric.RowsPerSecond = rate(float64(metric.RowsProcessed), dur)
	metric.QueriesPerSecond = rate(float64(queryCount), dur)
	metric.OpsPerSecond = metric.QueriesPerSecond
	metric.Matches = int64(last.Diagnostics.RowsMatched * queryCount)
	metric.MatchesPerSecond = rate(float64(metric.Matches), dur)
	metric.NsPerOp = nsPerOp(dur, int64(queryCount))
	metric.BytesPerOp = perOp(float64(allocDelta.Bytes), int64(queryCount))
	metric.AllocsPerOp = perOp(float64(allocDelta.Allocs), int64(queryCount))
	metric.Counters = collectionStorageCountersFromTypedColumnDiag(last.Diagnostics)
	collectionStorageScaleCounters(&metric.Counters, int64(queryCount))
	return metric, nil
}

func runCollectionStorageVectorSearch(rt *collectionStorageModeRuntime, metric collectionStorageWorkloadMetric, fixture []collectionStorageFixtureRow, queryCount, vectorTopK int, includeFinalFetch bool, vectorFullDocuments bool) (collectionStorageWorkloadMetric, error) {
	if len(fixture) == 0 {
		return metric, errors.New("collection_storage: vector search needs at least one fixture row")
	}
	query := append([]float32(nil), fixture[0].Embedding...)
	searchOpts, err := collectionStorageVectorSearchOptions(query, min(vectorTopK, len(fixture)), len(fixture), includeFinalFetch, vectorFullDocuments)
	if err != nil {
		return metric, err
	}
	validate, err := rt.Collection.SearchVectorIndex(searchOpts)
	if err != nil {
		return metric, fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness: %w", rt.Mode, err)
	}
	if err := validateCollectionStorageVectorResults(rt.Mode, validate.Results, fixture, min(vectorTopK, len(fixture)), includeFinalFetch, vectorFullDocuments); err != nil {
		return metric, err
	}
	allocStart := readCollectionStorageMemStats()
	start := time.Now()
	var last collections.VectorIndexSearchResponse
	for i := 0; i < queryCount; i++ {
		last, err = rt.Collection.SearchVectorIndex(searchOpts)
		if err != nil {
			return metric, fmt.Errorf("collection_storage: mode %s vector_search_smoke: %w", rt.Mode, err)
		}
	}
	dur := time.Since(start)
	allocDelta := collectionStorageMemDelta(allocStart, readCollectionStorageMemStats())
	metric.CorrectnessValidated = true
	metric.DurationMS = durationMS(dur)
	metric.Ops = int64(queryCount)
	metric.RowsProcessed = int64(last.Stats.Candidates) * int64(queryCount)
	metric.QueriesPerSecond = rate(float64(queryCount), dur)
	metric.OpsPerSecond = metric.QueriesPerSecond
	metric.NsPerOp = nsPerOp(dur, int64(queryCount))
	metric.BytesPerOp = perOp(float64(allocDelta.Bytes), int64(queryCount))
	metric.AllocsPerOp = perOp(float64(allocDelta.Allocs), int64(queryCount))
	metric.Matches = int64(len(last.Results) * queryCount)
	metric.MatchesPerSecond = rate(float64(metric.Matches), dur)
	metric.SemanticNote = collectionStorageVectorSearchSemanticNote(includeFinalFetch, vectorFullDocuments)
	metric.Counters.VectorCandidates = last.Stats.Candidates * uint64(queryCount)
	metric.Counters.VectorEdges = last.Stats.Edges * uint64(queryCount)
	metric.Counters.VectorDirectViews = last.Stats.VectorDirectViews * uint64(queryCount)
	metric.Counters.VectorScratchDecodes = last.Stats.VectorScratchDecodes * uint64(queryCount)
	metric.Counters.VectorDocumentsFetched = last.Stats.DocumentsFetched * uint64(queryCount)
	metric.Counters.VectorDocumentOutputBytes = last.Stats.DocumentOutputBytes * uint64(queryCount)
	metric.Counters.VectorDocumentFieldsSkipped = last.Stats.DocumentFieldsSkipped * uint64(queryCount)
	metric.Counters.VectorDocumentRetainedBytes = last.Stats.DocumentRetainedBytes * uint64(queryCount)
	metric.Counters.VectorTypedColumnMappedBytes = last.Stats.TypedColumnMappedBytes
	metric.Counters.VectorTypedColumnHeapBytes = last.Stats.TypedColumnHeapCopyBytes
	metric.Counters.VectorTypedColumnDecoded = last.Stats.TypedColumnDecodedBytes
	metric.Counters.MappedBytes = last.Stats.TypedColumnMappedBytes
	metric.Counters.HeapCopyBytes = last.Stats.TypedColumnHeapCopyBytes
	metric.Counters.DecodedBytes = last.Stats.TypedColumnDecodedBytes
	metric.Counters.DocumentReconstructions = int64(last.Stats.DocumentsFetched * uint64(queryCount))
	return metric, nil
}

func collectionStorageVectorSearchOptions(query []float32, topK, efSearch int, includeFinalFetch, vectorFullDocuments bool) (collections.VectorIndexSearchOptions, error) {
	opts := collections.VectorIndexSearchOptions{
		IndexName:        collectionStorageVectorIndexName,
		Query:            query,
		TopK:             topK,
		EfSearch:         efSearch,
		MaxDecodedBlocks: 1,
	}
	if !includeFinalFetch {
		return opts, nil
	}
	if vectorFullDocuments {
		opts.IncludeDocuments = true
		return opts, nil
	}
	preset, err := collections.ProjectionOrientedVectorDocumentFetchPresetForField("embedding")
	if err != nil {
		return opts, err
	}
	preset.ApplyToSearchOptions(&opts)
	return opts, nil
}

func collectionStorageVectorSearchSemanticNote(includeFinalFetch, vectorFullDocuments bool) string {
	if !includeFinalFetch {
		return "vector scoring/search returns top-k IDs/scores only; no post-top-k document fetch is timed"
	}
	if vectorFullDocuments {
		return "vector scoring/search excludes document materialization until top-k; final fetch is explicit full-document/embedding-echo comparison path"
	}
	return "vector scoring/search excludes document materialization until top-k; final fetch uses preferred projection_without_embedding preset path"
}

func validateCollectionStorageVectorResults(mode string, results []collections.VectorIndexSearchResult, fixture []collectionStorageFixtureRow, topK int, includeFinalFetch bool, vectorFullDocuments bool) error {
	if len(results) == 0 {
		return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness returned no results", mode)
	}
	if len(results) > topK {
		return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness returned %d results > topK %d", mode, len(results), topK)
	}
	known := make(map[string]struct{}, len(fixture))
	for _, row := range fixture {
		known[row.IDString] = struct{}{}
	}
	for _, result := range results {
		id := string(result.ID)
		if _, ok := known[id]; !ok {
			return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness returned unknown id %q", mode, id)
		}
		if math.IsNaN(float64(result.Score)) || math.IsInf(float64(result.Score), 0) {
			return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness returned non-finite score for id %q", mode, id)
		}
		if includeFinalFetch {
			if len(result.Document) == 0 {
				return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness id %q missing final document", mode, id)
			}
			if _, err := decodeCollectionStorageDocument(result.Document); err != nil {
				return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness id %q final document decode: %w", mode, id, err)
			}
			hasEmbedding, err := collectionStorageDocumentHasField(result.Document, "embedding")
			if err != nil {
				return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness id %q final document inspect: %w", mode, id, err)
			}
			if vectorFullDocuments && !hasEmbedding {
				return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness id %q full-document final fetch omitted embedding", mode, id)
			}
			if !vectorFullDocuments && hasEmbedding {
				return fmt.Errorf("collection_storage: mode %s vector_search_smoke correctness id %q projected final fetch retained embedding", mode, id)
			}
		}
	}
	return nil
}

func runCollectionStorageMixed(rt *collectionStorageModeRuntime, metric collectionStorageWorkloadMetric, fixture []collectionStorageFixtureRow, queryRange collectionStoragePredicateRange, queryCount int, assetReadIntegrity collections.ColumnAssetReadIntegrity) (collectionStorageWorkloadMetric, error) {
	if err := validateCollectionStoragePointGet(rt, fixture, 1); err != nil {
		return metric, err
	}
	agg, err := rt.Collection.RunTypedColumnInt64PredicateAggregate(collections.TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: queryRange.Low, High: queryRange.High, ColumnAssetReadIntegrity: assetReadIntegrity})
	if err != nil {
		return metric, fmt.Errorf("collection_storage: mode %s mixed aggregate correctness: %w", rt.Mode, err)
	}
	if agg.Count != queryRange.ExpectedRows {
		return metric, fmt.Errorf("collection_storage: mode %s mixed aggregate count=%d want %d", rt.Mode, agg.Count, queryRange.ExpectedRows)
	}
	allocStart := readCollectionStorageMemStats()
	start := time.Now()
	var lastAgg collections.TypedColumnInt64PredicateAggregateResult
	for i := 0; i < queryCount; i++ {
		idx := collectionStorageSampleIndex(i, len(fixture))
		if _, err := rt.Collection.Get(fixture[idx].ID); err != nil {
			return metric, fmt.Errorf("collection_storage: mode %s mixed point_get: %w", rt.Mode, err)
		}
		lastAgg, err = rt.Collection.RunTypedColumnInt64PredicateAggregate(collections.TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: queryRange.Low, High: queryRange.High, ColumnAssetReadIntegrity: assetReadIntegrity})
		if err != nil {
			return metric, fmt.Errorf("collection_storage: mode %s mixed aggregate: %w", rt.Mode, err)
		}
	}
	dur := time.Since(start)
	allocDelta := collectionStorageMemDelta(allocStart, readCollectionStorageMemStats())
	metric.CorrectnessValidated = true
	metric.DurationMS = durationMS(dur)
	metric.Ops = int64(queryCount * 2)
	metric.RowsProcessed = int64(lastAgg.Diagnostics.RowsScanned*queryCount + queryCount)
	metric.RowsPerSecond = rate(float64(metric.RowsProcessed), dur)
	metric.QueriesPerSecond = rate(float64(queryCount*2), dur)
	metric.OpsPerSecond = metric.QueriesPerSecond
	metric.Matches = int64(lastAgg.Diagnostics.RowsMatched * queryCount)
	metric.MatchesPerSecond = rate(float64(metric.Matches), dur)
	metric.NsPerOp = nsPerOp(dur, int64(queryCount*2))
	metric.BytesPerOp = perOp(float64(allocDelta.Bytes), int64(queryCount*2))
	metric.AllocsPerOp = perOp(float64(allocDelta.Allocs), int64(queryCount*2))
	metric.Counters = collectionStorageCountersFromTypedColumnDiag(lastAgg.Diagnostics)
	collectionStorageScaleCounters(&metric.Counters, int64(queryCount))
	metric.Counters.DocumentReconstructions += int64(queryCount)
	return metric, nil
}

func decodeCollectionStorageDocument(doc []byte) (collectionStorageDecodedDocument, error) {
	var decoded collectionStorageDecodedDocument
	if len(doc) == 0 {
		return decoded, errors.New("empty document")
	}
	if err := json.Unmarshal(doc, &decoded); err != nil {
		return decoded, err
	}
	return decoded, nil
}

func collectionStorageDocumentHasField(doc []byte, field string) (bool, error) {
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(doc, &decoded); err != nil {
		return false, err
	}
	_, ok := decoded[field]
	return ok, nil
}

func collectionStorageSampleIndex(i, rows int) int {
	if rows <= 1 {
		return 0
	}
	return (i * 7919) % rows
}

func collectionStorageCountersFromTypedColumnDiag(diag collections.TypedColumnInt64PredicateScanDiagnostics) collectionStorageWorkloadCounters {
	return collectionStorageWorkloadCounters{
		MappedBytes:                 diag.MappedBytes,
		HeapCopyBytes:               diag.HeapCopyBytes,
		DecodedBytes:                diag.DecodedMetadataBytes + diag.DecodedHeapCopyBytes,
		DocumentMaterializations:    int64(diag.DocumentMaterializations),
		DocumentReconstructions:     int64(diag.DocumentReconstructions),
		RowMaterializations:         int64(diag.RowMaterializations),
		RowLocatorDecodes:           int64(diag.RowLocatorDecodes),
		PhysicalRowAssetReads:       int64(diag.PhysicalRowAssetReads),
		PhysicalRowIDLookups:        int64(diag.PhysicalRowIDLookups),
		TypedColumnPartsConsidered:  int64(diag.PartsConsidered),
		TypedColumnPartsPruned:      int64(diag.PartsPruned),
		TypedColumnPartsDecoded:     int64(diag.PartsDecoded),
		TypedColumnBlocksConsidered: int64(diag.BlocksConsidered),
		TypedColumnBlocksPruned:     int64(diag.BlocksPruned),
		TypedColumnBlocksDecoded:    int64(diag.BlocksDecoded),
		DirectTypedColumnAssetReads: int64(diag.DirectTypedColumnAssetReads),
		AssetOpenMapChecksumReads:   int64(diag.DirectTypedColumnAssetReads + diag.PhysicalRowAssetReads),
		SegmentFileCacheHits:        diag.SegmentFileCacheHits,
		SegmentFileCacheMisses:      diag.SegmentFileCacheMisses,
	}
}

func collectionStorageScaleCounters(c *collectionStorageWorkloadCounters, n int64) {
	if c == nil || n <= 1 {
		return
	}
	c.DocumentMaterializations *= n
	c.DocumentReconstructions *= n
	c.RowMaterializations *= n
	c.RowLocatorDecodes *= n
	c.PhysicalRowAssetReads *= n
	c.PhysicalRowIDLookups *= n
	c.TypedColumnPartsConsidered *= n
	c.TypedColumnPartsPruned *= n
	c.TypedColumnPartsDecoded *= n
	c.TypedColumnBlocksConsidered *= n
	c.TypedColumnBlocksPruned *= n
	c.TypedColumnBlocksDecoded *= n
	c.DirectTypedColumnAssetReads *= n
	c.AssetOpenMapChecksumReads *= n
	c.SegmentFileCacheHits *= uint64(n)
	c.SegmentFileCacheMisses *= uint64(n)
}

func readCollectionStorageMemStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

func collectionStorageMemDelta(before, after runtime.MemStats) collectionStorageAllocDelta {
	var out collectionStorageAllocDelta
	if after.TotalAlloc >= before.TotalAlloc {
		out.Bytes = after.TotalAlloc - before.TotalAlloc
	}
	if after.Mallocs >= before.Mallocs {
		out.Allocs = after.Mallocs - before.Mallocs
	}
	return out
}

func collectionStorageStage(mode, name string, start time.Time, rows int, bytes int64) collectionStorageStageMetric {
	return collectionStorageStageFromDuration(mode, name, time.Since(start), rows, bytes)
}

func collectionStorageStageFromDuration(mode, name string, dur time.Duration, rows int, bytes int64) collectionStorageStageMetric {
	return collectionStorageStageMetric{Mode: mode, Name: name, DurationMS: durationMS(dur), Rows: rows, RowsPerSecond: rate(float64(rows), dur), Bytes: bytes, MiBPerSecond: mibRate(bytes, dur)}
}

func rate(n float64, d time.Duration) float64 {
	if n <= 0 || d <= 0 {
		return 0
	}
	return n / d.Seconds()
}

func mibRate(bytes int64, d time.Duration) float64 {
	if bytes <= 0 || d <= 0 {
		return 0
	}
	return (float64(bytes) / (1024 * 1024)) / d.Seconds()
}

func nsPerOp(d time.Duration, ops int64) float64 {
	if ops <= 0 {
		return 0
	}
	return float64(d.Nanoseconds()) / float64(ops)
}

func perOp(v float64, ops int64) float64 {
	if ops <= 0 {
		return 0
	}
	return v / float64(ops)
}

func collectionStorageReportSemantics(rows int, queryRange collectionStoragePredicateRange, includeFinalFetch bool, vectorFinalFetchShape string, checkpointReopen bool, assetReadIntegrity collections.ColumnAssetReadIntegrity) collectionStorageSemantics {
	return collectionStorageSemantics{
		LogicalRows:              rows,
		InsertedDocuments:        rows,
		FieldsProjected:          []string{"time_us", "kind", "score"},
		FieldsQueried:            []string{"time_us", "embedding"},
		ReturnedResultShape:      "point_get returns logical JSON fields; predicate_scan returns matched row count; aggregate returns count/sum/avg; vector_search_smoke returns top-k IDs/scores plus either no docs, projection_without_embedding docs, or explicit full-document embedding echo",
		FinalDocumentFetch:       includeFinalFetch,
		VectorFinalFetchShape:    vectorFinalFetchShape,
		CheckpointReopenIncluded: checkpointReopen,
		ReadIntegrityMode:        string(assetReadIntegrity),
		SelectivityDistribution:  fmt.Sprintf("clustered_monotonic time_us range [%d,%d] matching %d rows", queryRange.Low, queryRange.High, queryRange.ExpectedRows),
		MaterializationBoundary:  "setup/fixture/create/insert/checkpoint/reopen/vector_rebuild are timed as stages; workload metrics time only the selected operation loop after correctness validation; vector_search_smoke includes post-top-k document fetch only when include_final_fetch is true",
		SemanticComparability:    "scalar workloads use the same logical documents, ids, time_us/kind/score fields, predicate range, aggregate result, and point-get projection across all scalar modes; vector search is only semantically comparable for vector_typed_column and is marked unsupported elsewhere",
	}
}

func renderCollectionStorageSuiteMarkdown(report collectionStorageReport) string {
	var sb strings.Builder
	sb.WriteString("# unified_bench suite: collection_storage\n\n")
	sb.WriteString(fmt.Sprintf("- profile: `%s`\n", report.Profile))
	sb.WriteString(fmt.Sprintf("- rows: %s\n", formatInt(report.Rows)))
	sb.WriteString(fmt.Sprintf("- batchsize: %s\n", formatInt(report.BatchSize)))
	sb.WriteString(fmt.Sprintf("- modes: %s\n", markdownCodeList(report.Modes)))
	sb.WriteString(fmt.Sprintf("- workloads: %s\n", markdownCodeList(report.Workloads)))
	sb.WriteString(fmt.Sprintf("- query_count: %d\n", report.QueryCount))
	sb.WriteString(fmt.Sprintf("- point_get_count: %d\n", report.PointGetCount))
	sb.WriteString(fmt.Sprintf("- selectivity: %.6f\n", report.Selectivity))
	sb.WriteString(fmt.Sprintf("- vector_dims: %d vector_top_k: %d include_final_fetch: %v vector_final_fetch_shape: `%s`\n", report.VectorDims, report.VectorTopK, report.IncludeFinalFetch, report.VectorFinalFetchShape))
	sb.WriteString(fmt.Sprintf("- checkpoint_reopen: %v\n", report.CheckpointReopen))
	sb.WriteString(fmt.Sprintf("- collection asset read integrity: `%s`\n", report.ColumnAssetReadIntegrity))
	if report.PathLabel != "" {
		sb.WriteString(fmt.Sprintf("- path-label: `%s`\n", report.PathLabel))
	}
	if report.ProfileFinalizeError != "" {
		sb.WriteString(fmt.Sprintf("- profile finalize error: `%s`\n", report.ProfileFinalizeError))
	}
	sb.WriteString("\n")

	sb.WriteString("## Comparison Semantics\n\n")
	sb.WriteString(fmt.Sprintf("- logical rows/documents: %d\n", report.Semantics.LogicalRows))
	sb.WriteString(fmt.Sprintf("- projected fields: %s\n", markdownCodeList(report.Semantics.FieldsProjected)))
	sb.WriteString(fmt.Sprintf("- queried fields: %s\n", markdownCodeList(report.Semantics.FieldsQueried)))
	sb.WriteString(fmt.Sprintf("- returned result shape: %s\n", markdownTableText(report.Semantics.ReturnedResultShape)))
	sb.WriteString(fmt.Sprintf("- vector final fetch shape: `%s`\n", report.Semantics.VectorFinalFetchShape))
	sb.WriteString(fmt.Sprintf("- selectivity/distribution: %s\n", markdownTableText(report.Semantics.SelectivityDistribution)))
	sb.WriteString(fmt.Sprintf("- materialization boundary: %s\n", markdownTableText(report.Semantics.MaterializationBoundary)))
	sb.WriteString(fmt.Sprintf("- comparability: %s\n\n", markdownTableText(report.Semantics.SemanticComparability)))

	sb.WriteString("## Mode Layouts\n\n")
	sb.WriteString("| mode | retained payload | reconstruction | owner map | unsupported workloads |\n")
	sb.WriteString("|---|---|---|---|---|\n")
	for _, sem := range report.ModeSemantics {
		owners := collectionStorageOwnerMapString(sem.OwnerMap)
		unsupported := collectionStorageUnsupportedMapString(sem.UnsupportedWorkloads)
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n", markdownCodeTableText(sem.Mode), markdownCodeTableText(sem.RetainedPayloadPolicy), markdownCodeTableText(sem.ReconstructionPolicy), markdownCodeTableText(owners), markdownTableText(unsupported)))
	}
	sb.WriteString("\n")

	sb.WriteString("## Stage Timings\n\n")
	sb.WriteString("| mode | stage | ms | rows/s | MiB/s | bytes |\n")
	sb.WriteString("|---|---|---:|---:|---:|---:|\n")
	for _, st := range report.Stages {
		sb.WriteString(fmt.Sprintf("| %s | %s | %.3f | %.3f | %.3f | %d |\n", markdownCodeTableText(st.Mode), markdownCodeTableText(st.Name), st.DurationMS, st.RowsPerSecond, st.MiBPerSecond, st.Bytes))
	}
	sb.WriteString("\n")

	sb.WriteString("## Workload Metrics\n\n")
	sb.WriteString("| mode | workload | supported | rows/s | queries/s | matches/s | ns/op | B/op | allocs/op | rows processed | matches | row bytes | column bytes | materializations/reconstructions | typed parts decoded/pruned | typed blocks decoded/pruned | vector candidates/edges/docs/out_B/skipped | note |\n")
	sb.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|---|---|\n")
	for _, m := range report.Metrics {
		supported := "yes"
		note := m.SemanticNote
		if !m.Supported {
			supported = "no"
			note = m.UnsupportedReason
		}
		mat := fmt.Sprintf("%d/%d", m.Counters.DocumentMaterializations+m.Counters.RowMaterializations, m.Counters.DocumentReconstructions)
		parts := fmt.Sprintf("%d/%d", m.Counters.TypedColumnPartsDecoded, m.Counters.TypedColumnPartsPruned)
		blocks := fmt.Sprintf("%d/%d", m.Counters.TypedColumnBlocksDecoded, m.Counters.TypedColumnBlocksPruned)
		vector := fmt.Sprintf("%d/%d/%d/%d/%d", m.Counters.VectorCandidates, m.Counters.VectorEdges, m.Counters.VectorDocumentsFetched, m.Counters.VectorDocumentOutputBytes, m.Counters.VectorDocumentFieldsSkipped)
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %.3f | %.3f | %.3f | %.1f | %.1f | %.3f | %d | %d | %d | %d | %s | %s | %s | %s | %s |\n",
			markdownCodeTableText(m.Mode), markdownCodeTableText(m.Workload), supported, m.RowsPerSecond, m.QueriesPerSecond, m.MatchesPerSecond, m.NsPerOp, m.BytesPerOp, m.AllocsPerOp, m.RowsProcessed, m.Matches, m.TypedRowAssetBytes, m.TypedColumnAssetBytes, markdownCodeTableText(mat), markdownCodeTableText(parts), markdownCodeTableText(blocks), markdownCodeTableText(vector), markdownTableText(note)))
	}
	sb.WriteString("\n")

	if report.Artifacts.CollectionJSON != "" {
		sb.WriteString("## Artifacts\n\n")
		columnStoreWriteArtifactLine(&sb, "collection JSON", report.Artifacts.CollectionJSON)
		columnStoreWriteArtifactLine(&sb, "collection markdown", report.Artifacts.CollectionMarkdown)
		columnStoreWriteArtifactLine(&sb, "collection HTML", report.Artifacts.CollectionHTML)
		columnStoreWriteArtifactLine(&sb, "benchprof JSON", report.Artifacts.BenchprofJSON)
		columnStoreWriteArtifactLine(&sb, "benchprof markdown", report.Artifacts.BenchprofMarkdown)
		columnStoreWriteArtifactLine(&sb, "insights markdown", report.Artifacts.InsightsMarkdown)
		columnStoreWriteArtifactLine(&sb, "insights JSON", report.Artifacts.InsightsJSON)
		columnStoreWriteArtifactLine(&sb, "insights HTML", report.Artifacts.InsightsHTML)
		columnStoreWriteArtifactLine(&sb, "CPU profile", report.Artifacts.CPUProfile)
		columnStoreWriteArtifactLine(&sb, "allocs profile", report.Artifacts.AllocsProfile)
		columnStoreWriteArtifactLine(&sb, "checkpoint CPU profile", report.Artifacts.CheckpointCPUProfile)
		columnStoreWriteArtifactLine(&sb, "block profile", report.Artifacts.BlockProfile)
		columnStoreWriteArtifactLine(&sb, "mutex profile", report.Artifacts.MutexProfile)
		columnStoreWriteArtifactLine(&sb, "trace", report.Artifacts.TraceProfile)
	}
	return sb.String()
}

func collectionStorageOwnerMapString(ownerMap map[string]string) string {
	if len(ownerMap) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(ownerMap))
	for key := range ownerMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+ownerMap[key])
	}
	return strings.Join(parts, ", ")
}

func collectionStorageUnsupportedMapString(m map[string]string) string {
	if len(m) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+": "+m[key])
	}
	return strings.Join(parts, "; ")
}

func renderCollectionStorageSuiteHTML(report collectionStorageReport) string {
	md := renderCollectionStorageSuiteMarkdown(report)
	return `<!doctype html>
<html>
<head><meta charset="utf-8"><title>unified_bench collection_storage</title></head>
<body><pre>` + html.EscapeString(md) + `</pre></body>
</html>
`
}

func collectionStorageBenchRun(baseCfg BenchConfig, profile string, report collectionStorageReport, runtimes []*collectionStorageModeRuntime) BenchRun {
	cfg := baseCfg
	cfg.Keys = report.Rows
	cfg.BatchSize = report.BatchSize
	cfg.Profile = profile
	cfg.DBsArg = "treedb"
	testOrder := []string{collectionStorageBenchTestName}
	displayNames := map[string]string{collectionStorageBenchTestName: "Collection storage all workloads"}
	results := map[string]map[string]float64{collectionStorageBenchTestName: {}}
	instances := make([]*DBInstance, 0, len(report.Modes))
	stats := make(map[string]map[string]string, len(runtimes))
	disk := make(map[string]dirDiskUsage, len(runtimes))
	for _, rt := range runtimes {
		label := rt.Mode
		instances = append(instances, &DBInstance{Name: label, Wrapper: &columnStoreSuiteDBLabel{name: label}, Dir: rt.Dir})
		stats[label] = rt.TreeDBStats
		disk[label] = dirDiskUsage{TotalBytes: rt.DBTotalBytes, TotalFiles: rt.DBTotalFiles}
	}
	for _, workload := range report.Workloads {
		testName := collectionStorageBenchMetricPrefix + workload
		testOrder = append(testOrder, testName)
		displayNames[testName] = "Collection " + strings.ReplaceAll(workload, "_", " ")
		results[testName] = map[string]float64{}
	}
	var totalOps float64
	var totalMS float64
	collectionWorkloads := make([]benchprofCollectionWorkload, 0, len(report.Metrics))
	for _, m := range report.Metrics {
		if !m.Supported {
			continue
		}
		testName := collectionStorageBenchMetricPrefix + m.Workload
		value := m.RowsPerSecond
		if value == 0 {
			value = m.QueriesPerSecond
		}
		if value == 0 {
			value = m.OpsPerSecond
		}
		results[testName][m.Mode] = value
		totalOps += m.OpsPerSecond
		totalMS += m.DurationMS
		collectionWorkloads = append(collectionWorkloads, benchprofCollectionWorkload{
			Suite:                 collectionStorageSuiteName,
			Mode:                  m.Mode,
			Workload:              m.Workload,
			Rows:                  m.Rows,
			SemanticEquivalent:    m.SemanticEquivalent,
			SemanticNote:          m.SemanticNote,
			CorrectnessValidated:  m.CorrectnessValidated,
			RowsPerSecond:         m.RowsPerSecond,
			QueriesPerSecond:      m.QueriesPerSecond,
			MatchesPerSecond:      m.MatchesPerSecond,
			OpsPerSecond:          m.OpsPerSecond,
			NsPerOp:               m.NsPerOp,
			BytesPerOp:            m.BytesPerOp,
			AllocsPerOp:           m.AllocsPerOp,
			DBTotalBytes:          m.DBTotalBytes,
			TypedRowAssetBytes:    m.TypedRowAssetBytes,
			TypedColumnAssetBytes: m.TypedColumnAssetBytes,
			Counters:              m.Counters,
		})
	}
	if totalMS > 0 {
		for _, mode := range report.Modes {
			var modeOps float64
			for _, m := range report.Metrics {
				if m.Mode == mode && m.Supported {
					modeOps += m.OpsPerSecond
				}
			}
			results[collectionStorageBenchTestName][mode] = modeOps
		}
	}
	cfg.TestsArg = strings.Join(testOrder, ",")
	return BenchRun{Config: cfg, Instances: instances, TestOrder: testOrder, DisplayNames: displayNames, Results: results, TreeDBStats: stats, DiskUsage: disk, CollectionWorkloads: collectionWorkloads}
}

func collectionStorageArtifactPathsForProfileDir(dir string, cfg BenchConfig, runBenchprof bool) collectionStorageArtifactPaths {
	paths := collectionStorageArtifactPaths{
		CollectionJSON:     filepath.Join(dir, "collection_storage_results.json"),
		CollectionMarkdown: filepath.Join(dir, "collection_storage_results.md"),
		CollectionHTML:     filepath.Join(dir, "collection_storage_results.html"),
		BenchprofJSON:      filepath.Join(dir, "benchprof_results.json"),
		BenchprofMarkdown:  filepath.Join(dir, "benchprof_results.md"),
	}
	if shouldCPUProfile(cfg, collectionStorageBenchTestName) {
		paths.CPUProfile = collectionStorageCPUProfilePath(cfg)
	}
	if shouldAllocsProfile(cfg, collectionStorageBenchTestName) {
		paths.AllocsProfile = collectionStorageAllocsProfilePath(cfg)
	}
	if shouldCheckpointCPUProfile(cfg, collectionStorageBenchTestName) {
		paths.CheckpointCPUProfile = fmt.Sprintf("%s_checkpoint_%s_%s.pprof", strings.TrimSpace(cfg.CheckpointCPUProfile), sanitizeProfileSegment(collectionStorageBenchTestName), sanitizeProfileSegment(collectionStorageBenchDBName))
	}
	if blockProfile := strings.TrimSpace(cfg.BlockProfile); blockProfile != "" {
		paths.BlockProfile = blockProfile
	}
	if mutexProfile := strings.TrimSpace(cfg.MutexProfile); mutexProfile != "" {
		paths.MutexProfile = mutexProfile
	}
	if traceProfile := strings.TrimSpace(cfg.TraceProfile); traceProfile != "" {
		paths.TraceProfile = traceProfile
	}
	if runBenchprof {
		paths.InsightsMarkdown = filepath.Join(dir, "insights.md")
		paths.InsightsJSON = filepath.Join(dir, "insights.json")
		paths.InsightsHTML = filepath.Join(dir, "insights.html")
	}
	return paths
}

func collectionStorageCPUProfilePath(cfg BenchConfig) string {
	return fmt.Sprintf("%s_%s_%s.pprof", strings.TrimSpace(cfg.CPUProfile), collectionStorageBenchTestName, collectionStorageBenchDBName)
}

func collectionStorageAllocsProfilePath(cfg BenchConfig) string {
	return fmt.Sprintf("%s_%s_%s.pprof", strings.TrimSpace(cfg.AllocsProfile), collectionStorageBenchTestName, collectionStorageBenchDBName)
}

func writeCollectionStorageSuiteArtifacts(dir, executionPath string, report collectionStorageReport, md string, run BenchRun) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("collection_storage: create profile dir: %w", err)
	}
	js, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("collection_storage: marshal report: %w", err)
	}
	if err := writeColumnStoreSuiteArtifactFile(report.Artifacts.CollectionJSON, js); err != nil {
		return fmt.Errorf("collection_storage: write json: %w", err)
	}
	if err := writeColumnStoreSuiteArtifactFile(report.Artifacts.CollectionMarkdown, []byte(md)); err != nil {
		return fmt.Errorf("collection_storage: write markdown: %w", err)
	}
	if err := writeColumnStoreSuiteArtifactFile(report.Artifacts.CollectionHTML, []byte(renderCollectionStorageSuiteHTML(report))); err != nil {
		return fmt.Errorf("collection_storage: write html: %w", err)
	}
	if err := writeBenchprofArtifactsToPaths(report.Artifacts.BenchprofJSON, report.Artifacts.BenchprofMarkdown, strings.TrimSpace(executionPath), []BenchRun{run}); err != nil {
		return fmt.Errorf("collection_storage: write benchprof artifacts: %w", err)
	}
	return nil
}

func startCollectionStorageRuntimeProfiles(cfg BenchConfig) (func() error, error) {
	type runtimeProfileCleanup struct {
		finish func() error
		abort  func() error
	}
	var cleanups []runtimeProfileCleanup
	finish := func() error {
		var out error
		for i := len(cleanups) - 1; i >= 0; i-- {
			out = errors.Join(out, cleanups[i].finish())
		}
		cleanups = nil
		return out
	}
	abort := func() error {
		var out error
		for i := len(cleanups) - 1; i >= 0; i-- {
			out = errors.Join(out, cleanups[i].abort())
		}
		cleanups = nil
		return out
	}
	if shouldAllocsProfile(cfg, collectionStorageBenchTestName) {
		restoreMemRate := installAllocsProfileRateForEnabled(true, cfg.AllocsProfileRate)
		cleanups = append(cleanups, runtimeProfileCleanup{finish: func() error { restoreMemRate(); return nil }, abort: func() error { restoreMemRate(); return nil }})
	}
	if blockProfile := strings.TrimSpace(cfg.BlockProfile); blockProfile != "" {
		rate := cfg.BlockProfileRate
		if rate <= 0 {
			rate = 1
		}
		f, err := os.Create(blockProfile)
		if err != nil {
			_ = abort()
			return nil, fmt.Errorf("collection_storage: blockprofile: %w", err)
		}
		runtime.SetBlockProfileRate(rate)
		cleanups = append(cleanups, runtimeProfileCleanup{
			finish: func() error {
				runtime.SetBlockProfileRate(0)
				prof := pprof.Lookup("block")
				var writeErr error
				if prof == nil {
					writeErr = fmt.Errorf("block profile unavailable")
				} else {
					writeErr = prof.WriteTo(f, 0)
				}
				return errors.Join(writeErr, f.Close())
			},
			abort: func() error { runtime.SetBlockProfileRate(0); return errors.Join(f.Close(), os.Remove(blockProfile)) },
		})
	}
	if mutexProfile := strings.TrimSpace(cfg.MutexProfile); mutexProfile != "" {
		frac := cfg.MutexProfileFraction
		if frac <= 0 {
			frac = 1
		}
		f, err := os.Create(mutexProfile)
		if err != nil {
			_ = abort()
			return nil, fmt.Errorf("collection_storage: mutexprofile: %w", err)
		}
		prevFrac := runtime.SetMutexProfileFraction(frac)
		cleanups = append(cleanups, runtimeProfileCleanup{
			finish: func() error {
				runtime.SetMutexProfileFraction(0)
				prof := pprof.Lookup("mutex")
				var writeErr error
				if prof == nil {
					writeErr = fmt.Errorf("mutex profile unavailable")
				} else {
					writeErr = prof.WriteTo(f, 0)
				}
				runtime.SetMutexProfileFraction(prevFrac)
				return errors.Join(writeErr, f.Close())
			},
			abort: func() error {
				runtime.SetMutexProfileFraction(prevFrac)
				return errors.Join(f.Close(), os.Remove(mutexProfile))
			},
		})
	}
	if traceProfile := strings.TrimSpace(cfg.TraceProfile); traceProfile != "" {
		f, err := os.Create(traceProfile)
		if err != nil {
			_ = abort()
			return nil, fmt.Errorf("collection_storage: trace: %w", err)
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			_ = os.Remove(traceProfile)
			_ = abort()
			return nil, fmt.Errorf("collection_storage: trace start: %w", err)
		}
		cleanups = append(cleanups, runtimeProfileCleanup{finish: func() error { trace.Stop(); return f.Close() }, abort: func() error { trace.Stop(); return errors.Join(f.Close(), os.Remove(traceProfile)) }})
	}
	return finish, nil
}
