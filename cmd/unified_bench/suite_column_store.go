package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	columnStorePathRowStoreBaseline    = "row_store_baseline"
	columnStorePathBTreeIndexBaseline  = "b_tree_index_baseline"
	columnStorePathSerialColumnScan    = "serial_column_scan"
	columnStorePathAggregateMetadata   = "aggregate_metadata"
	columnStorePathParallelColumnScan  = "parallel_column_scan"
	columnStoreSuiteBenchTestName      = "column_store"
	columnStoreSuiteBenchDBName        = "treedb_column_store"
	columnStoreSuiteBenchDisplayName   = "TreeDB Column Store"
	columnStoreSuitePathCanonicalHelp  = "row_store_baseline, b_tree_index_baseline, serial_column_scan, aggregate_metadata, parallel_column_scan"
	columnStoreQueryQ1                 = "q1"
	columnStoreQueryQ2                 = "q2"
	columnStoreQueryQ3                 = "q3"
	columnStoreQueryQ4A                = "q4a"
	columnStoreQueryQ4B                = "q4b"
	columnStoreQueryQ5                 = "q5"
	columnStoreQueryQ5Metadata         = "q5_metadata"
	columnStoreSuiteBenchMetricPrefix  = "column_store_"
	columnStoreSuiteAliasFullScanQ1    = "alias_full_scan_from_" + columnStoreQueryQ1
	columnStoreSuiteAliasPrefixQ4A     = "alias_prefix_scan_from_" + columnStoreQueryQ4A
	columnStoreSuiteQ5AggregateMin     = "q5_did_time_span_min"
	columnStoreSuiteQ5AggregateMax     = "q5_did_time_span_max"
	columnStoreSuiteMaxInt64DecimalLen = 20 // len("-9223372036854775808")

	columnStoreCodecLayoutCurrent              = "current_column_store_physical_layout"
	columnStoreCompressionPolicyOff            = "compression_off"
	columnStoreCompressionPolicyDefault        = "current_default_none"
	columnStoreCompressionSupportSupported     = "supported"
	columnStoreCompressionSupportNotApplicable = "not_applicable"
	columnStoreCompressionSupportFallback      = "fallback"
	columnStoreCompressionSupportDeferred      = "deferred"
	columnStoreCompressionSupportUnsupported   = "unsupported"
	columnStoreCompressionNoneLabel            = "none"

	columnStoreJSONBenchCellRowScan                = "row-scan"
	columnStoreJSONBenchCellColumnDirect           = "column-direct"
	columnStoreJSONBenchCellColumnPrepared         = "column-prepared"
	columnStoreJSONBenchCellColumnDirectMetadata   = "column-direct-metadata"
	columnStoreJSONBenchCellColumnPreparedMetadata = "column-prepared-metadata"
	columnStoreJSONBenchScanPathData               = "data_scan"
	columnStoreJSONBenchScanPathMetadata           = "metadata"
	columnStoreJSONBenchModeRowScan                = "row_scan"
	columnStoreJSONBenchModeDirect                 = "direct"
	columnStoreJSONBenchModePrepared               = "prepared"
	columnStoreJSONBenchMutationInsertOnlyReopen   = "insert_only_checkpoint_reopen"
	columnStoreJSONBenchSyntheticFixtureCaveat     = "in-repo synthetic JSONBench-shaped fixture; not an external full-data JSONBench run"
	columnStoreJSONBenchFullDataCaveat             = "not an external full-data retained-JSON parity run; #2117 full retained JSON plus reconstruction parity is implemented in the external snissn/JSONBench TreeDB harness"
	columnStoreJSONBenchStorageCaveat              = "storage accounting is gomap-local synthetic evidence; WAL-excluded durable storage excludes only valid command WAL segment files named `wal/commit-l<lane>-<seq>.log` (numeric lane, non-zero sequence) while retaining value_vlog, leaf_vlog, index.db, column assets, and manifest/control bytes; #2118 apples-to-apples storage fields are implemented in the external snissn/JSONBench TreeDB harness and require an external run for headline ClickHouse comparison"
	columnStoreDurableStorageWALExcludedNote       = "steady-state durable storage label excludes only valid command WAL segment files named `wal/commit-l<lane>-<seq>.log` (numeric lane, non-zero sequence); value_vlog, leaf_vlog, index.db, column assets, and manifest/control bytes remain durable storage"
)

type columnStoreSuitePathAlias struct {
	alias     string
	canonical string
}

var (
	columnStoreSuitePathAliases = []columnStoreSuitePathAlias{
		{alias: "", canonical: columnStorePathRowStoreBaseline},
		{alias: "row-store-baseline", canonical: columnStorePathRowStoreBaseline},
		{alias: "row_store", canonical: columnStorePathRowStoreBaseline},
		{alias: "row", canonical: columnStorePathRowStoreBaseline},
		{alias: "b-tree-index-baseline", canonical: columnStorePathBTreeIndexBaseline},
		{alias: "btree_index_baseline", canonical: columnStorePathBTreeIndexBaseline},
		{alias: "b_tree", canonical: columnStorePathBTreeIndexBaseline},
		{alias: "index", canonical: columnStorePathBTreeIndexBaseline},
		{alias: "serial-column-scan", canonical: columnStorePathSerialColumnScan},
		{alias: "serial", canonical: columnStorePathSerialColumnScan},
		{alias: "aggregate-metadata", canonical: columnStorePathAggregateMetadata},
		{alias: "metadata", canonical: columnStorePathAggregateMetadata},
		{alias: "parallel-column-scan", canonical: columnStorePathParallelColumnScan},
		{alias: "parallel", canonical: columnStorePathParallelColumnScan},
	}
	columnStoreSuitePathUsage = fmt.Sprintf(
		"Forced column-store execution label for -suite column_store (canonical: %s; aliases: %s; executable: %s; accepted labels: %s; aggregate_metadata executes q4b and q5_metadata through typed aggregate metadata assets when available; other queries reroute to serial physical scan)",
		columnStoreSuitePathCanonicalHelp,
		columnStoreSuitePathAliasHelp(columnStoreSuitePathAliases),
		columnStoreSuitePathList(columnStoreSuiteExecutableForcedPaths),
		columnStoreSuitePathList(columnStoreSuiteAcceptedForcedPaths),
	)
	columnStoreSuitePathArg                     = flag.String("column-store-path", columnStorePathRowStoreBaseline, columnStoreSuitePathUsage)
	columnStoreSuiteFixtureArg                  = flag.String("column-store-fixture", "synthetic", "Fixture for -suite column_store (synthetic; JSONBENCH_DATA mode is reserved for the large local gate)")
	columnStoreSuiteQueryArg                    = flag.String("column-store-query", "", "Optional comma-separated query subset for -suite column_store profiling (q1,q2,q3,q4a,q4b,q5,q5_metadata; empty/all runs the full q1-q5/q5_metadata suite; duplicates are rejected)")
	columnStoreSuiteAssetReadIntegrityArg       = flag.String("column-store-asset-read-integrity", string(collections.ColumnAssetReadIntegrityVerify), "Column asset hot-read integrity for -suite column_store physical paths (verify, cached_verify, skip_checksums; relaxed modes are unsafe and require -treedb-allow-unsafe)")
	columnStoreSuiteTypedCompressionArg         = flag.String("column-store-typed-compression", "", "Benchmark-only typed_column_part compression policy for -suite column_store when typed-column assets are published (none,snappy,lz4,zstd,zstd_dict; zstd values fail closed; empty uses production default none)")
	columnStoreSuiteTypedInt64EncodingArg       = flag.String("column-store-typed-int64-encoding", "", "Benchmark-only typed_column_part int64 encoding override (default,raw_int64,delta_varint,double_delta_varint; empty uses production default)")
	columnStoreSuiteTypedRowsPerGranuleArg      = flag.Int("column-store-typed-rows-per-granule", 0, "Benchmark-only typed_column_part rows per granule override (0 uses production default)")
	columnStoreSuiteTypedAdaptiveArg            = flag.Bool("column-store-typed-adaptive", false, "Benchmark-only typed_column_part adaptive rows-per-granule sizing (off by default; public config remains unchanged)")
	columnStoreSuiteTypedAdaptiveTargetBytesArg = flag.Int("column-store-typed-adaptive-target-bytes", 0, "Benchmark-only typed_column_part adaptive target raw bytes per mark/granule (0 uses typedcolumn default when adaptive is enabled)")
	columnStoreSuiteTypedAdaptiveMinRowsArg     = flag.Int("column-store-typed-adaptive-min-rows", 0, "Benchmark-only typed_column_part adaptive minimum rows (0 uses typedcolumn default when adaptive is enabled)")
	columnStoreSuiteTypedAdaptiveMaxRowsArg     = flag.Int("column-store-typed-adaptive-max-rows", 0, "Benchmark-only typed_column_part adaptive maximum rows (0 uses rows-per-granule/default when adaptive is enabled)")

	columnStoreSuiteAcceptedForcedPaths = []string{
		columnStorePathRowStoreBaseline,
		columnStorePathBTreeIndexBaseline,
		columnStorePathSerialColumnScan,
		columnStorePathAggregateMetadata,
		columnStorePathParallelColumnScan,
	}
	columnStoreSuiteExecutableForcedPaths = []string{
		columnStorePathRowStoreBaseline,
		columnStorePathBTreeIndexBaseline,
		columnStorePathSerialColumnScan,
		columnStorePathAggregateMetadata,
		columnStorePathParallelColumnScan,
	}
	columnStoreSuiteFailClosedForcedPaths = []string{}
	// These files are opportunistic control-plane telemetry for the benchmark
	// report. Missing files are reported, not fatal, because the exact set can
	// vary as TreeDB control metadata evolves.
	columnStoreSuiteManifestControlFiles = []string{
		"vlog_ref_counts.meta",
	}
)

type columnStoreSuiteOptions struct {
	ProfileDir               string
	ExecutionPath            string
	ForcedPath               string
	Fixture                  string
	QueryNames               []string
	ColumnAssetReadIntegrity collections.ColumnAssetReadIntegrity
	RunBenchprof             bool
	CorruptReferenceForTest  string
}

type columnStoreSuiteReport struct {
	GeneratedAt              string                         `json:"generated_at"`
	Suite                    string                         `json:"suite"`
	Profile                  string                         `json:"profile"`
	Fixture                  string                         `json:"fixture"`
	DataDir                  string                         `json:"data_dir,omitempty"`
	PathLabel                string                         `json:"path_label,omitempty"`
	ForcedPath               string                         `json:"forced_path"`
	QueryNames               []string                       `json:"query_names"`
	Rows                     int                            `json:"rows"`
	BatchSize                int                            `json:"batch_size"`
	Seed                     int64                          `json:"seed"`
	CacheLabel               string                         `json:"cache_label"`
	AcceptedForcedPaths      []string                       `json:"accepted_forced_paths"`
	FailClosedForcedPaths    []string                       `json:"fail_closed_forced_paths"`
	Stages                   []columnStoreStageMetric       `json:"stages"`
	Queries                  []columnStoreQueryMetric       `json:"queries"`
	JSONBenchCells           []columnStoreJSONBenchCell     `json:"jsonbench_cells"`
	Parity                   map[string]columnStoreParity   `json:"parity"`
	ByteAccounting           columnStoreByteAccounting      `json:"byte_accounting"`
	CodecLayouts             []columnStoreCodecLayoutMetric `json:"codec_layouts"`
	CompressionMatrixNote    string                         `json:"compression_matrix_note"`
	Manifest                 columnStoreManifestMetric      `json:"manifest"`
	Artifacts                columnStoreArtifactPaths       `json:"artifacts,omitempty"`
	ProductionScope          string                         `json:"production_scope"`
	PhysicalColumnQuery      string                         `json:"physical_column_query"`
	ExternalJSONBenchStatus  string                         `json:"external_jsonbench_status"`
	ReportCaveats            []string                       `json:"report_caveats"`
	ColgranuleReuseMap       []columnStoreColgranuleReuse   `json:"colgranule_reuse_map"`
	ColumnAssetReadIntegrity string                         `json:"column_asset_read_integrity"`
	BenchmarkOnlyRelaxed     bool                           `json:"benchmark_only_relaxed"`
	StageSeparatedBoundary   string                         `json:"stage_separated_boundary"`
	ProfileFinalizeError     string                         `json:"profile_finalize_error,omitempty"`
}

type columnStoreStageMetric struct {
	Name          string  `json:"name"`
	DurationMS    float64 `json:"duration_ms"`
	Rows          int     `json:"rows,omitempty"`
	RowsPerSecond float64 `json:"rows_per_second,omitempty"`
	Bytes         int64   `json:"bytes,omitempty"`
	MiBPerSecond  float64 `json:"mib_per_second,omitempty"`
}

type columnStoreQueryMetric struct {
	Name                     string                            `json:"name"`
	PlanLabel                string                            `json:"plan_label"`
	AliasOf                  string                            `json:"alias_of,omitempty"`
	ImplementationNote       string                            `json:"implementation_note,omitempty"`
	ThroughputInterpretation string                            `json:"throughput_interpretation,omitempty"`
	StorageSource            string                            `json:"storage_source"`
	FallbackReason           string                            `json:"fallback_reason"`
	ManifestRootName         string                            `json:"manifest_root_name,omitempty"`
	ManifestRoot             uint64                            `json:"manifest_root,omitempty"`
	ManifestGeneration       uint64                            `json:"manifest_generation,omitempty"`
	ActiveManifestChecksum   uint64                            `json:"active_manifest_checksum,omitempty"`
	DurationMS               float64                           `json:"duration_ms"`
	Rows                     int                               `json:"rows"`
	RowsProcessed            int                               `json:"rows_processed"`
	RowsProcessedKnown       bool                              `json:"rows_processed_known"`
	RowsPerSecond            float64                           `json:"rows_per_second"`
	MiBPerSecond             float64                           `json:"mib_per_second"`
	NsPerRow                 float64                           `json:"ns_per_row"`
	BytesRead                int64                             `json:"bytes_read"`
	RowMaterializations      int                               `json:"row_materializations"`
	ResultCount              int                               `json:"result_count"`
	RawHash                  uint64                            `json:"raw_hash"`
	ProductionHash           uint64                            `json:"production_hash"`
	MetadataHits             int                               `json:"metadata_hits"`
	DictionaryCodeHits       int                               `json:"dictionary_code_hits"`
	Int64ValueHits           int                               `json:"int64_value_hits"`
	SkippedGranules          int                               `json:"skipped_granules"`
	ScheduledGranules        int                               `json:"scheduled_granules"`
	WorkerCount              int                               `json:"worker_count"`
	PlannerDurationMS        float64                           `json:"planner_duration_ms"`
	ScanDurationMS           float64                           `json:"scan_duration_ms"`
	ReduceDurationMS         float64                           `json:"reduce_duration_ms"`
	AdapterDurationMS        float64                           `json:"adapter_duration_ms"`
	ParityHashDurationMS     float64                           `json:"parity_hash_duration_ms"`
	RowsScanned              int                               `json:"rows_scanned"`
	RowsMatched              int                               `json:"rows_matched"`
	ReduceRows               int                               `json:"reduce_rows"`
	DecodedGranules          int                               `json:"decoded_granules"`
	PlannerCandidates        int                               `json:"planner_candidates"`
	PlannerReason            string                            `json:"planner_reason,omitempty"`
	SegmentFileCacheHits     uint64                            `json:"segment_file_cache_hits"`
	SegmentFileCacheMisses   uint64                            `json:"segment_file_cache_misses"`
	CacheLabel               string                            `json:"cache_label"`
	CompressionAttribution   columnStoreCompressionAttribution `json:"compression_attribution"`

	duration       time.Duration
	hotRunDuration time.Duration
}

type columnStoreJSONBenchCell struct {
	CellLabel                     string                            `json:"cell_label"`
	Query                         string                            `json:"query"`
	AliasOf                       string                            `json:"alias_of,omitempty"`
	SortLayout                    string                            `json:"sort_layout"`
	SortKey                       []string                          `json:"sort_key"`
	PlanLabel                     string                            `json:"plan_label"`
	StorageSource                 string                            `json:"storage_source"`
	FallbackReason                string                            `json:"fallback_reason"`
	ExecutionMode                 string                            `json:"execution_mode"`
	MetadataDataScanPath          string                            `json:"metadata_data_scan_path"`
	CompressionMode               string                            `json:"compression_mode"`
	RequestedCompression          string                            `json:"requested_compression"`
	ActualCompression             string                            `json:"actual_compression"`
	MutationMode                  string                            `json:"mutation_mode"`
	RetainedPayloadPolicy         string                            `json:"retained_payload_policy"`
	RetainedPayloadEncoding       string                            `json:"retained_payload_encoding"`
	RetainedPayloadEncodingStatus string                            `json:"retained_payload_encoding_status"`
	RetainedPayloadBytes          int64                             `json:"retained_payload_bytes"`
	TypedStorageOwner             string                            `json:"typed_storage_owner"`
	TypedStorageOwnerColumns      []columnStoreTypedOwnerColumn     `json:"typed_storage_owner_columns"`
	RowCount                      int                               `json:"row_count"`
	RowsProcessed                 int                               `json:"rows_processed"`
	RowsProcessedKnown            bool                              `json:"rows_processed_known"`
	BytesRead                     int64                             `json:"bytes_read"`
	ResultCount                   int                               `json:"result_count"`
	RawHash                       uint64                            `json:"raw_hash"`
	ResultHash                    uint64                            `json:"result_hash"`
	ParityWithRowScan             bool                              `json:"parity_with_row_scan"`
	ManifestRootName              string                            `json:"manifest_root_name,omitempty"`
	ManifestRoot                  uint64                            `json:"manifest_root,omitempty"`
	ManifestGeneration            uint64                            `json:"manifest_generation,omitempty"`
	ActiveManifestChecksum        uint64                            `json:"active_manifest_checksum,omitempty"`
	PlannerDurationMS             float64                           `json:"planner_duration_ms"`
	PreparedSetupDurationMS       float64                           `json:"prepared_setup_duration_ms,omitempty"`
	HotRunDurationMS              float64                           `json:"hot_run_duration_ms,omitempty"`
	ScanDurationMS                float64                           `json:"scan_duration_ms"`
	ReduceDurationMS              float64                           `json:"reduce_duration_ms"`
	ResultShapeDurationMS         float64                           `json:"result_shape_duration_ms"`
	ParityHashDurationMS          float64                           `json:"parity_hash_duration_ms"`
	MetadataHits                  int                               `json:"metadata_hits"`
	RowsScanned                   int                               `json:"rows_scanned"`
	RowsMatched                   int                               `json:"rows_matched"`
	ReduceRows                    int                               `json:"reduce_rows"`
	DecodedGranules               int                               `json:"decoded_granules"`
	SkippedGranules               int                               `json:"skipped_granules"`
	ScheduledGranules             int                               `json:"scheduled_granules"`
	PredicateMode                 string                            `json:"predicate_mode"`
	RealPredicates                bool                              `json:"real_predicates"`
	RetainedPayloadPolicyCaveat   string                            `json:"retained_payload_policy_caveat"`
	ReconstructionStatus          string                            `json:"reconstruction_status"`
	FullDataCell                  bool                              `json:"full_data_cell"`
	FullDataCaveat                string                            `json:"full_data_caveat"`
	StorageAccountingCaveat       string                            `json:"storage_accounting_caveat"`
	CompatibilityStatus           string                            `json:"compatibility_status"`
	CompatibilityStatusReason     string                            `json:"compatibility_status_reason,omitempty"`
	CompressionAttribution        columnStoreCompressionAttribution `json:"compression_attribution"`
}

type columnStoreTypedOwnerColumn struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

type columnStoreColgranuleReuse struct {
	Source           string `json:"source"`
	ProductionTarget string `json:"production_target"`
	Decision         string `json:"decision"`
	DivergenceReason string `json:"divergence_reason,omitempty"`
	Evidence         string `json:"evidence"`
}

type columnStoreCompressionAttribution struct {
	CodecLayoutLabel            string  `json:"codec_layout_label"`
	CompressionPolicyLabel      string  `json:"compression_policy_label"`
	RequestedCompression        string  `json:"requested_compression"`
	ActualCompression           string  `json:"actual_compression"`
	SupportState                string  `json:"support_state"`
	SupportReason               string  `json:"support_reason,omitempty"`
	CompressedBytes             int64   `json:"compressed_bytes"`
	CompressedBytesSource       string  `json:"compressed_bytes_source"`
	RawBytes                    int64   `json:"raw_bytes"`
	RawBytesSource              string  `json:"raw_bytes_source"`
	DecompressedBytes           int64   `json:"decompressed_bytes"`
	DecompressedBytesSource     string  `json:"decompressed_bytes_source"`
	CompressionRatio            float64 `json:"compression_ratio"`
	CompressionRatioSource      string  `json:"compression_ratio_source"`
	CompressionDurationMS       float64 `json:"compression_duration_ms"`
	CompressionDurationSource   string  `json:"compression_duration_source"`
	DecompressionDurationMS     float64 `json:"decompression_duration_ms"`
	DecompressionDurationSource string  `json:"decompression_duration_source"`
	BenchmarkBPerOp             float64 `json:"benchmark_b_per_op"`
	BenchmarkAllocsPerOp        float64 `json:"benchmark_allocs_per_op"`
	BenchmarkAllocationSource   string  `json:"benchmark_allocation_source"`
}

type columnStoreCodecLayoutMetric struct {
	columnStoreCompressionAttribution
	Rows    int `json:"rows"`
	Columns int `json:"columns"`
}

type columnStoreParity struct {
	Pass           bool   `json:"pass"`
	RawHash        uint64 `json:"raw_hash"`
	ProductionHash uint64 `json:"production_hash"`
}

type columnStoreQueryExecution struct {
	Lines                  []string
	ProductionHash         uint64
	ProductionHashKnown    bool
	StorageSource          string
	FallbackReason         string
	ManifestRootName       string
	ManifestRoot           uint64
	ManifestGeneration     uint64
	ActiveManifestChecksum uint64
	RowsProcessed          int
	RowsProcessedKnown     bool
	RowsScanned            int
	RowsMatched            int
	ReduceRows             int
	DecodedGranules        int
	DecodedBlocks          int
	DecodedPayloadBytes    uint64
	MappedBytes            uint64
	HeapCopyBytes          uint64
	PredicateCount         int
	BytesRead              int64
	RowMaterializations    int
	ResultCount            int
	MetadataHits           int
	DictionaryCodeHits     int
	Int64ValueHits         int
	SkippedGranules        int
	ScheduledGranules      int
	WorkerCount            int
	SegmentFileCacheHits   uint64
	SegmentFileCacheMisses uint64
	SetupDuration          time.Duration
	HotRunDuration         time.Duration
	ScanDuration           time.Duration
	ReduceDuration         time.Duration
	AdapterDuration        time.Duration
	ResultShapeDuration    time.Duration
	// Set when ProductionHashKnown is true. Fallback line-hash timing is
	// measured at the call site because it is derived from Lines, not execution.
	ParityHashDuration time.Duration
}

type columnStoreByteAccounting struct {
	SourceDocumentBytes                int64    `json:"source_document_bytes"`
	RetainedPayloadBytes               int64    `json:"retained_payload_bytes"`
	RetainedPayloadBytesNote           string   `json:"retained_payload_bytes_note,omitempty"`
	ColumnAssetBytes                   int64    `json:"column_asset_bytes"`
	ColumnAssetBytesNote               string   `json:"column_asset_bytes_note,omitempty"`
	ColumnAssetStoreBytes              int64    `json:"column_asset_store_bytes"`
	ManifestControlBytes               int64    `json:"manifest_control_bytes"`
	ManifestControlMissing             []string `json:"manifest_control_missing,omitempty"`
	PrimaryIndexBytes                  int64    `json:"primary_index_bytes"`
	OrdinaryValueLogBytes              int64    `json:"ordinary_value_vlog_bytes"`
	LeafLogBytes                       int64    `json:"leaf_vlog_bytes"`
	CommandWALBytesBeforeCheckpoint    int64    `json:"command_wal_bytes_before_checkpoint"`
	WALBytesExcludedFromDurable        int64    `json:"wal_bytes_excluded_from_durable_storage"`
	DurableStorageBytesWALExcluded     int64    `json:"durable_storage_bytes_wal_excluded"`
	DurableStorageBytesWALExcludedNote string   `json:"durable_storage_bytes_wal_excluded_note,omitempty"`
	TotalReconstructableBytes          int64    `json:"total_reconstructable_bytes"`
	DBTotalBytes                       int64    `json:"db_total_bytes"`
	DBTotalFiles                       int      `json:"db_total_files"`
}

type columnStoreManifestMetric struct {
	ActiveGeneration                uint64 `json:"active_generation"`
	ActiveChecksum                  uint64 `json:"active_checksum"`
	RecoveryAuthoritativeGeneration uint64 `json:"recovery_authoritative_generation"`
	RecoveryAuthoritativeChecksum   uint64 `json:"recovery_authoritative_checksum"`
	AppliedCommandLSN               uint64 `json:"applied_command_lsn"`
	ManifestRootName                string `json:"manifest_root_name"`
	ManifestRoot                    uint64 `json:"manifest_root"`
	SchemaHash                      uint64 `json:"schema_hash"`
}

type columnStoreArtifactPaths struct {
	ColumnJSON           string `json:"column_json,omitempty"`
	ColumnMarkdown       string `json:"column_markdown,omitempty"`
	ColumnHTML           string `json:"column_html,omitempty"`
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
	BlockDeltaProfile    string `json:"block_delta_profile,omitempty"`
	MutexDeltaProfile    string `json:"mutex_delta_profile,omitempty"`
}

type columnStoreFixtureEvent struct {
	ID      string
	IDBytes []byte
	TimeUS  int64
	Kind    string
	Did     string
	Doc     []byte
}

type columnStoreDecodedEvent struct {
	TimeUS int64  `json:"time_us"`
	Kind   string `json:"kind"`
	Did    string `json:"did"`
}

type columnStoreSuiteDBLabel struct {
	name string
}

func (d *columnStoreSuiteDBLabel) Name() string               { return d.name }
func (d *columnStoreSuiteDBLabel) Close() error               { return nil }
func (d *columnStoreSuiteDBLabel) Get([]byte) ([]byte, error) { return nil, nil }
func (d *columnStoreSuiteDBLabel) Set([]byte, []byte) error   { return nil }
func (d *columnStoreSuiteDBLabel) Delete([]byte) error        { return nil }

func runColumnStoreSuite(baseCfg BenchConfig, opts columnStoreSuiteOptions) (string, error) {
	profileHooks := profileHooksFromConfig(baseCfg)
	profile, err := columnStoreSuiteEffectiveProfile(baseCfg.Profile)
	if err != nil {
		return "", err
	}
	fixture := strings.TrimSpace(opts.Fixture)
	if fixture == "" {
		fixture = strings.TrimSpace(*columnStoreSuiteFixtureArg)
	}
	if fixture == "" {
		fixture = "synthetic"
	}
	if fixture != "synthetic" {
		return "", fmt.Errorf("column_store: unsupported fixture %q; synthetic is the M11B CI fixture and JSONBENCH_DATA large mode is not wired yet", fixture)
	}
	forcedPath := normalizeColumnStoreSuitePath(opts.ForcedPath)
	if forcedPath == "" {
		forcedPath = normalizeColumnStoreSuitePath(*columnStoreSuitePathArg)
	}
	if _, err := columnStoreSuitePlanKind(forcedPath); err != nil {
		return "", err
	}
	queryNames, err := columnStoreSuiteEffectiveQueryNames(opts.QueryNames, strings.TrimSpace(*columnStoreSuiteQueryArg))
	if err != nil {
		return "", err
	}
	assetReadIntegrity, err := columnStoreSuiteEffectiveAssetReadIntegrity(opts.ColumnAssetReadIntegrity)
	if err != nil {
		return "", err
	}
	cleanupTypedPolicyEnv, err := columnStoreSuiteInstallTypedBenchmarkPolicyEnv()
	if err != nil {
		return "", err
	}
	defer cleanupTypedPolicyEnv()
	if err := validateColumnStoreSuiteDBSelection(baseCfg.DBsArg, baseCfg.DBsExcludeArg); err != nil {
		return "", err
	}
	if err := validateColumnStoreSuiteExecutionPath(opts.ProfileDir, opts.ExecutionPath); err != nil {
		return "", err
	}
	finishRuntimeProfiles, err := startColumnStoreSuiteRuntimeProfiles(baseCfg)
	if err != nil {
		return "", err
	}
	runtimeProfilesActive := true
	defer func() {
		if runtimeProfilesActive {
			_ = finishRuntimeProfiles()
		}
	}()

	rows := baseCfg.Keys
	if rows <= 0 {
		return "", fmt.Errorf("column_store: invalid keys: %d", rows)
	}
	batchSize := baseCfg.BatchSize
	if batchSize <= 0 {
		return "", fmt.Errorf("column_store: invalid batchsize: %d", batchSize)
	}
	if batchSize > rows {
		batchSize = rows
	}
	seed := baseCfg.SeedUsed
	if seed == 0 {
		seed = 1
	}

	stages := make([]columnStoreStageMetric, 0, 5)
	start := time.Now()
	fixtureEvents, sourceBytes := buildColumnStoreSyntheticFixture(rows, seed)
	stages = append(stages, columnStoreStage("fixture_generate", start, rows, sourceBytes))

	dataDir, err := os.MkdirTemp("", "unified-bench-column-store-*")
	if err != nil {
		return "", fmt.Errorf("column_store: create temp dir: %w", err)
	}
	if !baseCfg.KeepDir {
		defer os.RemoveAll(dataDir)
	}

	start = time.Now()
	db, err := openColumnStoreSuiteDB(dataDir)
	if err != nil {
		return "", err
	}
	manager := collections.NewCollectionManager(db)
	if _, err := manager.CreateCollection(columnStoreSuiteCollectionMeta(forcedPath)); err != nil {
		_ = db.Close()
		return "", fmt.Errorf("column_store: create collection: %w", err)
	}
	collection, err := manager.OpenCollection("events")
	if err != nil {
		_ = db.Close()
		return "", fmt.Errorf("column_store: open collection: %w", err)
	}
	stages = append(stages, columnStoreStage("open_create", start, 0, 0))

	start = time.Now()
	if err := insertColumnStoreFixture(collection, fixtureEvents, batchSize); err != nil {
		_ = db.Close()
		return "", err
	}
	stages = append(stages, columnStoreStage("ingest_insert_batch", start, rows, sourceBytes))
	commandWALBytesBeforeCheckpoint, err := columnStoreSuiteCommandWALLogBytes(dataDir)
	if err != nil {
		_ = db.Close()
		return "", fmt.Errorf("column_store: command WAL byte accounting: %w", err)
	}

	var checkpointCPUFile *os.File
	if shouldCheckpointCPUProfile(baseCfg, columnStoreSuiteBenchTestName) {
		checkpointCPUFile, err = startCheckpointCPUProfile(baseCfg, profileHooks, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
		if err != nil {
			_ = db.Close()
			return "", fmt.Errorf("column_store: checkpoint profiling: %w", err)
		}
	}

	start = time.Now()
	checkpointErr := db.Checkpoint()
	if checkpointCPUFile != nil {
		profileHooks.stopCPUProfile()
		_ = checkpointCPUFile.Close()
	}
	if checkpointErr != nil {
		_ = db.Close()
		return "", fmt.Errorf("column_store: checkpoint: %w", checkpointErr)
	}
	checkpointDuration := time.Since(start)
	stages = append(stages, columnStoreStageFromDuration("checkpoint", checkpointDuration, rows, sourceBytes))
	if err := db.Close(); err != nil {
		return "", fmt.Errorf("column_store: close before reopen: %w", err)
	}

	start = time.Now()
	db, err = openColumnStoreSuiteDB(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: reopen: %w", err)
	}
	defer db.Close()
	collection, err = collections.NewCollectionManager(db).OpenCollection("events")
	if err != nil {
		return "", fmt.Errorf("column_store: reopen collection: %w", err)
	}
	reopenDuration := time.Since(start)
	stages = append(stages, columnStoreStageFromDuration("reopen_recovery", reopenDuration, rows, sourceBytes))

	manifestIdentity, ok := collection.ColumnStoreCacheIdentity()
	if !ok {
		return "", errors.New("column_store: reopened collection has no column-store manifest identity")
	}

	rawHashes, err := columnStoreReferenceHashes(fixtureEvents)
	if err != nil {
		return "", err
	}
	if corrupt := strings.TrimSpace(opts.CorruptReferenceForTest); corrupt != "" {
		if !columnStoreQueryNameKnown(corrupt) {
			return "", fmt.Errorf("column_store: unknown corrupt reference query %q", corrupt)
		}
		if !columnStoreQueryNameSelected(queryNames, corrupt) {
			return "", fmt.Errorf("column_store: corrupt reference query %q is not selected", corrupt)
		}
		rawHashes[corrupt]++
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(baseCfg, collection, rows, rawHashes, forcedPath, assetReadIntegrity, queryNames)
	if err != nil {
		return "", err
	}
	profileFinalizeErr := finishRuntimeProfiles()
	runtimeProfilesActive = false

	totalBytes, totalFiles, err := columnStoreSuiteDirUsage(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: DB byte accounting: %w", err)
	}
	walBytesExcludedFromDurable, err := columnStoreSuiteCommandWALLogBytes(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: WAL-excluded durable storage accounting: %w", err)
	}
	primaryIndexBytes, err := columnStoreSuiteOptionalFileBytes(filepath.Join(dataDir, "index.db"))
	if err != nil {
		return "", fmt.Errorf("column_store: primary index byte accounting: %w", err)
	}
	manifestControlBytes, manifestControlMissing, err := columnStoreSuiteManifestControlUsage(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: manifest/control byte accounting: %w", err)
	}
	columnAssetBytes, err := columnStoreSuiteColumnAssetUsage(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: column asset byte accounting: %w", err)
	}
	ordinaryValueLogBytes, err := columnStoreSuiteOptionalDirBytes(backenddb.ValueLogDirPath(dataDir))
	if err != nil {
		return "", fmt.Errorf("column_store: ordinary value_vlog byte accounting: %w", err)
	}
	leafLogBytes, err := columnStoreSuiteOptionalDirBytes(backenddb.LeafLogDirPath(dataDir))
	if err != nil {
		return "", fmt.Errorf("column_store: leaf_vlog byte accounting: %w", err)
	}
	columnAssetBytesNote := ""
	if columnAssetBytes == 0 {
		columnAssetBytesNote = "M12A expected isolated physical column assets; zero bytes means no column assets were published"
	}
	cfgForPath := columnStoreSuiteConfigForPath(forcedPath)
	retainedPayloadBytes, retainedPayloadBytesNote, err := columnStoreSuiteRetainedPayloadAccounting(fixtureEvents, cfgForPath, forcedPath)
	if err != nil {
		return "", fmt.Errorf("column_store: retained-payload byte accounting: %w", err)
	}
	// Build direct/prepared JSONBench report cells after query CPU/alloc/block/
	// mutex/trace capture has been finalized. These rows are a separately timed
	// report phase and must not contaminate the measured column_store query-phase
	// profiles or BenchmarkColumnStoreSuite* query-loop benchmarks.
	start = time.Now()
	jsonbenchCells, jsonbenchCellsErr := buildColumnStoreJSONBenchCells(collection, rows, rawHashes, forcedPath, assetReadIntegrity, queryNames, queries, cfgForPath, retainedPayloadBytes)
	stages = append(stages, columnStoreStage("jsonbench_cell_report", start, rows*len(queryNames), 0))
	totalReconstructableBytes := retainedPayloadBytes + columnAssetBytes + manifestControlBytes
	physicalAccounting, physicalAccountingErr := collection.ColumnStorePhysicalAccounting(nil, collections.ColumnStorePhysicalAccountingOptions{DetailedSections: true, ReadIntegrity: assetReadIntegrity})
	codecLayouts := columnStoreCodecLayoutMetrics(rows, len(columnStoreSuiteConfigForPath(forcedPath).Columns), columnAssetBytes, physicalAccounting)
	compressionMatrixNote := "current production default remains compression=none; snappy/lz4 rows appear when typed_column_part assets were explicitly published with those opt-in policies; locator section compression is benchmark-relaxed and opt-in only; dictionaries/pruning_metadata are reported as next byte targets but remain uncompressed/deferred without a section raw-length format gate; zstd/zstd_dict are reported as unsupported/deferred despite enum names"
	if physicalAccountingErr != nil {
		compressionMatrixNote += "; physical accounting unavailable: " + physicalAccountingErr.Error()
	}
	report := columnStoreSuiteReport{
		GeneratedAt:           time.Now().UTC().Format(time.RFC3339),
		Suite:                 "column_store",
		Profile:               profile,
		Fixture:               fixture,
		PathLabel:             strings.TrimSpace(opts.ExecutionPath),
		ForcedPath:            forcedPath,
		QueryNames:            cloneStringSlice(queryNames),
		Rows:                  rows,
		BatchSize:             batchSize,
		Seed:                  seed,
		CacheLabel:            "reopened_warm_process",
		AcceptedForcedPaths:   cloneStringSlice(columnStoreSuiteAcceptedForcedPaths),
		FailClosedForcedPaths: cloneStringSlice(columnStoreSuiteFailClosedForcedPaths),
		Stages:                stages,
		Queries:               queries,
		JSONBenchCells:        jsonbenchCells,
		Parity:                parity,
		ByteAccounting: columnStoreByteAccounting{
			SourceDocumentBytes:                sourceBytes,
			RetainedPayloadBytes:               retainedPayloadBytes,
			RetainedPayloadBytesNote:           retainedPayloadBytesNote,
			ColumnAssetBytes:                   columnAssetBytes,
			ColumnAssetBytesNote:               columnAssetBytesNote,
			ColumnAssetStoreBytes:              columnAssetBytes,
			ManifestControlBytes:               manifestControlBytes,
			ManifestControlMissing:             manifestControlMissing,
			PrimaryIndexBytes:                  primaryIndexBytes,
			OrdinaryValueLogBytes:              ordinaryValueLogBytes,
			LeafLogBytes:                       leafLogBytes,
			CommandWALBytesBeforeCheckpoint:    commandWALBytesBeforeCheckpoint,
			WALBytesExcludedFromDurable:        walBytesExcludedFromDurable,
			DurableStorageBytesWALExcluded:     columnStoreSuiteDurableStorageBytesWALExcluded(totalBytes, walBytesExcludedFromDurable),
			DurableStorageBytesWALExcludedNote: columnStoreDurableStorageWALExcludedNote,
			TotalReconstructableBytes:          totalReconstructableBytes,
			DBTotalBytes:                       totalBytes,
			DBTotalFiles:                       totalFiles,
		},
		CodecLayouts:          codecLayouts,
		CompressionMatrixNote: compressionMatrixNote,
		Manifest: columnStoreManifestMetric{
			ActiveGeneration:                manifestIdentity.ManifestGeneration,
			ActiveChecksum:                  manifestIdentity.ManifestChecksum,
			RecoveryAuthoritativeGeneration: manifestIdentity.RecoveryAuthoritativeGeneration,
			RecoveryAuthoritativeChecksum:   manifestIdentity.RecoveryAuthoritativeChecksum,
			AppliedCommandLSN:               manifestIdentity.RecoveryAuthoritativeAppliedCommandLSN,
			ManifestRootName:                manifestIdentity.ManifestRootName,
			ManifestRoot:                    manifestIdentity.ManifestRoot,
			SchemaHash:                      manifestIdentity.SchemaHash,
		},
		ProductionScope:          "production column-enabled TreeDB collection manifest/control-plane path plus isolated physical column assets and M14B planner-routed physical query execution",
		PhysicalColumnQuery:      "M14B routes forced serial and insert-only parallel_column_scan labels through the TreeDB physical query adapter; forced aggregate_metadata is executable for q4b and q5_metadata through typed aggregate metadata assets and other queries reroute to serial physical scan; unsupported prerequisites fail closed before row fallback",
		ExternalJSONBenchStatus:  "gomap-only synthetic report cells are implemented here; external snissn/JSONBench full-data and comparison cells are implemented separately and require a fresh run against the selected gomap dependency",
		ReportCaveats:            columnStoreReportCaveats(),
		ColgranuleReuseMap:       columnStoreColgranuleReuseMap(),
		ColumnAssetReadIntegrity: string(assetReadIntegrity),
		BenchmarkOnlyRelaxed:     columnStoreSuiteAssetReadIntegrityBenchmarkRelaxed(assetReadIntegrity),
		StageSeparatedBoundary:   "fixture generation, collection create, insert, checkpoint, reopen/recovery, planner, physical scan/reducer execution, row/B-tree reduce, and parity hash stages are timed separately for the forced execution label; M14B direct physical reducers are fused into scan timing unless visibility reconstruction reports a separate reduce phase",
	}
	if baseCfg.KeepDir {
		report.DataDir = dataDir
	}
	if profileFinalizeErr != nil {
		report.ProfileFinalizeError = profileFinalizeErr.Error()
	}
	populateColumnStoreThroughputInterpretations(report.Queries)

	md := renderColumnStoreSuiteMarkdown(report)
	run := columnStoreBenchRun(baseCfg, profile, dataDir, report, db.Stats(), checkpointDuration)
	if strings.TrimSpace(opts.ProfileDir) != "" {
		report.Artifacts = columnStoreArtifactPathsForProfileDir(opts.ProfileDir, baseCfg, opts.RunBenchprof)
		report.Artifacts = columnStoreSuitePruneMissingRuntimeDeltaArtifacts(report.Artifacts)
		md = renderColumnStoreSuiteMarkdown(report)
		if err := writeColumnStoreSuiteArtifacts(opts.ProfileDir, opts.ExecutionPath, report, md, run); err != nil {
			return "", err
		}
		if opts.RunBenchprof {
			if err := runBenchprofStrict(opts.ProfileDir); err != nil {
				return "", err
			}
		}
	}
	// Keep artifacts available for diagnosis even when parity/report-cell gates fail.
	if parityErr != nil || profileFinalizeErr != nil || jsonbenchCellsErr != nil {
		return "", errors.Join(parityErr, profileFinalizeErr, jsonbenchCellsErr)
	}
	return md, nil
}

func columnStoreSuiteEffectiveProfile(profile string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "", "durable":
		return "durable", nil
	case "balanced":
		// Accept unified-bench's default profile as an alias for the durable gate.
		return "durable", nil
	case "fast", "unsafe", "wal_on_fast":
		return "", fmt.Errorf("column_store: profile %q is benchmark-relaxed and unsupported for M11B production column-store writes; use -profile durable", profile)
	default:
		return "", fmt.Errorf("column_store: unsupported profile %q; use durable for the M11B production gate", profile)
	}
}

func columnStoreSuiteEffectiveAssetReadIntegrity(opt collections.ColumnAssetReadIntegrity) (collections.ColumnAssetReadIntegrity, error) {
	value := strings.ToLower(strings.TrimSpace(string(opt)))
	if value == "" {
		if flagExplicit("column-store-asset-read-integrity") {
			value = strings.ToLower(strings.TrimSpace(*columnStoreSuiteAssetReadIntegrityArg))
		} else if *treedbDisableReadChecksum {
			value = string(collections.ColumnAssetReadIntegritySkipChecksums)
		} else {
			value = strings.ToLower(strings.TrimSpace(*columnStoreSuiteAssetReadIntegrityArg))
		}
	}
	switch value {
	case "", string(collections.ColumnAssetReadIntegrityVerify):
		return collections.ColumnAssetReadIntegrityVerify, nil
	case string(collections.ColumnAssetReadIntegrityCachedVerify), "cached-verify", "verify_once", "verify-once":
		if !*treedbAllowUnsafe {
			return "", errors.New("column_store: column asset read integrity cached_verify requires -treedb-allow-unsafe")
		}
		return collections.ColumnAssetReadIntegrityCachedVerify, nil
	case string(collections.ColumnAssetReadIntegritySkipChecksums), "skip-checksums", "none":
		if !*treedbAllowUnsafe {
			return "", errors.New("column_store: column asset read integrity skip_checksums requires -treedb-allow-unsafe")
		}
		return collections.ColumnAssetReadIntegritySkipChecksums, nil
	default:
		return "", fmt.Errorf("column_store: unsupported column asset read integrity %q; use verify, cached_verify, or skip_checksums", value)
	}
}

func columnStoreSuiteAssetReadIntegrityBenchmarkRelaxed(integrity collections.ColumnAssetReadIntegrity) bool {
	switch integrity {
	case collections.ColumnAssetReadIntegrityCachedVerify, collections.ColumnAssetReadIntegritySkipChecksums:
		return true
	default:
		return false
	}
}

func normalizeColumnStoreSuitePath(path string) string {
	path = strings.ToLower(strings.TrimSpace(path))
	for _, alias := range columnStoreSuitePathAliases {
		if path == alias.alias {
			return alias.canonical
		}
	}
	return path
}

func validateColumnStoreSuiteExecutionPath(profileDir, executionPath string) error {
	executionPath = strings.TrimSpace(executionPath)
	if strings.TrimSpace(profileDir) == "" && executionPath == "" {
		return nil
	}
	if err := validateBenchprofExecutionPath(executionPath); err != nil {
		return err
	}
	if executionPath != "native-fastpath" {
		return fmt.Errorf("column_store: native suite requires -path-label native-fastpath; got %q", executionPath)
	}
	return nil
}

func columnStoreSuitePathAliasHelp(aliases []columnStoreSuitePathAlias) string {
	out := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		if alias.alias == "" {
			continue
		}
		out = append(out, alias.alias)
	}
	return strings.Join(out, ", ")
}

func columnStoreSuitePlanKind(path string) (collections.ColumnQueryPlanKind, error) {
	switch path {
	case columnStorePathRowStoreBaseline:
		return collections.ColumnQueryPlanRowStoreBaseline, nil
	case columnStorePathBTreeIndexBaseline:
		return collections.ColumnQueryPlanBTreeIndexBaseline, nil
	case columnStorePathSerialColumnScan:
		return collections.ColumnQueryPlanSerialColumnScan, nil
	case columnStorePathAggregateMetadata:
		return collections.ColumnQueryPlanAggregateMetadata, nil
	case columnStorePathParallelColumnScan:
		return collections.ColumnQueryPlanParallelColumnScan, nil
	default:
		return "", fmt.Errorf("column_store: unknown forced path %q; accepted=%s aliases=%s fail_closed=%s; see -column-store-path help", path, columnStoreSuitePathList(columnStoreSuiteAcceptedForcedPaths), columnStoreSuitePathAliasHelp(columnStoreSuitePathAliases), columnStoreSuitePathList(columnStoreSuiteFailClosedForcedPaths))
	}
}

func columnStoreArtifactPathsForProfileDir(profileDir string, cfg BenchConfig, runBenchprof bool) columnStoreArtifactPaths {
	paths := columnStoreArtifactPaths{
		ColumnJSON:        filepath.Join(profileDir, "column_store_results.json"),
		ColumnMarkdown:    filepath.Join(profileDir, "column_store_results.md"),
		ColumnHTML:        filepath.Join(profileDir, "column_store_results.html"),
		BenchprofJSON:     filepath.Join(profileDir, "benchprof_results.json"),
		BenchprofMarkdown: filepath.Join(profileDir, "benchprof_results.md"),
	}
	if runBenchprof {
		paths.InsightsMarkdown = filepath.Join(profileDir, "insights.md")
		paths.InsightsJSON = filepath.Join(profileDir, "insights.json")
		paths.InsightsHTML = filepath.Join(profileDir, "insights.html")
	}
	if shouldCPUProfile(cfg, columnStoreSuiteBenchTestName) {
		paths.CPUProfile = fmt.Sprintf("%s_%s_%s.pprof", strings.TrimSpace(cfg.CPUProfile), columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	if shouldAllocsProfile(cfg, columnStoreSuiteBenchTestName) {
		paths.AllocsProfile = fmt.Sprintf("%s_%s_%s.pprof", strings.TrimSpace(cfg.AllocsProfile), columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	if shouldCheckpointCPUProfile(cfg, columnStoreSuiteBenchTestName) {
		paths.CheckpointCPUProfile = fmt.Sprintf("%s_checkpoint_%s_%s.pprof", strings.TrimSpace(cfg.CheckpointCPUProfile), sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName))
	}
	blockProfile := strings.TrimSpace(cfg.BlockProfile)
	if blockProfile != "" {
		paths.BlockProfile = blockProfile
		paths.BlockDeltaProfile = contentionProfilePath(blockProfile, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	mutexProfile := strings.TrimSpace(cfg.MutexProfile)
	if mutexProfile != "" {
		paths.MutexProfile = mutexProfile
		paths.MutexDeltaProfile = contentionProfilePath(mutexProfile, "mutex", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	traceProfile := strings.TrimSpace(cfg.TraceProfile)
	if traceProfile != "" {
		paths.TraceProfile = traceProfile
	}
	return paths
}

func validateColumnStoreSuiteDBSelection(dbsArg, excludeArg string) error {
	for _, db := range parseList(excludeArg) {
		normalized := canonicalDBName(db)
		switch normalized {
		case "", "none":
			continue
		case "all", "treedb":
			return fmt.Errorf("column_store: native suite requires TreeDB but -dbs-exclude=%q excludes it", excludeArg)
		}
	}

	hasTreeDB := false
	for _, db := range resolveDBs(dbsArg, excludeArg) {
		switch db {
		case "treedb":
			hasTreeDB = true
		default:
			return fmt.Errorf("column_store: native suite only supports TreeDB; got -dbs=%q", dbsArg)
		}
	}
	if !hasTreeDB {
		return fmt.Errorf("column_store: native suite requires TreeDB; got -dbs=%q", dbsArg)
	}
	return nil
}

func columnStoreSuitePathList(paths []string) string {
	return strings.Join(paths, ",")
}

func cloneStringSlice(in []string) []string {
	return append([]string(nil), in...)
}

func buildColumnStoreSyntheticFixture(rows int, seed int64) ([]columnStoreFixtureEvent, int64) {
	rng := rand.New(rand.NewSource(seed))
	out := make([]columnStoreFixtureEvent, rows)
	var bytesTotal int64
	const baseTimeUS = int64(1_700_000_000_000_000)
	for i := 0; i < rows; i++ {
		timeUS := baseTimeUS + int64((i*7919)%86400)*1_000_000 + int64(rng.Intn(1000))
		kind := fmt.Sprintf("kind_%02d", i%8)
		did := fmt.Sprintf("d%06d", i%1024)
		id := fmt.Sprintf("e%09d", i)
		payloadID := uint32(uint64(i) * uint64(2654435761))
		doc := []byte(fmt.Sprintf(`{"time_us":%d,"kind":"%s","did":"%s","payload":"p%08x","group":%d}`,
			timeUS, kind, did, payloadID, i%32))
		out[i] = columnStoreFixtureEvent{ID: id, IDBytes: []byte(id), TimeUS: timeUS, Kind: kind, Did: did, Doc: doc}
		bytesTotal += int64(len(doc))
	}
	return out, bytesTotal
}

func columnStoreSuiteConfig() *collections.ColumnStoreConfig {
	typedBenchmarkPolicyActive := columnStoreSuiteTypedBenchmarkPolicyFlagsActive()
	profileSupport := collections.ColumnStoreProfileDurableOnly
	if typedBenchmarkPolicyActive {
		profileSupport = collections.ColumnStoreProfileBenchmarkRelaxed
	}
	cfg := &collections.ColumnStoreConfig{
		Enabled: true,
		Columns: []collections.ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "did", Path: "did", ValueType: collections.ColumnStoreValueString, Dictionary: true},
		},
		SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
		AggregateMetadata: []collections.ColumnAggregateMetadata{
			{Name: columnStoreSuiteQ5AggregateMin, Column: "time_us", GroupColumn: "did", Kind: collections.ColumnAggregateMin},
			{Name: columnStoreSuiteQ5AggregateMax, Column: "time_us", GroupColumn: "did", Kind: collections.ColumnAggregateMax},
		},
		RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
		Reconstruction:  collections.ColumnReconstructionRetainedPayloadAndColumns,
		ProfileSupport:  profileSupport,
	}
	if typedBenchmarkPolicyActive {
		for i := range cfg.Columns {
			cfg.Columns[i].Owner = collections.TypedStorageOwnerColumnPart
		}
	}
	return cfg
}

func columnStoreSuiteTypedBenchmarkPolicyFlagsActive() bool {
	return strings.TrimSpace(*columnStoreSuiteTypedCompressionArg) != "" ||
		strings.TrimSpace(*columnStoreSuiteTypedInt64EncodingArg) != "" ||
		*columnStoreSuiteTypedRowsPerGranuleArg != 0 ||
		*columnStoreSuiteTypedAdaptiveArg ||
		*columnStoreSuiteTypedAdaptiveTargetBytesArg != 0 ||
		*columnStoreSuiteTypedAdaptiveMinRowsArg != 0 ||
		*columnStoreSuiteTypedAdaptiveMaxRowsArg != 0
}

func columnStoreSuiteValidateTypedCompressionFlag(value string) error {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "default", "none", "off", "compression_off", "snappy", "lz4":
		return nil
	case "zstd", "zstd_dict", "zstd-dict":
		return fmt.Errorf("column_store: typed compression %q is unsupported/deferred for production; enum labels are reported but not executable", value)
	default:
		return fmt.Errorf("column_store: unknown typed compression %q", value)
	}
}

func columnStoreSuiteValidateTypedInt64EncodingFlag(value string) error {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "default", "raw", "raw_int64", "delta", "delta_varint", "double_delta", "double-delta", "double_delta_varint":
		return nil
	default:
		return fmt.Errorf("column_store: unknown typed int64 encoding %q", value)
	}
}

func columnStoreSuiteInstallTypedBenchmarkPolicyEnv() (func(), error) {
	type envKV struct{ name, value string }
	vars := make([]envKV, 0, 7)
	if value := strings.TrimSpace(*columnStoreSuiteTypedCompressionArg); value != "" {
		if err := columnStoreSuiteValidateTypedCompressionFlag(value); err != nil {
			return nil, err
		}
		vars = append(vars, envKV{"TREEDB_COLUMN_STORE_TYPED_COMPRESSION", value})
	}
	if value := strings.TrimSpace(*columnStoreSuiteTypedInt64EncodingArg); value != "" {
		if err := columnStoreSuiteValidateTypedInt64EncodingFlag(value); err != nil {
			return nil, err
		}
		vars = append(vars, envKV{"TREEDB_COLUMN_STORE_TYPED_INT64_ENCODING", value})
	}
	if *columnStoreSuiteTypedRowsPerGranuleArg < 0 {
		return nil, fmt.Errorf("column_store: -column-store-typed-rows-per-granule must be >= 0")
	}
	if *columnStoreSuiteTypedRowsPerGranuleArg > 0 {
		vars = append(vars, envKV{"TREEDB_COLUMN_STORE_TYPED_ROWS_PER_GRANULE", strconv.Itoa(*columnStoreSuiteTypedRowsPerGranuleArg)})
	}
	if *columnStoreSuiteTypedAdaptiveArg {
		vars = append(vars, envKV{"TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_ENABLED", "true"})
	}
	for _, item := range []struct {
		flagValue int
		flagName  string
		envName   string
	}{
		{*columnStoreSuiteTypedAdaptiveTargetBytesArg, "-column-store-typed-adaptive-target-bytes", "TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_TARGET_BYTES"},
		{*columnStoreSuiteTypedAdaptiveMinRowsArg, "-column-store-typed-adaptive-min-rows", "TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_MIN_ROWS"},
		{*columnStoreSuiteTypedAdaptiveMaxRowsArg, "-column-store-typed-adaptive-max-rows", "TREEDB_COLUMN_STORE_TYPED_ADAPTIVE_MAX_ROWS"},
	} {
		if item.flagValue < 0 {
			return nil, fmt.Errorf("column_store: %s must be >= 0", item.flagName)
		}
		if item.flagValue > 0 {
			vars = append(vars, envKV{item.envName, strconv.Itoa(item.flagValue)})
		}
	}
	if len(vars) == 0 {
		return func() {}, nil
	}
	previous := make([]struct {
		name  string
		value string
		ok    bool
	}, 0, len(vars))
	for _, item := range vars {
		old, ok := os.LookupEnv(item.name)
		previous = append(previous, struct {
			name  string
			value string
			ok    bool
		}{item.name, old, ok})
		if err := os.Setenv(item.name, item.value); err != nil {
			return nil, err
		}
	}
	return func() {
		for i := len(previous) - 1; i >= 0; i-- {
			item := previous[i]
			if item.ok {
				_ = os.Setenv(item.name, item.value)
			} else {
				_ = os.Unsetenv(item.name)
			}
		}
	}, nil
}

func columnStoreSuiteConfigForPath(path string) *collections.ColumnStoreConfig {
	cfg := columnStoreSuiteConfig()
	if path == columnStorePathBTreeIndexBaseline {
		// Secondary-index write paths have not yet been wired to store only
		// retained row payloads while building indexes from full documents.
		// Keep this explicit comparison baseline full-retained until that
		// production shape is supported.
		cfg.RetainedPayload = collections.ColumnRetainedPayloadFull
	}
	return cfg
}

func columnStoreSuiteCollectionMeta(path string) *collections.CollectionMeta {
	meta := &collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfigForPath(path),
		},
	}
	if path == columnStorePathBTreeIndexBaseline {
		meta.Indexes = columnStoreSuiteIndexes()
	}
	return meta
}

func columnStoreSuiteIndexes() []collections.IndexDefinition {
	return []collections.IndexDefinition{
		{Name: "kind_idx", Field: "kind", ValueType: collections.IndexValueString},
		{Name: "time_us_idx", Field: "time_us", ValueType: collections.IndexValueInt64},
		{Name: "did_idx", Field: "did", ValueType: collections.IndexValueString},
	}
}

func openColumnStoreSuiteDB(dir string) (*backenddb.DB, error) {
	return backenddb.Open(backenddb.Options{
		Dir:                    dir,
		CommandWAL:             true,
		CommandWALStatsScan:    true,
		Durability:             backenddb.DurabilityDurable,
		DisableBackgroundPrune: true,
	})
}

func insertColumnStoreFixture(collection *collections.Collection, events []columnStoreFixtureEvent, batchSize int) error {
	idsAll := make([][]byte, len(events))
	docsAll := make([][]byte, len(events))
	for i := range events {
		idsAll[i] = events[i].IDBytes
		docsAll[i] = events[i].Doc
	}
	for start := 0; start < len(events); start += batchSize {
		end := start + batchSize
		if end > len(events) {
			end = len(events)
		}
		if _, err := collection.InsertBatch(idsAll[start:end], docsAll[start:end]); err != nil {
			return fmt.Errorf("column_store: InsertBatch rows %d-%d: %w", start, end, err)
		}
	}
	return nil
}

func columnStoreReferenceHashes(events []columnStoreFixtureEvent) (map[string]uint64, error) {
	decoded := make([]columnStoreDecodedEvent, len(events))
	for i := range events {
		decoded[i] = columnStoreDecodedEvent{TimeUS: events[i].TimeUS, Kind: events[i].Kind, Did: events[i].Did}
	}
	out := make(map[string]uint64)
	for _, name := range columnStoreQueryNameList {
		hash, _, err := columnStoreQueryHash(name, decoded)
		if err != nil {
			return nil, err
		}
		out[name] = hash
	}
	return out, nil
}

func startColumnStoreSuiteRuntimeProfiles(cfg BenchConfig) (func() error, error) {
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

	if shouldAllocsProfile(cfg, columnStoreSuiteBenchTestName) {
		restoreMemRate := installAllocsProfileRateForEnabled(true, cfg.AllocsProfileRate)
		restore := func() error {
			restoreMemRate()
			return nil
		}
		cleanups = append(cleanups, runtimeProfileCleanup{
			finish: restore,
			abort:  restore,
		})
	}

	blockProfile := strings.TrimSpace(cfg.BlockProfile)
	mutexProfile := strings.TrimSpace(cfg.MutexProfile)
	traceProfile := strings.TrimSpace(cfg.TraceProfile)

	if blockProfile != "" {
		rate := cfg.BlockProfileRate
		if rate <= 0 {
			rate = 1
		}
		f, err := os.Create(blockProfile)
		if err != nil {
			_ = abort()
			return nil, fmt.Errorf("column_store: blockprofile: %w", err)
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
			abort: func() error {
				runtime.SetBlockProfileRate(0)
				return errors.Join(f.Close(), os.Remove(blockProfile))
			},
		})
	}

	if mutexProfile != "" {
		frac := cfg.MutexProfileFraction
		if frac <= 0 {
			frac = 1
		}
		f, err := os.Create(mutexProfile)
		if err != nil {
			_ = abort()
			return nil, fmt.Errorf("column_store: mutexprofile: %w", err)
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

	if traceProfile != "" {
		f, err := os.Create(traceProfile)
		if err != nil {
			_ = abort()
			return nil, fmt.Errorf("column_store: trace: %w", err)
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			_ = os.Remove(traceProfile)
			_ = abort()
			return nil, fmt.Errorf("column_store: trace start: %w", err)
		}
		cleanups = append(cleanups, runtimeProfileCleanup{
			finish: func() error {
				trace.Stop()
				return f.Close()
			},
			abort: func() error {
				trace.Stop()
				return errors.Join(f.Close(), os.Remove(traceProfile))
			},
		})
	}

	return finish, nil
}

func runColumnStoreSuiteQueriesProfiled(cfg BenchConfig, collection *collections.Collection, rows int, rawHashes map[string]uint64, path string, assetReadIntegrity collections.ColumnAssetReadIntegrity, queryNames []string) ([]columnStoreQueryMetric, map[string]columnStoreParity, error, error) {
	profileHooks := profileHooksFromConfig(cfg)
	cleanup := func(paths ...string) {
		for _, path := range paths {
			if path != "" {
				_ = os.Remove(path)
			}
		}
	}
	allocBasePath := ""
	var err error
	if shouldAllocsProfile(cfg, columnStoreSuiteBenchTestName) {
		allocBasePath, err = profileHooks.writeAllocsSnapshotTemp("unified_bench_column_store_allocs_base")
		if err != nil {
			return nil, nil, nil, fmt.Errorf("column_store: allocsprofile baseline: %w", err)
		}
	}
	blockBasePath := ""
	blockProfile := strings.TrimSpace(cfg.BlockProfile)
	if blockProfile != "" {
		blockBasePath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_column_store_block_base", "block")
		if err != nil {
			cleanup(allocBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile baseline: %w", err)
		}
	}
	mutexBasePath := ""
	mutexProfile := strings.TrimSpace(cfg.MutexProfile)
	if mutexProfile != "" {
		mutexBasePath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_column_store_mutex_base", "mutex")
		if err != nil {
			cleanup(allocBasePath, blockBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile baseline: %w", err)
		}
	}

	var cpuFile *os.File
	cpuProfilePath := ""
	if shouldCPUProfile(cfg, columnStoreSuiteBenchTestName) {
		profilePath := fmt.Sprintf("%s_%s_%s.pprof", strings.TrimSpace(cfg.CPUProfile), columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
		f, err := os.Create(profilePath)
		if err != nil {
			cleanup(allocBasePath, blockBasePath, mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: cpuprofile %s: %w", profilePath, err)
		}
		cpuFile = f
		cpuProfilePath = profilePath
		if err := profileHooks.startCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			cleanup(profilePath, allocBasePath, blockBasePath, mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: cpuprofile start %s: %w", profilePath, err)
		}
	}

	queries, parity, runErr := runColumnStoreSuiteQueries(collection, rows, rawHashes, path, assetReadIntegrity, queryNames)

	if cpuFile != nil {
		profileHooks.stopCPUProfile()
		_ = cpuFile.Close()
	}
	if queries == nil {
		cleanup(cpuProfilePath, allocBasePath, blockBasePath, mutexBasePath)
		return nil, nil, nil, runErr
	}

	if allocBasePath != "" {
		allocAfterPath, snapErr := profileHooks.writeAllocsSnapshotTemp("unified_bench_column_store_allocs_after")
		if snapErr != nil {
			cleanup(allocBasePath, blockBasePath, mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: allocsprofile snapshot: %w", snapErr)
		}
		allocPath := fmt.Sprintf("%s_%s_%s.pprof", strings.TrimSpace(cfg.AllocsProfile), columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
		deltaErr := profileHooks.writeAllocsDeltaProfile(allocBasePath, allocAfterPath, allocPath)
		cleanup(allocBasePath, allocAfterPath)
		if deltaErr != nil {
			cleanup(blockBasePath, mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: allocsprofile %s: %w", allocPath, deltaErr)
		}
	}
	if blockBasePath != "" {
		blockAfterPath, snapErr := profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_column_store_block_after", "block")
		if snapErr != nil {
			cleanup(blockBasePath, mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile snapshot: %w", snapErr)
		}
		blockPath := contentionProfilePath(blockProfile, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
		wrote, deltaErr := profileHooks.writeRuntimeProfileDeltaProfile(blockBasePath, blockAfterPath, blockPath)
		cleanup(blockBasePath, blockAfterPath)
		if deltaErr != nil {
			cleanup(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile %s: %w", blockPath, deltaErr)
		}
		if !wrote {
			cleanup(blockPath)
		}
	}
	if mutexBasePath != "" {
		mutexAfterPath, snapErr := profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_column_store_mutex_after", "mutex")
		if snapErr != nil {
			cleanup(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile snapshot: %w", snapErr)
		}
		mutexPath := contentionProfilePath(mutexProfile, "mutex", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
		wrote, deltaErr := profileHooks.writeRuntimeProfileDeltaProfile(mutexBasePath, mutexAfterPath, mutexPath)
		cleanup(mutexBasePath, mutexAfterPath)
		if deltaErr != nil {
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile %s: %w", mutexPath, deltaErr)
		}
		if !wrote {
			cleanup(mutexPath)
		}
	}

	return queries, parity, runErr, nil
}

func runColumnStoreSuiteQueries(collection *collections.Collection, rows int, rawHashes map[string]uint64, path string, assetReadIntegrity collections.ColumnAssetReadIntegrity, queryNames []string) ([]columnStoreQueryMetric, map[string]columnStoreParity, error) {
	path = normalizeColumnStoreSuitePath(path)
	forceKind, err := columnStoreSuitePlanKind(path)
	if err != nil {
		return nil, nil, err
	}
	queryNames, err = columnStoreSuiteEffectiveQueryNames(queryNames, "")
	if err != nil {
		return nil, nil, err
	}
	queries := make([]columnStoreQueryMetric, 0, len(queryNames))
	parity := make(map[string]columnStoreParity, len(queryNames))
	manifestIdentity, hasManifestIdentity := collection.ColumnStoreCacheIdentity()
	var firstErr error
	for _, name := range queryNames {
		queryForceKind := columnStoreSuitePlanKindForQuery(path, name, forceKind)
		plannerStart := time.Now()
		plan, err := collection.PlanColumnQuery(columnStoreSuitePlanRequest(name, rows, queryForceKind))
		plannerElapsed := time.Since(plannerStart)
		if err != nil {
			return nil, nil, err
		}
		if !plan.Supported {
			reason := strings.TrimSpace(plan.Diagnostics.UnsupportedPlanReason)
			if reason == "" {
				reason = "planner did not report an unsupported reason"
			}
			return nil, nil, fmt.Errorf("column_store: forced path %q unsupported for %s: refusing to route through row store; reason=%s: %w", path, name, reason, collections.ErrColumnQueryPlanUnsupported)
		}

		exec, err := executeColumnStoreSuiteQueryWithPlan(collection, rows, name, plan, assetReadIntegrity)
		if err != nil {
			return nil, nil, err
		}
		planLabel := string(plan.Kind)
		var hash uint64
		var parityHashElapsed time.Duration
		if exec.ProductionHashKnown {
			hash = exec.ProductionHash
			parityHashElapsed = exec.ParityHashDuration
		} else {
			parityHashStart := time.Now()
			hash = columnStoreHashLines(exec.Lines)
			parityHashElapsed = time.Since(parityHashStart)
		}
		elapsed := plannerElapsed + exec.ScanDuration + exec.ReduceDuration + exec.AdapterDuration + parityHashElapsed
		rawHash := rawHashes[name]
		pass := rawHash == hash
		parity[name] = columnStoreParity{Pass: pass, RawHash: rawHash, ProductionHash: hash}
		if !pass && firstErr == nil {
			firstErr = fmt.Errorf("column_store: parity mismatch for %s: raw=%016x production=%016x", name, rawHash, hash)
		}
		storageSource := exec.StorageSource
		if storageSource == "" {
			storageSource = string(collections.ColumnPhysicalQueryStorageSourceFallback)
		}
		fallbackReason := exec.FallbackReason
		if fallbackReason == "" {
			fallbackReason = string(collections.ColumnPhysicalQueryFallbackNone)
		}
		if path == columnStorePathAggregateMetadata && planLabel == columnStorePathSerialColumnScan && fallbackReason == string(collections.ColumnPhysicalQueryFallbackNone) {
			fallbackReason = string(collections.ColumnPhysicalQueryFallbackAggregateMetadataUnsupported)
		}
		manifestRootName := exec.ManifestRootName
		manifestRoot := exec.ManifestRoot
		manifestGeneration := exec.ManifestGeneration
		activeManifestChecksum := exec.ActiveManifestChecksum
		if hasManifestIdentity {
			if manifestRootName == "" {
				manifestRootName = manifestIdentity.ManifestRootName
			}
			if manifestRoot == 0 {
				manifestRoot = manifestIdentity.ManifestRoot
			}
			if manifestGeneration == 0 {
				manifestGeneration = manifestIdentity.ManifestGeneration
			}
			if activeManifestChecksum == 0 {
				activeManifestChecksum = manifestIdentity.ManifestChecksum
			}
		}
		metric := columnStoreQueryMetric{
			Name: name,
			// PlanLabel records the executed planner kind after alias
			// normalization, not necessarily the raw requested path string.
			PlanLabel:              planLabel,
			AliasOf:                columnStoreQueryAliasOf(name, planLabel),
			ImplementationNote:     columnStoreQueryImplementationNote(name, path, planLabel),
			StorageSource:          storageSource,
			FallbackReason:         fallbackReason,
			ManifestRootName:       manifestRootName,
			ManifestRoot:           manifestRoot,
			ManifestGeneration:     manifestGeneration,
			ActiveManifestChecksum: activeManifestChecksum,
			DurationMS:             durationMS(elapsed),
			duration:               elapsed,
			hotRunDuration:         exec.HotRunDuration,
			Rows:                   rows,
			RowsProcessed:          exec.RowsProcessed,
			RowsProcessedKnown:     exec.RowsProcessedKnown,
			RowsPerSecond:          ratePerSecond(float64(exec.RowsProcessed), elapsed),
			MiBPerSecond:           ratePerSecond(float64(exec.BytesRead)/(1024*1024), elapsed),
			NsPerRow:               nsPerRow(elapsed, exec.RowsProcessed),
			BytesRead:              exec.BytesRead,
			RowMaterializations:    exec.RowMaterializations,
			ResultCount:            exec.ResultCount,
			RawHash:                rawHash,
			ProductionHash:         hash,
			MetadataHits:           exec.MetadataHits,
			DictionaryCodeHits:     exec.DictionaryCodeHits,
			Int64ValueHits:         exec.Int64ValueHits,
			SkippedGranules:        exec.SkippedGranules,
			ScheduledGranules:      exec.ScheduledGranules,
			WorkerCount:            exec.WorkerCount,
			PlannerDurationMS:      durationMS(plannerElapsed),
			ScanDurationMS:         durationMS(exec.ScanDuration),
			ReduceDurationMS:       durationMS(exec.ReduceDuration),
			AdapterDurationMS:      durationMS(exec.AdapterDuration),
			ParityHashDurationMS:   durationMS(parityHashElapsed),
			RowsScanned:            exec.RowsScanned,
			RowsMatched:            exec.RowsMatched,
			ReduceRows:             exec.ReduceRows,
			DecodedGranules:        exec.DecodedGranules,
			PlannerCandidates:      plan.Diagnostics.CandidatePlans,
			PlannerReason:          plan.Diagnostics.Reason,
			SegmentFileCacheHits:   exec.SegmentFileCacheHits,
			SegmentFileCacheMisses: exec.SegmentFileCacheMisses,
			CacheLabel:             "reopened_warm_process",
			CompressionAttribution: columnStoreQueryCompressionAttribution(
				planLabel,
				storageSource,
				fallbackReason,
				exec.BytesRead,
			),
		}
		queries = append(queries, metric)
	}
	return queries, parity, firstErr
}

func columnStoreSuitePlanRequest(name string, rows int, forceKind collections.ColumnQueryPlanKind) collections.ColumnQueryPlanRequest {
	return collections.ColumnQueryPlanRequest{
		Name:                  name,
		ProjectedColumns:      []string{"time_us", "kind", "did"},
		CandidateIndexColumns: columnStoreSuiteQueryIndexCandidates(name),
		AggregateMetadataName: columnStoreSuiteAggregateMetadataName(name),
		EstimatedRows:         rows,
		ForceKind:             forceKind,
		Capabilities: collections.ColumnQueryPlannerCapabilities{
			// M14B still requires planner discovery to replace these caller
			// placeholders with recovery-authoritative manifest facts.
			SerialColumnScan:       true,
			AggregateMetadata:      true,
			ParallelColumnScan:     true,
			PhysicalAssetCount:     0,
			PartCount:              2,
			GranuleCount:           2,
			MaxParallelWorkers:     2,
			PlannerCandidateBudget: 5,
		},
	}
}

func columnStoreSuitePlanKindForQuery(path, name string, forceKind collections.ColumnQueryPlanKind) collections.ColumnQueryPlanKind {
	if path == columnStorePathAggregateMetadata && columnStoreSuiteAggregateMetadataName(name) == "" {
		return collections.ColumnQueryPlanSerialColumnScan
	}
	return forceKind
}

func columnStoreSuiteQueryIndexCandidates(name string) []string {
	// Candidates must stay one-index-entry-per-document scalar fields because
	// the M11B B-tree baseline does a full ordered index pass for parity, then
	// verifies the materialized count against the fixture row count. The selected
	// index records which secondary structure is traversed; it is not a
	// predicate-selective read path until range pushdown lands.
	switch name {
	case columnStoreQueryQ1, columnStoreQueryQ2:
		return []string{"kind"}
	case columnStoreQueryQ3:
		return []string{"time_us"}
	case columnStoreQueryQ4A, columnStoreQueryQ4B, columnStoreQueryQ5, columnStoreQueryQ5Metadata:
		return []string{"did"}
	default:
		return nil
	}
}

func columnStoreSuiteAggregateMetadataName(name string) string {
	switch name {
	case columnStoreQueryQ4B:
		return columnStoreSuiteQ5AggregateMax
	case columnStoreQueryQ5Metadata:
		return columnStoreSuiteQ5AggregateMin
	default:
		return ""
	}
}

func executeColumnStoreSuiteQueryWithPlan(collection *collections.Collection, rows int, queryName string, plan collections.ColumnQueryPlan, assetReadIntegrity collections.ColumnAssetReadIntegrity) (columnStoreQueryExecution, error) {
	switch plan.Kind {
	case collections.ColumnQueryPlanRowStoreBaseline:
		scanStart := time.Now()
		events, materialized, bytesRead, err := scanColumnStoreSuiteEvents(collection, rows)
		scanElapsed := time.Since(scanStart)
		if err != nil {
			return columnStoreQueryExecution{}, err
		}
		reduceStart := time.Now()
		lines, err := columnStoreQueryLines(columnStoreQueryHashLineName(queryName), events)
		reduceElapsed := time.Since(reduceStart)
		if err != nil {
			return columnStoreQueryExecution{}, fmt.Errorf("column_store: reduce %s: %w", queryName, err)
		}
		return columnStoreQueryExecution{
			Lines:                  lines,
			StorageSource:          string(collections.ColumnPhysicalQueryStorageSourceRowScan),
			FallbackReason:         string(collections.ColumnPhysicalQueryFallbackNone),
			RowsProcessed:          materialized,
			RowsProcessedKnown:     true,
			BytesRead:              bytesRead,
			RowMaterializations:    materialized,
			ResultCount:            len(lines),
			ScheduledGranules:      plan.Diagnostics.ScheduledGranules,
			SkippedGranules:        plan.Diagnostics.SkippedGranules,
			WorkerCount:            1,
			SegmentFileCacheHits:   plan.Diagnostics.SegmentFileCacheHits,
			SegmentFileCacheMisses: plan.Diagnostics.SegmentFileCacheMisses,
			ScanDuration:           scanElapsed,
			ReduceDuration:         reduceElapsed,
		}, nil
	case collections.ColumnQueryPlanBTreeIndexBaseline:
		scanStart := time.Now()
		events, materialized, bytesRead, err := scanColumnStoreSuiteEventsByIndex(collection, rows, queryName, plan.IndexName)
		scanElapsed := time.Since(scanStart)
		if err != nil {
			return columnStoreQueryExecution{}, err
		}
		reduceStart := time.Now()
		lines, err := columnStoreQueryLines(columnStoreQueryHashLineName(queryName), events)
		reduceElapsed := time.Since(reduceStart)
		if err != nil {
			return columnStoreQueryExecution{}, fmt.Errorf("column_store: reduce %s: %w", queryName, err)
		}
		return columnStoreQueryExecution{
			Lines:                  lines,
			StorageSource:          string(collections.ColumnPhysicalQueryStorageSourceRowScan),
			FallbackReason:         string(collections.ColumnPhysicalQueryFallbackNone),
			RowsProcessed:          materialized,
			RowsProcessedKnown:     true,
			BytesRead:              bytesRead,
			RowMaterializations:    materialized,
			ResultCount:            len(lines),
			ScheduledGranules:      plan.Diagnostics.ScheduledGranules,
			SkippedGranules:        plan.Diagnostics.SkippedGranules,
			WorkerCount:            1,
			SegmentFileCacheHits:   plan.Diagnostics.SegmentFileCacheHits,
			SegmentFileCacheMisses: plan.Diagnostics.SegmentFileCacheMisses,
			ScanDuration:           scanElapsed,
			ReduceDuration:         reduceElapsed,
		}, nil
	case collections.ColumnQueryPlanSerialColumnScan, collections.ColumnQueryPlanAggregateMetadata, collections.ColumnQueryPlanParallelColumnScan:
		return executeColumnStoreSuitePhysicalQuery(collection, queryName, plan, assetReadIntegrity)
	default:
		return columnStoreQueryExecution{}, fmt.Errorf("column_store: executable path %q is not implemented: %w", plan.Kind, collections.ErrColumnQueryPlanUnsupported)
	}
}

func executeColumnStoreSuitePhysicalQuery(collection *collections.Collection, queryName string, plan collections.ColumnQueryPlan, assetReadIntegrity collections.ColumnAssetReadIntegrity) (columnStoreQueryExecution, error) {
	req, err := columnStoreSuitePhysicalQueryRequest(queryName)
	if err != nil {
		return columnStoreQueryExecution{}, err
	}
	if plan.Kind == collections.ColumnQueryPlanAggregateMetadata {
		req.AggregateMetadataName = columnStoreSuiteAggregateMetadataName(queryName)
	} else {
		req.AggregateMetadataName = ""
	}
	req.ColumnAssetReadIntegrity = assetReadIntegrity
	start := time.Now()
	var result collections.ColumnPhysicalQueryResult
	switch plan.Kind {
	case collections.ColumnQueryPlanParallelColumnScan:
		workers := plan.Diagnostics.WorkerCount
		if workers <= 1 {
			return columnStoreQueryExecution{}, fmt.Errorf("column_store: parallel physical plan for %s has worker_count=%d: %w", queryName, workers, collections.ErrColumnQueryPlanUnsupported)
		}
		result, err = collection.RunColumnPhysicalQueryParallel(req, workers)
	case collections.ColumnQueryPlanSerialColumnScan, collections.ColumnQueryPlanAggregateMetadata:
		result, err = collection.RunColumnPhysicalQuery(req)
	default:
		return columnStoreQueryExecution{}, fmt.Errorf("column_store: executable path %q is not physical: %w", plan.Kind, collections.ErrColumnQueryPlanUnsupported)
	}
	elapsed := time.Since(start)
	if err != nil {
		return columnStoreQueryExecution{}, fmt.Errorf("column_store: physical query %s via %s: %w", queryName, plan.Kind, err)
	}
	parityHashStart := time.Now()
	productionHash, resultCount, err := columnStoreSuiteHashPhysicalQueryGroups(columnStoreQueryHashLineName(queryName), queryName, result.Groups)
	parityHashElapsed := time.Since(parityHashStart)
	if err != nil {
		return columnStoreQueryExecution{}, fmt.Errorf("column_store: physical query %s via %s parity hash: %w", queryName, plan.Kind, err)
	}
	diag := result.Diagnostics
	workers := diag.WorkerCount
	if workers <= 0 {
		workers = plan.Diagnostics.WorkerCount
	}
	if workers <= 0 {
		workers = 1
	}
	scanDuration, reduceDuration, resultShapeDuration := columnStorePhaseDurations(elapsed, diag)
	return columnStoreQueryExecution{
		ProductionHash:         productionHash,
		ProductionHashKnown:    true,
		StorageSource:          string(diag.StorageSource),
		FallbackReason:         string(diag.FallbackReason),
		ManifestRootName:       diag.ManifestRootName,
		ManifestRoot:           diag.ManifestRoot,
		ManifestGeneration:     diag.ManifestGeneration,
		ActiveManifestChecksum: diag.ActiveManifestChecksum,
		RowsProcessed:          diag.ReduceRows,
		RowsProcessedKnown:     true,
		RowsScanned:            diag.RowsScanned,
		RowsMatched:            diag.RowsMatched,
		ReduceRows:             diag.ReduceRows,
		DecodedGranules:        diag.DecodedGranules,
		DecodedBlocks:          diag.DecodedBlocks,
		DecodedPayloadBytes:    diag.DecodedPayloadBytes,
		MappedBytes:            diag.MappedBytes,
		HeapCopyBytes:          diag.HeapCopyBytes,
		PredicateCount:         diag.PredicateCount,
		BytesRead:              diag.PhysicalBytesScanned,
		RowMaterializations:    diag.RowMaterializations,
		ResultCount:            resultCount,
		MetadataHits:           diag.MetadataHits,
		DictionaryCodeHits:     diag.DictionaryCodeHits,
		Int64ValueHits:         diag.Int64ValueHits,
		SkippedGranules:        diag.SkippedGranules,
		ScheduledGranules:      diag.ScheduledGranules,
		WorkerCount:            workers,
		SegmentFileCacheHits:   diag.SegmentFileCacheHits,
		SegmentFileCacheMisses: diag.SegmentFileCacheMisses,
		HotRunDuration:         elapsed,
		ScanDuration:           scanDuration,
		ReduceDuration:         reduceDuration,
		AdapterDuration:        resultShapeDuration,
		ResultShapeDuration:    resultShapeDuration,
		ParityHashDuration:     parityHashElapsed,
	}, nil
}

func columnStorePhaseDurations(total time.Duration, diag collections.ColumnPhysicalQueryDiagnostics) (time.Duration, time.Duration, time.Duration) {
	reduceDuration := time.Duration(0)
	if diag.ReduceNanos > 0 {
		reduceDuration = time.Duration(diag.ReduceNanos)
	}
	resultShapeDuration := time.Duration(0)
	if diag.ResultShapeNanos > 0 {
		resultShapeDuration = time.Duration(diag.ResultShapeNanos)
	}
	scanDuration := total - reduceDuration - resultShapeDuration
	if scanDuration < 0 {
		scanDuration = 0
	}
	return scanDuration, reduceDuration, resultShapeDuration
}

func executeColumnStoreSuitePreparedPhysicalQuery(collection *collections.Collection, queryName string, planKind collections.ColumnQueryPlanKind, assetReadIntegrity collections.ColumnAssetReadIntegrity) (columnStoreQueryExecution, error) {
	req, err := columnStoreSuitePhysicalQueryRequest(queryName)
	if err != nil {
		return columnStoreQueryExecution{}, err
	}
	if planKind == collections.ColumnQueryPlanAggregateMetadata {
		req.AggregateMetadataName = columnStoreSuiteAggregateMetadataName(queryName)
	} else {
		req.AggregateMetadataName = ""
	}
	req.ColumnAssetReadIntegrity = assetReadIntegrity
	setupStart := time.Now()
	runner, err := collection.PrepareColumnPhysicalQuery(req)
	setupElapsed := time.Since(setupStart)
	if err != nil {
		return columnStoreQueryExecution{SetupDuration: setupElapsed}, fmt.Errorf("column_store: prepare physical query %s via %s: %w", queryName, planKind, err)
	}
	defer runner.Close()
	runStart := time.Now()
	result, err := runner.Run()
	hotRunElapsed := time.Since(runStart)
	if err != nil {
		return columnStoreQueryExecution{SetupDuration: setupElapsed, HotRunDuration: hotRunElapsed}, fmt.Errorf("column_store: prepared physical query %s via %s: %w", queryName, planKind, err)
	}
	parityHashStart := time.Now()
	productionHash, resultCount, err := columnStoreSuiteHashPhysicalQueryGroups(columnStoreQueryHashLineName(queryName), queryName, result.Groups)
	parityHashElapsed := time.Since(parityHashStart)
	if err != nil {
		return columnStoreQueryExecution{SetupDuration: setupElapsed, HotRunDuration: hotRunElapsed}, fmt.Errorf("column_store: prepared physical query %s via %s parity hash: %w", queryName, planKind, err)
	}
	diag := result.Diagnostics
	workers := diag.WorkerCount
	if workers <= 0 {
		workers = 1
	}
	scanDuration, reduceDuration, resultShapeDuration := columnStorePhaseDurations(hotRunElapsed, diag)
	return columnStoreQueryExecution{
		ProductionHash:         productionHash,
		ProductionHashKnown:    true,
		StorageSource:          string(diag.StorageSource),
		FallbackReason:         string(diag.FallbackReason),
		ManifestRootName:       diag.ManifestRootName,
		ManifestRoot:           diag.ManifestRoot,
		ManifestGeneration:     diag.ManifestGeneration,
		ActiveManifestChecksum: diag.ActiveManifestChecksum,
		RowsProcessed:          diag.ReduceRows,
		RowsProcessedKnown:     true,
		RowsScanned:            diag.RowsScanned,
		RowsMatched:            diag.RowsMatched,
		ReduceRows:             diag.ReduceRows,
		DecodedGranules:        diag.DecodedGranules,
		DecodedBlocks:          diag.DecodedBlocks,
		DecodedPayloadBytes:    diag.DecodedPayloadBytes,
		MappedBytes:            diag.MappedBytes,
		HeapCopyBytes:          diag.HeapCopyBytes,
		PredicateCount:         diag.PredicateCount,
		BytesRead:              diag.PhysicalBytesScanned,
		RowMaterializations:    diag.RowMaterializations,
		ResultCount:            resultCount,
		MetadataHits:           diag.MetadataHits,
		DictionaryCodeHits:     diag.DictionaryCodeHits,
		Int64ValueHits:         diag.Int64ValueHits,
		SkippedGranules:        diag.SkippedGranules,
		ScheduledGranules:      diag.ScheduledGranules,
		WorkerCount:            workers,
		SegmentFileCacheHits:   diag.SegmentFileCacheHits,
		SegmentFileCacheMisses: diag.SegmentFileCacheMisses,
		SetupDuration:          setupElapsed,
		HotRunDuration:         hotRunElapsed,
		ScanDuration:           scanDuration,
		ReduceDuration:         reduceDuration,
		AdapterDuration:        resultShapeDuration,
		ResultShapeDuration:    resultShapeDuration,
		ParityHashDuration:     parityHashElapsed,
	}, nil
}

func columnStoreSuitePhysicalQueryRequest(name string) (collections.ColumnPhysicalQueryRequest, error) {
	switch name {
	case columnStoreQueryQ1:
		return collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}, nil
	case columnStoreQueryQ2:
		return collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}, nil
	case columnStoreQueryQ3:
		return collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryHourCount, ValueColumn: "time_us"}, nil
	case columnStoreQueryQ4A:
		return collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"}, nil
	case columnStoreQueryQ4B:
		return collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"}, nil
	case columnStoreQueryQ5, columnStoreQueryQ5Metadata:
		return collections.ColumnPhysicalQueryRequest{Kind: collections.ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}, nil
	default:
		return collections.ColumnPhysicalQueryRequest{}, fmt.Errorf("column_store: unknown physical query %q", name)
	}
}

func columnStoreSuitePhysicalQueryLines(prefix, queryName string, groups []collections.ColumnPhysicalQueryGroup) ([]string, error) {
	lines := make([]string, 0, len(groups))
	switch queryName {
	case columnStoreQueryQ1, columnStoreQueryQ2, columnStoreQueryQ3:
		for _, group := range groups {
			lines = append(lines, columnStoreSuiteFormatPhysicalQueryLine(prefix, group.Key, int64(group.Count)))
		}
	case columnStoreQueryQ4A, columnStoreQueryQ4B, columnStoreQueryQ5, columnStoreQueryQ5Metadata:
		for _, group := range groups {
			lines = append(lines, columnStoreSuiteFormatPhysicalQueryLine(prefix, group.Key, group.Int64))
		}
	default:
		return nil, fmt.Errorf("column_store: unsupported physical query line mapping %q", queryName)
	}
	return lines, nil
}

// columnStoreSuiteHashPhysicalQueryGroups sorts groups in-place before hashing.
// The caller passes a result slice that is no longer used for ordered reporting
// after hashing; mutating it here avoids copying on the hot parity path.
func columnStoreSuiteHashPhysicalQueryGroups(prefix, queryName string, groups []collections.ColumnPhysicalQueryGroup) (uint64, int, error) {
	columnStoreSuiteSortPhysicalQueryGroups(queryName, groups)
	hash := columnStoreFNV64Offset
	switch queryName {
	case columnStoreQueryQ1, columnStoreQueryQ2, columnStoreQueryQ3:
		for _, group := range groups {
			hash = columnStoreHashPhysicalQueryGroup(hash, prefix, group.Key, int64(group.Count))
		}
	case columnStoreQueryQ4A, columnStoreQueryQ4B, columnStoreQueryQ5, columnStoreQueryQ5Metadata:
		for _, group := range groups {
			hash = columnStoreHashPhysicalQueryGroup(hash, prefix, group.Key, group.Int64)
		}
	default:
		return 0, 0, fmt.Errorf("column_store: unsupported physical query hash mapping %q", queryName)
	}
	return hash, len(groups), nil
}

func columnStoreSuiteSortPhysicalQueryGroups(queryName string, groups []collections.ColumnPhysicalQueryGroup) {
	for i := 1; i < len(groups); i++ {
		group := groups[i]
		j := i - 1
		for ; j >= 0 && columnStoreSuitePhysicalQueryGroupLess(queryName, group, groups[j]); j-- {
			groups[j+1] = groups[j]
		}
		groups[j+1] = group
	}
}

func columnStoreSuitePhysicalQueryGroupLess(queryName string, left, right collections.ColumnPhysicalQueryGroup) bool {
	leftValue, rightValue := left.Int64, right.Int64
	switch queryName {
	case columnStoreQueryQ1, columnStoreQueryQ2, columnStoreQueryQ3:
		leftValue, rightValue = int64(left.Count), int64(right.Count)
	case columnStoreQueryQ4A, columnStoreQueryQ4B, columnStoreQueryQ5, columnStoreQueryQ5Metadata:
	default:
		return false
	}
	if cmp := columnStoreSuitePhysicalQueryLineKeyPrefixCompare(left.Key, right.Key); cmp != 0 {
		return cmp < 0
	}
	var leftNum [columnStoreSuiteMaxInt64DecimalLen]byte
	var rightNum [columnStoreSuiteMaxInt64DecimalLen]byte
	return columnStoreSuiteBytesLess(
		strconv.AppendInt(leftNum[:0], leftValue, 10),
		strconv.AppendInt(rightNum[:0], rightValue, 10),
	)
}

func columnStoreSuitePhysicalQueryLineKeyPrefixCompare(leftKey, rightKey string) int {
	for idx := 0; ; idx++ {
		leftByte, leftOK := columnStoreSuitePhysicalQueryLineKeyPrefixByte(leftKey, idx)
		rightByte, rightOK := columnStoreSuitePhysicalQueryLineKeyPrefixByte(rightKey, idx)
		if !leftOK || !rightOK {
			if leftOK == rightOK {
				return 0
			}
			if leftOK {
				return 1
			}
			return -1
		}
		if leftByte != rightByte {
			if leftByte < rightByte {
				return -1
			}
			return 1
		}
	}
}

func columnStoreSuitePhysicalQueryLineKeyPrefixByte(key string, idx int) (byte, bool) {
	if idx < len(key) {
		return key[idx], true
	}
	if idx == len(key) {
		return '=', true
	}
	return 0, false
}

func columnStoreSuiteBytesLess(left, right []byte) bool {
	for idx := 0; ; idx++ {
		leftOK := idx < len(left)
		rightOK := idx < len(right)
		if !leftOK || !rightOK {
			return !leftOK && rightOK
		}
		if left[idx] != right[idx] {
			return left[idx] < right[idx]
		}
	}
}

func columnStoreHashPhysicalQueryGroup(hash uint64, prefix, key string, value int64) uint64 {
	var num [columnStoreSuiteMaxInt64DecimalLen]byte
	hash = columnStoreHashString(hash, prefix)
	hash = columnStoreHashByte(hash, ':')
	hash = columnStoreHashString(hash, key)
	hash = columnStoreHashByte(hash, '=')
	hash = columnStoreHashBytes(hash, strconv.AppendInt(num[:0], value, 10))
	return columnStoreHashByte(hash, 0)
}

func columnStoreSuiteFormatPhysicalQueryLine(prefix, key string, value int64) string {
	var b strings.Builder
	var num [columnStoreSuiteMaxInt64DecimalLen]byte
	b.Grow(len(prefix) + 1 + len(key) + 1 + len(num))
	b.WriteString(prefix)
	b.WriteByte(':')
	b.WriteString(key)
	b.WriteByte('=')
	b.Write(strconv.AppendInt(num[:0], value, 10))
	return b.String()
}

func scanColumnStoreSuiteEvents(collection *collections.Collection, rows int) ([]columnStoreDecodedEvent, int, int64, error) {
	events := make([]columnStoreDecodedEvent, 0, rows)
	var materialized int
	var bytesRead int64
	truncated, err := collection.ScanDocumentsFunc(rows+1, func(record collections.DocumentRecord) (bool, error) {
		var event columnStoreDecodedEvent
		if err := json.Unmarshal(record.Document, &event); err != nil {
			return false, err
		}
		events = append(events, event)
		materialized++
		bytesRead += int64(len(record.Document))
		return true, nil
	})
	if err != nil {
		return nil, 0, 0, fmt.Errorf("column_store: scan documents: %w", err)
	}
	if truncated {
		return nil, 0, 0, fmt.Errorf("column_store: scan exceeded expected rows=%d (sentinel limit=%d)", rows, rows+1)
	}
	if materialized != rows {
		return nil, 0, 0, fmt.Errorf("column_store: scanned %d rows, want %d", materialized, rows)
	}
	return events, materialized, bytesRead, nil
}

// scanColumnStoreSuiteEventsByIndex is an M11B B-tree baseline, not predicate
// pushdown. It scans one selected scalar secondary index fully in index order,
// materializes every source document, and relies on deterministic reduction
// hashing for parity. queryName is intentionally only diagnostic until M11C
// wires query predicates into bounded index/column scans.
func scanColumnStoreSuiteEventsByIndex(collection *collections.Collection, rows int, queryName, indexName string) ([]columnStoreDecodedEvent, int, int64, error) {
	if strings.TrimSpace(indexName) == "" {
		return nil, 0, 0, fmt.Errorf("column_store: no B-tree index selected for %s", queryName)
	}
	events := make([]columnStoreDecodedEvent, 0, rows)
	var materialized int
	var bytesRead int64
	// The M11B B-tree baseline intentionally performs a full ordered pass over
	// the planner-selected secondary index for parity. The selected index affects
	// the secondary structure traversed and write-amplification accounting, not
	// read selectivity; range pushdown is deferred to M11C. The collection index
	// implementation emits one secondary entry per document for these scalar
	// fields, so rows+1 is a sentinel limit: truncated catches the sentinel
	// overflow boundary and materialized != rows catches underruns.
	truncated, err := collection.ScanBorrowedDocumentsByIndexRange(indexName, collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Unbounded: true},
		Upper: collections.IndexRangeBound{Unbounded: true},
		Limit: rows + 1,
	}, func(record collections.BorrowedDocumentRecord) (bool, error) {
		var event columnStoreDecodedEvent
		// The decoded event must remain value-owned; do not add fields that
		// retain slices backed by BorrowedDocumentRecord.Document.
		if err := json.Unmarshal(record.Document, &event); err != nil {
			return false, err
		}
		events = append(events, event)
		materialized++
		bytesRead += int64(len(record.Document))
		return true, nil
	})
	if err != nil {
		return nil, 0, 0, fmt.Errorf("column_store: scan B-tree index %s for %s: %w", indexName, queryName, err)
	}
	if truncated {
		return nil, 0, 0, fmt.Errorf("column_store: B-tree index scan exceeded expected row count: expected exactly %d document entries, observed %d materialized rows at sentinel limit %d; scalar index should emit one secondary entry per document", rows, materialized, rows+1)
	}
	if materialized != rows {
		return nil, 0, 0, fmt.Errorf("column_store: B-tree index scan materialized %d rows, want %d", materialized, rows)
	}
	return events, materialized, bytesRead, nil
}

func columnStoreSuiteRetainedPayloadAccounting(events []columnStoreFixtureEvent, cfg *collections.ColumnStoreConfig, path string) (int64, string, error) {
	if cfg == nil || !cfg.Enabled || cfg.RetainedPayload == collections.ColumnRetainedPayloadFull {
		var bytesTotal int64
		for _, event := range events {
			bytesTotal += int64(len(event.Doc))
		}
		if path == columnStorePathBTreeIndexBaseline {
			return bytesTotal, "M13C b_tree_index_baseline keeps full row payload because retained-payload indexed writes remain fail-closed until secondary-index reconstruction is wired", nil
		}
		return bytesTotal, "full row payload retained", nil
	}
	var bytesTotal int64
	for _, event := range events {
		retained, err := columnStoreSuiteRetainedPayloadFromDocument(event.Doc, cfg)
		if err != nil {
			return 0, "", err
		}
		bytesTotal += int64(len(retained))
	}
	switch cfg.RetainedPayload {
	case collections.ColumnRetainedPayloadNonColumn:
		return bytesTotal, "M13C stores only non-column retained payload in the row lane; declared columns are reconstructed from physical column assets", nil
	case collections.ColumnRetainedPayloadNone:
		return bytesTotal, "M13C stores no row payload beyond an empty JSON object; declared columns are reconstructed from physical column assets", nil
	default:
		return 0, "", fmt.Errorf("unsupported retained-payload policy %q", cfg.RetainedPayload)
	}
}

func columnStoreSuiteRetainedPayloadFromDocument(document []byte, cfg *collections.ColumnStoreConfig) ([]byte, error) {
	if cfg == nil {
		return nil, errors.New("column_store: retained-payload transform requires column-store config")
	}
	return collections.ColumnRetainedPayloadFromJSONDocument(*cfg, document)
}

func buildColumnStoreJSONBenchCells(collection *collections.Collection, rows int, rawHashes map[string]uint64, forcedPath string, assetReadIntegrity collections.ColumnAssetReadIntegrity, queryNames []string, queries []columnStoreQueryMetric, cfg *collections.ColumnStoreConfig, retainedPayloadBytes int64) ([]columnStoreJSONBenchCell, error) {
	var reportErr error
	byQuery := make(map[string]columnStoreQueryMetric, len(queries))
	for _, q := range queries {
		byQuery[q.Name] = q
	}
	cells := make([]columnStoreJSONBenchCell, 0, len(queryNames)*2)
	for _, name := range queryNames {
		q, ok := byQuery[name]
		if !ok {
			return nil, fmt.Errorf("column_store: missing query metric for JSONBench cell %s", name)
		}
		cells = append(cells, columnStoreJSONBenchCellFromQueryMetric(q, cfg, retainedPayloadBytes))
		preparedKind, ok := columnStorePreparedCellPlanKind(q.PlanLabel)
		if !ok {
			continue
		}
		exec, err := executeColumnStoreSuitePreparedPhysicalQuery(collection, name, preparedKind, assetReadIntegrity)
		if err != nil {
			cells = append(cells, columnStoreJSONBenchUnavailablePreparedCell(name, q, exec, cfg, retainedPayloadBytes, err))
			continue
		}
		rawHash, ok := rawHashes[name]
		if !ok {
			rawHash = q.RawHash
		}
		cell := columnStoreJSONBenchCellFromPreparedExecution(name, rawHash, q, exec, preparedKind, cfg, retainedPayloadBytes)
		if err := columnStoreValidatePreparedJSONBenchCellParity(cell); err != nil {
			reportErr = errors.Join(reportErr, err)
		}
		cells = append(cells, cell)
	}
	return cells, reportErr
}

func columnStorePreparedCellPlanKind(planLabel string) (collections.ColumnQueryPlanKind, bool) {
	switch planLabel {
	case columnStorePathSerialColumnScan:
		return collections.ColumnQueryPlanSerialColumnScan, true
	case columnStorePathAggregateMetadata:
		return collections.ColumnQueryPlanAggregateMetadata, true
	default:
		return "", false
	}
}

func columnStoreJSONBenchCellFromQueryMetric(q columnStoreQueryMetric, cfg *collections.ColumnStoreConfig, retainedPayloadBytes int64) columnStoreJSONBenchCell {
	cell := columnStoreBaseJSONBenchCell(q.Name, q.PlanLabel, cfg, retainedPayloadBytes)
	if q.PlanLabel == columnStorePathRowStoreBaseline || q.PlanLabel == columnStorePathBTreeIndexBaseline {
		cell.CellLabel = columnStoreJSONBenchCellRowScan
		cell.ExecutionMode = columnStoreJSONBenchModeRowScan
	} else {
		cell.ExecutionMode = columnStoreJSONBenchModeDirect
		cell.CellLabel = columnStoreJSONBenchActualCellLabel(q.PlanLabel, cell.ExecutionMode, q.StorageSource, q.MetadataHits)
	}
	cell.AliasOf = q.AliasOf
	cell.StorageSource = q.StorageSource
	cell.FallbackReason = q.FallbackReason
	cell.MetadataDataScanPath = columnStoreJSONBenchActualScanPath(q.PlanLabel, q.StorageSource, q.MetadataHits)
	cell.CompressionMode = q.CompressionAttribution.CompressionPolicyLabel
	cell.RequestedCompression = q.CompressionAttribution.RequestedCompression
	cell.ActualCompression = q.CompressionAttribution.ActualCompression
	cell.RowCount = q.Rows
	cell.RowsProcessed = q.RowsProcessed
	cell.RowsProcessedKnown = q.RowsProcessedKnown
	cell.BytesRead = q.BytesRead
	cell.ResultCount = q.ResultCount
	cell.RawHash = q.RawHash
	cell.ResultHash = q.ProductionHash
	cell.ParityWithRowScan = q.RawHash == q.ProductionHash
	cell.ManifestRootName = q.ManifestRootName
	cell.ManifestRoot = q.ManifestRoot
	cell.ManifestGeneration = q.ManifestGeneration
	cell.ActiveManifestChecksum = q.ActiveManifestChecksum
	cell.PlannerDurationMS = q.PlannerDurationMS
	cell.HotRunDurationMS = durationMS(q.hotRunDuration)
	if cell.HotRunDurationMS == 0 {
		cell.HotRunDurationMS = q.ScanDurationMS + q.ReduceDurationMS + q.AdapterDurationMS
	}
	cell.ScanDurationMS = q.ScanDurationMS
	cell.ReduceDurationMS = q.ReduceDurationMS
	cell.ResultShapeDurationMS = q.AdapterDurationMS
	cell.ParityHashDurationMS = q.ParityHashDurationMS
	cell.MetadataHits = q.MetadataHits
	cell.RowsScanned = q.RowsScanned
	cell.RowsMatched = q.RowsMatched
	cell.ReduceRows = q.ReduceRows
	cell.DecodedGranules = q.DecodedGranules
	cell.SkippedGranules = q.SkippedGranules
	cell.ScheduledGranules = q.ScheduledGranules
	cell.CompatibilityStatus = "available"
	cell.CompressionAttribution = q.CompressionAttribution
	return cell
}

func columnStoreJSONBenchCellFromPreparedExecution(name string, rawHash uint64, direct columnStoreQueryMetric, exec columnStoreQueryExecution, planKind collections.ColumnQueryPlanKind, cfg *collections.ColumnStoreConfig, retainedPayloadBytes int64) columnStoreJSONBenchCell {
	planLabel := string(planKind)
	cell := columnStoreBaseJSONBenchCell(name, planLabel, cfg, retainedPayloadBytes)
	cell.ExecutionMode = columnStoreJSONBenchModePrepared
	cell.AliasOf = columnStoreQueryAliasOf(name, planLabel)
	cell.StorageSource = exec.StorageSource
	if cell.StorageSource == "" {
		cell.StorageSource = direct.StorageSource
	}
	cell.FallbackReason = exec.FallbackReason
	if cell.FallbackReason == "" {
		cell.FallbackReason = string(collections.ColumnPhysicalQueryFallbackNone)
	}
	cell.CellLabel = columnStoreJSONBenchActualCellLabel(planLabel, cell.ExecutionMode, cell.StorageSource, exec.MetadataHits)
	cell.MetadataDataScanPath = columnStoreJSONBenchActualScanPath(planLabel, cell.StorageSource, exec.MetadataHits)
	cell.CompressionAttribution = columnStoreQueryCompressionAttribution(planLabel, cell.StorageSource, cell.FallbackReason, exec.BytesRead)
	cell.CompressionMode = cell.CompressionAttribution.CompressionPolicyLabel
	cell.RequestedCompression = cell.CompressionAttribution.RequestedCompression
	cell.ActualCompression = cell.CompressionAttribution.ActualCompression
	cell.RowCount = direct.Rows
	cell.RowsProcessed = exec.RowsProcessed
	cell.RowsProcessedKnown = exec.RowsProcessedKnown
	cell.BytesRead = exec.BytesRead
	cell.ResultCount = exec.ResultCount
	cell.RawHash = rawHash
	cell.ResultHash = exec.ProductionHash
	cell.ParityWithRowScan = rawHash == exec.ProductionHash
	cell.ManifestRootName = exec.ManifestRootName
	if cell.ManifestRootName == "" {
		cell.ManifestRootName = direct.ManifestRootName
	}
	cell.ManifestRoot = exec.ManifestRoot
	if cell.ManifestRoot == 0 {
		cell.ManifestRoot = direct.ManifestRoot
	}
	cell.ManifestGeneration = exec.ManifestGeneration
	if cell.ManifestGeneration == 0 {
		cell.ManifestGeneration = direct.ManifestGeneration
	}
	cell.ActiveManifestChecksum = exec.ActiveManifestChecksum
	if cell.ActiveManifestChecksum == 0 {
		cell.ActiveManifestChecksum = direct.ActiveManifestChecksum
	}
	cell.PreparedSetupDurationMS = durationMS(exec.SetupDuration)
	cell.HotRunDurationMS = durationMS(exec.HotRunDuration)
	cell.ScanDurationMS = durationMS(exec.ScanDuration)
	cell.ReduceDurationMS = durationMS(exec.ReduceDuration)
	cell.ResultShapeDurationMS = durationMS(exec.ResultShapeDuration)
	cell.ParityHashDurationMS = durationMS(exec.ParityHashDuration)
	cell.MetadataHits = exec.MetadataHits
	cell.RowsScanned = exec.RowsScanned
	cell.RowsMatched = exec.RowsMatched
	cell.ReduceRows = exec.ReduceRows
	cell.DecodedGranules = exec.DecodedGranules
	cell.SkippedGranules = exec.SkippedGranules
	cell.ScheduledGranules = exec.ScheduledGranules
	cell.CompatibilityStatus = "available"
	return cell
}

func columnStoreValidatePreparedJSONBenchCellParity(cell columnStoreJSONBenchCell) error {
	if cell.ExecutionMode == columnStoreJSONBenchModePrepared && cell.CompatibilityStatus == "available" && !cell.ParityWithRowScan {
		return fmt.Errorf("column_store: prepared JSONBench cell %s/%s parity mismatch: raw_hash=%016x result_hash=%016x", cell.Query, cell.PlanLabel, cell.RawHash, cell.ResultHash)
	}
	return nil
}

func columnStoreJSONBenchUnavailablePreparedCell(name string, direct columnStoreQueryMetric, exec columnStoreQueryExecution, cfg *collections.ColumnStoreConfig, retainedPayloadBytes int64, err error) columnStoreJSONBenchCell {
	planLabel := direct.PlanLabel
	cell := columnStoreBaseJSONBenchCell(name, planLabel, cfg, retainedPayloadBytes)
	cell.ExecutionMode = columnStoreJSONBenchModePrepared
	cell.AliasOf = direct.AliasOf
	cell.StorageSource = exec.StorageSource
	if cell.StorageSource == "" {
		cell.StorageSource = direct.StorageSource
	}
	cell.FallbackReason = exec.FallbackReason
	if cell.FallbackReason == "" {
		cell.FallbackReason = direct.FallbackReason
	}
	metadataHits := exec.MetadataHits
	if metadataHits == 0 {
		metadataHits = direct.MetadataHits
	}
	cell.CellLabel = columnStoreJSONBenchActualCellLabel(planLabel, cell.ExecutionMode, cell.StorageSource, metadataHits)
	cell.MetadataDataScanPath = columnStoreJSONBenchActualScanPath(planLabel, cell.StorageSource, metadataHits)
	cell.PreparedSetupDurationMS = durationMS(exec.SetupDuration)
	cell.HotRunDurationMS = durationMS(exec.HotRunDuration)
	cell.RowCount = direct.Rows
	cell.RawHash = direct.RawHash
	cell.ResultHash = 0
	cell.ParityWithRowScan = false
	cell.ManifestRootName = direct.ManifestRootName
	cell.ManifestRoot = direct.ManifestRoot
	cell.ManifestGeneration = direct.ManifestGeneration
	cell.ActiveManifestChecksum = direct.ActiveManifestChecksum
	cell.CompressionAttribution = direct.CompressionAttribution
	cell.CompressionMode = direct.CompressionAttribution.CompressionPolicyLabel
	cell.RequestedCompression = direct.CompressionAttribution.RequestedCompression
	cell.ActualCompression = direct.CompressionAttribution.ActualCompression
	cell.CompatibilityStatus = "unavailable"
	cell.CompatibilityStatusReason = err.Error()
	return cell
}

func columnStoreBaseJSONBenchCell(query, planLabel string, cfg *collections.ColumnStoreConfig, retainedPayloadBytes int64) columnStoreJSONBenchCell {
	retainedEncoding, retainedEncodingStatus := collections.ColumnRetainedPayloadEncodingStatus(cfg)
	return columnStoreJSONBenchCell{
		Query:                         query,
		SortLayout:                    columnStoreSortLayoutLabel(cfg),
		SortKey:                       columnStoreSortKeyLabels(cfg),
		PlanLabel:                     planLabel,
		MutationMode:                  columnStoreJSONBenchMutationInsertOnlyReopen,
		RetainedPayloadPolicy:         columnStoreRetainedPayloadPolicyLabel(cfg),
		RetainedPayloadEncoding:       retainedEncoding,
		RetainedPayloadEncodingStatus: retainedEncodingStatus,
		RetainedPayloadBytes:          retainedPayloadBytes,
		TypedStorageOwner:             columnStoreTypedStorageOwnerSummary(cfg),
		TypedStorageOwnerColumns:      columnStoreTypedStorageOwnerColumns(cfg),
		PredicateMode:                 columnStorePredicateModeLabel(query),
		RealPredicates:                columnStoreQueryUsesRealPredicates(query),
		RetainedPayloadPolicyCaveat:   columnStoreRetainedPayloadCaveat(cfg),
		ReconstructionStatus:          columnStoreReconstructionStatus(cfg),
		FullDataCell:                  false,
		FullDataCaveat:                columnStoreJSONBenchFullDataCaveat,
		StorageAccountingCaveat:       columnStoreJSONBenchStorageCaveat,
	}
}

func columnStoreJSONBenchCellLabel(planLabel, executionMode string) string {
	if executionMode == columnStoreJSONBenchModePrepared {
		if planLabel == columnStorePathAggregateMetadata {
			return columnStoreJSONBenchCellColumnPreparedMetadata
		}
		return columnStoreJSONBenchCellColumnPrepared
	}
	if planLabel == columnStorePathRowStoreBaseline || planLabel == columnStorePathBTreeIndexBaseline {
		return columnStoreJSONBenchCellRowScan
	}
	if planLabel == columnStorePathAggregateMetadata {
		return columnStoreJSONBenchCellColumnDirectMetadata
	}
	return columnStoreJSONBenchCellColumnDirect
}

func columnStoreJSONBenchActualCellLabel(planLabel, executionMode, storageSource string, metadataHits int) string {
	if columnStoreJSONBenchActualScanPath(planLabel, storageSource, metadataHits) == columnStoreJSONBenchScanPathMetadata {
		if executionMode == columnStoreJSONBenchModePrepared {
			return columnStoreJSONBenchCellColumnPreparedMetadata
		}
		return columnStoreJSONBenchCellColumnDirectMetadata
	}
	if executionMode == columnStoreJSONBenchModePrepared {
		return columnStoreJSONBenchCellColumnPrepared
	}
	return columnStoreJSONBenchCellLabel(planLabel, executionMode)
}

func columnStoreJSONBenchActualScanPath(planLabel, storageSource string, metadataHits int) string {
	if planLabel == columnStorePathAggregateMetadata && storageSource == string(collections.ColumnPhysicalQueryStorageSourceAggregateMetadata) && metadataHits > 0 {
		return columnStoreJSONBenchScanPathMetadata
	}
	return columnStoreJSONBenchScanPathData
}

func columnStoreSortKeyLabels(cfg *collections.ColumnStoreConfig) []string {
	if cfg == nil || len(cfg.SortKey) == 0 {
		return []string{"__treedb_primary_id"}
	}
	out := make([]string, 0, len(cfg.SortKey))
	for _, key := range cfg.SortKey {
		if key.Column == "" {
			continue
		}
		out = append(out, key.Column)
	}
	if len(out) == 0 {
		return []string{"__treedb_primary_id"}
	}
	return out
}

func columnStoreSortLayoutLabel(cfg *collections.ColumnStoreConfig) string {
	keys := columnStoreSortKeyLabels(cfg)
	if len(keys) == 1 && keys[0] == "__treedb_primary_id" {
		return "typed-column-primary-id-control"
	}
	allTyped := columnStoreSortKeyAllTypedColumnOwned(cfg, keys)
	joined := strings.Join(keys, ",")
	if joined == "time_us" {
		if allTyped {
			return "typed-column-time"
		}
		return "declared-time_us-row-asset-compatibility"
	}
	if joined == "kind,operation,collection,did,time_us" {
		if allTyped {
			return "typed-column-filter-user-time"
		}
		return "declared-filter-user-time-mixed-owner-compatibility"
	}
	if allTyped {
		return "typed-column-sortkey(" + joined + ")"
	}
	return "declared-sortkey-mixed-owner(" + joined + ")"
}

func columnStoreSortKeyAllTypedColumnOwned(cfg *collections.ColumnStoreConfig, keys []string) bool {
	if cfg == nil || len(keys) == 0 {
		return false
	}
	owners := make(map[string]collections.TypedStorageFieldOwner, len(cfg.Columns))
	for _, col := range cfg.Columns {
		owners[col.Name] = columnStoreColumnOwnerLabelValue(col)
	}
	for _, key := range keys {
		if owners[key] != collections.TypedStorageOwnerColumnPart {
			return false
		}
	}
	return true
}

func columnStoreRetainedPayloadPolicyLabel(cfg *collections.ColumnStoreConfig) string {
	if cfg == nil || cfg.RetainedPayload == "" {
		return string(collections.ColumnRetainedPayloadFull)
	}
	return string(cfg.RetainedPayload)
}

func columnStoreRetainedPayloadCaveat(cfg *collections.ColumnStoreConfig) string {
	switch columnStoreRetainedPayloadPolicyLabel(cfg) {
	case string(collections.ColumnRetainedPayloadFull):
		return "full source document retained for this gomap synthetic cell"
	case string(collections.ColumnRetainedPayloadNonColumn):
		return "declared columns are stripped into column assets; retained payload is query-shaped and not a full-data JSONBench row"
	case string(collections.ColumnRetainedPayloadNone):
		return "no source JSON payload retained; attribution-only query-shaped cell"
	default:
		return columnStoreJSONBenchSyntheticFixtureCaveat
	}
}

func columnStoreReconstructionStatus(cfg *collections.ColumnStoreConfig) string {
	if cfg == nil || cfg.Reconstruction == "" {
		return "not_configured"
	}
	return fmt.Sprintf("configured_%s; #2117 full JSONBench retained-document reconstruction parity is implemented in the external snissn/JSONBench TreeDB harness", cfg.Reconstruction)
}

func columnStoreColumnOwnerLabelValue(col collections.ColumnStoreColumn) collections.TypedStorageFieldOwner {
	if col.Owner == "" {
		return collections.TypedStorageOwnerRowAsset
	}
	return col.Owner
}

func columnStoreTypedStorageOwnerColumns(cfg *collections.ColumnStoreConfig) []columnStoreTypedOwnerColumn {
	if cfg == nil {
		return nil
	}
	out := make([]columnStoreTypedOwnerColumn, 0, len(cfg.Columns))
	for _, col := range cfg.Columns {
		out = append(out, columnStoreTypedOwnerColumn{Name: col.Name, Owner: string(columnStoreColumnOwnerLabelValue(col))})
	}
	return out
}

func columnStoreTypedStorageOwnerSummary(cfg *collections.ColumnStoreConfig) string {
	owners := columnStoreTypedStorageOwnerColumns(cfg)
	if len(owners) == 0 {
		return "unknown"
	}
	first := owners[0].Owner
	for _, owner := range owners[1:] {
		if owner.Owner != first {
			return "mixed"
		}
	}
	return first
}

func columnStorePredicateModeLabel(query string) string {
	switch query {
	case columnStoreQueryQ2, columnStoreQueryQ3, columnStoreQueryQ4A, columnStoreQueryQ4B, columnStoreQueryQ5, columnStoreQueryQ5Metadata:
		return "synthetic_projection_fixture_no_jsonbench_kind_operation_collection_predicates; TreeDB/collections parity tests cover real predicate q1-q5 semantics"
	default:
		return "predicate_free"
	}
}

func columnStoreQueryUsesRealPredicates(query string) bool {
	// The unified_bench synthetic fixture currently validates production query
	// modes and result hashes, not the external JSONBench predicate-bearing input
	// shape. TreeDB/collections parity tests cover real predicate q1-q5 semantics.
	return false
}

func columnStoreReportCaveats() []string {
	return []string{
		columnStoreJSONBenchSyntheticFixtureCaveat,
		columnStoreJSONBenchFullDataCaveat,
		columnStoreJSONBenchStorageCaveat,
		"external snissn/JSONBench full-data and comparison cells are implemented separately and must be rerun against the selected gomap dependency before headline ClickHouse claims",
		"current production default compression remains none; benchmark-relaxed locator section compression is opt-in only; dictionary/pruning section compression remains deferred; zstd/zstd_dict rows are unsupported/deferred report rows only",
	}
}

func columnStoreColgranuleReuseMap() []columnStoreColgranuleReuse {
	return []columnStoreColgranuleReuse{
		{Source: "experiments/colgranule/jsonbench_split_document.go", ProductionTarget: "TreeDB/collections ColumnRetainedPayloadFromJSONDocument plus cmd/unified_bench synthetic fixture", Decision: "adapted", DivergenceReason: "gomap suite uses generated synthetic documents instead of external JSONBench input", Evidence: "retained_payload_bytes, retained_payload_policy, and full_data_caveat are reported per cell"},
		{Source: "experiments/colgranule/jsonbench_part_queries.go", ProductionTarget: "TreeDB/collections RunColumnPhysicalQuery/PrepareColumnPhysicalQuery and cmd/unified_bench query matrix", Decision: "adapted", DivergenceReason: "production APIs own direct/prepared execution; experiments remain reference only", Evidence: "jsonbench_cells record direct/prepared mode, scan path, storage source, hashes, rows, and timing fields"},
		{Source: "experiments/colgranule/jsonbench_report.go", ProductionTarget: "cmd/unified_bench/column_store_results.{json,md,html}", Decision: "adapted", DivergenceReason: "unified_bench keeps native TreeDB artifact names and benchprof integration", Evidence: "report shape tests require query, sort layout, source, mode, compression, mutation, owner, row count, and caveat labels"},
		{Source: "experiments/colgranule/jsonbench_part_build_report.go", ProductionTarget: "cmd/unified_bench byte_accounting and codec_layouts", Decision: "adapted", DivergenceReason: "#2118 implements external apples-to-apples full storage accounting; gomap synthetic reports remain local smoke evidence", Evidence: "storage_accounting_caveat, column_asset_bytes, retained_payload_bytes, codec_layouts, and compression_attribution remain machine-readable"},
		{Source: "experiments/colgranule/part_accounting.go and part_image.go", ProductionTarget: "TreeDB/collections ColumnStorePhysicalAccounting plus codec/layout matrix", Decision: "ported", DivergenceReason: "production accounting comes from persisted column assets and typed_column_part details", Evidence: "typed_column_part codec rows, section byte-target rows, and unsupported zstd rows surface when accounting is available"},
		{Source: "experiments/colgranule/granule.go admitCompressionInto keep-if-smaller", ProductionTarget: "TreeDB/internal/typedcolumn row locator section compression", Decision: "adapted", DivergenceReason: "section compression is limited to locator sections because their raw length is recoverable from existing manifest rows without a format bump", Evidence: "snappy/lz4 benchmark-relaxed locator rows report actual section compression; dictionaries/pruning_metadata stay deferred byte targets"},
	}
}

var columnStoreQueryNameList = [...]string{
	columnStoreQueryQ1,
	columnStoreQueryQ2,
	columnStoreQueryQ3,
	columnStoreQueryQ4A,
	columnStoreQueryQ4B,
	columnStoreQueryQ5,
	columnStoreQueryQ5Metadata,
}

func columnStoreQueryNames() []string {
	return append([]string(nil), columnStoreQueryNameList[:]...)
}

func columnStoreSuiteEffectiveQueryNames(explicit []string, flagValue string) ([]string, error) {
	if len(explicit) > 0 {
		return columnStoreSuiteNormalizeQueryNames(explicit)
	}
	return columnStoreSuiteParseQueryNames(flagValue)
}

func columnStoreSuiteParseQueryNames(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "all") {
		return columnStoreQueryNames(), nil
	}
	return columnStoreSuiteNormalizeQueryNames(strings.Split(value, ","))
}

func columnStoreSuiteNormalizeQueryNames(names []string) ([]string, error) {
	if len(names) == 0 {
		return columnStoreQueryNames(), nil
	}
	out := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, raw := range names {
		name := strings.ToLower(strings.TrimSpace(raw))
		if name == "" {
			return nil, errors.New("column_store: empty query name")
		}
		if name == "all" {
			if len(names) == 1 {
				return columnStoreQueryNames(), nil
			}
			return nil, errors.New("column_store: all cannot be combined with explicit query names")
		}
		if !columnStoreQueryNameKnown(name) {
			return nil, fmt.Errorf("column_store: unknown query name %q; accepted=%s", name, strings.Join(columnStoreQueryNames(), ","))
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("column_store: duplicate query name %q", name)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

func columnStoreQueryNameKnown(name string) bool {
	for _, candidate := range columnStoreQueryNameList {
		if name == candidate {
			return true
		}
	}
	return false
}

func columnStoreQueryNameSelected(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

func columnStoreQueryAliasOf(name, path string) string {
	if name == columnStoreQueryQ5Metadata {
		return columnStoreQueryQ5
	}
	return ""
}

func columnStoreQueryImplementationNote(name, requestedPath, planPath string) string {
	if requestedPath == columnStorePathAggregateMetadata && planPath == columnStorePathSerialColumnScan && columnStoreSuiteAggregateMetadataName(name) == "" {
		return "aggregate_metadata_forced_path_rerouted_to_serial_column_scan_no_metadata_asset_for_query_m14b"
	}
	if name == columnStoreQueryQ4B && planPath == columnStorePathAggregateMetadata {
		return "q4b_physical_aggregate_metadata_asset_fast_path"
	}
	if name == columnStoreQueryQ5Metadata && planPath == columnStorePathAggregateMetadata {
		return "q5_metadata_physical_aggregate_metadata_asset_fast_path"
	}
	if name == columnStoreQueryQ5Metadata && (planPath == columnStorePathSerialColumnScan || planPath == columnStorePathParallelColumnScan) {
		return planPath + "_q5_alias_physical_column_scan"
	}
	if name == columnStoreQueryQ5Metadata && planPath == columnStorePathBTreeIndexBaseline {
		return "q5_alias_full_unbounded_secondary_index_scan_no_predicate_pushdown"
	}
	if name == columnStoreQueryQ5Metadata && planPath == columnStorePathRowStoreBaseline {
		return planPath + "_q5_alias_row_materialization_baseline"
	}
	if planPath == columnStorePathBTreeIndexBaseline {
		return "full_unbounded_secondary_index_scan_no_predicate_pushdown_m11b"
	}
	return ""
}

func columnStoreQueryThroughputInterpretation(q columnStoreQueryMetric) string {
	markPruning := "mark-pruning not active"
	if q.SkippedGranules > 0 {
		markPruning = "mark-pruning active"
	}
	evidence := columnStoreQueryInterpretationEvidence(q)
	switch q.PlanLabel {
	case columnStorePathRowStoreBaseline:
		return "decode-bound row-store baseline: full JSON row materialization before reduction; " + markPruning + evidence
	case columnStorePathBTreeIndexBaseline:
		return "decode-bound B-tree baseline: full unbounded secondary-index walk plus JSON row materialization before reduction; " + markPruning + evidence
	case columnStorePathSerialColumnScan:
		if q.DictionaryCodeHits > 0 && q.Int64ValueHits > 0 {
			return fmt.Sprintf("dictionary-code and int64-value sidecar serial path: dictionary_hits=%d int64_hits=%d avoid full TCPA row-record scan for covered declared columns; setup/read/decode still occurs in this routed measurement; %s%s", q.DictionaryCodeHits, q.Int64ValueHits, markPruning, evidence)
		}
		if q.DictionaryCodeHits > 0 {
			return fmt.Sprintf("dictionary-code sidecar serial path: %d sidecar hits avoid full TCPA row-record scan for declared dictionary columns; setup/read/decode still occurs in this routed measurement; %s%s", q.DictionaryCodeHits, markPruning, evidence)
		}
		if q.Int64ValueHits > 0 {
			return fmt.Sprintf("int64-value sidecar serial path: %d sidecar hits avoid full TCPA row-record scan for declared int64 columns; setup/read/decode still occurs in this routed measurement; %s%s", q.Int64ValueHits, markPruning, evidence)
		}
		return "physical serial scan: TCPA decode plus reducer aggregation over declared columns; memory-bandwidth bound on asset bytes when cache-warm; " + markPruning + evidence
	case columnStorePathAggregateMetadata:
		if q.MetadataHits > 0 {
			return fmt.Sprintf("metadata-bound aggregate metadata path: %d metadata hits avoid full physical row scan; %s%s", q.MetadataHits, markPruning, evidence)
		}
		return "fallback-bound aggregate metadata label: no metadata hits reported, so evidence must be treated as a physical scan/reroute rather than the metadata-asset fast path; " + markPruning + evidence
	case columnStorePathParallelColumnScan:
		if q.WorkerCount <= 0 {
			return fmt.Sprintf("parallel physical scan: invalid reported worker_count=%d; overhead-bound interpretation is unavailable until worker diagnostics are valid; %s%s", q.WorkerCount, markPruning, evidence)
		}
		return fmt.Sprintf("parallel physical scan: manifest-ref partition across reported worker_count=%d; overhead-bound on small fixtures and memory-bandwidth/TCPA-decode bound on larger asset bytes; %s%s", q.WorkerCount, markPruning, evidence)
	default:
		return fmt.Sprintf("fallback/error-bound: unknown executed plan label %q for query %q; %s%s", q.PlanLabel, q.Name, markPruning, evidence)
	}
}

func columnStoreQueryInterpretationEvidence(q columnStoreQueryMetric) string {
	rowsProcessed, rowsProcessedOK := columnStoreQueryEffectiveRowsProcessed(q)
	rowDenominator := q.Rows
	if rowDenominator <= 0 {
		rowDenominator = rowsProcessed
	}
	rowMaterializations := fmt.Sprintf("%d/unknown", q.RowMaterializations)
	if rowDenominator > 0 {
		rowMaterializations = fmt.Sprintf("%d/%d", q.RowMaterializations, rowDenominator)
	}
	rowsProcessedText := "unknown"
	if rowsProcessedOK {
		rowsProcessedText = strconv.Itoa(rowsProcessed)
	}
	return fmt.Sprintf("; effective_rows_processed=%s row_materializations=%s bytes_read=%d metadata_hits=%d dictionary_code_hits=%d int64_value_hits=%d scheduled_granules=%d skipped_granules=%d", rowsProcessedText, rowMaterializations, q.BytesRead, q.MetadataHits, q.DictionaryCodeHits, q.Int64ValueHits, q.ScheduledGranules, q.SkippedGranules)
}

func populateColumnStoreThroughputInterpretations(queries []columnStoreQueryMetric) {
	for i := range queries {
		if strings.TrimSpace(queries[i].ThroughputInterpretation) == "" {
			queries[i].ThroughputInterpretation = columnStoreQueryThroughputInterpretation(queries[i])
		}
	}
}

func columnStoreQueryHash(name string, events []columnStoreDecodedEvent) (uint64, int, error) {
	lines, err := columnStoreQueryLines(columnStoreQueryHashLineName(name), events)
	if err != nil {
		return 0, 0, err
	}
	// ResultCount is the reduced result row count; the parity hash covers the
	// same reduced rows after deterministic ordering.
	return columnStoreHashLines(lines), len(lines), nil
}

func columnStoreHashLines(lines []string) uint64 {
	sort.Strings(lines)
	hash := columnStoreFNV64Offset
	for _, line := range lines {
		hash = columnStoreHashString(hash, line)
		hash = columnStoreHashByte(hash, 0)
	}
	return hash
}

const (
	columnStoreFNV64Offset uint64 = 14695981039346656037
	columnStoreFNV64Prime  uint64 = 1099511628211
)

func columnStoreHashString(hash uint64, value string) uint64 {
	for i := 0; i < len(value); i++ {
		hash = columnStoreHashByte(hash, value[i])
	}
	return hash
}

func columnStoreHashBytes(hash uint64, value []byte) uint64 {
	for _, b := range value {
		hash = columnStoreHashByte(hash, b)
	}
	return hash
}

func columnStoreHashByte(hash uint64, value byte) uint64 {
	hash ^= uint64(value)
	return hash * columnStoreFNV64Prime
}

func columnStoreQueryHashLineName(name string) string {
	// q5_metadata is an execution/reporting label until the physical metadata
	// path exists; parity hashes use q5's logical result lines so alias
	// equivalence is directly testable.
	if name == columnStoreQueryQ5Metadata {
		return columnStoreQueryQ5
	}
	return name
}

func columnStoreSuiteUTCHour(timeUS int64) int {
	const hourUS = int64(3_600_000_000)
	hours := timeUS / hourUS
	if timeUS < 0 && timeUS%hourUS != 0 {
		hours--
	}
	hour := int(hours % 24)
	if hour < 0 {
		hour += 24
	}
	return hour
}

func columnStoreSuiteHourKey(hour int) string {
	if hour < 0 || hour >= 24 {
		return "hour_invalid"
	}
	return fmt.Sprintf("hour_%02d", hour)
}

func columnStoreQueryLines(name string, events []columnStoreDecodedEvent) ([]string, error) {
	switch name {
	case columnStoreQueryQ1:
		counts := make(map[string]int)
		for _, event := range events {
			counts[event.Kind]++
		}
		return formatIntMapLines(name, counts), nil
	case columnStoreQueryQ2:
		distinct := make(map[string]map[string]struct{})
		for _, event := range events {
			set := distinct[event.Kind]
			if set == nil {
				set = make(map[string]struct{})
				distinct[event.Kind] = set
			}
			set[event.Did] = struct{}{}
		}
		counts := make(map[string]int)
		for kind, set := range distinct {
			counts[kind] = len(set)
		}
		return formatIntMapLines(name, counts), nil
	case columnStoreQueryQ3:
		counts := make(map[string]int)
		for _, event := range events {
			counts[columnStoreSuiteHourKey(columnStoreSuiteUTCHour(event.TimeUS))]++
		}
		return formatIntMapLines(name, counts), nil
	case columnStoreQueryQ4A:
		mins := make(map[string]int64)
		for _, event := range events {
			if cur, ok := mins[event.Did]; !ok || event.TimeUS < cur {
				mins[event.Did] = event.TimeUS
			}
		}
		return formatInt64MapLines(name, mins), nil
	case columnStoreQueryQ4B:
		maxs := make(map[string]int64)
		for _, event := range events {
			if cur, ok := maxs[event.Did]; !ok || event.TimeUS > cur {
				maxs[event.Did] = event.TimeUS
			}
		}
		return formatInt64MapLines(name, maxs), nil
	case columnStoreQueryQ5:
		type span struct {
			min int64
			max int64
		}
		spans := make(map[string]span)
		for _, event := range events {
			cur, ok := spans[event.Did]
			if !ok {
				spans[event.Did] = span{min: event.TimeUS, max: event.TimeUS}
				continue
			}
			if event.TimeUS < cur.min {
				cur.min = event.TimeUS
			}
			if event.TimeUS > cur.max {
				cur.max = event.TimeUS
			}
			spans[event.Did] = cur
		}
		lines := make([]string, 0, len(spans))
		for did, sp := range spans {
			lines = append(lines, fmt.Sprintf("%s:%s=%d", name, did, sp.max-sp.min))
		}
		return lines, nil
	default:
		return nil, fmt.Errorf("unknown column_store query %q", name)
	}
}

func formatIntMapLines(prefix string, values map[string]int) []string {
	lines := make([]string, 0, len(values))
	for key, value := range values {
		lines = append(lines, fmt.Sprintf("%s:%s=%d", prefix, key, value))
	}
	return lines
}

func formatInt64MapLines(prefix string, values map[string]int64) []string {
	lines := make([]string, 0, len(values))
	for key, value := range values {
		lines = append(lines, fmt.Sprintf("%s:%s=%d", prefix, key, value))
	}
	return lines
}

func columnStoreStage(name string, start time.Time, rows int, bytes int64) columnStoreStageMetric {
	return columnStoreStageFromDuration(name, time.Since(start), rows, bytes)
}

func columnStoreStageFromDuration(name string, elapsed time.Duration, rows int, bytes int64) columnStoreStageMetric {
	return columnStoreStageMetric{
		Name:          name,
		DurationMS:    durationMS(elapsed),
		Rows:          rows,
		RowsPerSecond: ratePerSecond(float64(rows), elapsed),
		Bytes:         bytes,
		MiBPerSecond:  ratePerSecond(float64(bytes)/(1024*1024), elapsed),
	}
}

func durationMS(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func ratePerSecond(v float64, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return v / d.Seconds()
}

func nsPerRow(d time.Duration, rows int) float64 {
	if rows <= 0 {
		return math.NaN()
	}
	return float64(d.Nanoseconds()) / float64(rows)
}

func columnStoreSuiteDirUsage(root string) (int64, int, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return 0, 0, fmt.Errorf("empty path")
	}
	var bytes int64
	var files int
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry == nil || entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return statErr
		}
		bytes += info.Size()
		files++
		return nil
	}); err != nil {
		return 0, 0, err
	}
	return bytes, files, nil
}

func columnStoreSuiteCommandWALLogBytes(root string) (int64, error) {
	walRoot := backenddb.WALDirPath(root)
	entries, err := os.ReadDir(walRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var bytes int64
	for _, entry := range entries {
		if entry == nil || entry.IsDir() || !columnStoreSuiteCommandWALLogName(entry.Name()) {
			continue
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return 0, statErr
		}
		if !info.Mode().IsRegular() {
			continue
		}
		bytes += info.Size()
	}
	return bytes, nil
}

func columnStoreSuiteCommandWALLogName(name string) bool {
	if !strings.HasPrefix(name, "commit-l") || !strings.HasSuffix(name, ".log") {
		return false
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, "commit-l"), ".log")
	parts := strings.SplitN(rest, "-", 2)
	if len(parts) != 2 {
		return false
	}
	if !columnStoreSuiteDecimalDigits(parts[0]) || !columnStoreSuiteDecimalDigits(parts[1]) {
		return false
	}
	lane, err := strconv.Atoi(parts[0])
	if err != nil || lane < 0 {
		return false
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	return err == nil && seq != 0
}

func columnStoreSuiteDecimalDigits(value string) bool {
	if value == "" {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func columnStoreSuiteDurableStorageBytesWALExcluded(totalBytes, walBytes int64) int64 {
	if totalBytes <= 0 || walBytes >= totalBytes {
		return 0
	}
	if walBytes <= 0 {
		return totalBytes
	}
	return totalBytes - walBytes
}

func columnStoreSuiteColumnAssetUsage(root string) (int64, error) {
	assetRoot := backenddb.ColumnAssetRootDirPath(root)
	return columnStoreSuiteOptionalDirBytes(assetRoot)
}

func columnStoreSuiteOptionalDirBytes(root string) (int64, error) {
	bytes, _, err := columnStoreSuiteDirUsage(root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return bytes, nil
}

func columnStoreSuiteOptionalFileBytes(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	if info.IsDir() {
		return 0, fmt.Errorf("%s is a directory", path)
	}
	return info.Size(), nil
}

func columnStoreSuiteManifestControlUsage(root string) (int64, []string, error) {
	var total int64
	var missing []string
	for _, rel := range columnStoreSuiteManifestControlFiles {
		path := filepath.Join(root, rel)
		info, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				missing = append(missing, rel)
				continue
			}
			return 0, nil, fmt.Errorf("%s: %w", rel, err)
		}
		if info.IsDir() {
			return 0, nil, fmt.Errorf("%s is a directory", rel)
		}
		total += info.Size()
	}
	return total, missing, nil
}

func columnStoreCodecLayoutMetrics(rows, columns int, physicalBytes int64, accounting collections.ColumnStorePhysicalAccounting) []columnStoreCodecLayoutMetric {
	out := columnStoreBaselineCodecLayoutMetrics(rows, columns, physicalBytes)
	for _, part := range accounting.TypedColumnParts {
		for _, detail := range part.Image.CompressionDetail {
			out = append(out, columnStoreCodecLayoutMetric{
				columnStoreCompressionAttribution: columnStoreCompressionAttribution{
					CodecLayoutLabel:            "typed_column_part/" + detail.Column + "/" + detail.Encoding,
					CompressionPolicyLabel:      columnStoreCompressionPolicyLabel(detail.RequestedCompression),
					RequestedCompression:        detail.RequestedCompression,
					ActualCompression:           detail.ActualCompression,
					SupportState:                columnStoreCompressionSupportSupported,
					SupportReason:               detail.FallbackReason,
					CompressedBytes:             detail.StoredBytes,
					CompressedBytesSource:       "typed_column_part_byte_accounting.compression_detail.stored_bytes",
					RawBytes:                    detail.EncodedRawBytes,
					RawBytesSource:              "typed_column_part_byte_accounting.compression_detail.encoded_raw_bytes",
					DecompressedBytes:           detail.EncodedRawBytes,
					DecompressedBytesSource:     "typed_column_part_byte_accounting.compression_detail.encoded_raw_bytes",
					CompressionRatio:            columnStoreCompressionRatio(detail.StoredBytes, detail.EncodedRawBytes),
					CompressionRatioSource:      "typed_column_part_compressed_bytes/encoded_raw_bytes",
					CompressionDurationMS:       float64(detail.CompressionNanos) / float64(time.Millisecond),
					CompressionDurationSource:   "typed_column_part_codec_report_compression_nanos",
					DecompressionDurationMS:     0,
					DecompressionDurationSource: "not_measured_in_column_store_suite_accounting; query rows report scan/reduce timings separately",
					BenchmarkBPerOp:             0,
					BenchmarkAllocsPerOp:        0,
					BenchmarkAllocationSource:   columnStoreBenchmarkAllocationSource(),
				},
				Rows:    part.Image.Rows,
				Columns: part.Image.Columns,
			})
		}
		for _, section := range part.Image.SerializedSections {
			if !columnStoreTypedColumnPartSectionCodecTarget(section) {
				continue
			}
			out = append(out, columnStoreCodecLayoutMetric{
				columnStoreCompressionAttribution: columnStoreSectionCompressionAttribution(section),
				Rows:                              part.Image.Rows,
				Columns:                           part.Image.Columns,
			})
		}
	}
	return out
}

func columnStoreBaselineCodecLayoutMetrics(rows, columns int, physicalBytes int64) []columnStoreCodecLayoutMetric {
	out := []columnStoreCodecLayoutMetric{
		{
			columnStoreCompressionAttribution: columnStoreSuiteCompressionAttribution(
				columnStoreCompressionPolicyOff,
				physicalBytes,
			),
			Rows:    rows,
			Columns: columns,
		},
		{
			columnStoreCompressionAttribution: columnStoreSuiteCompressionAttribution(
				columnStoreCompressionPolicyDefault,
				physicalBytes,
			),
			Rows:    rows,
			Columns: columns,
		},
	}
	out = append(out,
		columnStoreUnsupportedCodecLayoutMetric(rows, columns, "zstd", "zstd production encode/decode is deferred; enum names are rejected by typedcolumn/production validation"),
		columnStoreUnsupportedCodecLayoutMetric(rows, columns, "zstd_dict", "zstd dictionary production encode/decode/dictionary training is deferred; enum name is rejected by typedcolumn/production validation"),
	)
	return out
}

func columnStoreUnsupportedCodecLayoutMetric(rows, columns int, compression string, reason string) columnStoreCodecLayoutMetric {
	return columnStoreCodecLayoutMetric{
		columnStoreCompressionAttribution: columnStoreCompressionAttribution{
			CodecLayoutLabel:            columnStoreCodecLayoutCurrent,
			CompressionPolicyLabel:      "requested_" + compression,
			RequestedCompression:        compression,
			ActualCompression:           columnStoreCompressionNoneLabel,
			SupportState:                columnStoreCompressionSupportUnsupported,
			SupportReason:               reason,
			CompressedBytes:             0,
			CompressedBytesSource:       "unsupported_codec_not_run",
			RawBytes:                    0,
			RawBytesSource:              "unsupported_codec_not_run",
			DecompressedBytes:           0,
			DecompressedBytesSource:     "unsupported_codec_not_run",
			CompressionRatio:            0,
			CompressionRatioSource:      "unsupported_codec_not_run",
			CompressionDurationMS:       0,
			CompressionDurationSource:   "unsupported_codec_not_run",
			DecompressionDurationMS:     0,
			DecompressionDurationSource: "unsupported_codec_not_run",
			BenchmarkBPerOp:             0,
			BenchmarkAllocsPerOp:        0,
			BenchmarkAllocationSource:   columnStoreBenchmarkAllocationSource(),
		},
		Rows:    rows,
		Columns: columns,
	}
}

func columnStoreTypedColumnPartSectionCodecTarget(section collections.ColumnStoreTypedColumnPartSectionAccounting) bool {
	switch section.Category {
	case "locators", "dictionaries", "pruning_metadata":
		return true
	default:
		return false
	}
}

func columnStoreSectionCompressionAttribution(section collections.ColumnStoreTypedColumnPartSectionAccounting) columnStoreCompressionAttribution {
	requested := columnStoreSuiteRequestedTypedCompressionLabel()
	actual := strings.TrimSpace(section.Compression)
	if actual == "" {
		actual = columnStoreCompressionNoneLabel
	}
	policyLabel := columnStoreCompressionPolicyLabel(requested)
	supportState := columnStoreCompressionSupportSupported
	supportReason := "typed_column_part section byte target currently stored uncompressed"
	compressionDurationSource := "not_measured_current_default_none_section_reporting_placeholder"
	decompressionDurationSource := "not_measured_current_default_none_no_section_decompression"
	if section.Category == "locators" {
		if requested != columnStoreCompressionNoneLabel && actual == columnStoreCompressionNoneLabel {
			supportReason = "row locator section compression was requested but keep-if-smaller admission stored the raw section"
			compressionDurationSource = "row_locator_section_compression_nanos_not_persisted"
		} else if actual != columnStoreCompressionNoneLabel {
			supportReason = "row locator section compressed under benchmark-relaxed typed compression opt-in"
			compressionDurationSource = "row_locator_section_compression_nanos_not_persisted"
			decompressionDurationSource = "row_locator_section_decompression_not_separated_from_part_image_decode"
		}
	} else if section.Category == "dictionaries" {
		if requested != columnStoreCompressionNoneLabel && actual == columnStoreCompressionNoneLabel {
			supportReason = "dictionary section compression was requested but keep-if-smaller admission stored the raw section"
			compressionDurationSource = "dictionary_section_compression_nanos_not_persisted"
		} else if actual != columnStoreCompressionNoneLabel {
			supportReason = "dictionary section compressed under benchmark-relaxed typed compression opt-in"
			compressionDurationSource = "dictionary_section_compression_nanos_not_persisted"
			decompressionDurationSource = "dictionary_section_decompression_not_separated_from_part_image_decode"
		}
	} else if requested != columnStoreCompressionNoneLabel {
		supportState = columnStoreCompressionSupportDeferred
		supportReason = "section compression is deferred for this section category"
		compressionDurationSource = "deferred_section_compression_not_run"
		decompressionDurationSource = "deferred_section_decompression_not_run"
	}
	rawBytes := section.RawBytes
	if rawBytes <= 0 && section.Kind != "row_locators" {
		rawBytes = section.Bytes
	}
	storedBytes := section.StoredBytes
	if storedBytes <= 0 {
		storedBytes = section.Bytes
	}
	return columnStoreCompressionAttribution{
		CodecLayoutLabel:            "typed_column_part/section/" + section.Category + "/" + columnStoreSectionNameLabel(section),
		CompressionPolicyLabel:      policyLabel,
		RequestedCompression:        requested,
		ActualCompression:           actual,
		SupportState:                supportState,
		SupportReason:               supportReason,
		CompressedBytes:             storedBytes,
		CompressedBytesSource:       "typed_column_part_byte_accounting.serialized_sections.stored_bytes",
		RawBytes:                    rawBytes,
		RawBytesSource:              "typed_column_part_byte_accounting.serialized_sections.raw_bytes",
		DecompressedBytes:           rawBytes,
		DecompressedBytesSource:     "typed_column_part_byte_accounting.serialized_sections.raw_bytes",
		CompressionRatio:            columnStoreCompressionRatio(storedBytes, rawBytes),
		CompressionRatioSource:      "typed_column_part_section_stored_bytes/raw_bytes",
		CompressionDurationMS:       0,
		CompressionDurationSource:   compressionDurationSource,
		DecompressionDurationMS:     0,
		DecompressionDurationSource: decompressionDurationSource,
		BenchmarkBPerOp:             0,
		BenchmarkAllocsPerOp:        0,
		BenchmarkAllocationSource:   columnStoreBenchmarkAllocationSource(),
	}
}

func columnStoreSectionNameLabel(section collections.ColumnStoreTypedColumnPartSectionAccounting) string {
	if strings.TrimSpace(section.Name) != "" {
		return section.Name
	}
	if strings.TrimSpace(section.Kind) != "" {
		return section.Kind
	}
	return "unnamed"
}

func columnStoreSuiteRequestedTypedCompressionLabel() string {
	switch strings.ToLower(strings.TrimSpace(*columnStoreSuiteTypedCompressionArg)) {
	case "", "default", "none", "off", "compression_off":
		return columnStoreCompressionNoneLabel
	case "snappy":
		return "snappy"
	case "lz4":
		return "lz4"
	case "zstd", "zstd_dict", "zstd-dict":
		return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(*columnStoreSuiteTypedCompressionArg)), "-", "_")
	default:
		return strings.ToLower(strings.TrimSpace(*columnStoreSuiteTypedCompressionArg))
	}
}

func columnStoreCompressionPolicyLabel(requested string) string {
	if requested == "" || requested == columnStoreCompressionNoneLabel {
		return columnStoreCompressionPolicyOff
	}
	return "requested_" + requested
}

func columnStoreSuiteCompressionAttribution(policyLabel string, physicalBytes int64) columnStoreCompressionAttribution {
	return columnStoreCompressionAttribution{
		CodecLayoutLabel:            columnStoreCodecLayoutCurrent,
		CompressionPolicyLabel:      policyLabel,
		RequestedCompression:        columnStoreCompressionNoneLabel,
		ActualCompression:           columnStoreCompressionNoneLabel,
		SupportState:                columnStoreCompressionSupportSupported,
		CompressedBytes:             physicalBytes,
		CompressedBytesSource:       "filesystem_column_asset_store_bytes_after_checkpoint",
		RawBytes:                    physicalBytes,
		RawBytesSource:              "current_default_none_uncompressed_physical_column_asset_bytes_proxy",
		DecompressedBytes:           physicalBytes,
		DecompressedBytesSource:     "current_default_none_no_decompression_physical_column_asset_bytes_proxy",
		CompressionRatio:            columnStoreCompressionRatio(physicalBytes, physicalBytes),
		CompressionRatioSource:      "current_default_none_no_compression_physical_bytes_proxy",
		CompressionDurationMS:       0,
		CompressionDurationSource:   "not_measured_current_default_none_reporting_placeholder",
		DecompressionDurationMS:     0,
		DecompressionDurationSource: "not_measured_current_default_none_no_decompression_reporting_placeholder",
		BenchmarkBPerOp:             0,
		BenchmarkAllocsPerOp:        0,
		BenchmarkAllocationSource:   columnStoreBenchmarkAllocationSource(),
	}
}

func columnStoreQueryCompressionAttribution(planLabel, storageSource, fallbackReason string, bytes int64) columnStoreCompressionAttribution {
	layoutLabel := columnStoreCodecLayoutCurrent
	policyLabel := columnStoreCompressionPolicyDefault
	supportState := columnStoreCompressionSupportSupported
	supportReason := ""
	bytesSource := "query_physical_bytes_scanned"
	rawSource := "current_default_none_uncompressed_query_physical_bytes"
	decompressedSource := "current_default_none_no_decompression_query_physical_bytes"
	ratioSource := "compressed_bytes/raw_bytes_current_default_none"
	compressionDurationSource := "not_measured_query_phase_current_default_none_reporting_placeholder"
	decompressionDurationSource := "not_separated_from_physical_scan_current_default_none_reporting_placeholder"

	switch planLabel {
	case columnStorePathRowStoreBaseline:
		layoutLabel = "row_store_baseline_no_column_codec"
		policyLabel = "not_applicable_row_store"
		supportState = columnStoreCompressionSupportNotApplicable
		bytesSource = "row_store_document_bytes_read"
		rawSource = "row_store_document_bytes_read_not_column_codec_raw_bytes"
		decompressedSource = "row_store_no_column_codec_decompression"
		ratioSource = "not_applicable_row_store_no_column_codec"
		compressionDurationSource = "not_applicable_row_store_no_column_codec"
		decompressionDurationSource = "not_applicable_row_store_no_column_codec"
	case columnStorePathBTreeIndexBaseline:
		layoutLabel = "b_tree_index_baseline_no_column_codec"
		policyLabel = "not_applicable_b_tree_index"
		supportState = columnStoreCompressionSupportNotApplicable
		bytesSource = "b_tree_index_document_bytes_read"
		rawSource = "b_tree_index_document_bytes_read_not_column_codec_raw_bytes"
		decompressedSource = "b_tree_index_no_column_codec_decompression"
		ratioSource = "not_applicable_b_tree_index_no_column_codec"
		compressionDurationSource = "not_applicable_b_tree_index_no_column_codec"
		decompressionDurationSource = "not_applicable_b_tree_index_no_column_codec"
	default:
		if storageSource == string(collections.ColumnPhysicalQueryStorageSourceRowScan) || storageSource == string(collections.ColumnPhysicalQueryStorageSourceFallback) {
			layoutLabel = planLabel + "_fallback_" + storageSource
			policyLabel = "not_applicable_fallback"
			supportState = columnStoreCompressionSupportFallback
			supportReason = fallbackReason
			bytesSource = "fallback_bytes_read"
			rawSource = "fallback_bytes_read_not_column_codec_raw_bytes"
			decompressedSource = "fallback_no_column_codec_decompression"
			ratioSource = "not_applicable_fallback_no_column_codec"
			compressionDurationSource = "not_applicable_fallback_no_column_codec"
			decompressionDurationSource = "not_applicable_fallback_no_column_codec"
		} else if fallbackReason != string(collections.ColumnPhysicalQueryFallbackNone) {
			supportReason = fallbackReason
		}
	}
	if strings.TrimSpace(supportReason) == "" && supportState == columnStoreCompressionSupportFallback {
		supportReason = "fallback path did not report a reason"
	}
	return columnStoreCompressionAttribution{
		CodecLayoutLabel:            layoutLabel,
		CompressionPolicyLabel:      policyLabel,
		RequestedCompression:        columnStoreCompressionNoneLabel,
		ActualCompression:           columnStoreCompressionNoneLabel,
		SupportState:                supportState,
		SupportReason:               supportReason,
		CompressedBytes:             bytes,
		CompressedBytesSource:       bytesSource,
		RawBytes:                    bytes,
		RawBytesSource:              rawSource,
		DecompressedBytes:           bytes,
		DecompressedBytesSource:     decompressedSource,
		CompressionRatio:            columnStoreCompressionRatio(bytes, bytes),
		CompressionRatioSource:      ratioSource,
		CompressionDurationMS:       0,
		CompressionDurationSource:   compressionDurationSource,
		DecompressionDurationMS:     0,
		DecompressionDurationSource: decompressionDurationSource,
		BenchmarkBPerOp:             0,
		BenchmarkAllocsPerOp:        0,
		BenchmarkAllocationSource:   columnStoreBenchmarkAllocationSource(),
	}
}

func columnStoreCompressionRatio(compressedBytes, rawBytes int64) float64 {
	if rawBytes <= 0 {
		return 0
	}
	return float64(compressedBytes) / float64(rawBytes)
}

func columnStoreBenchmarkAllocationSource() string {
	return "not_measured_by_unified_bench_custom_runner; use Go -benchmem output and TreeDB/collections direct-query allocation guard tests for B/op and allocs/op"
}

func renderColumnStoreSuiteMarkdown(report columnStoreSuiteReport) string {
	var sb strings.Builder
	sb.WriteString("# unified_bench suite: column_store\n\n")
	sb.WriteString(fmt.Sprintf("- profile: `%s`\n", report.Profile))
	sb.WriteString(fmt.Sprintf("- fixture: `%s`\n", report.Fixture))
	sb.WriteString(fmt.Sprintf("- rows: %s\n", formatInt(report.Rows)))
	sb.WriteString(fmt.Sprintf("- batchsize: %s\n", formatInt(report.BatchSize)))
	sb.WriteString(fmt.Sprintf("- forced path: `%s`\n", report.ForcedPath))
	sb.WriteString(fmt.Sprintf("- query names: %s\n", markdownCodeList(report.QueryNames)))
	sb.WriteString(fmt.Sprintf("- column asset read integrity: `%s`\n", report.ColumnAssetReadIntegrity))
	if report.DataDir != "" {
		sb.WriteString(fmt.Sprintf("- data-dir: `%s`\n", report.DataDir))
	}
	if report.PathLabel != "" {
		sb.WriteString(fmt.Sprintf("- path-label: `%s`\n", report.PathLabel))
	}
	sb.WriteString(fmt.Sprintf("- scope: %s\n", report.ProductionScope))
	sb.WriteString(fmt.Sprintf("- physical column query: %s\n", report.PhysicalColumnQuery))
	if report.CompressionMatrixNote != "" {
		sb.WriteString(fmt.Sprintf("- compression matrix note: %s\n", report.CompressionMatrixNote))
	}
	if report.ExternalJSONBenchStatus != "" {
		sb.WriteString(fmt.Sprintf("- external JSONBench status: %s\n", report.ExternalJSONBenchStatus))
	}
	if len(report.ReportCaveats) != 0 {
		sb.WriteString("- report caveats:\n")
		for _, caveat := range report.ReportCaveats {
			sb.WriteString(fmt.Sprintf("  - %s\n", caveat))
		}
	}
	if report.ProfileFinalizeError != "" {
		sb.WriteString(fmt.Sprintf("- profile finalize error: `%s`\n", report.ProfileFinalizeError))
	}
	sb.WriteString(fmt.Sprintf("- timing boundary: %s\n\n", report.StageSeparatedBoundary))

	sb.WriteString("## Stage Timings\n\n")
	sb.WriteString("| stage | ms | rows/s | MiB/s | bytes |\n")
	sb.WriteString("|---|---:|---:|---:|---:|\n")
	for _, st := range report.Stages {
		sb.WriteString(fmt.Sprintf("| `%s` | %.3f | %.3f | %.3f | %d |\n", st.Name, st.DurationMS, st.RowsPerSecond, st.MiBPerSecond, st.Bytes))
	}
	sb.WriteString("\n")

	renderColumnStoreJSONBenchCellsMarkdown(&sb, report)
	renderColumnStoreColgranuleReuseMarkdown(&sb, report)

	sb.WriteString("## Query Throughput And Parity\n\n")
	sb.WriteString("| query | plan | storage source | fallback | manifest root | active gen/checksum | rows/s | MiB/s | ns/row | planner ms | scan ms | reduce ms | adapter ms | parity hash ms | workers | scheduled granules | skipped granules | metadata hits | dictionary code hits | int64 value hits | B/read | rows materialized | segment file cache hit/miss | hash parity | note |\n")
	sb.WriteString("|---|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|\n")
	for _, q := range report.Queries {
		parity := "pass"
		if p, ok := report.Parity[q.Name]; ok && !p.Pass {
			parity = "FAIL"
		}
		note := q.ImplementationNote
		if note == "" && q.AliasOf != "" {
			note = "alias_of_" + q.AliasOf
		}
		noteCell := "-"
		if note != "" {
			noteCell = markdownCodeTableText(note)
		}
		manifestRootCell := "-"
		if q.ManifestRootName != "" || q.ManifestRoot != 0 {
			manifestRootCell = fmt.Sprintf("%s/%d", q.ManifestRootName, q.ManifestRoot)
		}
		activeManifestCell := "-"
		if q.ManifestGeneration != 0 || q.ActiveManifestChecksum != 0 {
			activeManifestCell = fmt.Sprintf("%d/%d", q.ManifestGeneration, q.ActiveManifestChecksum)
		}
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %.3f | %.3f | %.1f | %.3f | %.3f | %.3f | %.3f | %.3f | %d | %d | %d | %d | %d | %d | %d | %d | %d/%d | %s | %s |\n",
			markdownCodeTableText(q.Name), markdownCodeTableText(q.PlanLabel), markdownCodeTableText(q.StorageSource), markdownCodeTableText(q.FallbackReason), markdownCodeTableText(manifestRootCell), markdownCodeTableText(activeManifestCell), q.RowsPerSecond, q.MiBPerSecond, q.NsPerRow, q.PlannerDurationMS, q.ScanDurationMS, q.ReduceDurationMS, q.AdapterDurationMS, q.ParityHashDurationMS, q.WorkerCount, q.ScheduledGranules, q.SkippedGranules, q.MetadataHits, q.DictionaryCodeHits, q.Int64ValueHits, q.BytesRead, q.RowMaterializations, q.SegmentFileCacheHits, q.SegmentFileCacheMisses, parity, noteCell))
	}
	sb.WriteString("\n")

	renderColumnStoreQueryCompressionAttributionMarkdown(&sb, report)
	renderColumnStoreCodecLayoutMarkdown(&sb, report)

	sb.WriteString("## Throughput Interpretation\n\n")
	sb.WriteString("| query | plan | interpretation |\n")
	sb.WriteString("|---|---|---|\n")
	for _, q := range report.Queries {
		sb.WriteString(fmt.Sprintf("| %s | %s | %s |\n", markdownCodeTableText(q.Name), markdownCodeTableText(q.PlanLabel), markdownTableText(q.ThroughputInterpretation)))
	}
	sb.WriteString("\n")

	sb.WriteString("## Byte Accounting\n\n")
	sb.WriteString(fmt.Sprintf("- source_document_bytes: %d\n", report.ByteAccounting.SourceDocumentBytes))
	sb.WriteString(fmt.Sprintf("- retained_payload_bytes: %d\n", report.ByteAccounting.RetainedPayloadBytes))
	if report.ByteAccounting.RetainedPayloadBytesNote != "" {
		sb.WriteString(fmt.Sprintf("- retained_payload_bytes_note: %s\n", report.ByteAccounting.RetainedPayloadBytesNote))
	}
	sb.WriteString(fmt.Sprintf("- column_asset_bytes: %d\n", report.ByteAccounting.ColumnAssetBytes))
	if report.ByteAccounting.ColumnAssetBytesNote != "" {
		sb.WriteString(fmt.Sprintf("- column_asset_bytes_note: %s\n", report.ByteAccounting.ColumnAssetBytesNote))
	}
	sb.WriteString(fmt.Sprintf("- column_asset_store_bytes: %d\n", report.ByteAccounting.ColumnAssetStoreBytes))
	sb.WriteString(fmt.Sprintf("- manifest_control_bytes: %d\n", report.ByteAccounting.ManifestControlBytes))
	if len(report.ByteAccounting.ManifestControlMissing) != 0 {
		sb.WriteString(fmt.Sprintf("- manifest_control_missing: %s\n", markdownCodeList(report.ByteAccounting.ManifestControlMissing)))
	}
	sb.WriteString(fmt.Sprintf("- primary_index_bytes: %d\n", report.ByteAccounting.PrimaryIndexBytes))
	sb.WriteString(fmt.Sprintf("- ordinary_value_vlog_bytes: %d\n", report.ByteAccounting.OrdinaryValueLogBytes))
	sb.WriteString(fmt.Sprintf("- leaf_vlog_bytes: %d\n", report.ByteAccounting.LeafLogBytes))
	sb.WriteString(fmt.Sprintf("- command_wal_bytes_before_checkpoint: %d\n", report.ByteAccounting.CommandWALBytesBeforeCheckpoint))
	sb.WriteString(fmt.Sprintf("- wal_bytes_excluded_from_durable_storage: %d\n", report.ByteAccounting.WALBytesExcludedFromDurable))
	sb.WriteString(fmt.Sprintf("- durable_storage_bytes_wal_excluded: %d\n", report.ByteAccounting.DurableStorageBytesWALExcluded))
	if report.ByteAccounting.DurableStorageBytesWALExcludedNote != "" {
		sb.WriteString(fmt.Sprintf("- durable_storage_bytes_wal_excluded_note: %s\n", report.ByteAccounting.DurableStorageBytesWALExcludedNote))
	}
	sb.WriteString(fmt.Sprintf("- total_reconstructable_bytes: %d\n", report.ByteAccounting.TotalReconstructableBytes))
	sb.WriteString(fmt.Sprintf("- db_total_bytes: %d across %d files\n\n", report.ByteAccounting.DBTotalBytes, report.ByteAccounting.DBTotalFiles))

	sb.WriteString("## Manifest Recovery\n\n")
	sb.WriteString(fmt.Sprintf("- active_generation: %d\n", report.Manifest.ActiveGeneration))
	sb.WriteString(fmt.Sprintf("- active_checksum: %d\n", report.Manifest.ActiveChecksum))
	sb.WriteString(fmt.Sprintf("- recovery_authoritative_generation: %d\n", report.Manifest.RecoveryAuthoritativeGeneration))
	sb.WriteString(fmt.Sprintf("- recovery_authoritative_checksum: %d\n", report.Manifest.RecoveryAuthoritativeChecksum))
	sb.WriteString(fmt.Sprintf("- applied_command_lsn: %d\n", report.Manifest.AppliedCommandLSN))
	sb.WriteString(fmt.Sprintf("- manifest_root_name: `%s`\n", report.Manifest.ManifestRootName))
	sb.WriteString(fmt.Sprintf("- manifest_root: %d\n", report.Manifest.ManifestRoot))
	sb.WriteString(fmt.Sprintf("- schema_hash: %d\n\n", report.Manifest.SchemaHash))

	sb.WriteString("## Forced Path Labels\n\n")
	sb.WriteString("- accepted labels are CLI/planner labels, not a promise that every run has the required physical assets\n")
	sb.WriteString(fmt.Sprintf("- accepted: %s\n", markdownCodeList(report.AcceptedForcedPaths)))
	sb.WriteString(fmt.Sprintf("- fail-closed: %s\n", markdownCodeList(report.FailClosedForcedPaths)))
	if report.Artifacts.ColumnJSON != "" {
		sb.WriteString("\n## Artifacts\n\n")
		columnStoreWriteArtifactLine(&sb, "column JSON", report.Artifacts.ColumnJSON)
		columnStoreWriteArtifactLine(&sb, "column markdown", report.Artifacts.ColumnMarkdown)
		columnStoreWriteArtifactLine(&sb, "column HTML", report.Artifacts.ColumnHTML)
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
		columnStoreWriteArtifactLine(&sb, "block delta profile", report.Artifacts.BlockDeltaProfile)
		columnStoreWriteArtifactLine(&sb, "mutex delta profile", report.Artifacts.MutexDeltaProfile)
	}
	return sb.String()
}

func renderColumnStoreJSONBenchCellsMarkdown(sb *strings.Builder, report columnStoreSuiteReport) {
	sb.WriteString("## Production JSONBench Synthetic Cells\n\n")
	if len(report.JSONBenchCells) == 0 {
		sb.WriteString("No JSONBench synthetic cells were recorded.\n\n")
		return
	}
	sb.WriteString("| cell | query | sort layout | storage source | mode | metadata/data path | compression | mutation | retained payload | retained encoding | typed owner | rows | rows processed | bytes read | result hash | parity | full-data caveat | reconstruction | status |\n")
	sb.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---:|---:|---:|---:|---|---|---|---|\n")
	for _, cell := range report.JSONBenchCells {
		parity := "pass"
		if !cell.ParityWithRowScan {
			parity = "FAIL"
		}
		status := cell.CompatibilityStatus
		if cell.CompatibilityStatusReason != "" {
			status += ": " + cell.CompatibilityStatusReason
		}
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %d | %d | %d | %016x | %s | %s | %s | %s |\n",
			markdownCodeTableText(cell.CellLabel),
			markdownCodeTableText(cell.Query),
			markdownCodeTableText(cell.SortLayout),
			markdownCodeTableText(cell.StorageSource),
			markdownCodeTableText(cell.ExecutionMode),
			markdownCodeTableText(cell.MetadataDataScanPath),
			markdownCodeTableText(cell.CompressionMode),
			markdownCodeTableText(cell.MutationMode),
			markdownCodeTableText(cell.RetainedPayloadPolicy),
			markdownCodeTableText(cell.RetainedPayloadEncoding),
			markdownCodeTableText(cell.TypedStorageOwner),
			cell.RowCount,
			cell.RowsProcessed,
			cell.BytesRead,
			cell.ResultHash,
			parity,
			markdownTableText(cell.FullDataCaveat),
			markdownTableText(cell.ReconstructionStatus),
			markdownTableText(status),
		))
	}
	sb.WriteString("\n")
}

func renderColumnStoreColgranuleReuseMarkdown(sb *strings.Builder, report columnStoreSuiteReport) {
	if len(report.ColgranuleReuseMap) == 0 {
		return
	}
	sb.WriteString("## Colgranule Reuse Map\n\n")
	sb.WriteString("| colgranule source | production target | decision | divergence | evidence |\n")
	sb.WriteString("|---|---|---|---|---|\n")
	for _, row := range report.ColgranuleReuseMap {
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n",
			markdownTableText(row.Source),
			markdownTableText(row.ProductionTarget),
			markdownCodeTableText(row.Decision),
			markdownTableText(row.DivergenceReason),
			markdownTableText(row.Evidence),
		))
	}
	sb.WriteString("\n")
}

func renderColumnStoreQueryCompressionAttributionMarkdown(sb *strings.Builder, report columnStoreSuiteReport) {
	sb.WriteString("## Query Compression And Allocation Attribution\n\n")
	sb.WriteString("| query | codec/layout | compression policy | compressed bytes | raw bytes | decompressed bytes | ratio | compression ms | compression source | decompression ms | decompression source | B/op | allocs/op | allocation source |\n")
	sb.WriteString("|---|---|---|---:|---:|---:|---:|---:|---|---:|---|---:|---:|---|\n")
	for _, q := range report.Queries {
		a := q.CompressionAttribution
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %d | %d | %d | %.6f | %.3f | %s | %.3f | %s | %.3f | %.3f | %s |\n",
			markdownCodeTableText(q.Name),
			markdownCodeTableText(a.CodecLayoutLabel),
			markdownCodeTableText(a.CompressionPolicyLabel),
			a.CompressedBytes,
			a.RawBytes,
			a.DecompressedBytes,
			a.CompressionRatio,
			a.CompressionDurationMS,
			markdownTableText(a.CompressionDurationSource),
			a.DecompressionDurationMS,
			markdownTableText(a.DecompressionDurationSource),
			a.BenchmarkBPerOp,
			a.BenchmarkAllocsPerOp,
			markdownTableText(a.BenchmarkAllocationSource),
		))
	}
	sb.WriteString("\n")
}

func renderColumnStoreCodecLayoutMarkdown(sb *strings.Builder, report columnStoreSuiteReport) {
	sb.WriteString("## Codec/Layout Matrix\n\n")
	if report.CompressionMatrixNote != "" {
		sb.WriteString(report.CompressionMatrixNote)
		sb.WriteString("\n\n")
	}
	sb.WriteString("| codec/layout | compression policy | support | requested | actual | rows | columns | compressed bytes | raw bytes | decompressed bytes | ratio | compression ms | compression source | decompression ms | decompression source | B/op | allocs/op | allocation source |\n")
	sb.WriteString("|---|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---:|---|---:|---:|---|\n")
	for _, row := range report.CodecLayouts {
		a := row.columnStoreCompressionAttribution
		support := a.SupportState
		if a.SupportReason != "" {
			support += ": " + a.SupportReason
		}
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %d | %d | %d | %d | %d | %.6f | %.3f | %s | %.3f | %s | %.3f | %.3f | %s |\n",
			markdownCodeTableText(a.CodecLayoutLabel),
			markdownCodeTableText(a.CompressionPolicyLabel),
			markdownTableText(support),
			markdownCodeTableText(a.RequestedCompression),
			markdownCodeTableText(a.ActualCompression),
			row.Rows,
			row.Columns,
			a.CompressedBytes,
			a.RawBytes,
			a.DecompressedBytes,
			a.CompressionRatio,
			a.CompressionDurationMS,
			markdownTableText(a.CompressionDurationSource),
			a.DecompressionDurationMS,
			markdownTableText(a.DecompressionDurationSource),
			a.BenchmarkBPerOp,
			a.BenchmarkAllocsPerOp,
			markdownTableText(a.BenchmarkAllocationSource),
		))
	}
	sb.WriteString("\n")
}

func columnStoreWriteArtifactLine(sb *strings.Builder, label, path string) {
	if strings.TrimSpace(path) == "" {
		return
	}
	sb.WriteString(fmt.Sprintf("- %s: `%s`\n", label, path))
}

func markdownCodeList(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	var sb strings.Builder
	for i, value := range values {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('`')
		sb.WriteString(value)
		sb.WriteByte('`')
	}
	return sb.String()
}

const markdownTableEmptyCell = "(empty)"

func markdownTableText(value string) string {
	value = markdownNormalizeTableCellLineBreaks(value)
	value = strings.TrimSpace(value)
	if value == "" {
		return markdownTableEmptyCell
	}
	return markdownNormalizeTableCell(value, true)
}

func markdownCodeTableText(value string) string {
	value = markdownNormalizeTableCellLineBreaks(value)
	if strings.TrimSpace(value) == "" {
		return "`" + markdownTableEmptyCell + "`"
	}
	value = markdownNormalizeTableCell(value, false)
	delimiter := "`"
	for strings.Contains(value, delimiter) {
		delimiter += "`"
	}
	padding := ""
	if strings.HasPrefix(value, "`") || strings.HasSuffix(value, "`") || strings.HasPrefix(value, " ") || strings.HasSuffix(value, " ") {
		padding = " "
	}
	return delimiter + padding + value + padding + delimiter
}

func markdownNormalizeTableCell(value string, escapeHTML bool) string {
	value = markdownEscapeTablePipes(value)
	if escapeHTML {
		value = strings.ReplaceAll(value, "`", "\\`")
		value = html.EscapeString(value)
	}
	return value
}

func markdownNormalizeTableCellLineBreaks(value string) string {
	value = strings.ReplaceAll(value, "\r\n", " ")
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}

func markdownEscapeTablePipes(value string) string {
	if !strings.Contains(value, "|") {
		return value
	}
	var out strings.Builder
	out.Grow(len(value) + strings.Count(value, "|"))
	backslashes := 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\':
			backslashes++
			out.WriteByte(value[i])
			continue
		case '|':
			if backslashes%2 == 0 {
				out.WriteByte('\\')
			}
		default:
		}
		backslashes = 0
		out.WriteByte(value[i])
	}
	return out.String()
}

func renderColumnStoreSuiteHTML(report columnStoreSuiteReport) string {
	md := renderColumnStoreSuiteMarkdown(report)
	return `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>unified_bench column_store</title>
<style>
body{font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:32px;line-height:1.4;color:#17202a;background:#fbfaf7}
pre{white-space:pre-wrap;background:#111827;color:#f9fafb;padding:20px;border-radius:12px;overflow:auto}
h1{font-size:22px}
</style>
</head>
<body>
<h1>unified_bench column_store</h1>
<pre>` + html.EscapeString(md) + `</pre>
</body>
</html>
`
}

func columnStoreBenchRun(baseCfg BenchConfig, profile, dataDir string, report columnStoreSuiteReport, stats map[string]string, checkpointDuration time.Duration) BenchRun {
	cfg := baseCfg
	cfg.Keys = report.Rows
	cfg.BatchSize = report.BatchSize
	cfg.Profile = profile
	cfg.DBsArg = "treedb"
	testOrder := []string{
		columnStoreSuiteBenchTestName,
	}
	cfg.TestsArg = strings.Join(testOrder, ",")
	results := make(map[string]map[string]float64)
	displayNames := map[string]string{
		columnStoreSuiteBenchTestName: "Column store query phase",
	}
	byName := make(map[string]columnStoreQueryMetric, len(report.Queries))
	var queryDuration time.Duration
	var queryRowsProcessed int
	for _, q := range report.Queries {
		byName[q.Name] = q
		metricName := columnStoreSuiteBenchMetricPrefix + q.Name
		testOrder = append(testOrder, metricName)
		displayNames[metricName] = columnStoreSuiteBenchDisplayNameForQuery(q.Name)
		duration := q.duration
		if duration == 0 && q.DurationMS > 0 {
			duration = time.Duration(q.DurationMS * float64(time.Millisecond))
		}
		queryDuration += duration
		if rowsProcessed, ok := columnStoreQueryEffectiveRowsProcessed(q); ok {
			queryRowsProcessed += rowsProcessed
		}
		results[columnStoreSuiteBenchMetricPrefix+q.Name] = map[string]float64{columnStoreSuiteBenchDisplayName: q.RowsPerSecond}
	}
	if queryDuration > 0 {
		results[columnStoreSuiteBenchTestName] = map[string]float64{columnStoreSuiteBenchDisplayName: float64(queryRowsProcessed) / queryDuration.Seconds()}
	}
	if q, ok := byName[columnStoreQueryQ1]; ok {
		testOrder = append(testOrder, columnStoreSuiteAliasFullScanQ1)
		displayNames[columnStoreSuiteAliasFullScanQ1] = "Alias full scan from q1"
		results[columnStoreSuiteAliasFullScanQ1] = map[string]float64{columnStoreSuiteBenchDisplayName: q.RowsPerSecond}
	}
	if q, ok := byName[columnStoreQueryQ4A]; ok {
		testOrder = append(testOrder, columnStoreSuiteAliasPrefixQ4A)
		displayNames[columnStoreSuiteAliasPrefixQ4A] = "Alias prefix scan from q4a"
		results[columnStoreSuiteAliasPrefixQ4A] = map[string]float64{columnStoreSuiteBenchDisplayName: q.RowsPerSecond}
	}
	cfg.TestsArg = strings.Join(testOrder, ",")
	return BenchRun{
		Config:       cfg,
		Instances:    []*DBInstance{{Name: columnStoreSuiteBenchDBName, Wrapper: &columnStoreSuiteDBLabel{name: columnStoreSuiteBenchDisplayName}, Dir: dataDir}},
		TestOrder:    testOrder,
		DisplayNames: displayNames,
		Results:      results,
		CheckpointDurations: map[string]map[string]time.Duration{
			columnStoreSuiteBenchTestName: {columnStoreSuiteBenchDisplayName: checkpointDuration},
		},
		TreeDBStats: map[string]map[string]string{columnStoreSuiteBenchDisplayName: stats},
		DiskUsage:   map[string]dirDiskUsage{columnStoreSuiteBenchDisplayName: {TotalBytes: uint64(report.ByteAccounting.DBTotalBytes), TotalFiles: report.ByteAccounting.DBTotalFiles}},
	}
}

func columnStoreSuiteBenchDisplayNameForQuery(name string) string {
	switch name {
	case columnStoreQueryQ1:
		return "Column q1"
	case columnStoreQueryQ2:
		return "Column q2"
	case columnStoreQueryQ3:
		return "Column q3"
	case columnStoreQueryQ4A:
		return "Column q4a"
	case columnStoreQueryQ4B:
		return "Column q4b"
	case columnStoreQueryQ5:
		return "Column q5"
	case columnStoreQueryQ5Metadata:
		return "Column q5 metadata"
	default:
		return "Column " + name
	}
}

func columnStoreQueryEffectiveRowsProcessed(q columnStoreQueryMetric) (int, bool) {
	if q.RowsProcessedKnown {
		return q.RowsProcessed, true
	}
	if q.RowsProcessed != 0 {
		return q.RowsProcessed, true
	}
	if q.RowMaterializations > 0 {
		return q.RowMaterializations, true
	}
	return 0, false
}

func columnStoreSuitePruneMissingRuntimeDeltaArtifacts(paths columnStoreArtifactPaths) columnStoreArtifactPaths {
	paths.BlockDeltaProfile = columnStoreExistingOptionalArtifactPath(paths.BlockDeltaProfile)
	paths.MutexDeltaProfile = columnStoreExistingOptionalArtifactPath(paths.MutexDeltaProfile)
	return paths
}

func columnStoreExistingOptionalArtifactPath(path string) string {
	if strings.TrimSpace(path) == "" {
		return ""
	}
	if _, err := os.Stat(path); err == nil {
		return path
	} else if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	return path
}

func writeColumnStoreSuiteArtifacts(dir, executionPath string, report columnStoreSuiteReport, md string, run BenchRun) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("column_store: create profile dir: %w", err)
	}
	js, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("column_store: marshal report: %w", err)
	}
	if err := writeColumnStoreSuiteArtifactFile(columnStoreSuiteArtifactPath(report.Artifacts.ColumnJSON, filepath.Join(dir, "column_store_results.json")), js); err != nil {
		return fmt.Errorf("column_store: write json: %w", err)
	}
	if err := writeColumnStoreSuiteArtifactFile(columnStoreSuiteArtifactPath(report.Artifacts.ColumnMarkdown, filepath.Join(dir, "column_store_results.md")), []byte(md)); err != nil {
		return fmt.Errorf("column_store: write markdown: %w", err)
	}
	if err := writeColumnStoreSuiteArtifactFile(columnStoreSuiteArtifactPath(report.Artifacts.ColumnHTML, filepath.Join(dir, "column_store_results.html")), []byte(renderColumnStoreSuiteHTML(report))); err != nil {
		return fmt.Errorf("column_store: write html: %w", err)
	}
	benchprofJSON := columnStoreSuiteArtifactPath(report.Artifacts.BenchprofJSON, filepath.Join(dir, "benchprof_results.json"))
	benchprofMarkdown := columnStoreSuiteArtifactPath(report.Artifacts.BenchprofMarkdown, filepath.Join(dir, "benchprof_results.md"))
	if err := writeBenchprofArtifactsToPaths(benchprofJSON, benchprofMarkdown, strings.TrimSpace(executionPath), []BenchRun{run}); err != nil {
		return fmt.Errorf("column_store: write benchprof artifacts: %w", err)
	}
	return nil
}

func columnStoreSuiteArtifactPath(recorded, fallback string) string {
	if strings.TrimSpace(recorded) != "" {
		return recorded
	}
	return fallback
}

func writeColumnStoreSuiteArtifactFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
