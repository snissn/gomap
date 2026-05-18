package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"html"
	"math"
	"math/rand"
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
	columnStorePathRowStoreBaseline   = "row_store_baseline"
	columnStorePathBTreeIndexBaseline = "b_tree_index_baseline"
	columnStorePathSerialColumnScan   = "serial_column_scan"
	columnStorePathAggregateMetadata  = "aggregate_metadata"
	columnStorePathParallelColumnScan = "parallel_column_scan"
	columnStoreSuiteBenchTestName     = "column_store"
	columnStoreSuiteBenchDBName       = "treedb_column_store"
	columnStoreSuiteBenchDisplayName  = "TreeDB Column Store"
)

var (
	columnStoreSuitePathArg    = flag.String("column-store-path", columnStorePathRowStoreBaseline, "Forced column-store execution label for -suite column_store (row_store_baseline, b_tree_index_baseline, serial_column_scan, aggregate_metadata, parallel_column_scan; aliases: row, row-store-baseline, index, b_tree, b-tree-index-baseline, serial, serial-column-scan, metadata, aggregate-metadata, parallel, parallel-column-scan; physical column labels fail closed until implemented)")
	columnStoreSuiteFixtureArg = flag.String("column-store-fixture", "synthetic", "Fixture for -suite column_store (synthetic; JSONBENCH_DATA mode is reserved for the large local gate)")

	columnStoreSuiteSupportedForcedPaths = []string{
		columnStorePathRowStoreBaseline,
		columnStorePathBTreeIndexBaseline,
	}
	columnStoreSuiteUnsupportedForcedPaths = []string{
		columnStorePathSerialColumnScan,
		columnStorePathAggregateMetadata,
		columnStorePathParallelColumnScan,
	}
	// These files are opportunistic control-plane telemetry for the benchmark
	// report. Missing files are reported, not fatal, because the exact set can
	// vary as TreeDB control metadata evolves.
	columnStoreSuiteManifestControlFiles = []string{
		"vlog_ref_counts.meta",
	}
)

type columnStoreSuiteOptions struct {
	ProfileDir              string
	ExecutionPath           string
	ForcedPath              string
	Fixture                 string
	RunBenchprof            bool
	CorruptReferenceForTest string
}

type columnStoreSuiteReport struct {
	GeneratedAt            string                       `json:"generated_at"`
	Suite                  string                       `json:"suite"`
	Profile                string                       `json:"profile"`
	Fixture                string                       `json:"fixture"`
	DataDir                string                       `json:"data_dir,omitempty"`
	PathLabel              string                       `json:"path_label,omitempty"`
	ForcedPath             string                       `json:"forced_path"`
	Rows                   int                          `json:"rows"`
	BatchSize              int                          `json:"batch_size"`
	Seed                   int64                        `json:"seed"`
	CacheLabel             string                       `json:"cache_label"`
	SupportedForcedPaths   []string                     `json:"supported_forced_paths"`
	UnsupportedForcedPaths []string                     `json:"unsupported_forced_paths"`
	Stages                 []columnStoreStageMetric     `json:"stages"`
	Queries                []columnStoreQueryMetric     `json:"queries"`
	Parity                 map[string]columnStoreParity `json:"parity"`
	ByteAccounting         columnStoreByteAccounting    `json:"byte_accounting"`
	Manifest               columnStoreManifestMetric    `json:"manifest"`
	Artifacts              columnStoreArtifactPaths     `json:"artifacts,omitempty"`
	ProductionScope        string                       `json:"production_scope"`
	PhysicalColumnQuery    string                       `json:"physical_column_query"`
	BenchmarkOnlyRelaxed   bool                         `json:"benchmark_only_relaxed"`
	StageSeparatedBoundary string                       `json:"stage_separated_boundary"`
	ProfileFinalizeError   string                       `json:"profile_finalize_error,omitempty"`
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
	Name                 string  `json:"name"`
	PlanLabel            string  `json:"plan_label"`
	AliasOf              string  `json:"alias_of,omitempty"`
	ImplementationNote   string  `json:"implementation_note,omitempty"`
	DurationMS           float64 `json:"duration_ms"`
	Rows                 int     `json:"rows"`
	RowsPerSecond        float64 `json:"rows_per_second"`
	MiBPerSecond         float64 `json:"mib_per_second"`
	NsPerRow             float64 `json:"ns_per_row"`
	BytesRead            int64   `json:"bytes_read"`
	RowMaterializations  int     `json:"row_materializations"`
	ResultCount          int     `json:"result_count"`
	RawHash              uint64  `json:"raw_hash"`
	ProductionHash       uint64  `json:"production_hash"`
	MetadataHits         int     `json:"metadata_hits"`
	SkippedGranules      int     `json:"skipped_granules"`
	ScheduledGranules    int     `json:"scheduled_granules"`
	WorkerCount          int     `json:"worker_count"`
	PlannerDurationMS    float64 `json:"planner_duration_ms"`
	ScanDurationMS       float64 `json:"scan_duration_ms"`
	ReduceDurationMS     float64 `json:"reduce_duration_ms"`
	ParityHashDurationMS float64 `json:"parity_hash_duration_ms"`
	PlannerCandidates    int     `json:"planner_candidates"`
	PlannerReason        string  `json:"planner_reason,omitempty"`
	CacheHits            uint64  `json:"cache_hits"`
	CacheMisses          uint64  `json:"cache_misses"`
	CacheLabel           string  `json:"cache_label"`

	duration time.Duration
}

type columnStoreParity struct {
	Pass           bool   `json:"pass"`
	RawHash        uint64 `json:"raw_hash"`
	ProductionHash uint64 `json:"production_hash"`
}

type columnStoreByteAccounting struct {
	SourceDocumentBytes             int64    `json:"source_document_bytes"`
	RetainedPayloadBytes            int64    `json:"retained_payload_bytes"`
	RetainedPayloadBytesNote        string   `json:"retained_payload_bytes_note,omitempty"`
	ColumnAssetBytes                int64    `json:"column_asset_bytes"`
	ColumnAssetBytesNote            string   `json:"column_asset_bytes_note,omitempty"`
	ManifestControlBytes            int64    `json:"manifest_control_bytes"`
	ManifestControlMissing          []string `json:"manifest_control_missing,omitempty"`
	CommandWALBytesBeforeCheckpoint int64    `json:"command_wal_bytes_before_checkpoint"`
	TotalReconstructableBytes       int64    `json:"total_reconstructable_bytes"`
	DBTotalBytes                    int64    `json:"db_total_bytes"`
	DBTotalFiles                    int      `json:"db_total_files"`
}

type columnStoreManifestMetric struct {
	ActiveGeneration                uint64 `json:"active_generation"`
	RecoveryAuthoritativeGeneration uint64 `json:"recovery_authoritative_generation"`
	AppliedCommandLSN               uint64 `json:"applied_command_lsn"`
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
	if err := validateColumnStoreSuiteDBSelection(baseCfg.DBsArg, baseCfg.DBsExcludeArg); err != nil {
		return "", err
	}
	if strings.TrimSpace(opts.ProfileDir) != "" {
		if err := validateBenchprofExecutionPath(strings.TrimSpace(opts.ExecutionPath)); err != nil {
			return "", err
		}
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
		rows = 1
	}
	batchSize := baseCfg.BatchSize
	if batchSize <= 0 {
		batchSize = 128
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
	commandWALBytesBeforeCheckpoint, _, err := columnStoreSuiteDirUsage(backenddb.WALDirPath(dataDir))
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
		rawHashes[corrupt]++
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(baseCfg, collection, rows, rawHashes, forcedPath)
	if err != nil {
		return "", err
	}
	profileFinalizeErr := finishRuntimeProfiles()
	runtimeProfilesActive = false

	totalBytes, totalFiles, err := columnStoreSuiteDirUsage(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: DB byte accounting: %w", err)
	}
	manifestControlBytes, manifestControlMissing, err := columnStoreSuiteManifestControlUsage(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: manifest/control byte accounting: %w", err)
	}
	retainedPayloadBytes := sourceBytes
	columnAssetBytes := int64(0)
	totalReconstructableBytes := retainedPayloadBytes + columnAssetBytes + manifestControlBytes
	report := columnStoreSuiteReport{
		GeneratedAt:            time.Now().UTC().Format(time.RFC3339),
		Suite:                  "column_store",
		Profile:                profile,
		Fixture:                fixture,
		PathLabel:              strings.TrimSpace(opts.ExecutionPath),
		ForcedPath:             forcedPath,
		Rows:                   rows,
		BatchSize:              batchSize,
		Seed:                   seed,
		CacheLabel:             "reopened_warm_process",
		SupportedForcedPaths:   cloneStringSlice(columnStoreSuiteSupportedForcedPaths),
		UnsupportedForcedPaths: cloneStringSlice(columnStoreSuiteUnsupportedForcedPaths),
		Stages:                 stages,
		Queries:                queries,
		Parity:                 parity,
		ByteAccounting: columnStoreByteAccounting{
			SourceDocumentBytes:             sourceBytes,
			RetainedPayloadBytes:            retainedPayloadBytes,
			RetainedPayloadBytesNote:        "M11B retains the source JSONBench payload as the reconstructable row baseline; compressed retained payload accounting is future work",
			ColumnAssetBytes:                columnAssetBytes,
			ColumnAssetBytesNote:            "not measured in M11B because physical column assets are not published yet",
			ManifestControlBytes:            manifestControlBytes,
			ManifestControlMissing:          manifestControlMissing,
			CommandWALBytesBeforeCheckpoint: commandWALBytesBeforeCheckpoint,
			TotalReconstructableBytes:       totalReconstructableBytes,
			DBTotalBytes:                    totalBytes,
			DBTotalFiles:                    totalFiles,
		},
		Manifest: columnStoreManifestMetric{
			ActiveGeneration:                manifestIdentity.ManifestGeneration,
			RecoveryAuthoritativeGeneration: manifestIdentity.RecoveryAuthoritativeGeneration,
			AppliedCommandLSN:               manifestIdentity.RecoveryAuthoritativeAppliedCommandLSN,
			ManifestRoot:                    manifestIdentity.ManifestRoot,
			SchemaHash:                      manifestIdentity.SchemaHash,
		},
		ProductionScope:        "production column-enabled TreeDB collection manifest/control-plane path plus M11B planner diagnostics",
		PhysicalColumnQuery:    "physical column assets/scanners are not implemented in M11B; serial/aggregate/parallel physical labels fail closed through the planner",
		BenchmarkOnlyRelaxed:   false,
		StageSeparatedBoundary: "fixture generation, collection create, insert, checkpoint, reopen/recovery, planner, scan, reduce, and parity hash stages are timed separately for the forced execution label",
	}
	if baseCfg.KeepDir {
		report.DataDir = dataDir
	}
	if profileFinalizeErr != nil {
		report.ProfileFinalizeError = profileFinalizeErr.Error()
	}

	md := renderColumnStoreSuiteMarkdown(report)
	run := columnStoreBenchRun(baseCfg, profile, dataDir, report, db.Stats(), checkpointDuration)
	if strings.TrimSpace(opts.ProfileDir) != "" {
		report.Artifacts = columnStoreArtifactPathsForProfileDir(opts.ProfileDir, baseCfg)
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
	// Keep artifacts available for diagnosis even when parity is the failing gate.
	if parityErr != nil || profileFinalizeErr != nil {
		return "", errors.Join(parityErr, profileFinalizeErr)
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

func normalizeColumnStoreSuitePath(path string) string {
	path = strings.ToLower(strings.TrimSpace(path))
	switch path {
	case "", "row-store-baseline", "row_store", "row":
		return columnStorePathRowStoreBaseline
	case "b-tree-index-baseline", "btree_index_baseline", "b_tree", "index":
		return columnStorePathBTreeIndexBaseline
	case "serial-column-scan", "serial":
		return columnStorePathSerialColumnScan
	case "aggregate-metadata", "metadata":
		return columnStorePathAggregateMetadata
	case "parallel-column-scan", "parallel":
		return columnStorePathParallelColumnScan
	default:
		return path
	}
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
		return "", fmt.Errorf("column_store: unknown forced path %q; supported=%s fail_closed=%s", path, columnStoreSuitePathList(columnStoreSuiteSupportedForcedPaths), columnStoreSuitePathList(columnStoreSuiteUnsupportedForcedPaths))
	}
}

func columnStoreArtifactPathsForProfileDir(profileDir string, cfg BenchConfig) columnStoreArtifactPaths {
	paths := columnStoreArtifactPaths{
		ColumnJSON:        filepath.Join(profileDir, "column_store_results.json"),
		ColumnMarkdown:    filepath.Join(profileDir, "column_store_results.md"),
		ColumnHTML:        filepath.Join(profileDir, "column_store_results.html"),
		BenchprofJSON:     filepath.Join(profileDir, "benchprof_results.json"),
		BenchprofMarkdown: filepath.Join(profileDir, "benchprof_results.md"),
		InsightsMarkdown:  filepath.Join(profileDir, "insights.md"),
		InsightsJSON:      filepath.Join(profileDir, "insights.json"),
		InsightsHTML:      filepath.Join(profileDir, "insights.html"),
	}
	if shouldCPUProfile(cfg, columnStoreSuiteBenchTestName) {
		paths.CPUProfile = fmt.Sprintf("%s_%s_%s.pprof", cfg.CPUProfile, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	if shouldAllocsProfile(cfg, columnStoreSuiteBenchTestName) {
		paths.AllocsProfile = fmt.Sprintf("%s_%s_%s.pprof", cfg.AllocsProfile, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	if shouldCheckpointCPUProfile(cfg, columnStoreSuiteBenchTestName) {
		paths.CheckpointCPUProfile = fmt.Sprintf("%s_checkpoint_%s_%s.pprof", cfg.CheckpointCPUProfile, sanitizeProfileSegment(columnStoreSuiteBenchTestName), sanitizeProfileSegment(columnStoreSuiteBenchDBName))
	}
	if strings.TrimSpace(cfg.BlockProfile) != "" {
		paths.BlockProfile = cfg.BlockProfile
		paths.BlockDeltaProfile = contentionProfilePath(cfg.BlockProfile, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	if strings.TrimSpace(cfg.MutexProfile) != "" {
		paths.MutexProfile = cfg.MutexProfile
		paths.MutexDeltaProfile = contentionProfilePath(cfg.MutexProfile, "mutex", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
	}
	if strings.TrimSpace(cfg.TraceProfile) != "" {
		paths.TraceProfile = cfg.TraceProfile
	}
	return paths
}

func validateColumnStoreSuiteDBSelection(dbsArg, excludeArg string) error {
	excluded := make(map[string]struct{})
	for _, db := range parseList(excludeArg) {
		normalized := strings.ToLower(strings.TrimSpace(db))
		switch normalized {
		case "", "none":
			continue
		case "all", "treedb":
			return fmt.Errorf("column_store: native suite requires TreeDB but -dbs-exclude=%q excludes it", excludeArg)
		default:
			excluded[normalized] = struct{}{}
		}
	}

	dbs := parseList(dbsArg)
	hasTreeDB := false
	for _, db := range dbs {
		normalized := strings.ToLower(strings.TrimSpace(db))
		if _, skip := excluded[normalized]; skip {
			continue
		}
		switch normalized {
		case "", "all", "treedb":
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
	return &collections.ColumnStoreConfig{
		Enabled: true,
		Columns: []collections.ColumnStoreColumn{
			{Name: "time_us", Path: "time_us", ValueType: collections.ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString, Dictionary: true},
			{Name: "did", Path: "did", ValueType: collections.ColumnStoreValueString, Dictionary: true},
		},
		SortKey: []collections.ColumnSortKey{{Column: "time_us"}},
		AggregateMetadata: []collections.ColumnAggregateMetadata{
			{Name: "q5_did_time_span_min", Column: "time_us", Kind: collections.ColumnAggregateMin},
			{Name: "q5_did_time_span_max", Column: "time_us", Kind: collections.ColumnAggregateMax},
		},
		RetainedPayload: collections.ColumnRetainedPayloadNonColumn,
		Reconstruction:  collections.ColumnReconstructionRetainedPayloadAndColumns,
		ProfileSupport:  collections.ColumnStoreProfileDurableOnly,
	}
}

func columnStoreSuiteCollectionMeta(path string) *collections.CollectionMeta {
	meta := &collections.CollectionMeta{
		Name: "events",
		Options: collections.CollectionOptions{
			DocumentFormat:               collections.DocumentFormatJSON,
			DisableIndexedWriteMemtables: true,
			ColumnStore:                  columnStoreSuiteConfig(),
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
		hash, _, err := columnStoreQueryHash(columnStoreQueryCanonicalName(name, columnStorePathRowStoreBaseline), decoded)
		if err != nil {
			return nil, err
		}
		out[name] = hash
	}
	return out, nil
}

func startColumnStoreSuiteRuntimeProfiles(cfg BenchConfig) (func() error, error) {
	var cleanups []func() error
	finish := func() error {
		var out error
		for i := len(cleanups) - 1; i >= 0; i-- {
			out = errors.Join(out, cleanups[i]())
		}
		cleanups = nil
		return out
	}

	if shouldAllocsProfile(cfg, columnStoreSuiteBenchTestName) {
		rate := cfg.AllocsProfileRate
		if rate <= 0 {
			rate = 512 * 1024
		}
		prevRate := runtime.MemProfileRate
		runtime.MemProfileRate = rate
		cleanups = append(cleanups, func() error {
			runtime.MemProfileRate = prevRate
			return nil
		})
	}

	if cfg.BlockProfile != "" {
		rate := cfg.BlockProfileRate
		if rate <= 0 {
			rate = 1
		}
		f, err := os.Create(cfg.BlockProfile)
		if err != nil {
			_ = finish()
			return nil, fmt.Errorf("column_store: blockprofile: %w", err)
		}
		runtime.SetBlockProfileRate(rate)
		cleanups = append(cleanups, func() error {
			runtime.SetBlockProfileRate(0)
			prof := pprof.Lookup("block")
			var writeErr error
			if prof == nil {
				writeErr = fmt.Errorf("block profile unavailable")
			} else {
				writeErr = prof.WriteTo(f, 0)
			}
			return errors.Join(writeErr, f.Close())
		})
	}

	if cfg.MutexProfile != "" {
		frac := cfg.MutexProfileFraction
		if frac <= 0 {
			frac = 1
		}
		f, err := os.Create(cfg.MutexProfile)
		if err != nil {
			_ = finish()
			return nil, fmt.Errorf("column_store: mutexprofile: %w", err)
		}
		prevFrac := runtime.SetMutexProfileFraction(frac)
		cleanups = append(cleanups, func() error {
			runtime.SetMutexProfileFraction(prevFrac)
			prof := pprof.Lookup("mutex")
			var writeErr error
			if prof == nil {
				writeErr = fmt.Errorf("mutex profile unavailable")
			} else {
				writeErr = prof.WriteTo(f, 0)
			}
			return errors.Join(writeErr, f.Close())
		})
	}

	if cfg.TraceProfile != "" {
		f, err := os.Create(cfg.TraceProfile)
		if err != nil {
			_ = finish()
			return nil, fmt.Errorf("column_store: trace: %w", err)
		}
		if err := trace.Start(f); err != nil {
			_ = f.Close()
			_ = finish()
			return nil, fmt.Errorf("column_store: trace start: %w", err)
		}
		cleanups = append(cleanups, func() error {
			trace.Stop()
			return f.Close()
		})
	}

	return finish, nil
}

func runColumnStoreSuiteQueriesProfiled(cfg BenchConfig, collection *collections.Collection, rows int, rawHashes map[string]uint64, path string) ([]columnStoreQueryMetric, map[string]columnStoreParity, error, error) {
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
	if cfg.BlockProfile != "" {
		blockBasePath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_column_store_block_base", "block")
		if err != nil {
			cleanup(allocBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile baseline: %w", err)
		}
	}
	mutexBasePath := ""
	if cfg.MutexProfile != "" {
		mutexBasePath, err = profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_column_store_mutex_base", "mutex")
		if err != nil {
			cleanup(allocBasePath, blockBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile baseline: %w", err)
		}
	}

	var cpuFile *os.File
	cpuProfilePath := ""
	if shouldCPUProfile(cfg, columnStoreSuiteBenchTestName) {
		profilePath := fmt.Sprintf("%s_%s_%s.pprof", cfg.CPUProfile, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
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

	queries, parity, runErr := runColumnStoreSuiteQueries(collection, rows, rawHashes, path)

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
		allocPath := fmt.Sprintf("%s_%s_%s.pprof", cfg.AllocsProfile, columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
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
		blockPath := contentionProfilePath(cfg.BlockProfile, "block", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
		wrote, deltaErr := profileHooks.writeRuntimeProfileDeltaProfile(blockBasePath, blockAfterPath, blockPath)
		cleanup(blockBasePath, blockAfterPath)
		if deltaErr != nil {
			cleanup(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile %s: %w", blockPath, deltaErr)
		}
		if !wrote {
			cleanup(blockPath, mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile %s: %w", blockPath, errEmptyPprofDeltaOutput)
		}
	}
	if mutexBasePath != "" {
		mutexAfterPath, snapErr := profileHooks.writeRuntimeProfileSnapshotTemp("unified_bench_column_store_mutex_after", "mutex")
		if snapErr != nil {
			cleanup(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile snapshot: %w", snapErr)
		}
		mutexPath := contentionProfilePath(cfg.MutexProfile, "mutex", columnStoreSuiteBenchTestName, columnStoreSuiteBenchDBName)
		wrote, deltaErr := profileHooks.writeRuntimeProfileDeltaProfile(mutexBasePath, mutexAfterPath, mutexPath)
		cleanup(mutexBasePath, mutexAfterPath)
		if deltaErr != nil {
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile %s: %w", mutexPath, deltaErr)
		}
		if !wrote {
			cleanup(mutexPath)
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile %s: %w", mutexPath, errEmptyPprofDeltaOutput)
		}
	}

	return queries, parity, runErr, nil
}

func runColumnStoreSuiteQueries(collection *collections.Collection, rows int, rawHashes map[string]uint64, path string) ([]columnStoreQueryMetric, map[string]columnStoreParity, error) {
	path = normalizeColumnStoreSuitePath(path)
	forceKind, err := columnStoreSuitePlanKind(path)
	if err != nil {
		return nil, nil, err
	}
	queries := make([]columnStoreQueryMetric, 0, len(columnStoreQueryNameList))
	parity := make(map[string]columnStoreParity, len(columnStoreQueryNameList))
	var firstErr error
	for _, name := range columnStoreQueryNameList {
		plannerStart := time.Now()
		plan, err := collection.PlanColumnQuery(columnStoreSuitePlanRequest(name, rows, forceKind))
		plannerElapsed := time.Since(plannerStart)
		if err != nil {
			return nil, nil, err
		}
		if !plan.Supported {
			reason := strings.TrimSpace(plan.Diagnostics.UnsupportedPlanReason)
			if reason == "" {
				reason = "planner did not report an unsupported reason"
			}
			return nil, nil, fmt.Errorf("column_store: forced path %q unsupported for %s: reason=%s: %w", path, name, reason, collections.ErrColumnQueryPlanUnsupported)
		}

		scanStart := time.Now()
		events, materialized, bytesRead, err := scanColumnStoreSuiteEventsWithPlan(collection, rows, name, plan)
		scanElapsed := time.Since(scanStart)
		if err != nil {
			return nil, nil, err
		}
		reduceStart := time.Now()
		planLabel := string(plan.Kind)
		hashName := columnStoreQueryCanonicalName(name, planLabel)
		lines, err := columnStoreQueryLines(columnStoreQueryHashLineName(hashName), events)
		if err != nil {
			return nil, nil, fmt.Errorf("column_store: reduce %s: %w", name, err)
		}
		reduceElapsed := time.Since(reduceStart)
		parityHashStart := time.Now()
		hash := columnStoreHashLines(lines)
		parityHashElapsed := time.Since(parityHashStart)
		resultCount := len(lines)
		elapsed := plannerElapsed + scanElapsed + reduceElapsed + parityHashElapsed
		rawHash := rawHashes[name]
		pass := rawHash == hash
		parity[name] = columnStoreParity{Pass: pass, RawHash: rawHash, ProductionHash: hash}
		if !pass && firstErr == nil {
			firstErr = fmt.Errorf("column_store: parity mismatch for %s: raw=%016x production=%016x", name, rawHash, hash)
		}
		metric := columnStoreQueryMetric{
			Name: name,
			// PlanLabel records the executed planner kind after alias
			// normalization, not necessarily the raw requested path string.
			PlanLabel:           planLabel,
			AliasOf:             columnStoreQueryAliasOf(name, planLabel),
			ImplementationNote:  columnStoreQueryImplementationNote(name, planLabel),
			DurationMS:          durationMS(elapsed),
			duration:            elapsed,
			Rows:                rows,
			RowsPerSecond:       ratePerSecond(float64(materialized), elapsed),
			MiBPerSecond:        ratePerSecond(float64(bytesRead)/(1024*1024), elapsed),
			NsPerRow:            nsPerRow(elapsed, materialized),
			BytesRead:           bytesRead,
			RowMaterializations: materialized,
			ResultCount:         resultCount,
			RawHash:             rawHash,
			ProductionHash:      hash,
			// TODO(M11C): populate from physical aggregate-metadata diagnostics.
			MetadataHits:         0,
			SkippedGranules:      plan.Diagnostics.SkippedGranules,
			ScheduledGranules:    plan.Diagnostics.ScheduledGranules,
			WorkerCount:          plan.Diagnostics.WorkerCount,
			PlannerDurationMS:    durationMS(plannerElapsed),
			ScanDurationMS:       durationMS(scanElapsed),
			ReduceDurationMS:     durationMS(reduceElapsed),
			ParityHashDurationMS: durationMS(parityHashElapsed),
			PlannerCandidates:    plan.Diagnostics.CandidatePlans,
			PlannerReason:        plan.Diagnostics.Reason,
			CacheHits:            plan.Diagnostics.DecodedBlockCacheHits,
			CacheMisses:          plan.Diagnostics.DecodedBlockCacheMisses,
			CacheLabel:           "reopened_warm_process",
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
			// M11B deliberately has no physical column assets yet; this keeps
			// recovery-authoritative physical planner gates unreachable until
			// M11B/M11C wire real assets.
			PhysicalAssetCount:     0,
			PartCount:              0,
			GranuleCount:           0,
			MaxParallelWorkers:     1,
			PlannerCandidateBudget: 5,
		},
	}
}

func columnStoreSuiteQueryIndexCandidates(name string) []string {
	// Candidates must stay one-index-entry-per-document scalar fields because
	// the M11B B-tree baseline does a full ordered index pass for parity, then
	// verifies the materialized count against the fixture row count. The selected
	// index records which secondary structure is traversed; it is not a
	// predicate-selective read path until range pushdown lands.
	switch name {
	case "q1", "q2":
		return []string{"kind"}
	case "q3":
		return []string{"time_us"}
	case "q4a", "q4b", "q5", "q5_metadata":
		return []string{"did"}
	default:
		return nil
	}
}

func columnStoreSuiteAggregateMetadataName(name string) string {
	if name == "q5_metadata" {
		// The executable q5_metadata path still aliases q5 until physical
		// aggregate metadata assets exist, but forced aggregate_metadata planning
		// should validate against a real registered catalog entry.
		return "q5_did_time_span_min"
	}
	return ""
}

func scanColumnStoreSuiteEventsWithPlan(collection *collections.Collection, rows int, queryName string, plan collections.ColumnQueryPlan) ([]columnStoreDecodedEvent, int, int64, error) {
	switch plan.Kind {
	case collections.ColumnQueryPlanRowStoreBaseline:
		return scanColumnStoreSuiteEvents(collection, rows)
	case collections.ColumnQueryPlanBTreeIndexBaseline:
		return scanColumnStoreSuiteEventsByIndex(collection, rows, queryName, plan.IndexName)
	default:
		return nil, 0, 0, fmt.Errorf("column_store: executable path %q is not implemented", plan.Kind)
	}
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
	// read selectivity; range pushdown is deferred to M11C. The fixture expects
	// one indexed row entry per document, so rows+1 is a sentinel limit:
	// materialized != rows catches an exact sentinel hit, while truncated catches
	// entries beyond the sentinel.
	truncated, err := collection.ScanBorrowedDocumentsByIndexRange(indexName, collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Unbounded: true},
		Upper: collections.IndexRangeBound{Unbounded: true},
		Limit: rows + 1,
	}, func(record collections.BorrowedDocumentRecord) (bool, error) {
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
		return nil, 0, 0, fmt.Errorf("column_store: scan B-tree index %s for %s: %w", indexName, queryName, err)
	}
	if truncated {
		return nil, 0, 0, fmt.Errorf("column_store: B-tree index scan exceeded expected row count: materialized %d rows with sentinel limit %d; fixture expects one index entry per document", materialized, rows+1)
	}
	if materialized != rows {
		return nil, 0, 0, fmt.Errorf("column_store: B-tree index scan materialized %d rows, want %d", materialized, rows)
	}
	return events, materialized, bytesRead, nil
}

var columnStoreQueryNameList = [...]string{"q1", "q2", "q3", "q4a", "q4b", "q5", "q5_metadata"}

func columnStoreQueryNames() []string {
	return append([]string(nil), columnStoreQueryNameList[:]...)
}

func columnStoreQueryNameKnown(name string) bool {
	for _, candidate := range columnStoreQueryNameList {
		if name == candidate {
			return true
		}
	}
	return false
}

func columnStoreQueryAliasOf(name, path string) string {
	if name == "q5_metadata" && (path == columnStorePathRowStoreBaseline || path == columnStorePathBTreeIndexBaseline) {
		return "q5"
	}
	return ""
}

func columnStoreQueryImplementationNote(name, path string) string {
	if name == "q5_metadata" && path == columnStorePathBTreeIndexBaseline {
		return "q5_alias_full_unbounded_secondary_index_scan_no_predicate_pushdown_until_physical_aggregate_metadata_path"
	}
	if name == "q5_metadata" && path == columnStorePathRowStoreBaseline {
		return path + "_alias_until_physical_aggregate_metadata_path"
	}
	if path == columnStorePathBTreeIndexBaseline {
		return "full_unbounded_secondary_index_scan_no_predicate_pushdown_m11b"
	}
	return ""
}

func columnStoreQueryCanonicalName(name, path string) string {
	if alias := columnStoreQueryAliasOf(name, path); alias != "" {
		return alias
	}
	return name
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
	h := fnv.New64a()
	for _, line := range lines {
		_, _ = h.Write([]byte(line))
		_, _ = h.Write([]byte{0})
	}
	return h.Sum64()
}

func columnStoreQueryHashLineName(name string) string {
	// q5_metadata is an execution/reporting label until the physical metadata
	// path exists; parity hashes use q5's logical result lines so alias
	// equivalence is directly testable.
	if name == "q5_metadata" {
		return "q5"
	}
	return name
}

func columnStoreQueryLines(name string, events []columnStoreDecodedEvent) ([]string, error) {
	switch name {
	case "q1":
		counts := make(map[string]int)
		for _, event := range events {
			counts[event.Kind]++
		}
		return formatIntMapLines(name, counts), nil
	case "q2":
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
	case "q3":
		counts := make(map[string]int)
		for _, event := range events {
			hour := (event.TimeUS / 3_600_000_000) % 24
			counts[fmt.Sprintf("hour_%02d", hour)]++
		}
		return formatIntMapLines(name, counts), nil
	case "q4a":
		mins := make(map[string]int64)
		for _, event := range events {
			if cur, ok := mins[event.Did]; !ok || event.TimeUS < cur {
				mins[event.Did] = event.TimeUS
			}
		}
		return formatInt64MapLines(name, mins), nil
	case "q4b":
		maxs := make(map[string]int64)
		for _, event := range events {
			if cur, ok := maxs[event.Did]; !ok || event.TimeUS > cur {
				maxs[event.Did] = event.TimeUS
			}
		}
		return formatInt64MapLines(name, maxs), nil
	case "q5":
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

func renderColumnStoreSuiteMarkdown(report columnStoreSuiteReport) string {
	var sb strings.Builder
	sb.WriteString("# unified_bench suite: column_store\n\n")
	sb.WriteString(fmt.Sprintf("- profile: `%s`\n", report.Profile))
	sb.WriteString(fmt.Sprintf("- fixture: `%s`\n", report.Fixture))
	sb.WriteString(fmt.Sprintf("- rows: %s\n", formatInt(report.Rows)))
	sb.WriteString(fmt.Sprintf("- batchsize: %s\n", formatInt(report.BatchSize)))
	sb.WriteString(fmt.Sprintf("- forced path: `%s`\n", report.ForcedPath))
	if report.DataDir != "" {
		sb.WriteString(fmt.Sprintf("- data-dir: `%s`\n", report.DataDir))
	}
	if report.PathLabel != "" {
		sb.WriteString(fmt.Sprintf("- path-label: `%s`\n", report.PathLabel))
	}
	sb.WriteString(fmt.Sprintf("- scope: %s\n", report.ProductionScope))
	sb.WriteString(fmt.Sprintf("- physical column query: %s\n", report.PhysicalColumnQuery))
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

	sb.WriteString("## Query Throughput And Parity\n\n")
	sb.WriteString("| query | plan | rows/s | MiB/s | ns/row | planner ms | scan ms | reduce ms | parity hash ms | workers | scheduled granules | skipped granules | metadata hits | B/read | rows materialized | cache hit/miss | hash parity | note |\n")
	sb.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|\n")
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
			noteCell = "`" + note + "`"
		}
		sb.WriteString(fmt.Sprintf("| `%s` | `%s` | %.3f | %.3f | %.1f | %.3f | %.3f | %.3f | %.3f | %d | %d | %d | %d | %d | %d | %d/%d | %s | %s |\n",
			q.Name, q.PlanLabel, q.RowsPerSecond, q.MiBPerSecond, q.NsPerRow, q.PlannerDurationMS, q.ScanDurationMS, q.ReduceDurationMS, q.ParityHashDurationMS, q.WorkerCount, q.ScheduledGranules, q.SkippedGranules, q.MetadataHits, q.BytesRead, q.RowMaterializations, q.CacheHits, q.CacheMisses, parity, noteCell))
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
	sb.WriteString(fmt.Sprintf("- manifest_control_bytes: %d\n", report.ByteAccounting.ManifestControlBytes))
	if len(report.ByteAccounting.ManifestControlMissing) != 0 {
		sb.WriteString(fmt.Sprintf("- manifest_control_missing: %s\n", markdownCodeList(report.ByteAccounting.ManifestControlMissing)))
	}
	sb.WriteString(fmt.Sprintf("- command_wal_bytes_before_checkpoint: %d\n", report.ByteAccounting.CommandWALBytesBeforeCheckpoint))
	sb.WriteString(fmt.Sprintf("- total_reconstructable_bytes: %d\n", report.ByteAccounting.TotalReconstructableBytes))
	sb.WriteString(fmt.Sprintf("- db_total_bytes: %d across %d files\n\n", report.ByteAccounting.DBTotalBytes, report.ByteAccounting.DBTotalFiles))

	sb.WriteString("## Manifest Recovery\n\n")
	sb.WriteString(fmt.Sprintf("- active_generation: %d\n", report.Manifest.ActiveGeneration))
	sb.WriteString(fmt.Sprintf("- recovery_authoritative_generation: %d\n", report.Manifest.RecoveryAuthoritativeGeneration))
	sb.WriteString(fmt.Sprintf("- applied_command_lsn: %d\n", report.Manifest.AppliedCommandLSN))
	sb.WriteString(fmt.Sprintf("- manifest_root: %d\n", report.Manifest.ManifestRoot))
	sb.WriteString(fmt.Sprintf("- schema_hash: %d\n\n", report.Manifest.SchemaHash))

	sb.WriteString("## Forced Path Labels\n\n")
	sb.WriteString(fmt.Sprintf("- supported: %s\n", markdownCodeList(report.SupportedForcedPaths)))
	sb.WriteString(fmt.Sprintf("- fail-closed until physical planner paths exist: %s\n", markdownCodeList(report.UnsupportedForcedPaths)))
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
	testOrder := []string{columnStoreSuiteBenchTestName, "alias_full_scan_from_q1", "alias_prefix_scan_from_q4a", "column_store_q1", "column_store_q2", "column_store_q3", "column_store_q4a", "column_store_q4b", "column_store_q5", "column_store_q5_metadata"}
	cfg.TestsArg = strings.Join(testOrder, ",")
	results := make(map[string]map[string]float64)
	displayNames := map[string]string{
		columnStoreSuiteBenchTestName: "Column store query phase",
		"alias_full_scan_from_q1":     "Alias full scan from q1",
		"alias_prefix_scan_from_q4a":  "Alias prefix scan from q4a",
		"column_store_q1":             "Column q1",
		"column_store_q2":             "Column q2",
		"column_store_q3":             "Column q3",
		"column_store_q4a":            "Column q4a",
		"column_store_q4b":            "Column q4b",
		"column_store_q5":             "Column q5",
		"column_store_q5_metadata":    "Column q5 metadata",
	}
	byName := make(map[string]columnStoreQueryMetric, len(report.Queries))
	var queryDuration time.Duration
	var queryMaterializations int
	for _, q := range report.Queries {
		byName[q.Name] = q
		duration := q.duration
		if duration == 0 && q.DurationMS > 0 {
			duration = time.Duration(q.DurationMS * float64(time.Millisecond))
		}
		queryDuration += duration
		queryMaterializations += q.RowMaterializations
		results["column_store_"+q.Name] = map[string]float64{columnStoreSuiteBenchDisplayName: q.RowsPerSecond}
	}
	if queryDuration > 0 {
		results[columnStoreSuiteBenchTestName] = map[string]float64{columnStoreSuiteBenchDisplayName: float64(queryMaterializations) / queryDuration.Seconds()}
	}
	if q, ok := byName["q1"]; ok {
		results["alias_full_scan_from_q1"] = map[string]float64{columnStoreSuiteBenchDisplayName: q.RowsPerSecond}
	}
	if q, ok := byName["q4a"]; ok {
		results["alias_prefix_scan_from_q4a"] = map[string]float64{columnStoreSuiteBenchDisplayName: q.RowsPerSecond}
	}
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

func writeColumnStoreSuiteArtifacts(dir, executionPath string, report columnStoreSuiteReport, md string, run BenchRun) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("column_store: create profile dir: %w", err)
	}
	js, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("column_store: marshal report: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "column_store_results.json"), js, 0o644); err != nil {
		return fmt.Errorf("column_store: write json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "column_store_results.md"), []byte(md), 0o644); err != nil {
		return fmt.Errorf("column_store: write markdown: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "column_store_results.html"), []byte(renderColumnStoreSuiteHTML(report)), 0o644); err != nil {
		return fmt.Errorf("column_store: write html: %w", err)
	}
	if err := writeBenchprofArtifacts(dir, strings.TrimSpace(executionPath), []BenchRun{run}); err != nil {
		return fmt.Errorf("column_store: write benchprof artifacts: %w", err)
	}
	return nil
}
