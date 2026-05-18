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
	columnStorePathRowStoreBaseline   = string(collections.ColumnQueryPlanRowStoreBaseline)
	columnStorePathBTreeIndexBaseline = string(collections.ColumnQueryPlanBTreeIndexBaseline)
	columnStorePathSerialColumnScan   = string(collections.ColumnQueryPlanSerialColumnScan)
	columnStorePathAggregateMetadata  = string(collections.ColumnQueryPlanAggregateMetadata)
	columnStorePathParallelColumnScan = string(collections.ColumnQueryPlanParallelColumnScan)
)

var (
	columnStoreSuitePathArg    = flag.String("column-store-path", columnStorePathRowStoreBaseline, "Forced column-store execution label for -suite column_store (row_store_baseline, b_tree_index_baseline; physical column labels fail closed until implemented)")
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
	Name                string  `json:"name"`
	PlanLabel           string  `json:"plan_label"`
	DurationMS          float64 `json:"duration_ms"`
	Rows                int     `json:"rows"`
	RowsPerSecond       float64 `json:"rows_per_second"`
	MiBPerSecond        float64 `json:"mib_per_second"`
	NsPerRow            float64 `json:"ns_per_row"`
	BytesRead           int64   `json:"bytes_read"`
	RowMaterializations int     `json:"row_materializations"`
	ResultCount         int     `json:"result_count"`
	RawHash             uint64  `json:"raw_hash"`
	ProductionHash      uint64  `json:"production_hash"`
	MetadataHits        int     `json:"metadata_hits"`
	SkippedGranules     int     `json:"skipped_granules"`
	ScheduledGranules   int     `json:"scheduled_granules"`
	WorkerCount         int     `json:"worker_count"`
	PlannerDurationMS   float64 `json:"planner_duration_ms"`
	ScanDurationMS      float64 `json:"scan_duration_ms"`
	ReduceDurationMS    float64 `json:"reduce_duration_ms"`
	PlannerCandidates   int     `json:"planner_candidates"`
	PlannerReason       string  `json:"planner_reason,omitempty"`
	CacheHits           uint64  `json:"cache_hits"`
	CacheMisses         uint64  `json:"cache_misses"`
	CacheLabel          string  `json:"cache_label"`
}

type columnStoreParity struct {
	Pass           bool   `json:"pass"`
	RawHash        uint64 `json:"raw_hash"`
	ProductionHash uint64 `json:"production_hash"`
}

type columnStoreByteAccounting struct {
	SourceDocumentBytes       int64 `json:"source_document_bytes"`
	RetainedPayloadBytes      int64 `json:"retained_payload_bytes"`
	ColumnAssetBytes          int64 `json:"column_asset_bytes"`
	ManifestControlBytes      int64 `json:"manifest_control_bytes"`
	CommandWALBytes           int64 `json:"command_wal_bytes"`
	TotalReconstructableBytes int64 `json:"total_reconstructable_bytes"`
	DBTotalBytes              int64 `json:"db_total_bytes"`
	DBTotalFiles              int   `json:"db_total_files"`
}

type columnStoreManifestMetric struct {
	ActiveGeneration                uint64 `json:"active_generation"`
	RecoveryAuthoritativeGeneration uint64 `json:"recovery_authoritative_generation"`
	AppliedCommandLSN               uint64 `json:"applied_command_lsn"`
	ManifestRoot                    uint64 `json:"manifest_root"`
	SchemaHash                      uint64 `json:"schema_hash"`
}

type columnStoreArtifactPaths struct {
	ColumnJSON        string `json:"column_json,omitempty"`
	ColumnMarkdown    string `json:"column_markdown,omitempty"`
	ColumnHTML        string `json:"column_html,omitempty"`
	BenchprofJSON     string `json:"benchprof_json,omitempty"`
	BenchprofMarkdown string `json:"benchprof_markdown,omitempty"`
	InsightsMarkdown  string `json:"insights_markdown,omitempty"`
	InsightsJSON      string `json:"insights_json,omitempty"`
	InsightsHTML      string `json:"insights_html,omitempty"`
}

type columnStoreFixtureEvent struct {
	ID     string
	TimeUS int64
	Kind   string
	Did    string
	Doc    []byte
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
		return "", fmt.Errorf("column_store: unsupported fixture %q; synthetic is the M11A CI fixture and JSONBENCH_DATA large mode is not wired yet", fixture)
	}
	forcedPath := normalizeColumnStoreSuitePath(opts.ForcedPath)
	if forcedPath == "" {
		forcedPath = normalizeColumnStoreSuitePath(*columnStoreSuitePathArg)
	}
	if err := validateColumnStoreSuiteForcedPath(forcedPath); err != nil {
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
	if shouldCheckpointCPUProfile(baseCfg, checkpointPostRunLabel) {
		checkpointCPUFile, err = startCheckpointCPUProfile(baseCfg, checkpointPostRunLabel, "TreeDB Column Store")
		if err != nil {
			_ = db.Close()
			return "", fmt.Errorf("column_store: checkpoint profiling: %w", err)
		}
	}

	start = time.Now()
	checkpointErr := db.Checkpoint()
	if checkpointCPUFile != nil {
		stopCPUProfileFn()
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
	stages = append(stages, columnStoreStageFromDuration("reopen", reopenDuration, rows, sourceBytes))

	manifestIdentity, ok := collection.ColumnStoreCacheIdentity()
	if !ok {
		return "", errors.New("column_store: reopened collection has no column-store manifest identity")
	}

	rawHashes := columnStoreReferenceHashes(fixtureEvents)
	if corrupt := strings.TrimSpace(opts.CorruptReferenceForTest); corrupt != "" {
		rawHashes[corrupt]++
	}
	queries, parity, parityErr, err := runColumnStoreSuiteQueriesProfiled(baseCfg, collection, rows, rawHashes, forcedPath)
	if err != nil {
		return "", err
	}
	if err := finishRuntimeProfiles(); err != nil {
		return "", err
	}
	runtimeProfilesActive = false

	totalBytes, totalFiles, err := columnStoreSuiteDirUsage(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: DB byte accounting: %w", err)
	}
	manifestControlBytes, err := columnStoreSuiteManifestControlUsage(dataDir)
	if err != nil {
		return "", fmt.Errorf("column_store: manifest/control byte accounting: %w", err)
	}
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
			SourceDocumentBytes:       sourceBytes,
			RetainedPayloadBytes:      sourceBytes,
			ColumnAssetBytes:          0,
			ManifestControlBytes:      manifestControlBytes,
			CommandWALBytes:           commandWALBytesBeforeCheckpoint,
			TotalReconstructableBytes: sourceBytes + manifestControlBytes,
			DBTotalBytes:              totalBytes,
			DBTotalFiles:              totalFiles,
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
		StageSeparatedBoundary: "fixture generation, collection create, insert, checkpoint, reopen, planner, scan, and reduce stages are timed separately for the forced execution label",
	}

	md := renderColumnStoreSuiteMarkdown(report)
	run := columnStoreBenchRun(baseCfg, profile, dataDir, report, db.Stats(), checkpointDuration)
	if strings.TrimSpace(opts.ProfileDir) != "" {
		report.Artifacts = columnStoreArtifactPaths{
			ColumnJSON:        filepath.Join(opts.ProfileDir, "column_store_results.json"),
			ColumnMarkdown:    filepath.Join(opts.ProfileDir, "column_store_results.md"),
			ColumnHTML:        filepath.Join(opts.ProfileDir, "column_store_results.html"),
			BenchprofJSON:     filepath.Join(opts.ProfileDir, "benchprof_results.json"),
			BenchprofMarkdown: filepath.Join(opts.ProfileDir, "benchprof_results.md"),
			InsightsMarkdown:  filepath.Join(opts.ProfileDir, "insights.md"),
			InsightsJSON:      filepath.Join(opts.ProfileDir, "insights.json"),
			InsightsHTML:      filepath.Join(opts.ProfileDir, "insights.html"),
		}
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
	if parityErr != nil {
		return "", parityErr
	}
	return md, nil
}

func columnStoreSuiteEffectiveProfile(profile string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "", "durable", "balanced":
		return "durable", nil
	case "fast", "unsafe", "wal_on_fast":
		return "", fmt.Errorf("column_store: profile %q is benchmark-relaxed and unsupported for M11A production column-store writes; use -profile durable", profile)
	default:
		return "", fmt.Errorf("column_store: unsupported profile %q; use durable for the M11A production gate", profile)
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

func validateColumnStoreSuiteForcedPath(path string) error {
	switch path {
	case columnStorePathRowStoreBaseline, columnStorePathBTreeIndexBaseline:
		return nil
	case columnStorePathSerialColumnScan, columnStorePathAggregateMetadata, columnStorePathParallelColumnScan:
		// Physical labels are accepted here so the planner can return the
		// capability-specific fail-closed reason.
		return nil
	default:
		return fmt.Errorf("column_store: unknown forced path %q; supported=%s fail_closed=%s", path, columnStoreSuitePathList(columnStoreSuiteSupportedForcedPaths), columnStoreSuitePathList(columnStoreSuiteUnsupportedForcedPaths))
	}
}

func validateColumnStoreSuiteDBSelection(dbsArg, excludeArg string) error {
	for _, db := range parseList(excludeArg) {
		switch strings.ToLower(strings.TrimSpace(db)) {
		case "", "none":
			continue
		case "all", "treedb":
			return fmt.Errorf("column_store: native suite requires TreeDB but -dbs-exclude=%q excludes it", excludeArg)
		}
	}

	dbs := parseList(dbsArg)
	hasTreeDB := false
	for _, db := range dbs {
		switch strings.ToLower(strings.TrimSpace(db)) {
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
		doc := []byte(fmt.Sprintf(`{"time_us":%d,"kind":"%s","did":"%s","payload":"p%08x","group":%d}`,
			timeUS, kind, did, uint32(i*2654435761), i%32))
		out[i] = columnStoreFixtureEvent{ID: id, TimeUS: timeUS, Kind: kind, Did: did, Doc: doc}
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
			{Name: "q5_did_time_span", Column: "time_us", Kind: collections.ColumnAggregateMin},
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
	for start := 0; start < len(events); start += batchSize {
		end := start + batchSize
		if end > len(events) {
			end = len(events)
		}
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i := start; i < end; i++ {
			ids[i-start] = []byte(events[i].ID)
			docs[i-start] = events[i].Doc
		}
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			return fmt.Errorf("column_store: InsertBatch rows %d-%d: %w", start, end, err)
		}
	}
	return nil
}

func columnStoreReferenceHashes(events []columnStoreFixtureEvent) map[string]uint64 {
	decoded := make([]columnStoreDecodedEvent, len(events))
	for i := range events {
		decoded[i] = columnStoreDecodedEvent{TimeUS: events[i].TimeUS, Kind: events[i].Kind, Did: events[i].Did}
	}
	out := make(map[string]uint64)
	for _, name := range columnStoreQueryNames() {
		hash, _ := columnStoreQueryHash(name, decoded)
		out[name] = hash
	}
	return out
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

	if cfg.AllocsProfile != "" {
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
		runtime.SetBlockProfileRate(rate)
		f, err := os.Create(cfg.BlockProfile)
		if err != nil {
			_ = finish()
			return nil, fmt.Errorf("column_store: blockprofile: %w", err)
		}
		cleanups = append(cleanups, func() error {
			defer runtime.SetBlockProfileRate(0)
			defer f.Close()
			prof := pprof.Lookup("block")
			if prof == nil {
				return fmt.Errorf("block profile unavailable")
			}
			return prof.WriteTo(f, 0)
		})
	}

	if cfg.MutexProfile != "" {
		frac := cfg.MutexProfileFraction
		if frac <= 0 {
			frac = 1
		}
		runtime.SetMutexProfileFraction(frac)
		f, err := os.Create(cfg.MutexProfile)
		if err != nil {
			_ = finish()
			return nil, fmt.Errorf("column_store: mutexprofile: %w", err)
		}
		cleanups = append(cleanups, func() error {
			defer runtime.SetMutexProfileFraction(0)
			defer f.Close()
			prof := pprof.Lookup("mutex")
			if prof == nil {
				return fmt.Errorf("mutex profile unavailable")
			}
			return prof.WriteTo(f, 0)
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
	const testName = "column_store"
	const dbName = "treedb_column_store"

	var cpuFile *os.File
	if shouldCPUProfile(cfg, testName) {
		profilePath := fmt.Sprintf("%s_%s_%s.pprof", cfg.CPUProfile, testName, dbName)
		f, err := os.Create(profilePath)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("column_store: cpuprofile %s: %w", profilePath, err)
		}
		cpuFile = f
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			return nil, nil, nil, fmt.Errorf("column_store: cpuprofile start %s: %w", profilePath, err)
		}
	}

	allocBasePath := ""
	var err error
	if shouldAllocsProfile(cfg, testName) {
		allocBasePath, err = writeAllocsSnapshotTempFn("unified_bench_column_store_allocs_base")
		if err != nil {
			if cpuFile != nil {
				stopCPUProfileFn()
				_ = cpuFile.Close()
			}
			return nil, nil, nil, fmt.Errorf("column_store: allocsprofile baseline: %w", err)
		}
	}
	blockBasePath := ""
	if cfg.BlockProfile != "" {
		blockBasePath, err = writeRuntimeProfileSnapshotTempFn("unified_bench_column_store_block_base", "block")
		if err != nil {
			if cpuFile != nil {
				stopCPUProfileFn()
				_ = cpuFile.Close()
			}
			_ = os.Remove(allocBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile baseline: %w", err)
		}
	}
	mutexBasePath := ""
	if cfg.MutexProfile != "" {
		mutexBasePath, err = writeRuntimeProfileSnapshotTempFn("unified_bench_column_store_mutex_base", "mutex")
		if err != nil {
			if cpuFile != nil {
				stopCPUProfileFn()
				_ = cpuFile.Close()
			}
			_ = os.Remove(allocBasePath)
			_ = os.Remove(blockBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile baseline: %w", err)
		}
	}

	queries, parity, runErr := runColumnStoreSuiteQueries(collection, rows, rawHashes, path)

	if cpuFile != nil {
		stopCPUProfileFn()
		_ = cpuFile.Close()
	}
	if queries == nil {
		_ = os.Remove(allocBasePath)
		_ = os.Remove(blockBasePath)
		_ = os.Remove(mutexBasePath)
		return queries, parity, runErr, nil
	}

	if allocBasePath != "" {
		allocAfterPath, snapErr := writeAllocsSnapshotTempFn("unified_bench_column_store_allocs_after")
		if snapErr != nil {
			_ = os.Remove(allocBasePath)
			_ = os.Remove(blockBasePath)
			_ = os.Remove(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: allocsprofile snapshot: %w", snapErr)
		}
		allocPath := fmt.Sprintf("%s_%s_%s.pprof", cfg.AllocsProfile, testName, dbName)
		deltaErr := writeAllocsDeltaProfileFn(allocBasePath, allocAfterPath, allocPath)
		_ = os.Remove(allocBasePath)
		_ = os.Remove(allocAfterPath)
		if deltaErr != nil {
			_ = os.Remove(blockBasePath)
			_ = os.Remove(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: allocsprofile %s: %w", allocPath, deltaErr)
		}
	}
	if blockBasePath != "" {
		blockAfterPath, snapErr := writeRuntimeProfileSnapshotTempFn("unified_bench_column_store_block_after", "block")
		if snapErr != nil {
			_ = os.Remove(blockBasePath)
			_ = os.Remove(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile snapshot: %w", snapErr)
		}
		blockPath := contentionProfilePath(cfg.BlockProfile, "block", testName, dbName)
		_, deltaErr := writeRuntimeProfileDeltaProfileFn(blockBasePath, blockAfterPath, blockPath)
		_ = os.Remove(blockBasePath)
		_ = os.Remove(blockAfterPath)
		if deltaErr != nil {
			_ = os.Remove(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: blockprofile %s: %w", blockPath, deltaErr)
		}
	}
	if mutexBasePath != "" {
		mutexAfterPath, snapErr := writeRuntimeProfileSnapshotTempFn("unified_bench_column_store_mutex_after", "mutex")
		if snapErr != nil {
			_ = os.Remove(mutexBasePath)
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile snapshot: %w", snapErr)
		}
		mutexPath := contentionProfilePath(cfg.MutexProfile, "mutex", testName, dbName)
		_, deltaErr := writeRuntimeProfileDeltaProfileFn(mutexBasePath, mutexAfterPath, mutexPath)
		_ = os.Remove(mutexBasePath)
		_ = os.Remove(mutexAfterPath)
		if deltaErr != nil {
			return nil, nil, nil, fmt.Errorf("column_store: mutexprofile %s: %w", mutexPath, deltaErr)
		}
	}

	return queries, parity, runErr, nil
}

func runColumnStoreSuiteQueries(collection *collections.Collection, rows int, rawHashes map[string]uint64, path string) ([]columnStoreQueryMetric, map[string]columnStoreParity, error) {
	if err := validateColumnStoreSuiteForcedPath(path); err != nil {
		return nil, nil, err
	}
	queries := make([]columnStoreQueryMetric, 0, len(columnStoreQueryNames()))
	parity := make(map[string]columnStoreParity, len(columnStoreQueryNames()))
	var firstErr error
	for _, name := range columnStoreQueryNames() {
		plannerStart := time.Now()
		plan, err := collection.PlanColumnQuery(columnStoreSuitePlanRequest(name, rows, path))
		plannerElapsed := time.Since(plannerStart)
		if err != nil {
			return nil, nil, err
		}
		if !plan.Supported {
			reason := strings.TrimSpace(plan.Diagnostics.UnsupportedPlanReason)
			if reason == "" {
				reason = "planner did not report an unsupported reason"
			}
			return nil, nil, fmt.Errorf("column_store: forced path %q unsupported for %s: %s (%w)", path, name, reason, collections.ErrColumnQueryPlanUnsupported)
		}

		scanStart := time.Now()
		events, materialized, bytesRead, err := scanColumnStoreSuiteEventsWithPlan(collection, rows, name, plan)
		scanElapsed := time.Since(scanStart)
		if err != nil {
			return nil, nil, err
		}
		reduceHashStart := time.Now()
		hash, resultCount := columnStoreQueryHash(name, events)
		reduceHashElapsed := time.Since(reduceHashStart)
		elapsed := plannerElapsed + scanElapsed + reduceHashElapsed
		rawHash := rawHashes[name]
		pass := rawHash == hash
		parity[name] = columnStoreParity{Pass: pass, RawHash: rawHash, ProductionHash: hash}
		if !pass && firstErr == nil {
			firstErr = fmt.Errorf("column_store: parity mismatch for %s: raw=%016x production=%016x", name, rawHash, hash)
		}
		metric := columnStoreQueryMetric{
			Name:                name,
			PlanLabel:           string(plan.Kind),
			DurationMS:          durationMS(elapsed),
			Rows:                rows,
			RowsPerSecond:       ratePerSecond(float64(materialized), elapsed),
			MiBPerSecond:        ratePerSecond(float64(bytesRead)/(1024*1024), elapsed),
			NsPerRow:            nsPerRow(elapsed, materialized),
			BytesRead:           bytesRead,
			RowMaterializations: materialized,
			ResultCount:         resultCount,
			RawHash:             rawHash,
			ProductionHash:      hash,
			MetadataHits:        0,
			SkippedGranules:     plan.Diagnostics.SkippedGranules,
			ScheduledGranules:   plan.Diagnostics.ScheduledGranules,
			WorkerCount:         plan.Diagnostics.WorkerCount,
			PlannerDurationMS:   durationMS(plannerElapsed),
			ScanDurationMS:      durationMS(scanElapsed),
			ReduceDurationMS:    durationMS(reduceHashElapsed),
			PlannerCandidates:   plan.Diagnostics.CandidatePlans,
			PlannerReason:       plan.Diagnostics.Reason,
			CacheHits:           plan.Diagnostics.DecodedBlockCacheHits,
			CacheMisses:         plan.Diagnostics.DecodedBlockCacheMisses,
			CacheLabel:          "reopened_warm_process",
		}
		queries = append(queries, metric)
	}
	return queries, parity, firstErr
}

func columnStoreSuitePlanRequest(name string, rows int, path string) collections.ColumnQueryPlanRequest {
	return collections.ColumnQueryPlanRequest{
		Name:                  name,
		ProjectedColumns:      []string{"time_us", "kind", "did"},
		CandidateIndexColumns: columnStoreSuiteQueryIndexCandidates(name),
		AggregateMetadataName: columnStoreSuiteAggregateMetadataName(name),
		EstimatedRows:         rows,
		// runColumnStoreSuiteQueries validates path before this cast.
		ForceKind: collections.ColumnQueryPlanKind(path),
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
		return "q5_did_time_span"
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
		return nil, 0, 0, fmt.Errorf("column_store: scan truncated at %d rows", rows+1)
	}
	if materialized != rows {
		return nil, 0, 0, fmt.Errorf("column_store: scanned %d rows, want %d", materialized, rows)
	}
	return events, materialized, bytesRead, nil
}

func scanColumnStoreSuiteEventsByIndex(collection *collections.Collection, rows int, queryName, indexName string) ([]columnStoreDecodedEvent, int, int64, error) {
	if strings.TrimSpace(indexName) == "" {
		return nil, 0, 0, fmt.Errorf("column_store: no B-tree index selected for %s", queryName)
	}
	events := make([]columnStoreDecodedEvent, 0, rows)
	var materialized int
	var bytesRead int64
	// The M11B B-tree baseline is intentionally a full secondary-index ordered
	// pass for q1-q5 parity; none of these aggregate queries currently has a
	// selective predicate that can safely narrow the range.
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
		return nil, 0, 0, fmt.Errorf("column_store: B-tree index scan truncated at %d rows", rows+1)
	}
	if materialized != rows {
		return nil, 0, 0, fmt.Errorf("column_store: B-tree index scan materialized %d rows, want %d", materialized, rows)
	}
	return events, materialized, bytesRead, nil
}

func columnStoreQueryNames() []string {
	return []string{"q1", "q2", "q3", "q4a", "q4b", "q5", "q5_metadata"}
}

func columnStoreQueryHash(name string, events []columnStoreDecodedEvent) (uint64, int) {
	lines := columnStoreQueryLines(name, events)
	sort.Strings(lines)
	h := fnv.New64a()
	for _, line := range lines {
		_, _ = h.Write([]byte(line))
		_, _ = h.Write([]byte{0})
	}
	// ResultCount is the reduced result row count; the parity hash covers the
	// same reduced rows after deterministic ordering.
	return h.Sum64(), len(lines)
}

func columnStoreQueryLines(name string, events []columnStoreDecodedEvent) []string {
	switch name {
	case "q1":
		counts := make(map[string]int)
		for _, event := range events {
			counts[event.Kind]++
		}
		return formatIntMapLines(name, counts)
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
		return formatIntMapLines(name, counts)
	case "q3":
		counts := make(map[string]int)
		for _, event := range events {
			hour := (event.TimeUS / 3_600_000_000) % 24
			counts[fmt.Sprintf("hour_%02d", hour)]++
		}
		return formatIntMapLines(name, counts)
	case "q4a":
		mins := make(map[string]int64)
		for _, event := range events {
			if cur, ok := mins[event.Did]; !ok || event.TimeUS < cur {
				mins[event.Did] = event.TimeUS
			}
		}
		return formatInt64MapLines(name, mins)
	case "q4b":
		maxs := make(map[string]int64)
		for _, event := range events {
			if cur, ok := maxs[event.Did]; !ok || event.TimeUS > cur {
				maxs[event.Did] = event.TimeUS
			}
		}
		return formatInt64MapLines(name, maxs)
	case "q5", "q5_metadata":
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
		return lines
	default:
		return nil
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

func columnStoreSuiteManifestControlUsage(root string) (int64, error) {
	var total int64
	for _, rel := range columnStoreSuiteManifestControlFiles {
		path := filepath.Join(root, rel)
		info, err := os.Stat(path)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", rel, err)
		}
		if info.IsDir() {
			return 0, fmt.Errorf("%s is a directory", rel)
		}
		total += info.Size()
	}
	return total, nil
}

func renderColumnStoreSuiteMarkdown(report columnStoreSuiteReport) string {
	var sb strings.Builder
	sb.WriteString("# unified_bench suite: column_store\n\n")
	sb.WriteString(fmt.Sprintf("- profile: `%s`\n", report.Profile))
	sb.WriteString(fmt.Sprintf("- fixture: `%s`\n", report.Fixture))
	sb.WriteString(fmt.Sprintf("- rows: %s\n", formatInt(report.Rows)))
	sb.WriteString(fmt.Sprintf("- batchsize: %s\n", formatInt(report.BatchSize)))
	sb.WriteString(fmt.Sprintf("- forced path: `%s`\n", report.ForcedPath))
	if report.PathLabel != "" {
		sb.WriteString(fmt.Sprintf("- path-label: `%s`\n", report.PathLabel))
	}
	sb.WriteString(fmt.Sprintf("- scope: %s\n", report.ProductionScope))
	sb.WriteString(fmt.Sprintf("- physical column query: %s\n", report.PhysicalColumnQuery))
	sb.WriteString(fmt.Sprintf("- timing boundary: %s\n\n", report.StageSeparatedBoundary))

	sb.WriteString("## Stage Timings\n\n")
	sb.WriteString("| stage | ms | rows/s | MiB/s | bytes |\n")
	sb.WriteString("|---|---:|---:|---:|---:|\n")
	for _, st := range report.Stages {
		sb.WriteString(fmt.Sprintf("| `%s` | %.3f | %.3f | %.3f | %d |\n", st.Name, st.DurationMS, st.RowsPerSecond, st.MiBPerSecond, st.Bytes))
	}
	sb.WriteString("\n")

	sb.WriteString("## Query Throughput And Parity\n\n")
	sb.WriteString("| query | plan | rows/s | MiB/s | ns/row | planner ms | scan ms | reduce/hash ms | workers | scheduled granules | skipped granules | B/read | rows materialized | cache hit/miss | hash parity |\n")
	sb.WriteString("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n")
	for _, q := range report.Queries {
		parity := "pass"
		if p, ok := report.Parity[q.Name]; ok && !p.Pass {
			parity = "FAIL"
		}
		sb.WriteString(fmt.Sprintf("| `%s` | `%s` | %.3f | %.3f | %.1f | %.3f | %.3f | %.3f | %d | %d | %d | %d | %d | %d/%d | %s |\n",
			q.Name, q.PlanLabel, q.RowsPerSecond, q.MiBPerSecond, q.NsPerRow, q.PlannerDurationMS, q.ScanDurationMS, q.ReduceDurationMS, q.WorkerCount, q.ScheduledGranules, q.SkippedGranules, q.BytesRead, q.RowMaterializations, q.CacheHits, q.CacheMisses, parity))
	}
	sb.WriteString("\n")

	sb.WriteString("## Byte Accounting\n\n")
	sb.WriteString(fmt.Sprintf("- source_document_bytes: %d\n", report.ByteAccounting.SourceDocumentBytes))
	sb.WriteString(fmt.Sprintf("- retained_payload_bytes: %d\n", report.ByteAccounting.RetainedPayloadBytes))
	sb.WriteString(fmt.Sprintf("- column_asset_bytes: %d\n", report.ByteAccounting.ColumnAssetBytes))
	sb.WriteString(fmt.Sprintf("- manifest_control_bytes: %d\n", report.ByteAccounting.ManifestControlBytes))
	sb.WriteString(fmt.Sprintf("- command_wal_bytes: %d\n", report.ByteAccounting.CommandWALBytes))
	sb.WriteString(fmt.Sprintf("- total_reconstructable_bytes: %d\n", report.ByteAccounting.TotalReconstructableBytes))
	sb.WriteString(fmt.Sprintf("- db_total_bytes: %d across %d files\n\n", report.ByteAccounting.DBTotalBytes, report.ByteAccounting.DBTotalFiles))

	sb.WriteString("## Manifest Recovery\n\n")
	sb.WriteString(fmt.Sprintf("- active_generation: %d\n", report.Manifest.ActiveGeneration))
	sb.WriteString(fmt.Sprintf("- recovery_authoritative_generation: %d\n", report.Manifest.RecoveryAuthoritativeGeneration))
	sb.WriteString(fmt.Sprintf("- applied_command_lsn: %d\n", report.Manifest.AppliedCommandLSN))
	sb.WriteString(fmt.Sprintf("- manifest_root: %d\n", report.Manifest.ManifestRoot))
	sb.WriteString(fmt.Sprintf("- schema_hash: %d\n\n", report.Manifest.SchemaHash))

	sb.WriteString("## Forced Path Labels\n\n")
	sb.WriteString(fmt.Sprintf("- supported: `%s`\n", strings.Join(report.SupportedForcedPaths, "`, `")))
	sb.WriteString(fmt.Sprintf("- fail-closed until physical planner paths exist: `%s`\n", strings.Join(report.UnsupportedForcedPaths, "`, `")))
	if report.Artifacts.ColumnJSON != "" {
		sb.WriteString("\n## Artifacts\n\n")
		sb.WriteString(fmt.Sprintf("- column JSON: `%s`\n", report.Artifacts.ColumnJSON))
		sb.WriteString(fmt.Sprintf("- column markdown: `%s`\n", report.Artifacts.ColumnMarkdown))
		sb.WriteString(fmt.Sprintf("- column HTML: `%s`\n", report.Artifacts.ColumnHTML))
		sb.WriteString(fmt.Sprintf("- benchprof JSON: `%s`\n", report.Artifacts.BenchprofJSON))
		sb.WriteString(fmt.Sprintf("- benchprof markdown: `%s`\n", report.Artifacts.BenchprofMarkdown))
		sb.WriteString(fmt.Sprintf("- insights markdown: `%s`\n", report.Artifacts.InsightsMarkdown))
		sb.WriteString(fmt.Sprintf("- insights JSON: `%s`\n", report.Artifacts.InsightsJSON))
		sb.WriteString(fmt.Sprintf("- insights HTML: `%s`\n", report.Artifacts.InsightsHTML))
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
	const aggregateTestName = "column_store"

	cfg := baseCfg
	cfg.Keys = report.Rows
	cfg.BatchSize = report.BatchSize
	cfg.Profile = profile
	cfg.DBsArg = "treedb"
	testOrder := []string{aggregateTestName, "alias_full_scan_from_q1", "alias_prefix_scan_from_q4a", "column_store_q1", "column_store_q2", "column_store_q3", "column_store_q4a", "column_store_q4b", "column_store_q5", "column_store_q5_metadata"}
	cfg.TestsArg = strings.Join(testOrder, ",")
	dbName := "TreeDB Column Store"
	results := make(map[string]map[string]float64)
	displayNames := map[string]string{
		aggregateTestName:            "Column store query phase",
		"alias_full_scan_from_q1":    "Alias full scan from q1",
		"alias_prefix_scan_from_q4a": "Alias prefix scan from q4a",
		"column_store_q1":            "Column q1",
		"column_store_q2":            "Column q2",
		"column_store_q3":            "Column q3",
		"column_store_q4a":           "Column q4a",
		"column_store_q4b":           "Column q4b",
		"column_store_q5":            "Column q5",
		"column_store_q5_metadata":   "Column q5 metadata",
	}
	byName := make(map[string]columnStoreQueryMetric, len(report.Queries))
	var queryDurationMS float64
	var queryMaterializations int
	for _, q := range report.Queries {
		byName[q.Name] = q
		queryDurationMS += q.DurationMS
		queryMaterializations += q.RowMaterializations
		results["column_store_"+q.Name] = map[string]float64{dbName: q.RowsPerSecond}
	}
	if queryDurationMS > 0 {
		results[aggregateTestName] = map[string]float64{dbName: float64(queryMaterializations) / (queryDurationMS / 1000)}
	}
	if q, ok := byName["q1"]; ok {
		results["alias_full_scan_from_q1"] = map[string]float64{dbName: q.RowsPerSecond}
	}
	if q, ok := byName["q4a"]; ok {
		results["alias_prefix_scan_from_q4a"] = map[string]float64{dbName: q.RowsPerSecond}
	}
	return BenchRun{
		Config:       cfg,
		Instances:    []*DBInstance{{Name: "treedb_column_store", Wrapper: &columnStoreSuiteDBLabel{name: dbName}, Dir: dataDir}},
		TestOrder:    testOrder,
		DisplayNames: displayNames,
		Results:      results,
		CheckpointDurations: map[string]map[string]time.Duration{
			checkpointPostRunLabel: {dbName: checkpointDuration},
		},
		TreeDBStats: map[string]map[string]string{dbName: stats},
		DiskUsage:   map[string]dirDiskUsage{dbName: {TotalBytes: uint64(report.ByteAccounting.DBTotalBytes), TotalFiles: report.ByteAccounting.DBTotalFiles}},
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
