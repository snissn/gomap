package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	defaultDocs              = 1_000_000
	defaultBatchSize         = 8_000
	defaultCollectionName    = "bench_shape_insert_2"
	indexVacuumModeNone      = "none"
	indexVacuumModeOnline    = "online"
	indexVacuumModeOffline   = "offline"
	collectionFixtureCities  = 64
	collectionFixturePad     = "01234567890123456789"
	maxFixtureTopFileEntries = 20
)

type config struct {
	Dir                        string
	Reset                      bool
	Docs                       int
	BatchSize                  int
	Collection                 string
	DocumentFormat             collections.DocumentFormat
	IndexCount                 int
	Profile                    treedb.Profile
	DataOuterLeavesInValueLog  bool
	IndexOuterLeavesInValueLog bool
	ChunkSize                  int64
	KeepRecent                 uint64
	PreferAppendAlloc          bool
	PagerSyncConcurrency       int
	DisableBackgroundPrune     bool
	PruneInterval              time.Duration
	PruneMaxPages              int
	PruneMaxDuration           time.Duration
	Checkpoint                 bool
	CheckpointEachBatch        bool
	ReopenVerify               bool
	VerifySamples              int
	ValueLogRewrite            bool
	ValueLogGC                 bool
	IndexVacuum                string
	Progress                   bool
	JSONOutput                 bool
	CPUProfile                 string
	MemProfile                 string
	createdTempDir             bool
}

type timingSummary struct {
	Seconds   float64 `json:"seconds"`
	SecPerOp  float64 `json:"sec_per_op,omitempty"`
	OpsPerSec float64 `json:"ops_per_sec,omitempty"`
}

type diskUsageSummary struct {
	TotalBytes  uint64        `json:"total_bytes"`
	BytesPerDoc float64       `json:"bytes_per_doc,omitempty"`
	FileCount   int           `json:"file_count"`
	TopFiles    []fileSummary `json:"top_files,omitempty"`
}

type fileSummary struct {
	Path  string `json:"path"`
	Bytes uint64 `json:"bytes"`
}

type insertPhaseSummary struct {
	Documents                     int                   `json:"documents"`
	Indexes                       int                   `json:"indexes"`
	Runs                          int                   `json:"runs"`
	PrepareDocumentsNSPerDoc      float64               `json:"prepare_documents_ns_per_doc,omitempty"`
	IndexStateExtractionNSPerDoc  float64               `json:"index_state_extraction_ns_per_doc,omitempty"`
	DuplicatePreflightNSPerDoc    float64               `json:"duplicate_preflight_ns_per_doc,omitempty"`
	UniquePreflightNSPerDoc       float64               `json:"unique_preflight_ns_per_doc,omitempty"`
	TemplateRunBuildNSPerDoc      float64               `json:"template_run_build_ns_per_doc,omitempty"`
	PrimaryRunBuildNSPerDoc       float64               `json:"primary_run_build_ns_per_doc,omitempty"`
	IndexStateRunBuildNSPerDoc    float64               `json:"index_state_run_build_ns_per_doc,omitempty"`
	SecondaryRunBuildNSPerDoc     float64               `json:"secondary_run_build_ns_per_doc,omitempty"`
	PublishNSPerDoc               float64               `json:"publish_ns_per_doc,omitempty"`
	SecondaryEntriesPerDoc        float64               `json:"secondary_entries_per_doc,omitempty"`
	SecondaryKeyBytesPerDoc       float64               `json:"secondary_key_bytes_per_doc,omitempty"`
	SecondarySortedRunsPerBatch   float64               `json:"secondary_sorted_runs_per_batch,omitempty"`
	SecondaryUnsortedRunsPerBatch float64               `json:"secondary_unsorted_runs_per_batch,omitempty"`
	SecondaryRuns                 []secondaryRunSummary `json:"secondary_runs,omitempty"`
}

type secondaryRunSummary struct {
	IndexName      string  `json:"index_name"`
	Runs           int     `json:"runs"`
	Entries        int     `json:"entries"`
	KeyBytes       int     `json:"key_bytes"`
	SortedRuns     int     `json:"sorted_runs"`
	UnsortedRuns   int     `json:"unsorted_runs"`
	BuildNSPerDoc  float64 `json:"build_ns_per_doc,omitempty"`
	EntriesPerDoc  float64 `json:"entries_per_doc,omitempty"`
	KeyBytesPerDoc float64 `json:"key_bytes_per_doc,omitempty"`
}

type rewriteSummary struct {
	Enabled               bool          `json:"enabled"`
	RewriteTiming         timingSummary `json:"rewrite_timing,omitempty"`
	GCTiming              timingSummary `json:"gc_timing,omitempty"`
	DiskBytesBefore       uint64        `json:"disk_bytes_before,omitempty"`
	DiskBytesAfterRewrite uint64        `json:"disk_bytes_after_rewrite,omitempty"`
	DiskBytesAfterGC      uint64        `json:"disk_bytes_after_gc,omitempty"`
	SegmentsBefore        int           `json:"segments_before,omitempty"`
	SegmentsAfter         int           `json:"segments_after,omitempty"`
	RecordsCopied         int           `json:"records_copied,omitempty"`
	ValueRecordsCopied    int           `json:"value_records_copied,omitempty"`
	ValueBytesCopied      int64         `json:"value_bytes_copied,omitempty"`
	TemplateRecordsKept   int           `json:"template_records_kept,omitempty"`
	GCSegmentsDeleted     int           `json:"gc_segments_deleted,omitempty"`
	GCBytesDeleted        int64         `json:"gc_bytes_deleted,omitempty"`
}

type indexStorageSummary struct {
	IndexDBBytes                uint64 `json:"index_db_bytes,omitempty"`
	PagesTotal                  uint64 `json:"pages_total,omitempty"`
	KeepRecent                  uint64 `json:"keep_recent,omitempty"`
	PreferAppendAlloc           bool   `json:"prefer_append_alloc"`
	FreelistReclaimablePages    uint64 `json:"freelist_reclaimable_pages,omitempty"`
	FreelistAllocPagesTotal     uint64 `json:"freelist_alloc_pages_total,omitempty"`
	FreelistAppendPagesTotal    uint64 `json:"freelist_append_pages_total,omitempty"`
	FreelistReusePagesTotal     uint64 `json:"freelist_reuse_pages_total,omitempty"`
	FreelistFreePagesTotal      uint64 `json:"freelist_free_pages_total,omitempty"`
	GraveyardBatches            uint64 `json:"graveyard_batches,omitempty"`
	GraveyardPages              uint64 `json:"graveyard_pages,omitempty"`
	CollectionRoots             uint64 `json:"collection_roots,omitempty"`
	CollectionLeafRefRoots      uint64 `json:"collection_leafref_roots,omitempty"`
	CollectionPagerRoots        uint64 `json:"collection_pager_roots,omitempty"`
	CollectionRootPages         uint64 `json:"collection_root_pages,omitempty"`
	CollectionRootLeafPages     uint64 `json:"collection_root_leaf_pages,omitempty"`
	CollectionRootInternalPages uint64 `json:"collection_root_internal_pages,omitempty"`
	CollectionRootDuplicateRefs uint64 `json:"collection_root_duplicate_refs,omitempty"`
	PruneEnabled                bool   `json:"prune_enabled"`
	PruneRuns                   uint64 `json:"prune_runs,omitempty"`
	PrunePagesFreed             uint64 `json:"prune_pages_freed,omitempty"`
}

type indexVacuumSummary struct {
	Mode               string              `json:"mode"`
	Enabled            bool                `json:"enabled"`
	Timing             timingSummary       `json:"timing,omitempty"`
	DiskBytesBefore    uint64              `json:"disk_bytes_before,omitempty"`
	DiskBytesAfter     uint64              `json:"disk_bytes_after,omitempty"`
	IndexDBBytesBefore uint64              `json:"index_db_bytes_before,omitempty"`
	IndexDBBytesAfter  uint64              `json:"index_db_bytes_after,omitempty"`
	StorageBefore      indexStorageSummary `json:"storage_before,omitempty"`
	StorageAfter       indexStorageSummary `json:"storage_after,omitempty"`
}

type verifySummary struct {
	Enabled bool `json:"enabled"`
	Samples int  `json:"samples,omitempty"`
}

type loadSummary struct {
	GeneratedAt                   string              `json:"generated_at"`
	Dir                           string              `json:"dir"`
	CreatedTempDir                bool                `json:"created_temp_dir,omitempty"`
	Collection                    string              `json:"collection"`
	DocumentFormat                string              `json:"document_format"`
	Profile                       string              `json:"profile"`
	Docs                          int                 `json:"docs"`
	BatchSize                     int                 `json:"batch_size"`
	Batches                       int                 `json:"batches"`
	IndexCount                    int                 `json:"index_count"`
	DataOuterLeavesInValueLog     bool                `json:"data_outer_leaves_in_value_log"`
	IndexOuterLeavesInValueLog    bool                `json:"index_outer_leaves_in_value_log"`
	ChunkSize                     int64               `json:"chunk_size,omitempty"`
	KeepRecent                    uint64              `json:"keep_recent"`
	PreferAppendAlloc             bool                `json:"prefer_append_alloc"`
	PagerSyncConcurrency          int                 `json:"pager_sync_concurrency,omitempty"`
	DisableBackgroundPrune        bool                `json:"disable_background_prune,omitempty"`
	PruneInterval                 string              `json:"prune_interval,omitempty"`
	PruneMaxPages                 int                 `json:"prune_max_pages,omitempty"`
	PruneMaxDuration              string              `json:"prune_max_duration,omitempty"`
	WallTiming                    timingSummary       `json:"wall_timing"`
	GenerationTiming              timingSummary       `json:"generation_timing"`
	InsertTiming                  timingSummary       `json:"insert_timing"`
	CheckpointTiming              timingSummary       `json:"checkpoint_timing,omitempty"`
	InsertPhases                  insertPhaseSummary  `json:"insert_phases"`
	IndexStorageBeforeMaintenance indexStorageSummary `json:"index_storage_before_maintenance"`
	DiskUsageBeforeMaintenance    diskUsageSummary    `json:"disk_usage_before_maintenance"`
	DiskUsageFinal                diskUsageSummary    `json:"disk_usage_final"`
	Rewrite                       rewriteSummary      `json:"rewrite,omitempty"`
	IndexVacuum                   indexVacuumSummary  `json:"index_vacuum,omitempty"`
	IndexStorageFinal             indexStorageSummary `json:"index_storage_final"`
	Verify                        verifySummary       `json:"verify"`
	CPUProfile                    string              `json:"cpu_profile,omitempty"`
	MemProfile                    string              `json:"mem_profile,omitempty"`
	GoVersion                     string              `json:"go_version"`
	GOOS                          string              `json:"goos"`
	GOARCH                        string              `json:"goarch"`
}

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "collection-load-fixture: %v\n", err)
		os.Exit(2)
	}
	summary, err := runFixture(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection-load-fixture: %v\n", err)
		os.Exit(1)
	}
	if cfg.JSONOutput {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(summary); err != nil {
			fmt.Fprintf(os.Stderr, "collection-load-fixture: write json summary: %v\n", err)
			os.Exit(1)
		}
		return
	}
	printHumanSummary(os.Stdout, summary)
}

func parseConfig(args []string, output io.Writer) (config, error) {
	cfg := config{
		Docs:                       defaultDocs,
		BatchSize:                  defaultBatchSize,
		Collection:                 defaultCollectionName,
		DocumentFormat:             collections.DocumentFormatTemplateV1,
		IndexCount:                 2,
		Profile:                    treedb.ProfileFast,
		DataOuterLeavesInValueLog:  true,
		IndexOuterLeavesInValueLog: true,
		KeepRecent:                 1,
		PreferAppendAlloc:          false,
		Checkpoint:                 true,
		ReopenVerify:               true,
		VerifySamples:              8,
		ValueLogGC:                 true,
		IndexVacuum:                indexVacuumModeNone,
		Progress:                   true,
	}
	var documentFormat string
	var profile string
	fs := flag.NewFlagSet("collection-load-fixture", flag.ContinueOnError)
	if output == nil {
		output = io.Discard
	}
	fs.SetOutput(output)
	fs.StringVar(&cfg.Dir, "dir", "", "directory to create and keep; empty creates a kept OS temp directory")
	fs.BoolVar(&cfg.Reset, "reset", false, "remove -dir before loading; ignored when -dir is empty")
	fs.IntVar(&cfg.Docs, "docs", cfg.Docs, "number of documents to insert")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "documents per InsertBatch call")
	fs.StringVar(&cfg.Collection, "collection", cfg.Collection, "collection name")
	fs.StringVar(&documentFormat, "format", string(cfg.DocumentFormat), "document format: json or template-v1")
	fs.IntVar(&cfg.IndexCount, "indexes", cfg.IndexCount, "secondary index count for the benchmark shape: 0, 1, 2, or 3")
	fs.StringVar(&profile, "profile", string(cfg.Profile), "TreeDB profile: fast, wal_on_fast, durable, or bench")
	fs.BoolVar(&cfg.DataOuterLeavesInValueLog, "data-outer-leaves-in-vlog", cfg.DataOuterLeavesInValueLog, "store collection primary/index-state outer leaves through the value log")
	fs.BoolVar(&cfg.IndexOuterLeavesInValueLog, "index-outer-leaves-in-vlog", cfg.IndexOuterLeavesInValueLog, "store secondary-index outer leaves through the value log")
	fs.Int64Var(&cfg.ChunkSize, "chunk-size", 0, "override TreeDB pager chunk size in bytes")
	fs.Uint64Var(&cfg.KeepRecent, "keep-recent", cfg.KeepRecent, "TreeDB index page versions to retain before page reuse")
	fs.BoolVar(&cfg.PreferAppendAlloc, "prefer-append-alloc", cfg.PreferAppendAlloc, "append new index pages instead of reusing the freelist")
	fs.IntVar(&cfg.PagerSyncConcurrency, "pager-sync-concurrency", 0, "override TreeDB pager sync concurrency")
	fs.BoolVar(&cfg.DisableBackgroundPrune, "disable-background-prune", false, "disable background page pruning and prune on the commit path")
	fs.DurationVar(&cfg.PruneInterval, "prune-interval", 0, "TreeDB background prune interval (0 uses engine default)")
	fs.IntVar(&cfg.PruneMaxPages, "prune-max-pages", 0, "TreeDB max pages freed per prune tick (0 uses engine default; <0 unlimited)")
	fs.DurationVar(&cfg.PruneMaxDuration, "prune-max-duration", 0, "TreeDB max prune duration per tick (0 uses engine default; <0 unlimited)")
	fs.BoolVar(&cfg.Checkpoint, "checkpoint", cfg.Checkpoint, "checkpoint after loading")
	fs.BoolVar(&cfg.CheckpointEachBatch, "checkpoint-each-batch", false, "checkpoint after every batch")
	fs.BoolVar(&cfg.ReopenVerify, "reopen-verify", cfg.ReopenVerify, "close, reopen, and verify sampled primary/index reads")
	fs.IntVar(&cfg.VerifySamples, "verify-samples", cfg.VerifySamples, "sample count for -reopen-verify")
	fs.BoolVar(&cfg.ValueLogRewrite, "vlog-rewrite", false, "run TreeDB online value-log rewrite after loading")
	fs.BoolVar(&cfg.ValueLogGC, "vlog-gc", cfg.ValueLogGC, "run value-log GC after -vlog-rewrite")
	fs.StringVar(&cfg.IndexVacuum, "index-vacuum", cfg.IndexVacuum, "run index vacuum after loading: none, online, or offline")
	fs.BoolVar(&cfg.Progress, "progress", cfg.Progress, "print load progress to stderr")
	fs.BoolVar(&cfg.JSONOutput, "json", false, "print summary as JSON")
	fs.StringVar(&cfg.CPUProfile, "cpuprofile", "", "write CPU profile to this path")
	fs.StringVar(&cfg.MemProfile, "memprofile", "", "write heap profile to this path")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	parsedFormat, err := parseDocumentFormat(documentFormat)
	if err != nil {
		return cfg, err
	}
	cfg.DocumentFormat = parsedFormat
	parsedProfile, err := parseProfile(profile)
	if err != nil {
		return cfg, err
	}
	cfg.Profile = parsedProfile
	if cfg.Docs <= 0 {
		return cfg, fmt.Errorf("-docs must be > 0")
	}
	if cfg.BatchSize <= 0 {
		return cfg, fmt.Errorf("-batch-size must be > 0")
	}
	if cfg.IndexCount < 0 || cfg.IndexCount > 3 {
		return cfg, fmt.Errorf("-indexes must be 0, 1, 2, or 3")
	}
	if strings.TrimSpace(cfg.Collection) == "" {
		return cfg, fmt.Errorf("-collection cannot be empty")
	}
	if cfg.ChunkSize < 0 {
		return cfg, fmt.Errorf("-chunk-size must be >= 0")
	}
	if cfg.KeepRecent == 0 {
		return cfg, fmt.Errorf("-keep-recent must be > 0")
	}
	if cfg.PagerSyncConcurrency < 0 {
		return cfg, fmt.Errorf("-pager-sync-concurrency must be >= 0")
	}
	if cfg.PruneInterval < 0 {
		return cfg, fmt.Errorf("-prune-interval must be >= 0")
	}
	if cfg.VerifySamples < 0 {
		return cfg, fmt.Errorf("-verify-samples must be >= 0")
	}
	cfg.IndexVacuum = strings.ToLower(strings.TrimSpace(cfg.IndexVacuum))
	switch cfg.IndexVacuum {
	case "", indexVacuumModeNone:
		cfg.IndexVacuum = indexVacuumModeNone
	case indexVacuumModeOnline, indexVacuumModeOffline:
	default:
		return cfg, fmt.Errorf("unsupported -index-vacuum %q; use none, online, or offline", cfg.IndexVacuum)
	}
	return cfg, nil
}

func parseDocumentFormat(raw string) (collections.DocumentFormat, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "json":
		return collections.DocumentFormatJSON, nil
	case string(collections.DocumentFormatTemplateV1):
		return collections.DocumentFormatTemplateV1, nil
	case "":
		return "", fmt.Errorf("-format cannot be empty; use json or template-v1")
	default:
		return "", fmt.Errorf("unsupported -format %q", raw)
	}
}

func parseProfile(raw string) (treedb.Profile, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "fast", "production_fast", "backend_direct_fast", "backend_direct", "cached":
		return treedb.ProfileFast, nil
	case "wal_on_fast", "production_wal_on_fast", "backend_direct_wal_on_fast":
		return treedb.ProfileWALOnFast, nil
	case "durable":
		return treedb.ProfileDurable, nil
	case "bench":
		return treedb.ProfileBench, nil
	default:
		return "", fmt.Errorf("unsupported -profile %q", raw)
	}
}

func runFixture(cfg config) (loadSummary, error) {
	dir, createdTempDir, err := prepareFixtureDir(cfg)
	if err != nil {
		return loadSummary{}, err
	}
	cfg.Dir = dir
	cfg.createdTempDir = createdTempDir

	stopCPU, err := startCPUProfile(cfg.CPUProfile)
	if err != nil {
		return loadSummary{}, err
	}
	defer stopCPU()

	backend, cleanup, err := openBackend(cfg)
	if err != nil {
		return loadSummary{}, err
	}

	closed := false
	defer func() {
		if !closed {
			_ = cleanup()
		}
	}()

	collection, err := createFixtureCollection(backend, cfg)
	if err != nil {
		return loadSummary{}, err
	}

	var insertStats collections.CollectionInsertStats
	secondaryRuns := make(map[string]*secondaryRunSummary)
	var generationElapsed time.Duration
	var insertElapsed time.Duration
	var checkpointElapsed time.Duration
	wallStart := time.Now()
	batches := 0
	lastProgress := time.Now()

	for inserted := 0; inserted < cfg.Docs; {
		batchSize := cfg.BatchSize
		if remaining := cfg.Docs - inserted; remaining < batchSize {
			batchSize = remaining
		}
		genStart := time.Now()
		ids, docs, err := documentBatch(cfg.DocumentFormat, inserted, batchSize)
		if err != nil {
			return loadSummary{}, err
		}
		generationElapsed += time.Since(genStart)

		insertStart := time.Now()
		if _, err := collection.InsertBatch(ids, docs); err != nil {
			return loadSummary{}, fmt.Errorf("insert batch starting at document %d: %w", inserted, err)
		}
		insertElapsed += time.Since(insertStart)
		stats := collection.LastInsertStats()
		addInsertStats(&insertStats, stats)
		addSecondaryRuns(secondaryRuns, stats.SecondaryRuns)
		batches++
		inserted += batchSize

		if cfg.CheckpointEachBatch {
			checkpointStart := time.Now()
			if err := backend.Checkpoint(); err != nil {
				return loadSummary{}, fmt.Errorf("checkpoint after batch %d: %w", batches, err)
			}
			checkpointElapsed += time.Since(checkpointStart)
		}
		if cfg.Progress && time.Since(lastProgress) >= time.Second {
			fmt.Fprintf(os.Stderr, "inserted %d/%d documents into %s\n", inserted, cfg.Docs, cfg.Dir)
			lastProgress = time.Now()
		}
	}

	if err := collection.Flush(); err != nil {
		return loadSummary{}, fmt.Errorf("flush collection: %w", err)
	}
	if cfg.Checkpoint {
		checkpointStart := time.Now()
		if err := backend.Checkpoint(); err != nil {
			return loadSummary{}, fmt.Errorf("final checkpoint: %w", err)
		}
		checkpointElapsed += time.Since(checkpointStart)
	}

	beforeIndexStorage, err := captureIndexStorageSummary(cfg, backend)
	if err != nil {
		return loadSummary{}, err
	}
	beforeMaintenance, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return loadSummary{}, err
	}
	rewrite, err := maybeRewriteValueLog(cfg, backend, beforeMaintenance.TotalBytes)
	if err != nil {
		return loadSummary{}, err
	}
	indexVacuum, finalIndexStorage, err := maybeVacuumIndex(cfg, backend, cleanup, &closed)
	if err != nil {
		return loadSummary{}, err
	}
	wallElapsed := time.Since(wallStart)

	if !closed {
		if err := cleanup(); err != nil {
			closed = true
			return loadSummary{}, fmt.Errorf("close backend: %w", err)
		}
		closed = true
	}

	verify := verifySummary{Enabled: cfg.ReopenVerify}
	if cfg.ReopenVerify {
		samples, err := verifyReopen(cfg)
		if err != nil {
			return loadSummary{}, err
		}
		verify.Samples = samples
	}

	finalUsage, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return loadSummary{}, err
	}
	if err := writeMemProfile(cfg.MemProfile); err != nil {
		return loadSummary{}, err
	}

	return loadSummary{
		GeneratedAt:                   time.Now().UTC().Format(time.RFC3339),
		Dir:                           cfg.Dir,
		CreatedTempDir:                cfg.createdTempDir,
		Collection:                    cfg.Collection,
		DocumentFormat:                string(cfg.DocumentFormat),
		Profile:                       string(cfg.Profile),
		Docs:                          cfg.Docs,
		BatchSize:                     cfg.BatchSize,
		Batches:                       batches,
		IndexCount:                    cfg.IndexCount,
		DataOuterLeavesInValueLog:     cfg.DataOuterLeavesInValueLog,
		IndexOuterLeavesInValueLog:    cfg.IndexOuterLeavesInValueLog,
		ChunkSize:                     cfg.ChunkSize,
		KeepRecent:                    cfg.KeepRecent,
		PreferAppendAlloc:             cfg.PreferAppendAlloc,
		PagerSyncConcurrency:          cfg.PagerSyncConcurrency,
		DisableBackgroundPrune:        cfg.DisableBackgroundPrune,
		PruneInterval:                 durationString(cfg.PruneInterval),
		PruneMaxPages:                 cfg.PruneMaxPages,
		PruneMaxDuration:              durationString(cfg.PruneMaxDuration),
		WallTiming:                    timing(wallElapsed, cfg.Docs),
		GenerationTiming:              timing(generationElapsed, cfg.Docs),
		InsertTiming:                  timing(insertElapsed, cfg.Docs),
		CheckpointTiming:              timing(checkpointElapsed, cfg.Docs),
		InsertPhases:                  summarizeInsertPhases(insertStats, secondaryRuns, cfg.Docs, batches),
		IndexStorageBeforeMaintenance: beforeIndexStorage,
		DiskUsageBeforeMaintenance:    beforeMaintenance,
		DiskUsageFinal:                finalUsage,
		Rewrite:                       rewrite,
		IndexVacuum:                   indexVacuum,
		IndexStorageFinal:             finalIndexStorage,
		Verify:                        verify,
		CPUProfile:                    cfg.CPUProfile,
		MemProfile:                    cfg.MemProfile,
		GoVersion:                     runtime.Version(),
		GOOS:                          runtime.GOOS,
		GOARCH:                        runtime.GOARCH,
	}, nil
}

func prepareFixtureDir(cfg config) (string, bool, error) {
	if strings.TrimSpace(cfg.Dir) == "" {
		dir, err := os.MkdirTemp("", "treedb_collection_fixture_")
		if err != nil {
			return "", false, fmt.Errorf("create temp fixture dir: %w", err)
		}
		abs, err := filepath.Abs(dir)
		if err != nil {
			return "", false, err
		}
		return abs, true, nil
	}
	dir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return "", false, err
	}
	if cfg.Reset {
		if err := os.RemoveAll(dir); err != nil {
			return "", false, fmt.Errorf("reset fixture dir %s: %w", dir, err)
		}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", false, fmt.Errorf("create fixture dir %s: %w", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false, fmt.Errorf("read fixture dir %s: %w", dir, err)
	}
	if len(entries) != 0 {
		return "", false, fmt.Errorf("fixture dir %s is not empty; choose an empty dir or pass -reset", dir)
	}
	return dir, false, nil
}

func openBackend(cfg config) (*backenddb.DB, func() error, error) {
	opts := treedb.OptionsFor(cfg.Profile, cfg.Dir)
	opts.IndexOuterLeavesInValueLog = cfg.DataOuterLeavesInValueLog || cfg.IndexOuterLeavesInValueLog
	opts.IndexInternalBaseDelta = !opts.IndexOuterLeavesInValueLog
	opts.KeepRecent = cfg.KeepRecent
	opts.PreferAppendAlloc = cfg.PreferAppendAlloc
	opts.DisableBackgroundPrune = cfg.DisableBackgroundPrune
	if cfg.ChunkSize > 0 {
		opts.ChunkSize = cfg.ChunkSize
	}
	if cfg.PagerSyncConcurrency > 0 {
		opts.PagerSyncConcurrency = cfg.PagerSyncConcurrency
	}
	if cfg.PruneInterval > 0 {
		opts.PruneInterval = cfg.PruneInterval
	}
	if cfg.PruneMaxPages != 0 {
		opts.PruneMaxPages = cfg.PruneMaxPages
	}
	if cfg.PruneMaxDuration != 0 {
		opts.PruneMaxDuration = cfg.PruneMaxDuration
	}
	open := treedb.OpenBackend
	if opts.IndexOuterLeavesInValueLog {
		open = treedb.OpenBackendWithCachedLeafLog
	}
	backend, cleanup, err := open(opts)
	if err != nil {
		return nil, nil, fmt.Errorf("open backend: %w", err)
	}
	return backend, cleanup, nil
}

func createFixtureCollection(backend *backenddb.DB, cfg config) (*collections.Collection, error) {
	manager := collections.NewCollectionManager(backend)
	indexes := collectionShapeIndexes(cfg.IndexCount)
	for i := range indexes {
		indexes[i].StoragePolicy = rootStoragePolicy(cfg.IndexOuterLeavesInValueLog)
	}
	_, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: cfg.Collection,
		Options: collections.CollectionOptions{
			DocumentFormat:          cfg.DocumentFormat,
			DataRootStoragePolicy:   rootStoragePolicy(cfg.DataOuterLeavesInValueLog),
			IndexStateStoragePolicy: rootStoragePolicy(cfg.DataOuterLeavesInValueLog),
		},
		Indexes: indexes,
	})
	if err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}
	collection, err := manager.OpenCollection(cfg.Collection)
	if err != nil {
		return nil, fmt.Errorf("open collection: %w", err)
	}
	return collection, nil
}

func rootStoragePolicy(outerLeavesInValueLog bool) collections.RootStoragePolicy {
	if outerLeavesInValueLog {
		return collections.RootStorageCompressed
	}
	return collections.RootStorageFast
}

func collectionShapeIndexes(indexCount int) []collections.IndexDefinition {
	switch indexCount {
	case 0:
		return nil
	case 1:
		return []collections.IndexDefinition{{Name: "email_idx", Field: "email", Unique: true}}
	case 2:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", Unique: true},
			{Name: "city_idx", Field: "city"},
		}
	case 3:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", Unique: true},
			{Name: "city_idx", Field: "city"},
			{Name: "name_idx", Field: "name"},
		}
	default:
		panic(fmt.Sprintf("unsupported index count %d", indexCount))
	}
}

func documentBatch(format collections.DocumentFormat, start, count int) ([][]byte, [][]byte, error) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	var templateEncoder collections.TemplateV1Encoder
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = documentID(docNum)
		doc, err := document(format, &templateEncoder, docNum)
		if err != nil {
			return nil, nil, err
		}
		docs[i] = doc
	}
	return ids, docs, nil
}

func document(format collections.DocumentFormat, encoder *collections.TemplateV1Encoder, n int) ([]byte, error) {
	if format == collections.DocumentFormatTemplateV1 {
		if encoder == nil {
			encoder = &collections.TemplateV1Encoder{}
		}
		return encoder.EncodeDocument(
			[]string{"name", "email", "city", "pad"},
			[]any{
				fmt.Sprintf("user-%09d", n),
				fmt.Sprintf("user-%09d@example.com", n),
				fmt.Sprintf("city-%02d", n%collectionFixtureCities),
				collectionFixturePad,
			},
		)
	}
	return indexedJSONDocument(n), nil
}

func documentID(n int) []byte {
	out := make([]byte, 0, len("u-")+9)
	out = append(out, "u-"...)
	return appendZeroPaddedInt(out, n, 9)
}

func indexedJSONDocument(n int) []byte {
	out := make([]byte, 0, 112)
	out = append(out, `{"name":"user-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `","email":"user-`...)
	out = appendZeroPaddedInt(out, n, 9)
	out = append(out, `@example.com","city":"city-`...)
	out = appendZeroPaddedInt(out, n%collectionFixtureCities, 2)
	out = append(out, `","pad":"`...)
	out = append(out, collectionFixturePad...)
	out = append(out, `"}`...)
	return out
}

func appendZeroPaddedInt(dst []byte, n, width int) []byte {
	var scratch [20]byte
	pos := len(scratch)
	if n == 0 {
		pos--
		scratch[pos] = '0'
	} else {
		for n > 0 {
			pos--
			scratch[pos] = byte('0' + n%10)
			n /= 10
		}
	}
	for pad := width - (len(scratch) - pos); pad > 0; pad-- {
		dst = append(dst, '0')
	}
	return append(dst, scratch[pos:]...)
}

func addInsertStats(dst *collections.CollectionInsertStats, src collections.CollectionInsertStats) {
	dst.Documents += src.Documents
	dst.Indexes += src.Indexes
	dst.Runs += src.Runs
	dst.PrepareDocuments += src.PrepareDocuments
	dst.IndexStateExtraction += src.IndexStateExtraction
	dst.DuplicateDocumentPreflight += src.DuplicateDocumentPreflight
	dst.UniqueIndexPreflight += src.UniqueIndexPreflight
	dst.TemplateRunBuild += src.TemplateRunBuild
	dst.PrimaryRunBuild += src.PrimaryRunBuild
	dst.IndexStateRunBuild += src.IndexStateRunBuild
	dst.SecondaryRunBuild += src.SecondaryRunBuild
	dst.Publish += src.Publish
	dst.SecondaryEntries += src.SecondaryEntries
	dst.SecondaryKeyBytes += src.SecondaryKeyBytes
	dst.SecondarySortedRuns += src.SecondarySortedRuns
	dst.SecondaryUnsortedRuns += src.SecondaryUnsortedRuns
}

func addSecondaryRuns(dst map[string]*secondaryRunSummary, runs []collections.CollectionSecondaryRunStats) {
	for _, run := range runs {
		entry := dst[run.IndexName]
		if entry == nil {
			entry = &secondaryRunSummary{IndexName: run.IndexName}
			dst[run.IndexName] = entry
		}
		entry.Runs++
		entry.Entries += run.Entries
		entry.KeyBytes += run.KeyBytes
		if run.AlreadySorted {
			entry.SortedRuns++
		} else {
			entry.UnsortedRuns++
		}
		entry.BuildNSPerDoc += float64(run.Build.Nanoseconds())
	}
}

func summarizeInsertPhases(stats collections.CollectionInsertStats, secondaryRuns map[string]*secondaryRunSummary, docs, batches int) insertPhaseSummary {
	out := insertPhaseSummary{
		Documents: stats.Documents,
		Indexes:   stats.Indexes,
		Runs:      stats.Runs,
	}
	nsPerDoc := func(d time.Duration) float64 {
		if docs <= 0 || d <= 0 {
			return 0
		}
		return float64(d.Nanoseconds()) / float64(docs)
	}
	perDocInt := func(v int) float64 {
		if docs <= 0 || v <= 0 {
			return 0
		}
		return float64(v) / float64(docs)
	}
	perBatchInt := func(v int) float64 {
		if batches <= 0 || v <= 0 {
			return 0
		}
		return float64(v) / float64(batches)
	}
	out.PrepareDocumentsNSPerDoc = nsPerDoc(stats.PrepareDocuments)
	out.IndexStateExtractionNSPerDoc = nsPerDoc(stats.IndexStateExtraction)
	out.DuplicatePreflightNSPerDoc = nsPerDoc(stats.DuplicateDocumentPreflight)
	out.UniquePreflightNSPerDoc = nsPerDoc(stats.UniqueIndexPreflight)
	out.TemplateRunBuildNSPerDoc = nsPerDoc(stats.TemplateRunBuild)
	out.PrimaryRunBuildNSPerDoc = nsPerDoc(stats.PrimaryRunBuild)
	out.IndexStateRunBuildNSPerDoc = nsPerDoc(stats.IndexStateRunBuild)
	out.SecondaryRunBuildNSPerDoc = nsPerDoc(stats.SecondaryRunBuild)
	out.PublishNSPerDoc = nsPerDoc(stats.Publish)
	out.SecondaryEntriesPerDoc = perDocInt(stats.SecondaryEntries)
	out.SecondaryKeyBytesPerDoc = perDocInt(stats.SecondaryKeyBytes)
	out.SecondarySortedRunsPerBatch = perBatchInt(stats.SecondarySortedRuns)
	out.SecondaryUnsortedRunsPerBatch = perBatchInt(stats.SecondaryUnsortedRuns)

	keys := make([]string, 0, len(secondaryRuns))
	for key := range secondaryRuns {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		run := *secondaryRuns[key]
		if docs > 0 {
			run.BuildNSPerDoc /= float64(docs)
			run.EntriesPerDoc = float64(run.Entries) / float64(docs)
			run.KeyBytesPerDoc = float64(run.KeyBytes) / float64(docs)
		}
		out.SecondaryRuns = append(out.SecondaryRuns, run)
	}
	return out
}

func maybeRewriteValueLog(cfg config, backend *backenddb.DB, beforeBytes uint64) (rewriteSummary, error) {
	out := rewriteSummary{Enabled: cfg.ValueLogRewrite}
	if !cfg.ValueLogRewrite {
		return out, nil
	}
	out.DiskBytesBefore = beforeBytes
	rewriteStart := time.Now()
	rewriteStats, err := backend.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{})
	if err != nil {
		return out, fmt.Errorf("value-log rewrite: %w", err)
	}
	out.RewriteTiming = timing(time.Since(rewriteStart), 1)
	if err := backend.Checkpoint(); err != nil {
		return out, fmt.Errorf("checkpoint after value-log rewrite: %w", err)
	}
	afterRewrite, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return out, err
	}
	out.DiskBytesAfterRewrite = afterRewrite.TotalBytes
	out.SegmentsBefore = int(rewriteStats.SegmentsBefore)
	out.SegmentsAfter = int(rewriteStats.SegmentsAfter)
	out.RecordsCopied = int(rewriteStats.RecordsCopied)
	out.ValueRecordsCopied = int(rewriteStats.ValueRecordsCopied)
	out.ValueBytesCopied = rewriteStats.ValueBytesCopied
	out.TemplateRecordsKept = int(rewriteStats.TemplateRecordsKept)

	if cfg.ValueLogGC {
		gcStart := time.Now()
		gcStats, err := backend.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
		if err != nil {
			return out, fmt.Errorf("value-log GC after rewrite: %w", err)
		}
		out.GCTiming = timing(time.Since(gcStart), 1)
		if err := backend.Checkpoint(); err != nil {
			return out, fmt.Errorf("checkpoint after value-log GC: %w", err)
		}
		afterGC, err := directoryUsage(cfg.Dir, cfg.Docs)
		if err != nil {
			return out, err
		}
		out.DiskBytesAfterGC = afterGC.TotalBytes
		out.GCSegmentsDeleted = int(gcStats.SegmentsDeleted)
		out.GCBytesDeleted = gcStats.BytesDeleted
	}
	return out, nil
}

func maybeVacuumIndex(cfg config, backend *backenddb.DB, cleanup func() error, closed *bool) (indexVacuumSummary, indexStorageSummary, error) {
	out := indexVacuumSummary{
		Mode:    cfg.IndexVacuum,
		Enabled: cfg.IndexVacuum != indexVacuumModeNone,
	}
	if cfg.IndexVacuum == indexVacuumModeNone {
		finalStorage, err := captureIndexStorageSummary(cfg, backend)
		return out, finalStorage, err
	}

	beforeDisk, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return out, indexStorageSummary{}, err
	}
	beforeStorage, err := captureIndexStorageSummary(cfg, backend)
	if err != nil {
		return out, indexStorageSummary{}, err
	}
	out.DiskBytesBefore = beforeDisk.TotalBytes
	out.IndexDBBytesBefore = beforeStorage.IndexDBBytes
	out.StorageBefore = beforeStorage

	start := time.Now()
	switch cfg.IndexVacuum {
	case indexVacuumModeOnline:
		if err := backend.VacuumIndexOnline(context.Background()); err != nil {
			return out, indexStorageSummary{}, fmt.Errorf("index vacuum online: %w", err)
		}
		if err := backend.Checkpoint(); err != nil {
			return out, indexStorageSummary{}, fmt.Errorf("checkpoint after online index vacuum: %w", err)
		}
	case indexVacuumModeOffline:
		if cleanup == nil || closed == nil {
			return out, indexStorageSummary{}, errors.New("offline index vacuum requires backend cleanup state")
		}
		if err := cleanup(); err != nil {
			*closed = true
			return out, indexStorageSummary{}, fmt.Errorf("close backend before offline index vacuum: %w", err)
		}
		*closed = true
		opts := treedb.Options{
			Dir:        cfg.Dir,
			KeepRecent: cfg.KeepRecent,
		}
		if cfg.ChunkSize > 0 {
			opts.ChunkSize = cfg.ChunkSize
		}
		if err := treedb.VacuumIndexOffline(opts); err != nil {
			return out, indexStorageSummary{}, fmt.Errorf("index vacuum offline: %w", err)
		}
	default:
		return out, indexStorageSummary{}, fmt.Errorf("unsupported index vacuum mode %q", cfg.IndexVacuum)
	}
	out.Timing = timing(time.Since(start), 1)

	var afterStorage indexStorageSummary
	if cfg.IndexVacuum == indexVacuumModeOffline {
		afterStorage, err = captureIndexStorageSummaryReopen(cfg)
	} else {
		afterStorage, err = captureIndexStorageSummary(cfg, backend)
	}
	if err != nil {
		return out, indexStorageSummary{}, err
	}
	afterDisk, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return out, indexStorageSummary{}, err
	}
	out.DiskBytesAfter = afterDisk.TotalBytes
	out.IndexDBBytesAfter = afterStorage.IndexDBBytes
	out.StorageAfter = afterStorage
	return out, afterStorage, nil
}

func captureIndexStorageSummaryReopen(cfg config) (indexStorageSummary, error) {
	backend, cleanup, err := openBackend(cfg)
	if err != nil {
		return indexStorageSummary{}, fmt.Errorf("capture index storage reopen: %w", err)
	}
	defer func() { _ = cleanup() }()
	return captureIndexStorageSummary(cfg, backend)
}

func captureIndexStorageSummary(cfg config, backend *backenddb.DB) (indexStorageSummary, error) {
	if backend == nil {
		return indexStorageSummary{}, errors.New("capture index storage: missing backend")
	}
	stats := backend.Stats()
	frag, err := backend.FragmentationReport()
	if err != nil {
		return indexStorageSummary{}, fmt.Errorf("fragmentation report: %w", err)
	}
	merged := make(map[string]string, len(stats)+len(frag))
	for k, v := range frag {
		merged[k] = v
	}
	for k, v := range stats {
		merged[k] = v
	}

	indexBytes, err := indexDBSize(cfg.Dir)
	if err != nil {
		return indexStorageSummary{}, err
	}

	return indexStorageSummary{
		IndexDBBytes:                indexBytes,
		PagesTotal:                  statUint(merged, "treedb.pages.total"),
		KeepRecent:                  statUint(merged, "treedb.keep_recent"),
		PreferAppendAlloc:           statBool(merged, "treedb.prefer_append_alloc"),
		FreelistReclaimablePages:    statUint(merged, "treedb.freelist.reclaimable_pages"),
		FreelistAllocPagesTotal:     statUint(merged, "treedb.freelist.alloc_pages_total"),
		FreelistAppendPagesTotal:    statUint(merged, "treedb.freelist.append_alloc_pages_total"),
		FreelistReusePagesTotal:     statUint(merged, "treedb.freelist.reuse_alloc_pages_total"),
		FreelistFreePagesTotal:      statUint(merged, "treedb.freelist.free_pages_total"),
		GraveyardBatches:            statUint(merged, "treedb.graveyard.batches"),
		GraveyardPages:              statUint(merged, "treedb.graveyard.pages"),
		CollectionRoots:             statUint(merged, "treedb.collection_roots.count"),
		CollectionLeafRefRoots:      statUint(merged, "treedb.collection_roots.leafref_roots"),
		CollectionPagerRoots:        statUint(merged, "treedb.collection_roots.pager_roots"),
		CollectionRootPages:         statUint(merged, "treedb.collection_roots.pages"),
		CollectionRootLeafPages:     statUint(merged, "treedb.collection_roots.pages.leaf"),
		CollectionRootInternalPages: statUint(merged, "treedb.collection_roots.pages.internal"),
		CollectionRootDuplicateRefs: statUint(merged, "treedb.collection_roots.pages.duplicate_refs"),
		PruneEnabled:                statBool(merged, "treedb.prune.enabled"),
		PruneRuns:                   statUint(merged, "treedb.prune.runs"),
		PrunePagesFreed:             statUint(merged, "treedb.prune.pages_freed"),
	}, nil
}

func indexDBSize(root string) (uint64, error) {
	for _, path := range []string{
		filepath.Join(root, "maindb", "index.db"),
		filepath.Join(root, "index.db"),
	} {
		info, err := os.Stat(path)
		if err == nil {
			if !info.Mode().IsRegular() {
				return 0, fmt.Errorf("%s is not a regular file", path)
			}
			return uint64(info.Size()), nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("stat index.db %s: %w", path, err)
		}
	}
	return 0, fmt.Errorf("index.db not found under %s", root)
}

func statUint(stats map[string]string, key string) uint64 {
	raw := strings.TrimSpace(stats[key])
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func statBool(stats map[string]string, key string) bool {
	raw := strings.TrimSpace(stats[key])
	if raw == "" {
		return false
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return v
}

func verifyReopen(cfg config) (int, error) {
	backend, cleanup, err := openBackend(cfg)
	if err != nil {
		return 0, fmt.Errorf("reopen verify open: %w", err)
	}
	defer func() { _ = cleanup() }()
	manager := collections.NewCollectionManager(backend)
	collection, err := manager.OpenCollection(cfg.Collection)
	if err != nil {
		return 0, fmt.Errorf("reopen verify open collection: %w", err)
	}
	samples := sampleDocumentNumbers(cfg.Docs, cfg.VerifySamples)
	for _, n := range samples {
		id := documentID(n)
		got, err := collection.Get(id)
		if err != nil {
			return 0, fmt.Errorf("reopen verify get %s: %w", id, err)
		}
		if len(got) == 0 {
			return 0, fmt.Errorf("reopen verify get %s returned an empty document", id)
		}
		want, err := expectedStoredDocument(cfg.DocumentFormat, n)
		if err != nil {
			return 0, fmt.Errorf("reopen verify expected document %s: %w", id, err)
		}
		if !bytes.Equal(got, want) {
			return 0, fmt.Errorf("reopen verify get %s returned unexpected document content", id)
		}
		if cfg.IndexCount >= 1 {
			email := fmt.Sprintf("user-%09d@example.com", n)
			ids, err := collection.FindByIndex("email_idx", email)
			if err != nil {
				return 0, fmt.Errorf("reopen verify email index %s: %w", email, err)
			}
			if len(ids) != 1 || !bytes.Equal(ids[0], id) {
				return 0, fmt.Errorf("reopen verify email index %s returned %q, want %s", email, ids, id)
			}
		}
		if cfg.IndexCount >= 2 {
			city := fmt.Sprintf("city-%02d", n%collectionFixtureCities)
			ids, err := collection.FindByIndex("city_idx", city)
			if err != nil {
				return 0, fmt.Errorf("reopen verify city index %s: %w", city, err)
			}
			if !containsDocumentID(ids, id) {
				return 0, fmt.Errorf("reopen verify city index %s did not include %s", city, id)
			}
		}
		if cfg.IndexCount >= 3 {
			name := fmt.Sprintf("user-%09d", n)
			ids, err := collection.FindByIndex("name_idx", name)
			if err != nil {
				return 0, fmt.Errorf("reopen verify name index %s: %w", name, err)
			}
			if !containsDocumentID(ids, id) {
				return 0, fmt.Errorf("reopen verify name index %s did not include %s", name, id)
			}
		}
	}
	return len(samples), nil
}

func expectedStoredDocument(format collections.DocumentFormat, n int) ([]byte, error) {
	if format == collections.DocumentFormatJSON {
		return indexedJSONDocument(n), nil
	}
	doc, err := document(format, &collections.TemplateV1Encoder{}, n)
	if err != nil {
		return nil, err
	}
	return templateV1StoredDocument(doc)
}

func templateV1StoredDocument(raw []byte) ([]byte, error) {
	const (
		inputMagic  = "TD1I"
		storedMagic = "TD1D"
	)
	if bytes.HasPrefix(raw, []byte(storedMagic)) {
		return raw, nil
	}
	if !bytes.HasPrefix(raw, []byte(inputMagic)) {
		return nil, errors.New("template-v1 document is missing input envelope")
	}
	pos := len(inputMagic)
	templateCount, n := binary.Uvarint(raw[pos:])
	if n <= 0 {
		return nil, errors.New("template-v1 document has malformed template count")
	}
	pos += n
	for i := uint64(0); i < templateCount; i++ {
		if len(raw)-pos < 32 {
			return nil, errors.New("template-v1 document has malformed template id")
		}
		pos += 32
		recordLen, n := binary.Uvarint(raw[pos:])
		if n <= 0 {
			return nil, errors.New("template-v1 document has malformed template length")
		}
		pos += n
		if recordLen > uint64(len(raw)-pos) {
			return nil, errors.New("template-v1 document has truncated template record")
		}
		pos += int(recordLen)
	}
	if !bytes.HasPrefix(raw[pos:], []byte(storedMagic)) {
		return nil, errors.New("template-v1 document is missing stored payload")
	}
	return raw[pos:], nil
}

func containsDocumentID(ids [][]byte, want []byte) bool {
	for _, id := range ids {
		if bytes.Equal(id, want) {
			return true
		}
	}
	return false
}

func sampleDocumentNumbers(docs, sampleCount int) []int {
	if docs <= 0 || sampleCount <= 0 {
		return nil
	}
	if sampleCount > docs {
		sampleCount = docs
	}
	seen := make(map[int]struct{}, sampleCount)
	out := make([]int, 0, sampleCount)
	for i := 0; i < sampleCount; i++ {
		var n int
		if sampleCount == 1 {
			n = 0
		} else {
			n = i * (docs - 1) / (sampleCount - 1)
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

func directoryUsage(root string, docs int) (diskUsageSummary, error) {
	var total uint64
	var fileCount int
	files := make([]fileSummary, 0, maxFixtureTopFileEntries)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 {
			return nil
		}
		size := uint64(info.Size())
		total += size
		fileCount++
		rel, err := filepath.Rel(root, path)
		if err != nil {
			rel = path
		}
		files = append(files, fileSummary{Path: rel, Bytes: size})
		return nil
	})
	if err != nil {
		return diskUsageSummary{}, fmt.Errorf("disk usage for %s: %w", root, err)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Bytes == files[j].Bytes {
			return files[i].Path < files[j].Path
		}
		return files[i].Bytes > files[j].Bytes
	})
	if len(files) > maxFixtureTopFileEntries {
		files = files[:maxFixtureTopFileEntries]
	}
	out := diskUsageSummary{TotalBytes: total, FileCount: fileCount, TopFiles: files}
	if docs > 0 {
		out.BytesPerDoc = float64(total) / float64(docs)
	}
	return out, nil
}

func timing(d time.Duration, ops int) timingSummary {
	out := timingSummary{Seconds: d.Seconds()}
	if ops > 0 && d > 0 {
		out.SecPerOp = d.Seconds() / float64(ops)
		out.OpsPerSec = float64(ops) / d.Seconds()
	}
	return out
}

func durationString(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}

func startCPUProfile(path string) (func(), error) {
	if strings.TrimSpace(path) == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create CPU profile dir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create CPU profile: %w", err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("start CPU profile: %w", err)
	}
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		pprof.StopCPUProfile()
		_ = f.Close()
	}, nil
}

func writeMemProfile(path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create heap profile dir: %w", err)
	}
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create heap profile: %w", err)
	}
	defer func() { _ = f.Close() }()
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return fmt.Errorf("write heap profile: %w", err)
	}
	return nil
}

func printHumanSummary(w io.Writer, summary loadSummary) {
	fmt.Fprintf(w, "TreeDB collection load fixture\n")
	fmt.Fprintf(w, "dir: %s\n", summary.Dir)
	fmt.Fprintf(w, "collection: %s\n", summary.Collection)
	fmt.Fprintf(w, "shape: format=%s indexes=%d data_outer_leaves_in_vlog=%t index_outer_leaves_in_vlog=%t profile=%s\n",
		summary.DocumentFormat, summary.IndexCount, summary.DataOuterLeavesInValueLog, summary.IndexOuterLeavesInValueLog, summary.Profile)
	fmt.Fprintf(w, "index policy: keep_recent=%d prefer_append_alloc=%t background_prune=%t\n",
		summary.KeepRecent, summary.PreferAppendAlloc, !summary.DisableBackgroundPrune)
	fmt.Fprintf(w, "loaded: docs=%d batches=%d batch_size=%d\n", summary.Docs, summary.Batches, summary.BatchSize)
	printTiming(w, "wall", summary.WallTiming)
	printTiming(w, "document generation", summary.GenerationTiming)
	printTiming(w, "insert", summary.InsertTiming)
	if summary.CheckpointTiming.Seconds > 0 {
		printTiming(w, "checkpoint", summary.CheckpointTiming)
	}
	printIndexStorageSummary(w, "index before maintenance", summary.IndexStorageBeforeMaintenance)
	fmt.Fprintf(w, "disk before maintenance: %s total, %.2f bytes/doc, %d files\n",
		humanBytes(summary.DiskUsageBeforeMaintenance.TotalBytes), summary.DiskUsageBeforeMaintenance.BytesPerDoc, summary.DiskUsageBeforeMaintenance.FileCount)
	if summary.Rewrite.Enabled {
		fmt.Fprintf(w, "vlog rewrite: before=%s after_rewrite=%s after_gc=%s copied_records=%d copied_value_bytes=%s\n",
			humanBytes(summary.Rewrite.DiskBytesBefore),
			humanBytes(summary.Rewrite.DiskBytesAfterRewrite),
			humanBytes(summary.Rewrite.DiskBytesAfterGC),
			summary.Rewrite.RecordsCopied,
			humanBytes(uint64(max(summary.Rewrite.ValueBytesCopied, 0))))
	}
	if summary.IndexVacuum.Enabled {
		fmt.Fprintf(w, "index vacuum %s: before=%s after=%s index.db_before=%s index.db_after=%s\n",
			summary.IndexVacuum.Mode,
			humanBytes(summary.IndexVacuum.DiskBytesBefore),
			humanBytes(summary.IndexVacuum.DiskBytesAfter),
			humanBytes(summary.IndexVacuum.IndexDBBytesBefore),
			humanBytes(summary.IndexVacuum.IndexDBBytesAfter))
		printTiming(w, "index vacuum", summary.IndexVacuum.Timing)
	}
	printIndexStorageSummary(w, "index final", summary.IndexStorageFinal)
	fmt.Fprintf(w, "disk final: %s total, %.2f bytes/doc, %d files\n",
		humanBytes(summary.DiskUsageFinal.TotalBytes), summary.DiskUsageFinal.BytesPerDoc, summary.DiskUsageFinal.FileCount)
	if summary.Verify.Enabled {
		fmt.Fprintf(w, "reopen verify: %d sampled primary/index reads passed\n", summary.Verify.Samples)
	}
	if len(summary.DiskUsageFinal.TopFiles) > 0 {
		fmt.Fprintf(w, "largest files:\n")
		for _, file := range summary.DiskUsageFinal.TopFiles {
			fmt.Fprintf(w, "  %12s  %s\n", humanBytes(file.Bytes), file.Path)
		}
	}
	if summary.CPUProfile != "" {
		fmt.Fprintf(w, "cpu profile: %s\n", summary.CPUProfile)
	}
	if summary.MemProfile != "" {
		fmt.Fprintf(w, "heap profile: %s\n", summary.MemProfile)
	}
}

func printIndexStorageSummary(w io.Writer, label string, s indexStorageSummary) {
	if s.IndexDBBytes == 0 && s.PagesTotal == 0 {
		return
	}
	fmt.Fprintf(w, "%s: index.db=%s pages=%d collection_root_pages=%d freelist_reclaimable=%d graveyard_pages=%d append_alloc=%d reuse_alloc=%d prune_freed=%d\n",
		label,
		humanBytes(s.IndexDBBytes),
		s.PagesTotal,
		s.CollectionRootPages,
		s.FreelistReclaimablePages,
		s.GraveyardPages,
		s.FreelistAppendPagesTotal,
		s.FreelistReusePagesTotal,
		s.PrunePagesFreed)
}

func printTiming(w io.Writer, label string, t timingSummary) {
	fmt.Fprintf(w, "%s: %.3fs, %.9f sec/op, %.0f ops/sec\n", label, t.Seconds, t.SecPerOp, t.OpsPerSec)
}

func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return strconv.FormatUint(n, 10) + " B"
	}
	div, exp := uint64(unit), 0
	for value := n / unit; value >= unit; value /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
