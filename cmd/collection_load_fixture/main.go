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
	"github.com/snissn/gomap/cmd/internal/treedbstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	defaultDocs              = 1_000_000
	defaultBatchSize         = 8_000
	defaultCollectionName    = "bench_shape_insert_2"
	documentShapeDefault     = "default"
	documentShapeWide        = "wide"
	documentShapeHetero      = "heterogeneous"
	defaultFixtureFieldCount = 4
	indexVacuumModeAuto      = "auto"
	indexVacuumModeNone      = "none"
	indexVacuumModeOnline    = "online"
	indexVacuumModeOffline   = "offline"
	collectionFixtureCities  = 64
	collectionFixturePad     = "01234567890123456789"
	maxFixtureTopFileEntries = 20
)

type config struct {
	Dir                                     string
	Reset                                   bool
	Docs                                    int
	BatchSize                               int
	Collection                              string
	DocumentShape                           string
	FieldCount                              int
	ShapeCount                              int
	DocumentFormat                          collections.DocumentFormat
	IndexCount                              int
	BufferedIndexedWrites                   bool
	BufferedIndexedWriteMaxDocs             int
	BufferedIndexedWriteMaxBytes            int64
	BufferedIndexedWriteMaxRuns             int
	DisableBufferedIndexedAsyncFlush        bool
	BufferedIndexedAsyncFlush               bool
	BufferedIndexedAsyncFlushMaxQueuedUnits int
	Profile                                 treedb.Profile
	DataOuterLeavesInValueLog               bool
	IndexOuterLeavesInValueLog              bool
	ChunkSize                               int64
	KeepRecent                              uint64
	PreferAppendAlloc                       bool
	PagerSyncConcurrency                    int
	DisableBackgroundPrune                  bool
	PruneInterval                           time.Duration
	PruneMaxPages                           int
	PruneMaxDuration                        time.Duration
	LeafSegmentTargetBytes                  int64
	Checkpoint                              bool
	CheckpointEachBatch                     bool
	ReopenVerify                            bool
	VerifySamples                           int
	ValueLogRewrite                         bool
	ValueLogGC                              bool
	LeafGenerationPackGC                    bool
	LeafGenerationPackForce                 bool
	LeafGenerationPackMaxGen                int
	LeafGenerationPackFrameK                int
	IndexVacuum                             string
	Progress                                bool
	JSONOutput                              bool
	CPUProfile                              string
	MemProfile                              string
	createdTempDir                          bool
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

type leafGenerationSummary struct {
	Enabled                   bool          `json:"enabled"`
	Force                     bool          `json:"force,omitempty"`
	MaxGenerations            int           `json:"max_generations,omitempty"`
	PlanTiming                timingSummary `json:"plan_timing,omitempty"`
	PackTiming                timingSummary `json:"pack_timing,omitempty"`
	GCTiming                  timingSummary `json:"gc_timing,omitempty"`
	DiskBytesBefore           uint64        `json:"disk_bytes_before,omitempty"`
	DiskBytesAfterPack        uint64        `json:"disk_bytes_after_pack,omitempty"`
	DiskBytesAfterGC          uint64        `json:"disk_bytes_after_gc,omitempty"`
	PlanAdmission             string        `json:"plan_admission,omitempty"`
	CandidateGenerations      int           `json:"candidate_generations,omitempty"`
	CandidateBytesTotal       int64         `json:"candidate_bytes_total,omitempty"`
	CandidateBytesLive        int64         `json:"candidate_bytes_live,omitempty"`
	CandidateBytesDead        int64         `json:"candidate_bytes_dead,omitempty"`
	CandidateBytesToCopy      int64         `json:"candidate_bytes_to_copy,omitempty"`
	CandidateLivePages        int           `json:"candidate_live_pages,omitempty"`
	ExpectedReclaimBytes      int64         `json:"expected_reclaim_bytes,omitempty"`
	ExpectedReclaimRatioPPM   int           `json:"expected_reclaim_ratio_ppm,omitempty"`
	ExpectedReclaimPerCopyPPM int           `json:"expected_reclaim_per_copy_ppm,omitempty"`
	PackGenerationsMatched    int           `json:"pack_generations_matched,omitempty"`
	PackSourceBytesTotal      int64         `json:"pack_source_bytes_total,omitempty"`
	PackSourceBytesLive       int64         `json:"pack_source_bytes_live,omitempty"`
	PackSourceBytesDead       int64         `json:"pack_source_bytes_dead,omitempty"`
	PackSourceBytesToCopy     int64         `json:"pack_source_bytes_to_copy,omitempty"`
	PackLeafPagesCopied       int           `json:"pack_leaf_pages_copied,omitempty"`
	PackLeafFramesWritten     int           `json:"pack_leaf_frames_written,omitempty"`
	PackMaxLeafFrameK         int           `json:"pack_max_leaf_frame_k,omitempty"`
	PackBytesCopied           int64         `json:"pack_bytes_copied,omitempty"`
	PackCreatedFiles          int           `json:"pack_created_files,omitempty"`
	GCGenerationsDeleted      int           `json:"gc_generations_deleted,omitempty"`
	GCFilesDeleted            int           `json:"gc_files_deleted,omitempty"`
	GCBytesDeleted            int64         `json:"gc_bytes_deleted,omitempty"`
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
	RequestedMode      string              `json:"requested_mode,omitempty"`
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
	GeneratedAt                             string                 `json:"generated_at"`
	Dir                                     string                 `json:"dir"`
	CreatedTempDir                          bool                   `json:"created_temp_dir,omitempty"`
	Collection                              string                 `json:"collection"`
	DocumentFormat                          string                 `json:"document_format"`
	DocumentShape                           string                 `json:"document_shape,omitempty"`
	FieldCount                              int                    `json:"field_count,omitempty"`
	ShapeCount                              int                    `json:"shape_count,omitempty"`
	Profile                                 string                 `json:"profile"`
	Docs                                    int                    `json:"docs"`
	BatchSize                               int                    `json:"batch_size"`
	Batches                                 int                    `json:"batches"`
	IndexCount                              int                    `json:"index_count"`
	BufferedIndexedWrites                   bool                   `json:"buffered_indexed_writes,omitempty"`
	BufferedIndexedWriteMaxDocs             int                    `json:"buffered_indexed_write_max_docs,omitempty"`
	BufferedIndexedWriteMaxBytes            int64                  `json:"buffered_indexed_write_max_bytes,omitempty"`
	BufferedIndexedWriteMaxRuns             int                    `json:"buffered_indexed_write_max_root_runs,omitempty"`
	DisableBufferedIndexedAsyncFlush        bool                   `json:"disable_buffered_indexed_async_flush,omitempty"`
	BufferedIndexedAsyncFlush               bool                   `json:"buffered_indexed_async_flush,omitempty"`
	BufferedIndexedAsyncFlushMaxQueuedUnits int                    `json:"buffered_indexed_async_flush_max_queued_units,omitempty"`
	DataOuterLeavesInValueLog               bool                   `json:"data_outer_leaves_in_value_log"`
	IndexOuterLeavesInValueLog              bool                   `json:"index_outer_leaves_in_value_log"`
	ChunkSize                               int64                  `json:"chunk_size,omitempty"`
	KeepRecent                              uint64                 `json:"keep_recent"`
	PreferAppendAlloc                       bool                   `json:"prefer_append_alloc"`
	PagerSyncConcurrency                    int                    `json:"pager_sync_concurrency,omitempty"`
	DisableBackgroundPrune                  bool                   `json:"disable_background_prune,omitempty"`
	PruneInterval                           string                 `json:"prune_interval,omitempty"`
	PruneMaxPages                           int                    `json:"prune_max_pages,omitempty"`
	PruneMaxDuration                        string                 `json:"prune_max_duration,omitempty"`
	LeafSegmentTargetBytes                  int64                  `json:"leaf_segment_target_bytes,omitempty"`
	WallTiming                              timingSummary          `json:"wall_timing"`
	GenerationTiming                        timingSummary          `json:"generation_timing"`
	InsertTiming                            timingSummary          `json:"insert_timing"`
	FlushTiming                             timingSummary          `json:"flush_timing,omitempty"`
	CheckpointTiming                        timingSummary          `json:"checkpoint_timing,omitempty"`
	InsertPhases                            insertPhaseSummary     `json:"insert_phases"`
	IndexStorageBeforeMaintenance           indexStorageSummary    `json:"index_storage_before_maintenance"`
	DiskUsageBeforeMaintenance              diskUsageSummary       `json:"disk_usage_before_maintenance"`
	DiskUsageFinal                          diskUsageSummary       `json:"disk_usage_final"`
	Rewrite                                 rewriteSummary         `json:"rewrite,omitempty"`
	LeafGeneration                          *leafGenerationSummary `json:"leaf_generation,omitempty"`
	IndexVacuum                             indexVacuumSummary     `json:"index_vacuum,omitempty"`
	IndexStorageFinal                       indexStorageSummary    `json:"index_storage_final"`
	TreeDBStatsFinal                        map[string]string      `json:"treedb_stats_final,omitempty"`
	Verify                                  verifySummary          `json:"verify"`
	CPUProfile                              string                 `json:"cpu_profile,omitempty"`
	MemProfile                              string                 `json:"mem_profile,omitempty"`
	GoVersion                               string                 `json:"go_version"`
	GOOS                                    string                 `json:"goos"`
	GOARCH                                  string                 `json:"goarch"`
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
		DocumentShape:              documentShapeDefault,
		FieldCount:                 defaultFixtureFieldCount,
		ShapeCount:                 1,
		DocumentFormat:             collections.DocumentFormatTemplateV1,
		IndexCount:                 2,
		BufferedIndexedWrites:      true,
		Profile:                    treedb.ProfileBenchUnsafe,
		DataOuterLeavesInValueLog:  true,
		IndexOuterLeavesInValueLog: true,
		KeepRecent:                 1,
		PreferAppendAlloc:          false,
		Checkpoint:                 true,
		ReopenVerify:               true,
		VerifySamples:              8,
		ValueLogGC:                 true,
		LeafGenerationPackMaxGen:   1,
		IndexVacuum:                indexVacuumModeAuto,
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
	fs.StringVar(&cfg.DocumentShape, "document-shape", cfg.DocumentShape, "document shape: default, wide, or heterogeneous")
	fs.IntVar(&cfg.FieldCount, "field-count", cfg.FieldCount, "number of fields for wide/heterogeneous document shapes")
	fs.IntVar(&cfg.ShapeCount, "shape-count", cfg.ShapeCount, "number of distinct field-name sets for heterogeneous document shape")
	fs.StringVar(&documentFormat, "format", string(cfg.DocumentFormat), "document format: json, template-v1, or bson")
	fs.IntVar(&cfg.IndexCount, "indexes", cfg.IndexCount, "secondary index count for the benchmark shape: 0, 1, 2, or 3")
	fs.BoolVar(&cfg.BufferedIndexedWrites, "buffered-indexed-writes", cfg.BufferedIndexedWrites, "use native collection-local memtables for indexed InsertBatch writes; set false for immediate-publish baseline comparisons")
	fs.IntVar(&cfg.BufferedIndexedWriteMaxDocs, "buffered-indexed-write-max-docs", cfg.BufferedIndexedWriteMaxDocs, "flush indexed write buffers after this many staged documents; 0 uses the collection default")
	fs.Int64Var(&cfg.BufferedIndexedWriteMaxBytes, "buffered-indexed-write-max-bytes", 0, "flush indexed write buffers after this many staged root-run bytes; 0 means Flush/Close only")
	fs.IntVar(&cfg.BufferedIndexedWriteMaxRuns, "buffered-indexed-write-max-root-runs", cfg.BufferedIndexedWriteMaxRuns, "flush indexed write buffers after this many staged root-local mutation runs; explicit 0 disables this trigger; omitted with docs/bytes override keeps the compatibility default")
	fs.BoolVar(&cfg.BufferedIndexedAsyncFlush, "buffered-indexed-async-flush", false, "force-enable background indexed threshold publish; indexed schemas already enable this by default")
	fs.BoolVar(&cfg.DisableBufferedIndexedAsyncFlush, "disable-buffered-indexed-async-flush", false, "disable background indexed threshold publish for foreground-publish baseline comparisons")
	fs.IntVar(&cfg.BufferedIndexedAsyncFlushMaxQueuedUnits, "buffered-indexed-async-flush-max-queued-units", 0, "max immutable indexed flush units queued for background publish; 0 uses the collection default when async flush is enabled")
	fs.StringVar(&profile, "profile", string(cfg.Profile), "TreeDB profile: "+treedb.BenchmarkProfileFlagHelp)
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
	fs.Int64Var(&cfg.LeafSegmentTargetBytes, "leaf-segment-target-bytes", 0, "TreeDB leaf_vlog generation target segment size in bytes (0 uses engine default)")
	fs.BoolVar(&cfg.Checkpoint, "checkpoint", cfg.Checkpoint, "checkpoint after loading")
	fs.BoolVar(&cfg.CheckpointEachBatch, "checkpoint-each-batch", false, "checkpoint after every batch")
	fs.BoolVar(&cfg.ReopenVerify, "reopen-verify", cfg.ReopenVerify, "close, reopen, and verify sampled primary/index reads")
	fs.IntVar(&cfg.VerifySamples, "verify-samples", cfg.VerifySamples, "sample count for -reopen-verify")
	fs.BoolVar(&cfg.ValueLogRewrite, "vlog-rewrite", false, "run TreeDB online value-log rewrite after loading")
	fs.BoolVar(&cfg.ValueLogGC, "vlog-gc", cfg.ValueLogGC, "run value-log GC after -vlog-rewrite")
	fs.BoolVar(&cfg.LeafGenerationPackGC, "leafgen-pack-gc", false, "run TreeDB leaf_vlog generation pack plus GC after loading")
	fs.BoolVar(&cfg.LeafGenerationPackForce, "leafgen-pack-force", false, "force leaf-generation pack selection when -leafgen-pack-gc is enabled")
	fs.IntVar(&cfg.LeafGenerationPackMaxGen, "leafgen-pack-max-generations", cfg.LeafGenerationPackMaxGen, "max leaf generations to pack per run when -leafgen-pack-gc is enabled (0 means no limit)")
	fs.IntVar(&cfg.LeafGenerationPackFrameK, "leafgen-pack-frame-k", cfg.LeafGenerationPackFrameK, "leaf pages per grouped output frame during leafgen pack (0 uses engine default)")
	fs.StringVar(&cfg.IndexVacuum, "index-vacuum", cfg.IndexVacuum, "run index vacuum after loading: auto, none, online, or offline (auto uses none unless -vlog-rewrite or -leafgen-pack-gc is enabled)")
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
	seenFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		seenFlags[f.Name] = true
	})
	if cfg.DisableBufferedIndexedAsyncFlush && cfg.BufferedIndexedAsyncFlush {
		return config{}, fmt.Errorf("cannot set both -buffered-indexed-async-flush and -disable-buffered-indexed-async-flush")
	}
	if cfg.DisableBufferedIndexedAsyncFlush && cfg.BufferedIndexedAsyncFlushMaxQueuedUnits != 0 {
		return config{}, fmt.Errorf("cannot set -buffered-indexed-async-flush-max-queued-units when -disable-buffered-indexed-async-flush is set")
	}
	effectiveAsyncFlush := !cfg.DisableBufferedIndexedAsyncFlush
	if !seenFlags["buffered-indexed-write-max-root-runs"] &&
		(seenFlags["buffered-indexed-write-max-docs"] || seenFlags["buffered-indexed-write-max-bytes"]) &&
		(cfg.BufferedIndexedWriteMaxDocs != 0 || cfg.BufferedIndexedWriteMaxBytes != 0) {
		cfg.BufferedIndexedWriteMaxRuns = collections.DefaultIndexedWriteMemtableMaxRootRuns
		if effectiveAsyncFlush {
			cfg.BufferedIndexedWriteMaxRuns = collections.DefaultIndexedWriteMemtableAsyncFlushMaxRootRuns
		}
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
	if cfg.BufferedIndexedWriteMaxDocs < 0 {
		return cfg, fmt.Errorf("-buffered-indexed-write-max-docs must be >= 0")
	}
	if cfg.BufferedIndexedWriteMaxBytes < 0 {
		return cfg, fmt.Errorf("-buffered-indexed-write-max-bytes must be >= 0")
	}
	if cfg.BufferedIndexedWriteMaxRuns < 0 {
		return cfg, fmt.Errorf("-buffered-indexed-write-max-root-runs must be >= 0")
	}
	if cfg.BufferedIndexedAsyncFlushMaxQueuedUnits < 0 {
		return cfg, fmt.Errorf("-buffered-indexed-async-flush-max-queued-units must be >= 0")
	}
	if strings.TrimSpace(cfg.Collection) == "" {
		return cfg, fmt.Errorf("-collection cannot be empty")
	}
	cfg.DocumentShape = strings.ToLower(strings.TrimSpace(cfg.DocumentShape))
	switch cfg.DocumentShape {
	case "", documentShapeDefault:
		cfg.DocumentShape = documentShapeDefault
	case documentShapeWide, documentShapeHetero:
	default:
		return cfg, fmt.Errorf("unsupported -document-shape %q; use default, wide, or heterogeneous", cfg.DocumentShape)
	}
	if cfg.FieldCount <= 0 {
		return cfg, fmt.Errorf("-field-count must be > 0")
	}
	if cfg.ShapeCount <= 0 {
		return cfg, fmt.Errorf("-shape-count must be > 0")
	}
	if cfg.DocumentShape == documentShapeDefault && cfg.ShapeCount != 1 {
		return cfg, fmt.Errorf("-shape-count only applies to -document-shape=heterogeneous")
	}
	if cfg.DocumentShape == documentShapeWide {
		cfg.ShapeCount = 1
	}
	if cfg.DocumentShape == documentShapeHetero && cfg.ShapeCount > cfg.Docs {
		cfg.ShapeCount = cfg.Docs
	}
	if cfg.DocumentShape != documentShapeDefault && cfg.IndexCount != 0 {
		return cfg, fmt.Errorf("-document-shape=%s requires -indexes=0", cfg.DocumentShape)
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
	if cfg.LeafSegmentTargetBytes < 0 {
		return cfg, fmt.Errorf("-leaf-segment-target-bytes must be >= 0")
	}
	if cfg.VerifySamples < 0 {
		return cfg, fmt.Errorf("-verify-samples must be >= 0")
	}
	if cfg.LeafGenerationPackMaxGen < 0 {
		return cfg, fmt.Errorf("-leafgen-pack-max-generations must be >= 0")
	}
	if cfg.LeafGenerationPackFrameK < 0 {
		return cfg, fmt.Errorf("-leafgen-pack-frame-k must be >= 0")
	}
	cfg.IndexVacuum = strings.ToLower(strings.TrimSpace(cfg.IndexVacuum))
	switch cfg.IndexVacuum {
	case "":
		cfg.IndexVacuum = indexVacuumModeAuto
	case indexVacuumModeAuto, indexVacuumModeNone:
	case indexVacuumModeOnline, indexVacuumModeOffline:
	default:
		return cfg, fmt.Errorf("unsupported -index-vacuum %q; use auto, none, online, or offline", cfg.IndexVacuum)
	}
	return cfg, nil
}

func parseDocumentFormat(raw string) (collections.DocumentFormat, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "json":
		return collections.DocumentFormatJSON, nil
	case string(collections.DocumentFormatBSON):
		return collections.DocumentFormatBSON, nil
	case string(collections.DocumentFormatTemplateV1):
		return collections.DocumentFormatTemplateV1, nil
	case "":
		return "", fmt.Errorf("-format cannot be empty; use json, template-v1, or bson")
	default:
		return "", fmt.Errorf("unsupported -format %q", raw)
	}
}

func parseProfile(raw string) (treedb.Profile, error) {
	if profile, ok := treedb.ParseBenchmarkProfile(raw, treedb.ProfileBenchUnsafe); ok {
		return profile, nil
	}
	return "", fmt.Errorf("unsupported -profile %q; allowed: %s", raw, treedb.BenchmarkProfileFlagHelp)
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

	manager, collection, err := createFixtureCollection(backend, cfg)
	if err != nil {
		return loadSummary{}, err
	}
	collectionMeta := collection.Meta()
	bufferedIndexedWrites := collectionMeta.Options.BufferedIndexedWrites

	var insertStats collections.CollectionInsertStats
	secondaryRuns := make(map[string]*secondaryRunSummary)
	var generationElapsed time.Duration
	var insertElapsed time.Duration
	var flushElapsed time.Duration
	var checkpointElapsed time.Duration
	wallStart := time.Now()
	batches := 0
	lastProgress := time.Now()
	var templateEncoder collections.TemplateV1Encoder

	for inserted := 0; inserted < cfg.Docs; {
		batchSize := cfg.BatchSize
		if remaining := cfg.Docs - inserted; remaining < batchSize {
			batchSize = remaining
		}
		genStart := time.Now()
		ids, docs, err := documentBatch(cfg, &templateEncoder, inserted, batchSize)
		if err != nil {
			return loadSummary{}, err
		}
		generationElapsed += time.Since(genStart)

		insertStart := time.Now()
		if cfg.DocumentFormat == collections.DocumentFormatTemplateV1 {
			_, err = collection.InsertBatchWithTemplateV1Encoder(ids, docs, &templateEncoder)
		} else {
			_, err = collection.InsertBatch(ids, docs)
		}
		if err != nil {
			return loadSummary{}, fmt.Errorf("insert batch starting at document %d: %w", inserted, err)
		}
		insertElapsed += time.Since(insertStart)
		stats := collection.LastInsertStats()
		addInsertStats(&insertStats, stats)
		addSecondaryRuns(secondaryRuns, stats.SecondaryRuns)
		batches++
		inserted += batchSize

		if cfg.CheckpointEachBatch {
			if bufferedIndexedWrites {
				flushStart := time.Now()
				if err := collection.Flush(); err != nil {
					return loadSummary{}, fmt.Errorf("flush after batch %d: %w", batches, err)
				}
				flushElapsed += time.Since(flushStart)
			}
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

	flushStart := time.Now()
	if err := collection.Flush(); err != nil {
		return loadSummary{}, fmt.Errorf("flush collection: %w", err)
	}
	flushElapsed += time.Since(flushStart)
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
	leafGeneration, err := maybePackLeafGenerations(cfg, backend)
	if err != nil {
		return loadSummary{}, err
	}
	indexVacuum, finalIndexStorage, err := maybeVacuumIndex(cfg, backend, cleanup, &closed)
	if err != nil {
		return loadSummary{}, err
	}
	rawFinalStats := backend.Stats()
	if rawFinalStats == nil {
		rawFinalStats = make(map[string]string)
	}
	for key, value := range manager.Stats() {
		rawFinalStats[key] = value
	}
	finalStats := treedbstats.Selected(rawFinalStats)
	wallElapsed := time.Since(wallStart)

	if !closed {
		if err := cleanup(); err != nil {
			closed = true
			return loadSummary{}, fmt.Errorf("close backend: %w", err)
		}
		closed = true
	}

	finalUsage, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return loadSummary{}, err
	}

	verify := verifySummary{Enabled: cfg.ReopenVerify}
	if cfg.ReopenVerify {
		verifyReadOnly := reopenVerifyReadOnly(cfg)
		samples, err := verifyReopen(cfg)
		if err != nil {
			return loadSummary{}, err
		}
		verify.Samples = samples
		if !verifyReadOnly {
			finalUsage, err = directoryUsage(cfg.Dir, cfg.Docs)
			if err != nil {
				return loadSummary{}, err
			}
		}
	}

	if err := writeMemProfile(cfg.MemProfile); err != nil {
		return loadSummary{}, err
	}

	return loadSummary{
		GeneratedAt:                             time.Now().UTC().Format(time.RFC3339),
		Dir:                                     cfg.Dir,
		CreatedTempDir:                          cfg.createdTempDir,
		Collection:                              cfg.Collection,
		DocumentFormat:                          string(cfg.DocumentFormat),
		DocumentShape:                           cfg.DocumentShape,
		FieldCount:                              cfg.FieldCount,
		ShapeCount:                              cfg.ShapeCount,
		Profile:                                 string(cfg.Profile),
		Docs:                                    cfg.Docs,
		BatchSize:                               cfg.BatchSize,
		Batches:                                 batches,
		IndexCount:                              cfg.IndexCount,
		BufferedIndexedWrites:                   collectionMeta.Options.BufferedIndexedWrites,
		BufferedIndexedWriteMaxDocs:             collectionMeta.Options.BufferedIndexedWriteMaxDocuments,
		BufferedIndexedWriteMaxBytes:            collectionMeta.Options.BufferedIndexedWriteMaxBytes,
		BufferedIndexedWriteMaxRuns:             collectionMeta.Options.BufferedIndexedWriteMaxRootRuns,
		DisableBufferedIndexedAsyncFlush:        collectionMeta.Options.DisableBufferedIndexedAsyncFlush,
		BufferedIndexedAsyncFlush:               collectionMeta.Options.BufferedIndexedAsyncFlush && collectionMeta.Options.BufferedIndexedWrites,
		BufferedIndexedAsyncFlushMaxQueuedUnits: collectionMeta.Options.BufferedIndexedAsyncFlushMaxQueuedUnits,
		DataOuterLeavesInValueLog:               cfg.DataOuterLeavesInValueLog,
		IndexOuterLeavesInValueLog:              cfg.IndexOuterLeavesInValueLog,
		ChunkSize:                               cfg.ChunkSize,
		KeepRecent:                              cfg.KeepRecent,
		PreferAppendAlloc:                       cfg.PreferAppendAlloc,
		PagerSyncConcurrency:                    cfg.PagerSyncConcurrency,
		DisableBackgroundPrune:                  cfg.DisableBackgroundPrune,
		PruneInterval:                           durationString(cfg.PruneInterval),
		PruneMaxPages:                           cfg.PruneMaxPages,
		PruneMaxDuration:                        durationString(cfg.PruneMaxDuration),
		LeafSegmentTargetBytes:                  cfg.LeafSegmentTargetBytes,
		WallTiming:                              timing(wallElapsed, cfg.Docs),
		GenerationTiming:                        timing(generationElapsed, cfg.Docs),
		InsertTiming:                            timing(insertElapsed, cfg.Docs),
		FlushTiming:                             timing(flushElapsed, cfg.Docs),
		CheckpointTiming:                        timing(checkpointElapsed, cfg.Docs),
		InsertPhases:                            summarizeInsertPhases(insertStats, secondaryRuns, cfg.Docs, batches),
		IndexStorageBeforeMaintenance:           beforeIndexStorage,
		DiskUsageBeforeMaintenance:              beforeMaintenance,
		DiskUsageFinal:                          finalUsage,
		Rewrite:                                 rewrite,
		LeafGeneration:                          leafGeneration,
		IndexVacuum:                             indexVacuum,
		IndexStorageFinal:                       finalIndexStorage,
		TreeDBStatsFinal:                        finalStats,
		Verify:                                  verify,
		CPUProfile:                              cfg.CPUProfile,
		MemProfile:                              cfg.MemProfile,
		GoVersion:                               runtime.Version(),
		GOOS:                                    runtime.GOOS,
		GOARCH:                                  runtime.GOARCH,
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
	return openBackendReadOnly(cfg, false)
}

func openBackendReadOnly(cfg config, readOnly bool) (*backenddb.DB, func() error, error) {
	opts := backendOptions(cfg, readOnly)
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

func backendOptions(cfg config, readOnly bool) treedb.Options {
	opts := treedb.OptionsForBenchmark(cfg.Profile, cfg.Dir)
	opts.ReadOnly = readOnly
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
	if cfg.LeafSegmentTargetBytes > 0 {
		opts.ValueLog.Generational.Policy = treedb.ValueLogGenerationHotWarmCold
		opts.ValueLog.Generational.LeafSegmentTargetBytes = cfg.LeafSegmentTargetBytes
	}
	return opts
}

func createFixtureCollection(backend *backenddb.DB, cfg config) (*collections.CollectionManager, *collections.Collection, error) {
	manager := collections.NewCollectionManager(backend)
	indexes := collectionShapeIndexes(cfg.IndexCount)
	for i := range indexes {
		indexes[i].StoragePolicy = rootStoragePolicy(cfg.IndexOuterLeavesInValueLog)
	}
	bufferedIndexedWrites := cfg.BufferedIndexedWrites && cfg.IndexCount > 0
	bufferedIndexedWriteMaxDocs := 0
	var bufferedIndexedWriteMaxBytes int64
	bufferedIndexedWriteMaxRuns := 0
	bufferedIndexedAsyncFlush := false
	bufferedIndexedAsyncFlushMaxQueuedUnits := 0
	if bufferedIndexedWrites {
		bufferedIndexedWriteMaxDocs = cfg.BufferedIndexedWriteMaxDocs
		bufferedIndexedWriteMaxBytes = cfg.BufferedIndexedWriteMaxBytes
		bufferedIndexedWriteMaxRuns = cfg.BufferedIndexedWriteMaxRuns
		bufferedIndexedAsyncFlush = cfg.BufferedIndexedAsyncFlush
		bufferedIndexedAsyncFlushMaxQueuedUnits = cfg.BufferedIndexedAsyncFlushMaxQueuedUnits
	}
	_, err := manager.CreateCollection(&collections.CollectionMeta{
		Name: cfg.Collection,
		Options: collections.CollectionOptions{
			DocumentFormat:                          cfg.DocumentFormat,
			DataRootStoragePolicy:                   rootStoragePolicy(cfg.DataOuterLeavesInValueLog),
			IndexStateStoragePolicy:                 rootStoragePolicy(cfg.DataOuterLeavesInValueLog),
			DisableIndexedWriteMemtables:            !cfg.BufferedIndexedWrites && cfg.IndexCount > 0,
			BufferedIndexedWrites:                   bufferedIndexedWrites,
			BufferedIndexedWriteMaxDocuments:        bufferedIndexedWriteMaxDocs,
			BufferedIndexedWriteMaxBytes:            bufferedIndexedWriteMaxBytes,
			BufferedIndexedWriteMaxRootRuns:         bufferedIndexedWriteMaxRuns,
			DisableBufferedIndexedAsyncFlush:        cfg.DisableBufferedIndexedAsyncFlush,
			BufferedIndexedAsyncFlush:               bufferedIndexedAsyncFlush,
			BufferedIndexedAsyncFlushMaxQueuedUnits: bufferedIndexedAsyncFlushMaxQueuedUnits,
		},
		Indexes: indexes,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create collection: %w", err)
	}
	collection, err := manager.OpenCollection(cfg.Collection)
	if err != nil {
		return nil, nil, fmt.Errorf("open collection: %w", err)
	}
	return manager, collection, nil
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
		return []collections.IndexDefinition{{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true}}
	case 2:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
		}
	case 3:
		return []collections.IndexDefinition{
			{Name: "email_idx", Field: "email", ValueType: collections.IndexValueString, Unique: true},
			{Name: "city_idx", Field: "city", ValueType: collections.IndexValueString},
			{Name: "name_idx", Field: "name", ValueType: collections.IndexValueString},
		}
	default:
		panic(fmt.Sprintf("unsupported index count %d", indexCount))
	}
}

func documentBatch(cfg config, templateEncoder *collections.TemplateV1Encoder, start, count int) ([][]byte, [][]byte, error) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		docNum := start + i
		ids[i] = documentID(docNum)
		doc, err := document(cfg, templateEncoder, docNum)
		if err != nil {
			return nil, nil, err
		}
		docs[i] = doc
	}
	return ids, docs, nil
}

func document(cfg config, encoder *collections.TemplateV1Encoder, n int) ([]byte, error) {
	if cfg.DocumentFormat == collections.DocumentFormatJSON && cfg.DocumentShape == documentShapeDefault {
		return indexedJSONDocument(n), nil
	}
	fields, values := fixtureDocumentFieldsAndValues(cfg, n)
	if cfg.DocumentFormat == collections.DocumentFormatTemplateV1 {
		if encoder == nil {
			encoder = &collections.TemplateV1Encoder{}
		}
		return encoder.EncodeDocument(fields, values)
	}
	if cfg.DocumentFormat == collections.DocumentFormatBSON {
		return fixtureBSONDocument(fields, values)
	}
	return fixtureJSONDocument(fields, values)
}

func fixtureDocumentFieldsAndValues(cfg config, n int) ([]string, []any) {
	switch cfg.DocumentShape {
	case documentShapeWide, documentShapeHetero:
		return wideFixtureDocumentFieldsAndValues(cfg, n)
	default:
		return defaultFixtureDocumentFieldsAndValues(n)
	}
}

func defaultFixtureDocumentFieldsAndValues(n int) ([]string, []any) {
	return []string{"name", "email", "city", "pad"}, []any{
		fmt.Sprintf("user-%09d", n),
		fmt.Sprintf("user-%09d@example.com", n),
		fmt.Sprintf("city-%02d", n%collectionFixtureCities),
		collectionFixturePad,
	}
}

func wideFixtureDocumentFieldsAndValues(cfg config, n int) ([]string, []any) {
	shapeOrdinal := 0
	if cfg.DocumentShape == documentShapeHetero && cfg.ShapeCount > 1 {
		shapeOrdinal = n % cfg.ShapeCount
	}
	fields := make([]string, cfg.FieldCount)
	values := make([]any, cfg.FieldCount)
	for i := 0; i < cfg.FieldCount; i++ {
		if cfg.DocumentShape == documentShapeHetero {
			fields[i] = fmt.Sprintf("field_%06d_%03d", shapeOrdinal, i)
		} else {
			fields[i] = fmt.Sprintf("field_%03d", i)
		}
		values[i] = fmt.Sprintf("value_%03d_%09d", i, n)
	}
	return fields, values
}

func fixtureBSONDocument(fields []string, values []any) ([]byte, error) {
	doc := make(bson.D, len(fields))
	for i := range fields {
		doc[i] = bson.E{Key: fields[i], Value: values[i]}
	}
	return bson.Marshal(doc)
}

func fixtureJSONDocument(fields []string, values []any) ([]byte, error) {
	obj := make(map[string]any, len(fields))
	for i := range fields {
		obj[fields[i]] = values[i]
	}
	return json.Marshal(obj)
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

func maybePackLeafGenerations(cfg config, backend *backenddb.DB) (*leafGenerationSummary, error) {
	if !cfg.LeafGenerationPackGC {
		return nil, nil
	}
	out := &leafGenerationSummary{
		Enabled:        cfg.LeafGenerationPackGC,
		Force:          cfg.LeafGenerationPackForce,
		MaxGenerations: cfg.LeafGenerationPackMaxGen,
	}
	beforeDisk, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return out, err
	}
	out.DiskBytesBefore = beforeDisk.TotalBytes

	ctx := context.Background()
	planStart := time.Now()
	plan, err := backend.LeafGenerationPlan(ctx, backenddb.LeafGenerationPlanOptions{Force: cfg.LeafGenerationPackForce})
	if err != nil {
		return out, fmt.Errorf("leaf-generation plan: %w", err)
	}
	out.PlanTiming = timing(time.Since(planStart), 1)
	out.PlanAdmission = plan.Admission
	out.CandidateGenerations = len(plan.CandidateGenerationIDs)
	out.CandidateBytesTotal = plan.CandidateBytesTotal
	out.CandidateBytesLive = plan.CandidateBytesLive
	out.CandidateBytesDead = plan.CandidateBytesDead
	out.CandidateBytesToCopy = plan.CandidateBytesToCopy
	out.CandidateLivePages = plan.CandidateLivePages
	out.ExpectedReclaimBytes = plan.ExpectedReclaimBytes
	out.ExpectedReclaimRatioPPM = plan.ExpectedReclaimRatioPPM
	out.ExpectedReclaimPerCopyPPM = plan.ExpectedReclaimPerByteCopiedPPM
	if len(plan.CandidateGenerationIDs) == 0 {
		out.DiskBytesAfterPack = beforeDisk.TotalBytes
		gcStart := time.Now()
		gcStats, err := backend.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{})
		if err != nil {
			return out, fmt.Errorf("leaf-generation GC without pack candidates: %w", err)
		}
		out.GCTiming = timing(time.Since(gcStart), 1)
		out.GCGenerationsDeleted = gcStats.GenerationsDeleted
		out.GCFilesDeleted = gcStats.FilesDeleted
		out.GCBytesDeleted = gcStats.BytesDeleted
		if err := backend.Checkpoint(); err != nil {
			return out, fmt.Errorf("checkpoint after leaf-generation GC without pack candidates: %w", err)
		}
		afterGC, err := directoryUsage(cfg.Dir, cfg.Docs)
		if err != nil {
			return out, err
		}
		out.DiskBytesAfterGC = afterGC.TotalBytes
		return out, nil
	}

	packStart := time.Now()
	packStats, err := backend.LeafGenerationPackFromPlan(ctx, backenddb.LeafGenerationPackFromPlanOptions{
		Force:          cfg.LeafGenerationPackForce,
		MaxGenerations: cfg.LeafGenerationPackMaxGen,
		Sync:           true,
		LeafFrameK:     cfg.LeafGenerationPackFrameK,
	})
	if err != nil {
		return out, fmt.Errorf("leaf-generation pack: %w", err)
	}
	out.PackTiming = timing(time.Since(packStart), 1)
	out.PackGenerationsMatched = packStats.GenerationsMatched
	out.PackSourceBytesTotal = packStats.SourceBytesTotal
	out.PackSourceBytesLive = packStats.SourceBytesLive
	out.PackSourceBytesDead = packStats.SourceBytesDead
	out.PackSourceBytesToCopy = packStats.SourceBytesToCopy
	out.PackLeafPagesCopied = packStats.LeafPagesCopied
	out.PackLeafFramesWritten = packStats.LeafFramesWritten
	out.PackMaxLeafFrameK = packStats.MaxLeafFrameK
	out.PackBytesCopied = packStats.BytesCopied
	out.PackCreatedFiles = len(packStats.CreatedFileIDs)
	if err := backend.Checkpoint(); err != nil {
		return out, fmt.Errorf("checkpoint after leaf-generation pack: %w", err)
	}
	afterPack, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return out, err
	}
	out.DiskBytesAfterPack = afterPack.TotalBytes

	gcStart := time.Now()
	gcStats, err := backend.LeafGenerationGC(ctx, backenddb.LeafGenerationGCOptions{})
	if err != nil {
		return out, fmt.Errorf("leaf-generation GC after pack: %w", err)
	}
	out.GCTiming = timing(time.Since(gcStart), 1)
	out.GCGenerationsDeleted = gcStats.GenerationsDeleted
	out.GCFilesDeleted = gcStats.FilesDeleted
	out.GCBytesDeleted = gcStats.BytesDeleted
	if err := backend.Checkpoint(); err != nil {
		return out, fmt.Errorf("checkpoint after leaf-generation GC: %w", err)
	}
	afterGC, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return out, err
	}
	out.DiskBytesAfterGC = afterGC.TotalBytes
	return out, nil
}

func maybeVacuumIndex(cfg config, backend *backenddb.DB, cleanup func() error, closed *bool) (indexVacuumSummary, indexStorageSummary, error) {
	mode := effectiveIndexVacuumMode(cfg)
	out := indexVacuumSummary{
		Mode:          mode,
		RequestedMode: cfg.IndexVacuum,
		Enabled:       mode != indexVacuumModeNone,
	}
	if out.RequestedMode == out.Mode {
		out.RequestedMode = ""
	}
	if mode == indexVacuumModeNone {
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
	switch mode {
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
		opts := backendOptions(cfg, false)
		if err := treedb.VacuumIndexOffline(opts); err != nil {
			return out, indexStorageSummary{}, fmt.Errorf("index vacuum offline: %w", err)
		}
	default:
		return out, indexStorageSummary{}, fmt.Errorf("unsupported index vacuum mode %q", mode)
	}
	out.Timing = timing(time.Since(start), 1)

	afterDisk, err := directoryUsage(cfg.Dir, cfg.Docs)
	if err != nil {
		return out, indexStorageSummary{}, err
	}
	out.DiskBytesAfter = afterDisk.TotalBytes

	var afterStorage indexStorageSummary
	if mode == indexVacuumModeOffline {
		afterStorage, err = captureIndexStorageSummaryReopen(cfg)
	} else {
		afterStorage, err = captureIndexStorageSummary(cfg, backend)
	}
	if err != nil {
		return out, indexStorageSummary{}, err
	}
	out.IndexDBBytesAfter = afterStorage.IndexDBBytes
	out.StorageAfter = afterStorage
	return out, afterStorage, nil
}

func effectiveIndexVacuumMode(cfg config) string {
	switch cfg.IndexVacuum {
	case indexVacuumModeAuto, "":
		if cfg.ValueLogRewrite || cfg.LeafGenerationPackGC {
			return indexVacuumModeOffline
		}
		return indexVacuumModeNone
	default:
		return cfg.IndexVacuum
	}
}

func captureIndexStorageSummaryReopen(cfg config) (indexStorageSummary, error) {
	backend, cleanup, err := openBackendReadOnly(cfg, true)
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
	backend, cleanup, err := openBackendReadOnly(cfg, reopenVerifyReadOnly(cfg))
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
		if cfg.DocumentFormat == collections.DocumentFormatTemplateV1 && cfg.DocumentShape == documentShapeHetero {
			gotJSON, err := collection.StoredDocumentJSON(got)
			if err != nil {
				return 0, fmt.Errorf("reopen verify materialize %s: %w", id, err)
			}
			wantJSON, err := expectedDocumentJSON(cfg, n)
			if err != nil {
				return 0, fmt.Errorf("reopen verify expected JSON %s: %w", id, err)
			}
			if !bytes.Equal(gotJSON, wantJSON) {
				return 0, fmt.Errorf("reopen verify get %s returned unexpected materialized document content", id)
			}
		} else {
			want, err := expectedStoredDocument(cfg, n)
			if err != nil {
				return 0, fmt.Errorf("reopen verify expected document %s: %w", id, err)
			}
			if !bytes.Equal(got, want) {
				return 0, fmt.Errorf("reopen verify get %s returned unexpected document content", id)
			}
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

func reopenVerifyReadOnly(cfg config) bool {
	return cfg.Checkpoint
}

func expectedStoredDocument(cfg config, n int) ([]byte, error) {
	if cfg.DocumentFormat == collections.DocumentFormatJSON || cfg.DocumentFormat == collections.DocumentFormatBSON {
		return document(cfg, nil, n)
	}
	doc, err := document(cfg, &collections.TemplateV1Encoder{}, n)
	if err != nil {
		return nil, err
	}
	return templateV1StoredDocument(doc)
}

func expectedDocumentJSON(cfg config, n int) ([]byte, error) {
	fields, values := fixtureDocumentFieldsAndValues(cfg, n)
	return fixtureJSONDocument(fields, values)
}

func templateV1StoredDocument(raw []byte) ([]byte, error) {
	const (
		inputMagic  = "TD1I"
		insertMagic = "TD1H"
		storedMagic = "TD1D"
	)
	if bytes.HasPrefix(raw, []byte(storedMagic)) {
		return raw, nil
	}
	pos := 0
	if bytes.HasPrefix(raw, []byte(inputMagic)) {
		pos = len(inputMagic)
		templateCount, n := binary.Uvarint(raw[pos:])
		if n <= 0 {
			return nil, errors.New("template-v1 document has malformed template count")
		}
		pos += n
		for i := uint64(0); i < templateCount; i++ {
			if len(raw)-pos < 32 {
				return nil, errors.New("template-v1 document has malformed template hash")
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
	}
	if !bytes.HasPrefix(raw[pos:], []byte(insertMagic)) {
		return nil, errors.New("template-v1 document is missing insert payload")
	}
	pos += len(insertMagic)
	if len(raw)-pos < 32 {
		return nil, errors.New("template-v1 document has malformed root template hash")
	}
	pos += 32
	out := make([]byte, 0, len(storedMagic)+binary.MaxVarintLen64+len(raw)-pos)
	out = append(out, storedMagic...)
	out = binary.AppendUvarint(out, 1)
	out = append(out, raw[pos:]...)
	return out, nil
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
	fmt.Fprintf(w, "shape: format=%s document_shape=%s field_count=%d shape_count=%d indexes=%d data_outer_leaves_in_vlog=%t index_outer_leaves_in_vlog=%t profile=%s\n",
		summary.DocumentFormat, summary.DocumentShape, summary.FieldCount, summary.ShapeCount, summary.IndexCount, summary.DataOuterLeavesInValueLog, summary.IndexOuterLeavesInValueLog, summary.Profile)
	if summary.BufferedIndexedWrites {
		fmt.Fprintln(w, "indexed write buffering: enabled")
		if summary.BufferedIndexedWriteMaxDocs > 0 || summary.BufferedIndexedWriteMaxBytes > 0 || summary.BufferedIndexedWriteMaxRuns > 0 {
			fmt.Fprintf(w, "indexed write buffer limits: max_docs=%d max_bytes=%s max_root_runs=%d\n",
				summary.BufferedIndexedWriteMaxDocs, humanBytes(uint64(summary.BufferedIndexedWriteMaxBytes)), summary.BufferedIndexedWriteMaxRuns)
		}
		if summary.BufferedIndexedAsyncFlush {
			fmt.Fprintf(w, "indexed async flush: enabled max_queued_units=%d\n", summary.BufferedIndexedAsyncFlushMaxQueuedUnits)
		} else if summary.DisableBufferedIndexedAsyncFlush {
			fmt.Fprintln(w, "indexed async flush: disabled")
		}
	}
	fmt.Fprintf(w, "index policy: keep_recent=%d prefer_append_alloc=%t background_prune=%t\n",
		summary.KeepRecent, summary.PreferAppendAlloc, !summary.DisableBackgroundPrune)
	if summary.LeafSegmentTargetBytes > 0 {
		fmt.Fprintf(w, "leaf_vlog generation target: %s\n", humanBytes(uint64(summary.LeafSegmentTargetBytes)))
	}
	fmt.Fprintf(w, "loaded: docs=%d batches=%d batch_size=%d\n", summary.Docs, summary.Batches, summary.BatchSize)
	printTiming(w, "wall", summary.WallTiming)
	printTiming(w, "document generation", summary.GenerationTiming)
	printTiming(w, "insert", summary.InsertTiming)
	if summary.FlushTiming.Seconds > 0 {
		printTiming(w, "flush", summary.FlushTiming)
	}
	if summary.CheckpointTiming.Seconds > 0 {
		printTiming(w, "checkpoint", summary.CheckpointTiming)
	}
	printIndexStorageSummary(w, "index before maintenance", summary.IndexStorageBeforeMaintenance)
	fmt.Fprintf(w, "disk before maintenance: %s total, %.2f bytes/doc, %d files\n",
		humanBytes(summary.DiskUsageBeforeMaintenance.TotalBytes), summary.DiskUsageBeforeMaintenance.BytesPerDoc, summary.DiskUsageBeforeMaintenance.FileCount)
	if summary.Rewrite.Enabled {
		fmt.Fprintf(w, "value_vlog rewrite: before=%s after_rewrite=%s after_gc=%s copied_records=%d copied_value_bytes=%s\n",
			humanBytes(summary.Rewrite.DiskBytesBefore),
			humanBytes(summary.Rewrite.DiskBytesAfterRewrite),
			humanBytes(summary.Rewrite.DiskBytesAfterGC),
			summary.Rewrite.RecordsCopied,
			humanBytes(uint64(max(summary.Rewrite.ValueBytesCopied, 0))))
	}
	if summary.LeafGeneration != nil && summary.LeafGeneration.Enabled {
		fmt.Fprintf(w, "leaf_vlog pack/gc: before=%s after_pack=%s after_gc=%s candidates=%d live=%s dead=%s pages_copied=%d frames=%d max_k=%d files_deleted=%d\n",
			humanBytes(summary.LeafGeneration.DiskBytesBefore),
			humanBytes(summary.LeafGeneration.DiskBytesAfterPack),
			humanBytes(summary.LeafGeneration.DiskBytesAfterGC),
			summary.LeafGeneration.CandidateGenerations,
			humanBytes(uint64(max(summary.LeafGeneration.CandidateBytesLive, 0))),
			humanBytes(uint64(max(summary.LeafGeneration.CandidateBytesDead, 0))),
			summary.LeafGeneration.PackLeafPagesCopied,
			summary.LeafGeneration.PackLeafFramesWritten,
			summary.LeafGeneration.PackMaxLeafFrameK,
			summary.LeafGeneration.GCFilesDeleted)
	}
	if summary.IndexVacuum.Enabled {
		label := summary.IndexVacuum.Mode
		if summary.IndexVacuum.RequestedMode != "" {
			label = fmt.Sprintf("%s requested=%s", summary.IndexVacuum.Mode, summary.IndexVacuum.RequestedMode)
		}
		fmt.Fprintf(w, "index vacuum %s: before=%s after=%s index.db_before=%s index.db_after=%s\n",
			label,
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
