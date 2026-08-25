package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	scaleSchemaVersion = "treedb_text_hybrid_scale/v2"

	collectionName  = "docs"
	textIndexName   = "lexical"
	vectorIndexName = "embedding_graph"
	tenantIndexName = "tenant"
	regionIndexName = "region"

	rareTextTerm = "raretoken2731"
	rareTenant   = "tenant-rare-06pct"

	queryRowTextCommon          = "text_common_score_only"
	queryRowTextRare            = "text_rare_score_only"
	queryRowTextMultiTermAND    = "text_multi_term_and_score_only"
	queryRowTextMultiTermOR     = "text_multi_term_or_score_only"
	queryRowHybridText          = "hybrid_text_only_no_docs"
	queryRowHybridTextScalar    = "hybrid_text_scalar_no_docs"
	queryRowHybridTextVector    = "hybrid_text_vector_no_docs"
	queryRowHybridTextVecScalar = "hybrid_text_vector_scalar_no_docs"
)

type queryRowClass struct {
	hybrid         bool
	vectorRequired bool
}

// queryRowClasses is the public -query-rows contract and classifies every row
// emitted by runQueryMatrix.
var queryRowClasses = map[string]queryRowClass{
	queryRowTextCommon:          {},
	queryRowTextRare:            {},
	queryRowTextMultiTermAND:    {},
	queryRowTextMultiTermOR:     {},
	queryRowHybridText:          {hybrid: true},
	queryRowHybridTextScalar:    {hybrid: true},
	queryRowHybridTextVector:    {hybrid: true, vectorRequired: true},
	queryRowHybridTextVecScalar: {hybrid: true, vectorRequired: true},
}

type config struct {
	outDir              string
	dbDir               string
	keepDB              bool
	rows                int
	batchSize           int
	dims                int
	m                   int
	efConstruction      int
	efSearch            int
	topK                int
	candidateLimit      int
	queries             int
	readers             int
	includeVector       bool
	runBackfill         bool
	backfillRows        int
	runReopen           bool
	runConcurrent       bool
	concurrentWrites    int
	runRewrite          bool
	maintenanceUpdates  int
	maintenanceDeletes  int
	allowGuardrailFails bool
	baseRef             string
	baseSHA             string
	phases              string
	selectedPhases      map[string]bool
	queryRows           map[string]bool
	cpuProfile          string
	allocProfile        string
	allocBaseProfile    string
}

type report struct {
	SchemaVersion    string             `json:"schema_version"`
	GeneratedAt      string             `json:"generated_at"`
	Context          reportContext      `json:"context"`
	Config           reportConfig       `json:"config"`
	Artifacts        reportArtifacts    `json:"artifacts"`
	Load             loadReport         `json:"load"`
	Backfill         *backfillReport    `json:"backfill,omitempty"`
	Reopen           *reopenReport      `json:"reopen,omitempty"`
	Queries          []queryReport      `json:"queries,omitempty"`
	Concurrent       *concurrentReport  `json:"concurrent,omitempty"`
	Maintenance      *maintenanceReport `json:"maintenance,omitempty"`
	StorageSnapshots []storageSnapshot  `json:"storage_snapshots,omitempty"`
	Guardrails       []guardrailResult  `json:"guardrails,omitempty"`
	Bottlenecks      []bottleneckRow    `json:"bottlenecks,omitempty"`
	Caveats          []string           `json:"caveats,omitempty"`
	SelectedPhases   []string           `json:"selected_phases"`
	CompletedPhases  []string           `json:"completed_phases"`
	Complete         bool               `json:"complete"`
}

type reportContext struct {
	RepoRoot    string `json:"repo_root,omitempty"`
	Branch      string `json:"branch,omitempty"`
	Commit      string `json:"commit,omitempty"`
	BaseRef     string `json:"base_ref,omitempty"`
	BaseSHA     string `json:"base_sha,omitempty"`
	Go          string `json:"go,omitempty"`
	OS          string `json:"os,omitempty"`
	Arch        string `json:"arch,omitempty"`
	CPU         string `json:"cpu,omitempty"`
	NCPU        int    `json:"ncpu"`
	Uptime      string `json:"uptime,omitempty"`
	Command     string `json:"command,omitempty"`
	VCSClean    bool   `json:"vcs_clean"`
	VCSStatus   string `json:"vcs_status,omitempty"`
	BinaryState string `json:"binary_state,omitempty"`
	Corpus      string `json:"corpus,omitempty"`
	Cache       string `json:"cache,omitempty"`
	Durability  string `json:"durability,omitempty"`
	NoisePolicy string `json:"noise_policy,omitempty"`
}

type reportConfig struct {
	Rows               int    `json:"rows"`
	BatchSize          int    `json:"batch_size"`
	Dims               int    `json:"dims"`
	M                  int    `json:"m"`
	EfConstruction     int    `json:"ef_construction"`
	EfSearch           int    `json:"ef_search"`
	TopK               int    `json:"top_k"`
	CandidateLimit     int    `json:"candidate_limit"`
	Queries            int    `json:"queries"`
	Readers            int    `json:"readers"`
	IncludeVector      bool   `json:"include_vector"`
	RunBackfill        bool   `json:"run_backfill"`
	BackfillRows       int    `json:"backfill_rows,omitempty"`
	RunReopen          bool   `json:"run_reopen"`
	RunConcurrent      bool   `json:"run_concurrent"`
	ConcurrentWrites   int    `json:"concurrent_writes,omitempty"`
	RunRewrite         bool   `json:"run_rewrite"`
	MaintenanceUpdates int    `json:"maintenance_updates,omitempty"`
	MaintenanceDeletes int    `json:"maintenance_deletes,omitempty"`
	PhaseSelector      string `json:"phase_selector"`
}

type reportArtifacts struct {
	OutDir     string `json:"out_dir"`
	DBDir      string `json:"db_dir"`
	DBKept     bool   `json:"db_kept"`
	JSONReport string `json:"json_report"`
	Markdown   string `json:"markdown"`
}

type loadReport struct {
	Rows                  int                               `json:"rows"`
	Batches               int                               `json:"batches"`
	GenerationSeconds     float64                           `json:"generation_seconds"`
	InsertSeconds         float64                           `json:"insert_seconds"`
	FlushSeconds          float64                           `json:"flush_seconds"`
	VectorRebuildSeconds  float64                           `json:"vector_rebuild_seconds,omitempty"`
	CheckpointSeconds     float64                           `json:"checkpoint_seconds"`
	TotalSeconds          float64                           `json:"total_seconds"`
	RowsPerSecond         float64                           `json:"rows_per_second"`
	TextStorage           collections.TextIndexStorageStats `json:"text_storage"`
	VectorStatus          *collections.VectorIndexStatus    `json:"vector_status,omitempty"`
	StorageBytesAfterLoad int64                             `json:"storage_bytes_after_load"`
	StorageBytesPerDoc    float64                           `json:"storage_bytes_per_doc"`
}

type backfillReport struct {
	Rows               int                                `json:"rows"`
	GenerationSeconds  float64                            `json:"generation_seconds"`
	InsertSeconds      float64                            `json:"insert_seconds"`
	FlushSeconds       float64                            `json:"flush_seconds"`
	BackfillSeconds    float64                            `json:"backfill_seconds"`
	CheckpointSeconds  float64                            `json:"checkpoint_seconds"`
	TotalSeconds       float64                            `json:"total_seconds"`
	RowsPerSecond      float64                            `json:"rows_per_second"`
	Stats              collections.TextIndexBackfillStats `json:"stats"`
	TextStorage        collections.TextIndexStorageStats  `json:"text_storage"`
	StorageBytes       int64                              `json:"storage_bytes"`
	StorageBytesPerDoc float64                            `json:"storage_bytes_per_doc"`
}

type reopenReport struct {
	CloseSeconds          float64                           `json:"close_seconds"`
	OpenSeconds           float64                           `json:"open_seconds"`
	OpenCollectionSeconds float64                           `json:"open_collection_seconds"`
	ProbeSeconds          float64                           `json:"probe_seconds"`
	TotalSeconds          float64                           `json:"total_seconds"`
	TextStorage           collections.TextIndexStorageStats `json:"text_storage"`
	VectorStatus          *collections.VectorIndexStatus    `json:"vector_status,omitempty"`
	StorageBytes          int64                             `json:"storage_bytes"`
}

type queryReport struct {
	Name             string                         `json:"name"`
	Modality         string                         `json:"modality"`
	QueryShape       string                         `json:"query_shape"`
	Boundary         string                         `json:"boundary"`
	Rows             int                            `json:"rows"`
	TopK             int                            `json:"top_k"`
	CandidateBudget  int                            `json:"candidate_budget"`
	Samples          int                            `json:"samples"`
	Results          int                            `json:"results"`
	Latency          latencySummary                 `json:"latency"`
	RawLatencyNS     []int64                        `json:"raw_latency_ns,omitempty"`
	OpsPerSec        float64                        `json:"ops_per_sec"`
	AllocBytes       uint64                         `json:"allocation_bytes,omitempty"`
	AllocObjects     uint64                         `json:"allocation_objects,omitempty"`
	BytesPerOp       float64                        `json:"bytes_per_op,omitempty"`
	AllocsPerOp      float64                        `json:"allocs_per_op,omitempty"`
	TextStats        *collections.TextSearchStats   `json:"text_stats,omitempty"`
	HybridStats      *collections.HybridSearchStats `json:"hybrid_stats,omitempty"`
	GuardrailOK      bool                           `json:"guardrail_ok"`
	GuardrailFailure string                         `json:"guardrail_failure,omitempty"`
}

type latencySummary struct {
	MinNS  int64   `json:"min_ns"`
	P50NS  int64   `json:"p50_ns"`
	P95NS  int64   `json:"p95_ns"`
	P99NS  int64   `json:"p99_ns"`
	MaxNS  int64   `json:"max_ns"`
	MeanNS float64 `json:"mean_ns"`
}

type allocationSummary struct {
	Bytes   uint64
	Objects uint64
}

type concurrentReport struct {
	Readers             int                          `json:"readers"`
	Queries             int                          `json:"queries"`
	Writes              int                          `json:"writes"`
	SearchLatency       latencySummary               `json:"search_latency"`
	ThroughputOpsPerSec float64                      `json:"throughput_ops_per_sec"`
	WriterSeconds       float64                      `json:"writer_seconds"`
	TotalSeconds        float64                      `json:"total_seconds"`
	Errors              []string                     `json:"errors,omitempty"`
	LastTextStats       *collections.TextSearchStats `json:"last_text_stats,omitempty"`
	GuardrailOK         bool                         `json:"guardrail_ok"`
	GuardrailFailure    string                       `json:"guardrail_failure,omitempty"`
}

type maintenanceReport struct {
	Updates              int                               `json:"updates"`
	Deletes              int                               `json:"deletes"`
	UpdateSeconds        float64                           `json:"update_seconds"`
	DeleteSeconds        float64                           `json:"delete_seconds"`
	RewriteSeconds       float64                           `json:"rewrite_seconds"`
	CheckpointSeconds    float64                           `json:"checkpoint_seconds"`
	Stats                collections.TextIndexRewriteStats `json:"stats"`
	TextStorageAfter     collections.TextIndexStorageStats `json:"text_storage_after"`
	StorageBytesAfter    int64                             `json:"storage_bytes_after"`
	PostconditionOK      bool                              `json:"postcondition_ok"`
	PostconditionFailure string                            `json:"postcondition_failure,omitempty"`
}

type storageSnapshot struct {
	Label                       string  `json:"label"`
	Bytes                       int64   `json:"bytes"`
	BytesPerDoc                 float64 `json:"bytes_per_doc,omitempty"`
	TextEncodedBytes            uint64  `json:"text_encoded_bytes,omitempty"`
	TextBytesPerDoc             float64 `json:"text_bytes_per_doc,omitempty"`
	TextDocIDBytesPerDoc        float64 `json:"text_docid_bytes_per_doc,omitempty"`
	TextDocMapBytesPerDoc       float64 `json:"text_docmap_bytes_per_doc,omitempty"`
	TextPostingBlockBytesPerDoc float64 `json:"text_posting_block_bytes_per_doc,omitempty"`
	TextNormBlockBytesPerDoc    float64 `json:"text_norm_block_bytes_per_doc,omitempty"`
	TextPositionBytesPerDoc     float64 `json:"text_position_bytes_per_doc,omitempty"`
	TextTermStatsBytesPerDoc    float64 `json:"text_term_stats_bytes_per_doc,omitempty"`
	TextStatusFormatBytesPerDoc float64 `json:"text_status_format_bytes_per_doc,omitempty"`
	V2PostingBlocks             uint64  `json:"v2_posting_blocks,omitempty"`
	V2LiveDocuments             uint64  `json:"v2_live_documents,omitempty"`
	V2DeletedDocs               uint64  `json:"v2_deleted_docs,omitempty"`
	VectorNativeBytes           int64   `json:"vector_native_bytes,omitempty"`
}

type guardrailResult struct {
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Failure string `json:"failure,omitempty"`
}

type bottleneckRow struct {
	Rank     int     `json:"rank"`
	Name     string  `json:"name"`
	Metric   string  `json:"metric"`
	Value    float64 `json:"value"`
	Unit     string  `json:"unit"`
	FollowUp string  `json:"follow_up"`
}

type scaleFixture struct {
	db        *backenddb.DB
	col       *collections.Collection
	dir       string
	cleanup   func()
	vectorDef collections.VectorIndexDefinition
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "treedb_text_hybrid_scale: %v\n", err)
		os.Exit(2)
	}
	rep, err := run(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "treedb_text_hybrid_scale: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("scale report: %s\n", rep.Artifacts.Markdown)
}

func parseFlags(args []string) (config, error) {
	cfg := config{}
	fs := flag.NewFlagSet("treedb_text_hybrid_scale", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.outDir, "out-dir", "", "Output directory for scale_report.json and scale_report.md")
	fs.StringVar(&cfg.dbDir, "db-dir", "", "TreeDB directory to create/use for the primary fixture; defaults under -out-dir")
	fs.BoolVar(&cfg.keepDB, "keep-db", false, "Keep the generated DB directory after the run")
	fs.IntVar(&cfg.rows, "rows", 10_000, "Rows/documents in the primary scale fixture")
	fs.IntVar(&cfg.batchSize, "batch-size", 8_192, "InsertBatch size for fixture loading")
	fs.IntVar(&cfg.dims, "dims", 16, "Vector dimensions when -include-vector is true")
	fs.IntVar(&cfg.m, "m", 8, "HNSW M for the column_graph vector index")
	fs.IntVar(&cfg.efConstruction, "ef-construction", 128, "HNSW ef_construction for vector rebuild")
	fs.IntVar(&cfg.efSearch, "ef-search", 128, "Vector ef_search for hybrid rows")
	fs.IntVar(&cfg.topK, "top-k", 10, "TopK for retrieval rows")
	fs.IntVar(&cfg.candidateLimit, "candidate-limit", 64, "Candidate budget per hybrid source")
	fs.IntVar(&cfg.queries, "queries", 50, "Timed query samples per retrieval row")
	fs.IntVar(&cfg.readers, "readers", 4, "Concurrent readers for the serving sanity row")
	fs.BoolVar(&cfg.includeVector, "include-vector", true, "Build a column_graph vector index and run vector/hybrid rows")
	fs.BoolVar(&cfg.runBackfill, "run-backfill", true, "Run a separate text-v2 CreateTextIndex backfill fixture")
	fs.IntVar(&cfg.backfillRows, "backfill-rows", 0, "Rows in the separate backfill fixture; defaults to -rows")
	fs.BoolVar(&cfg.runReopen, "run-reopen", true, "Close/reopen the primary fixture and run a durability probe")
	fs.BoolVar(&cfg.runConcurrent, "run-concurrent", true, "Run concurrent text serving while a bounded writer mutates the index")
	fs.IntVar(&cfg.concurrentWrites, "concurrent-writes", 0, "Concurrent writer inserts; defaults to min(rows/100,1024)")
	fs.BoolVar(&cfg.runRewrite, "run-rewrite", true, "Run text-v2 rewrite/maintenance after bounded updates/deletes")
	fs.IntVar(&cfg.maintenanceUpdates, "maintenance-updates", 0, "Documents to update before rewrite; defaults to min(rows/100,10000)")
	fs.IntVar(&cfg.maintenanceDeletes, "maintenance-deletes", 0, "Documents to delete before rewrite; defaults to min(rows/200,5000)")
	fs.BoolVar(&cfg.allowGuardrailFails, "allow-guardrail-failures", false, "Write reports even when zero-doc/fail-closed guardrails fail")
	fs.StringVar(&cfg.baseRef, "base-ref", "origin/main", "Base ref label for report context")
	fs.StringVar(&cfg.baseSHA, "base-sha", "", "Base SHA for report context")
	fs.StringVar(&cfg.phases, "phases", "all", "Comma-separated phase selector: all or retrieval (load,queries,reopen)")
	var queryRows string
	fs.StringVar(&queryRows, "query-rows", "", "Comma-separated retrieval row names; empty runs the complete matrix")
	fs.StringVar(&cfg.cpuProfile, "cpu-profile", "", "Write a CPU profile for the selected single hybrid query row")
	fs.StringVar(&cfg.allocProfile, "alloc-profile", "", "Write post-query and .base allocation profiles for one selected hybrid row")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.outDir == "" {
		return config{}, fmt.Errorf("-out-dir is required")
	}
	if cfg.rows <= 0 {
		return config{}, fmt.Errorf("-rows must be positive")
	}
	if cfg.batchSize <= 0 {
		return config{}, fmt.Errorf("-batch-size must be positive")
	}
	if cfg.dims < 3 {
		return config{}, fmt.Errorf("-dims must be at least 3")
	}
	if cfg.m <= 0 {
		return config{}, fmt.Errorf("-m must be positive")
	}
	if cfg.efConstruction <= 0 {
		return config{}, fmt.Errorf("-ef-construction must be positive")
	}
	if cfg.efSearch <= 0 {
		return config{}, fmt.Errorf("-ef-search must be positive")
	}
	if cfg.topK <= 0 {
		return config{}, fmt.Errorf("-top-k must be positive")
	}
	if cfg.candidateLimit <= 0 {
		return config{}, fmt.Errorf("-candidate-limit must be positive")
	}
	if cfg.queries <= 0 {
		return config{}, fmt.Errorf("-queries must be positive")
	}
	if cfg.readers <= 0 {
		return config{}, fmt.Errorf("-readers must be positive")
	}
	if cfg.backfillRows < 0 {
		return config{}, fmt.Errorf("-backfill-rows cannot be negative")
	}
	if cfg.concurrentWrites < 0 || cfg.maintenanceUpdates < 0 || cfg.maintenanceDeletes < 0 {
		return config{}, fmt.Errorf("write/update/delete counts cannot be negative")
	}
	selected, err := parsePhaseSelector(cfg.phases)
	if err != nil {
		return config{}, err
	}
	cfg.selectedPhases = normalizeSelectedPhases(selected, cfg)
	if queryRows != "" {
		cfg.queryRows = make(map[string]bool)
		for _, name := range strings.Split(queryRows, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				return config{}, errors.New("-query-rows contains an empty row name")
			}
			cfg.queryRows[name] = true
		}
	}
	var selectedRow queryRowClass
	for name := range cfg.queryRows {
		class, ok := queryRowClasses[name]
		if !ok {
			return config{}, fmt.Errorf("unknown -query-rows value %q", name)
		}
		if class.vectorRequired && !cfg.includeVector {
			return config{}, fmt.Errorf("-query-rows value %q requires -include-vector=true", name)
		}
		selectedRow = class
	}
	if cfg.cpuProfile != "" || cfg.allocProfile != "" {
		if len(cfg.queryRows) != 1 || !selectedRow.hybrid {
			return config{}, errors.New("-cpu-profile/-alloc-profile require exactly one hybrid -query-rows value")
		}
	}
	effectiveDBDir := cfg.dbDir
	if effectiveDBDir == "" {
		effectiveDBDir = filepath.Join(cfg.outDir, "primary_db")
	}
	outDir, err := resolvePathSymlinks(cfg.outDir)
	if err != nil {
		return config{}, fmt.Errorf("resolve -out-dir: %w", err)
	}
	dbDir, err := resolvePathSymlinks(effectiveDBDir)
	if err != nil {
		return config{}, fmt.Errorf("resolve effective -db-dir: %w", err)
	}
	for _, profile := range []struct {
		name  string
		value *string
	}{
		{name: "-cpu-profile", value: &cfg.cpuProfile},
		{name: "-alloc-profile", value: &cfg.allocProfile},
	} {
		if *profile.value == "" {
			continue
		}
		resolved, err := resolvePathSymlinks(*profile.value)
		if err != nil {
			return config{}, fmt.Errorf("resolve %s: %w", profile.name, err)
		}
		if _, err := os.Lstat(resolved); err == nil {
			return config{}, fmt.Errorf("%s destination must not already exist", profile.name)
		} else if !errors.Is(err, os.ErrNotExist) {
			return config{}, fmt.Errorf("inspect %s destination: %w", profile.name, err)
		}
		*profile.value = resolved
	}
	if cfg.allocProfile != "" {
		cfg.allocBaseProfile, err = resolvePathSymlinks(cfg.allocProfile + ".base")
		if err != nil {
			return config{}, fmt.Errorf("resolve allocation baseline profile: %w", err)
		}
		if _, err := os.Lstat(cfg.allocBaseProfile); err == nil {
			return config{}, errors.New("allocation baseline profile destination must not already exist")
		} else if !errors.Is(err, os.ErrNotExist) {
			return config{}, fmt.Errorf("inspect allocation baseline profile destination: %w", err)
		}
	}
	profilePaths := []struct {
		name  string
		value string
	}{
		{name: "-cpu-profile", value: cfg.cpuProfile},
		{name: "-alloc-profile", value: cfg.allocProfile},
		{name: "allocation baseline profile", value: cfg.allocBaseProfile},
	}
	seenProfiles := make(map[string]string)
	for _, profile := range profilePaths {
		if profile.value == "" {
			continue
		}
		if profile.value == filepath.Join(outDir, "scale_report.json") || profile.value == filepath.Join(outDir, "scale_report.md") {
			return config{}, fmt.Errorf("%s must not resolve to a scale report artifact", profile.name)
		}
		outDirInsideProfile, err := pathIsSameOrDescendant(outDir, profile.value)
		if err != nil {
			return config{}, fmt.Errorf("compare %s and -out-dir: %w", profile.name, err)
		}
		if outDirInsideProfile {
			return config{}, fmt.Errorf("%s must not resolve to -out-dir or its ancestor", profile.name)
		}
		overlapsDBDir, err := pathsOverlap(profile.value, dbDir)
		if err != nil {
			return config{}, fmt.Errorf("compare %s and effective -db-dir: %w", profile.name, err)
		}
		if overlapsDBDir {
			return config{}, fmt.Errorf("%s must not overlap the effective -db-dir", profile.name)
		}
		for _, reserved := range []struct {
			name string
			dir  string
		}{
			{name: "maintenance", dir: filepath.Join(outDir, "maintenance_db")},
			{name: "backfill", dir: filepath.Join(outDir, "backfill_db")},
		} {
			reservedDir, err := resolvePathSymlinks(reserved.dir)
			if err != nil {
				return config{}, fmt.Errorf("resolve %s database directory: %w", reserved.name, err)
			}
			overlapsReservedDir, err := pathsOverlap(profile.value, reservedDir)
			if err != nil {
				return config{}, fmt.Errorf("compare %s and %s database directory: %w", profile.name, reserved.name, err)
			}
			if overlapsReservedDir {
				return config{}, fmt.Errorf("%s must not overlap the %s database directory", profile.name, reserved.name)
			}
		}
		for priorPath, priorName := range seenProfiles {
			overlapsProfile, err := pathsOverlap(profile.value, priorPath)
			if err != nil {
				return config{}, fmt.Errorf("compare %s and %s: %w", priorName, profile.name, err)
			}
			if overlapsProfile {
				return config{}, fmt.Errorf("%s and %s must not overlap", priorName, profile.name)
			}
		}
		seenProfiles[profile.value] = profile.name
	}
	if cfg.cpuProfile != "" && cfg.allocProfile != "" {
		return config{}, errors.New("-cpu-profile and -alloc-profile must be captured in separate runs")
	}
	if cfg.backfillRows <= 0 {
		cfg.backfillRows = cfg.rows
	}
	if cfg.concurrentWrites <= 0 {
		cfg.concurrentWrites = minInt(maxInt(cfg.rows/100, 1), 1024)
	}
	if cfg.maintenanceUpdates <= 0 {
		cfg.maintenanceUpdates = minInt(maxInt(cfg.rows/100, 1), 10_000)
	}
	if cfg.maintenanceDeletes <= 0 {
		cfg.maintenanceDeletes = minInt(maxInt(cfg.rows/200, 1), 5_000)
	}
	return cfg, nil
}

func parsePhaseSelector(raw string) (map[string]bool, error) {
	selected := make(map[string]bool)
	for _, phase := range strings.Split(raw, ",") {
		switch strings.TrimSpace(phase) {
		case "all":
			for _, name := range []string{"load", "queries", "reopen", "concurrent", "maintenance", "backfill"} {
				selected[name] = true
			}
		case "retrieval":
			selected["load"], selected["queries"], selected["reopen"] = true, true, true
		case "":
			return nil, errors.New("-phases cannot be empty")
		default:
			return nil, fmt.Errorf("unknown -phases value %q (want all or retrieval)", phase)
		}
	}
	return selected, nil
}

func normalizeSelectedPhases(selected map[string]bool, cfg config) map[string]bool {
	if !cfg.runReopen {
		delete(selected, "reopen")
	}
	if !cfg.runConcurrent {
		delete(selected, "concurrent")
	}
	if !cfg.runRewrite {
		delete(selected, "maintenance")
	}
	if !cfg.runBackfill {
		delete(selected, "backfill")
	}
	return selected
}

func selectedPhaseNames(selected map[string]bool) []string {
	all := []string{"load", "queries", "reopen", "concurrent", "maintenance", "backfill"}
	var names []string
	for _, phase := range all {
		if selected[phase] {
			names = append(names, phase)
		}
	}
	return names
}

func run(cfg config) (report, error) {
	if err := requireExactMemProfileRate(cfg.allocProfile != "", runtime.MemProfileRate); err != nil {
		return report{}, err
	}
	if cfg.selectedPhases == nil {
		selected, _ := parsePhaseSelector("all")
		cfg.selectedPhases = normalizeSelectedPhases(selected, cfg)
		cfg.phases = "all"
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return report{}, fmt.Errorf("create out dir: %w", err)
	}
	dbDir := cfg.dbDir
	if dbDir == "" {
		dbDir = filepath.Join(cfg.outDir, "primary_db")
	}
	dbContainsOut, err := dbDirContainsOutDir(dbDir, cfg.outDir)
	if err != nil {
		return report{}, err
	}
	if dbContainsOut {
		return report{}, fmt.Errorf("-db-dir %q must not be the same as or a parent of -out-dir %q", dbDir, cfg.outDir)
	}
	if err := prepareEmptyDir(dbDir); err != nil {
		return report{}, err
	}
	cfg.dbDir = dbDir

	rep := report{
		SchemaVersion: scaleSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Context:       captureContext(cfg),
		Config: reportConfig{
			Rows: cfg.rows, BatchSize: cfg.batchSize, Dims: cfg.dims, M: cfg.m,
			EfConstruction: cfg.efConstruction, EfSearch: cfg.efSearch,
			TopK: cfg.topK, CandidateLimit: cfg.candidateLimit, Queries: cfg.queries,
			Readers: cfg.readers, IncludeVector: cfg.includeVector,
			RunBackfill: cfg.runBackfill, BackfillRows: cfg.backfillRows,
			RunReopen: cfg.runReopen, RunConcurrent: cfg.runConcurrent,
			ConcurrentWrites: cfg.concurrentWrites, RunRewrite: cfg.runRewrite,
			MaintenanceUpdates: cfg.maintenanceUpdates, MaintenanceDeletes: cfg.maintenanceDeletes,
			PhaseSelector: cfg.phases,
		},
		Artifacts: reportArtifacts{
			OutDir:     cfg.outDir,
			DBDir:      dbDir,
			DBKept:     cfg.keepDB,
			JSONReport: filepath.Join(cfg.outDir, "scale_report.json"),
			Markdown:   filepath.Join(cfg.outDir, "scale_report.md"),
		},
		Caveats: []string{
			"Synthetic corpus uses deterministic customer-support text, scalar tenants, and small dense vectors; do not use as relevance-quality evidence.",
			"Retrieval rows time warm in-process queries after fixture load/reopen; B/op and allocs/op should be captured with the companion Go benchmark commands when making allocation claims.",
		},
		SelectedPhases: selectedPhaseNames(cfg.selectedPhases),
	}
	if !cfg.includeVector {
		rep.Caveats = append(rep.Caveats, "Vector/hybrid rows were skipped because -include-vector=false.")
	}

	fixture, load, err := loadPrimaryFixture(cfg)
	if err != nil {
		if !cfg.keepDB {
			_ = os.RemoveAll(dbDir)
		}
		return report{}, err
	}
	defer func() {
		if fixture.db != nil {
			_ = fixture.db.Close()
		}
		if !cfg.keepDB && fixture.cleanup != nil {
			fixture.cleanup()
		}
	}()
	rep.Load = load
	rep.StorageSnapshots = append(rep.StorageSnapshots, storageSnapshotFromText("after_load", cfg.rows, load.StorageBytesAfterLoad, load.TextStorage, load.VectorStatus))
	if err := completePhase(&rep, "load"); err != nil {
		return rep, err
	}

	if cfg.selectedPhases["queries"] {
		queries, guards, queryErr := runQueryMatrix(fixture.col, cfg)
		rep.Queries = append(rep.Queries, queries...)
		rep.Guardrails = append(rep.Guardrails, guards...)
		if queryErr != nil {
			if err := persistIncompleteReport(&rep); err != nil {
				return rep, err
			}
			return rep, queryErr
		}
		if err := completeGuardedPhase(&rep, "queries", guards, cfg.allowGuardrailFails); err != nil {
			return rep, err
		}
	}

	if cfg.selectedPhases["reopen"] && cfg.runReopen {
		reopen, reopenedFixture, err := runReopenProbe(fixture, cfg)
		if err != nil {
			recordReopenProbeFailure(&rep, err)
			if writeErr := writeReports(rep); writeErr != nil {
				return rep, writeErr
			}
			if cfg.allowGuardrailFails {
				return rep, nil
			}
			return rep, err
		}
		fixture = reopenedFixture
		rep.Reopen = &reopen
		rep.StorageSnapshots = append(rep.StorageSnapshots, storageSnapshotFromText("after_reopen", cfg.rows, reopen.StorageBytes, reopen.TextStorage, reopen.VectorStatus))
		if err := completePhase(&rep, "reopen"); err != nil {
			return rep, err
		}
	}

	if cfg.selectedPhases["concurrent"] && cfg.runConcurrent {
		concurrent, guard, err := runConcurrentProbe(fixture.col, cfg)
		if err != nil {
			return report{}, err
		}
		rep.Concurrent = &concurrent
		rep.Guardrails = append(rep.Guardrails, guard)
		if err := completeGuardedPhase(&rep, "concurrent", []guardrailResult{guard}, cfg.allowGuardrailFails); err != nil {
			return rep, err
		}
	}

	if cfg.selectedPhases["maintenance"] && cfg.runRewrite {
		maintenance, err := runMaintenanceProbe(cfg)
		if err != nil {
			return report{}, err
		}
		rep.Maintenance = &maintenance
		rep.StorageSnapshots = append(rep.StorageSnapshots, storageSnapshotFromText("maintenance_rewrite_fixture", maxInt(cfg.rows-maintenance.Deletes, 1), maintenance.StorageBytesAfter, maintenance.TextStorageAfter, nil))
		guard := guardrailResult{Name: "maintenance_rewrite_postconditions", OK: maintenance.PostconditionOK, Failure: maintenance.PostconditionFailure}
		rep.Guardrails = append(rep.Guardrails, guard)
		if err := completeGuardedPhase(&rep, "maintenance", []guardrailResult{guard}, cfg.allowGuardrailFails); err != nil {
			return rep, err
		}
	}

	if cfg.selectedPhases["backfill"] && cfg.runBackfill {
		backfill, err := runBackfillProbe(cfg)
		if err != nil {
			return report{}, err
		}
		rep.Backfill = &backfill
		rep.StorageSnapshots = append(rep.StorageSnapshots, storageSnapshotFromText("backfill_fixture", cfg.backfillRows, backfill.StorageBytes, backfill.TextStorage, nil))
		if err := completePhase(&rep, "backfill"); err != nil {
			return rep, err
		}
	}

	rep.Complete = len(rep.CompletedPhases) == len(rep.SelectedPhases)
	rep.Bottlenecks = rankBottlenecks(rep)
	if err := writeReports(rep); err != nil {
		return report{}, err
	}
	if err := failOnGuardrails(rep.Guardrails, cfg.allowGuardrailFails); err != nil {
		return rep, err
	}
	return rep, nil
}

func completeGuardedPhase(rep *report, phase string, guards []guardrailResult, allow bool) error {
	if err := failOnGuardrails(guards, false); err != nil {
		// Diagnostic continuation is distinct from qualification: persist the
		// observed rows and failed guards as incomplete evidence, but never mark
		// this phase (or its containing report) complete.
		if writeErr := persistIncompleteReport(rep); writeErr != nil {
			return writeErr
		}
		if allow {
			return nil
		}
		return err
	}
	return completePhase(rep, phase)
}

func persistIncompleteReport(rep *report) error {
	if rep == nil {
		return errors.New("nil report")
	}
	rep.Complete = false
	rep.Bottlenecks = rankBottlenecks(*rep)
	return writeReports(*rep)
}

func completePhase(rep *report, phase string) error {
	if rep == nil {
		return errors.New("nil report")
	}
	for _, completed := range rep.CompletedPhases {
		if completed == phase {
			return nil
		}
	}
	rep.CompletedPhases = append(rep.CompletedPhases, phase)
	rep.Complete = len(rep.CompletedPhases) == len(rep.SelectedPhases)
	rep.Bottlenecks = rankBottlenecks(*rep)
	return writeReports(*rep)
}

func recordReopenProbeFailure(rep *report, err error) {
	if rep == nil || err == nil {
		return
	}
	rep.Guardrails = append(rep.Guardrails, guardrailResult{Name: "reopen_probe", OK: false, Failure: err.Error()})
	rep.Caveats = append(rep.Caveats, "Reopen probe failed; later probes were skipped after writing the partial report.")
	rep.Bottlenecks = rankBottlenecks(*rep)
}

func prepareEmptyDir(dir string) error {
	if dir == "" {
		return errors.New("empty db dir")
	}
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%q exists and is not a directory", dir)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("read %q: %w", dir, err)
		}
		if len(entries) != 0 {
			return fmt.Errorf("%q already exists and is not empty", dir)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat %q: %w", dir, err)
	}
	return os.MkdirAll(dir, 0o755)
}

func dbDirContainsOutDir(dbDir, outDir string) (bool, error) {
	dbAbs, err := resolvePathSymlinks(dbDir)
	if err != nil {
		return false, fmt.Errorf("resolve db dir %q: %w", dbDir, err)
	}
	outAbs, err := resolvePathSymlinks(outDir)
	if err != nil {
		return false, fmt.Errorf("resolve out dir %q: %w", outDir, err)
	}
	return pathIsSameOrDescendant(outAbs, dbAbs)
}

func pathIsSameOrDescendant(path, dir string) (bool, error) {
	rel, err := filepath.Rel(filepath.Clean(dir), filepath.Clean(path))
	if err != nil {
		return false, err
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))), nil
}

func pathsOverlap(a, b string) (bool, error) {
	aInsideB, err := pathIsSameOrDescendant(a, b)
	if err != nil || aInsideB {
		return aInsideB, err
	}
	return pathIsSameOrDescendant(b, a)
}

func resolvePathSymlinks(path string) (string, error) {
	resolved, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for followed := 0; followed < 255; followed++ {
		volume := filepath.VolumeName(resolved)
		root := volume + string(os.PathSeparator)
		parts := strings.Split(strings.TrimPrefix(resolved, root), string(os.PathSeparator))
		current := root
		restart := false
		for i, part := range parts {
			if part == "" {
				continue
			}
			current = filepath.Join(current, part)
			info, err := os.Lstat(current)
			if errors.Is(err, os.ErrNotExist) {
				return filepath.Join(append([]string{current}, parts[i+1:]...)...), nil
			}
			if err != nil {
				return "", err
			}
			if info.Mode()&os.ModeSymlink == 0 {
				continue
			}
			target, err := os.Readlink(current)
			if err != nil {
				return "", err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(current), target)
			}
			resolved = filepath.Join(append([]string{target}, parts[i+1:]...)...)
			restart = true
			break
		}
		if !restart {
			return filepath.Clean(current), nil
		}
	}
	return "", fmt.Errorf("too many symbolic links in %q", path)
}

func loadPrimaryFixture(cfg config) (scaleFixture, loadReport, error) {
	start := time.Now()
	if cfg.includeVector {
		if err := backenddb.SaveFormatConfig(cfg.dbDir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
			return scaleFixture{}, loadReport{}, fmt.Errorf("enable command WAL for vector fixture: %w", err)
		}
	}
	db, err := backenddb.Open(backenddb.Options{Dir: cfg.dbDir, DisableBackgroundPrune: true})
	if err != nil {
		return scaleFixture{}, loadReport{}, fmt.Errorf("open primary db: %w", err)
	}
	fixture := scaleFixture{db: db, dir: cfg.dbDir, cleanup: func() { _ = os.RemoveAll(cfg.dbDir) }}
	mgr := collections.NewCollectionManager(db)
	meta := primaryCollectionMeta(cfg)
	if _, err := mgr.CreateCollection(meta); err != nil {
		_ = db.Close()
		return scaleFixture{}, loadReport{}, fmt.Errorf("create collection: %w", err)
	}
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		_ = db.Close()
		return scaleFixture{}, loadReport{}, fmt.Errorf("open collection: %w", err)
	}
	fixture.col = col
	if cfg.includeVector {
		fixture.vectorDef = meta.VectorIndexes[0]
	}

	load := loadReport{Rows: cfg.rows}
	var genElapsed, insertElapsed time.Duration
	for offset := 0; offset < cfg.rows; offset += cfg.batchSize {
		count := minInt(cfg.batchSize, cfg.rows-offset)
		genStart := time.Now()
		ids, docs := makeScaleBatch(offset, count, cfg.dims, "load")
		genElapsed += time.Since(genStart)
		insertStart := time.Now()
		if _, err := col.InsertBatch(ids, docs); err != nil {
			_ = db.Close()
			return scaleFixture{}, loadReport{}, fmt.Errorf("insert batch offset %d: %w", offset, err)
		}
		insertElapsed += time.Since(insertStart)
		load.Batches++
	}
	flushStart := time.Now()
	if err := col.Flush(); err != nil {
		_ = db.Close()
		return scaleFixture{}, loadReport{}, fmt.Errorf("flush primary fixture: %w", err)
	}
	load.FlushSeconds = secondsSince(flushStart)
	load.GenerationSeconds = genElapsed.Seconds()
	load.InsertSeconds = insertElapsed.Seconds()

	if cfg.includeVector {
		rebuildStart := time.Now()
		status, err := col.RebuildVectorIndex(vectorIndexName)
		if err != nil {
			_ = db.Close()
			return scaleFixture{}, loadReport{}, fmt.Errorf("rebuild vector index: %w", err)
		}
		load.VectorRebuildSeconds = secondsSince(rebuildStart)
		load.VectorStatus = &status
		if status.State != collections.VectorIndexStateColumnGraphLoaded || !status.Loaded {
			_ = db.Close()
			return scaleFixture{}, loadReport{}, fmt.Errorf("unexpected vector status after rebuild: %+v", status)
		}
	}

	checkpointStart := time.Now()
	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		return scaleFixture{}, loadReport{}, fmt.Errorf("checkpoint primary fixture: %w", err)
	}
	load.CheckpointSeconds = secondsSince(checkpointStart)
	stats, err := col.TextIndexStorageStats(textIndexName)
	if err != nil {
		_ = db.Close()
		return scaleFixture{}, loadReport{}, fmt.Errorf("text storage stats: %w", err)
	}
	load.TextStorage = stats
	bytes, err := dirSize(cfg.dbDir)
	if err != nil {
		_ = db.Close()
		return scaleFixture{}, loadReport{}, err
	}
	load.StorageBytesAfterLoad = bytes
	load.StorageBytesPerDoc = float64(bytes) / float64(cfg.rows)
	load.TotalSeconds = time.Since(start).Seconds()
	load.RowsPerSecond = float64(cfg.rows) / nonZero(load.TotalSeconds)
	return fixture, load, nil
}

func primaryCollectionMeta(cfg config) *collections.CollectionMeta {
	meta := &collections.CollectionMeta{
		Name:    collectionName,
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON},
		Indexes: []collections.IndexDefinition{
			{Name: tenantIndexName, Field: "tenant", ValueType: collections.IndexValueString},
			{Name: regionIndexName, Field: "region", ValueType: collections.IndexValueString},
		},
		TextIndexes: []collections.TextIndexDefinition{textDefinition()},
	}
	if cfg.includeVector {
		meta.Options.ColumnStore = columnStoreConfig(cfg.dims)
		meta.VectorIndexes = []collections.VectorIndexDefinition{vectorDefinition(cfg)}
	}
	return meta
}

func textDefinition() collections.TextIndexDefinition {
	return collections.TextIndexDefinition{
		Name:           textIndexName,
		Version:        collections.TextIndexVersionV2,
		Fields:         []collections.TextIndexField{{Field: "title", Weight: 3}, {Field: "body"}},
		StorePositions: false,
		StoreOffsets:   false,
	}
}

func vectorDefinition(cfg config) collections.VectorIndexDefinition {
	return collections.VectorIndexDefinition{
		Name:           vectorIndexName,
		Field:          "embedding",
		Metric:         collections.VectorMetricCosine,
		Dimensions:     cfg.dims,
		M:              cfg.m,
		EfConstruction: cfg.efConstruction,
		EfSearch:       cfg.efSearch,
		Strategy:       collections.VectorIndexStrategyColumnGraph,
	}
}

func columnStoreConfig(dims int) *collections.ColumnStoreConfig {
	return &collections.ColumnStoreConfig{
		Enabled:                 true,
		RetainedPayload:         collections.ColumnRetainedPayloadFull,
		RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		ProfileSupport:          collections.ColumnStoreProfileBenchmarkRelaxed,
		Columns: []collections.ColumnStoreColumn{{
			Name:       "embedding",
			Path:       "embedding",
			Owner:      collections.TypedStorageOwnerColumnPart,
			ValueType:  collections.ColumnStoreValueFloat32Vector,
			VectorDims: dims,
		}},
	}
}

func makeScaleBatch(offset, count, dims int, label string) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ordinal := offset + i
		ids[i] = scaleDocID(ordinal)
		docs[i] = scaleDocument(ordinal, dims, label)
	}
	return ids, docs
}

func scaleDocID(i int) []byte {
	return []byte(fmt.Sprintf("doc-%09d", i))
}

func scaleDocument(i, dims int, label string) []byte {
	topic := "shipping"
	title := "shipping status"
	bodyPrefix := "shipping status update parcel route"
	if i%2 == 0 {
		topic = "refund"
		title = "refund policy"
		bodyPrefix = "refund policy customer credit"
	}
	tenant := "tenant-common"
	if i%16 == 0 {
		tenant = rareTenant
	} else if i%4 == 2 {
		tenant = "tenant-narrow-25pct"
	}
	rare := i%997 == 0
	var b []byte
	b = append(b, `{"time_us":`...)
	b = strconv.AppendInt(b, int64(i+1), 10)
	b = append(b, `,"kind":"text-hybrid-scale","did":"`...)
	b = append(b, scaleDocID(i)...)
	b = append(b, `","tenant":"`...)
	b = append(b, tenant...)
	b = append(b, `","region":"region-`...)
	b = strconv.AppendInt(b, int64(i%8), 10)
	b = append(b, `","title":"`...)
	b = appendJSONStringContent(b, title)
	if rare {
		b = append(b, ' ')
		b = append(b, rareTextTerm...)
	}
	b = append(b, `","body":"`...)
	b = appendJSONStringContent(b, bodyPrefix)
	b = append(b, ` shard_`...)
	b = strconv.AppendInt(b, int64(i%64), 10)
	b = append(b, ` customer_`...)
	b = strconv.AppendInt(b, int64(i%1009), 10)
	b = append(b, ` corpus_`...)
	b = appendJSONStringContent(b, label)
	if rare {
		b = append(b, ' ')
		b = append(b, rareTextTerm...)
	}
	b = append(b, `","embedding":[`...)
	vec := scaleVector(dims, i, topic)
	for j, value := range vec {
		if j > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendFloat(b, float64(value), 'f', -1, 32)
	}
	b = append(b, `]}`...)
	return b
}

func appendJSONStringContent(dst []byte, s string) []byte {
	quoted := strconv.AppendQuote(nil, s)
	return append(dst, quoted[1:len(quoted)-1]...)
}

func scaleVector(dims, i int, topic string) []float32 {
	v := make([]float32, dims)
	if topic == "refund" {
		v[0] = 1
		v[1] = 0.10 + float32(i%7)*0.005
		v[2] = 0.05 + float32(i%11)*0.003
	} else {
		v[0] = 0.04 + float32(i%5)*0.004
		v[1] = 1
		v[2] = 0.15 + float32(i%13)*0.002
	}
	for j := 3; j < dims; j++ {
		v[j] = float32(((i+1)*(j+3))%23) * 0.002
	}
	return v
}

func queryVector(dims int) []float32 {
	q := make([]float32, dims)
	q[0] = 1
	q[1] = 0.12
	q[2] = 0.06
	for i := 3; i < dims; i++ {
		q[i] = float32((i+5)%11) * 0.001
	}
	return q
}

func runQueryMatrix(col *collections.Collection, cfg config) ([]queryReport, []guardrailResult, error) {
	rows := []queryReport{}
	guards := []guardrailResult{}
	textCases := []struct {
		name     string
		query    string
		operator collections.TextSearchOperator
		shape    string
	}{
		{name: queryRowTextCommon, query: "refund", shape: "common single-term BM25F block-max top-k"},
		{name: queryRowTextRare, query: rareTextTerm, shape: "rare single-term BM25F top-k"},
		{name: queryRowTextMultiTermAND, query: "refund AND policy", operator: collections.TextSearchOperatorAND, shape: "multi-term AND exact BM25F"},
		{name: queryRowTextMultiTermOR, query: "refund policy", operator: collections.TextSearchOperatorOR, shape: "multi-term OR current exact BM25F path"},
	}
	for _, tc := range textCases {
		if !selectedQueryRow(cfg, tc.name) {
			continue
		}
		report, guard, err := runTextQueryRow(col, cfg, tc.name, tc.query, tc.operator, tc.shape)
		rows = append(rows, report)
		guards = append(guards, guard)
		if err != nil {
			return rows, guards, err
		}
	}

	hybridCases := []struct {
		name  string
		shape string
		opts  collections.HybridSearchOptions
	}{
		{
			name:  queryRowHybridText,
			shape: "hybrid executor text source only, score-only result mode",
			opts:  collections.HybridSearchOptions{TopK: cfg.topK, ResultMode: collections.HybridResultModeScoreOnly, Text: &collections.HybridTextQuery{IndexName: textIndexName, Query: "refund policy", CandidateLimit: cfg.candidateLimit}},
		},
		{
			name:  queryRowHybridTextScalar,
			shape: "hybrid executor text source plus scalar prefilter, score-only result mode",
			opts:  collections.HybridSearchOptions{TopK: cfg.topK, ResultMode: collections.HybridResultModeScoreOnly, Text: &collections.HybridTextQuery{IndexName: textIndexName, Query: "refund policy", CandidateLimit: cfg.candidateLimit}, ScalarFilter: &collections.HybridScalarFilter{IndexName: tenantIndexName, Value: rareTenant}},
		},
	}
	if cfg.includeVector {
		hybridCases = append(hybridCases,
			struct {
				name  string
				shape string
				opts  collections.HybridSearchOptions
			}{
				name:  queryRowHybridTextVector,
				shape: "text+vector RRF, score-only result mode",
				opts:  collections.HybridSearchOptions{TopK: cfg.topK, ResultMode: collections.HybridResultModeScoreOnly, Text: &collections.HybridTextQuery{IndexName: textIndexName, Query: "refund policy", CandidateLimit: cfg.candidateLimit}, Vector: &collections.HybridVectorQuery{IndexName: vectorIndexName, Query: queryVector(cfg.dims), CandidateLimit: cfg.candidateLimit, EfSearch: cfg.efSearch, QueryMode: collections.VectorIndexQueryModeExact}},
			},
			struct {
				name  string
				shape string
				opts  collections.HybridSearchOptions
			}{
				name:  queryRowHybridTextVecScalar,
				shape: "text+vector RRF with scalar prefilter, score-only result mode",
				opts:  collections.HybridSearchOptions{TopK: cfg.topK, ResultMode: collections.HybridResultModeScoreOnly, Text: &collections.HybridTextQuery{IndexName: textIndexName, Query: "refund policy", CandidateLimit: cfg.candidateLimit}, Vector: &collections.HybridVectorQuery{IndexName: vectorIndexName, Query: queryVector(cfg.dims), CandidateLimit: cfg.candidateLimit, EfSearch: cfg.efSearch, QueryMode: collections.VectorIndexQueryModeExact}, ScalarFilter: &collections.HybridScalarFilter{IndexName: tenantIndexName, Value: rareTenant}},
			},
		)
	}
	for _, tc := range hybridCases {
		if !selectedQueryRow(cfg, tc.name) {
			continue
		}
		report, guard, err := runHybridQueryRow(col, cfg, tc.name, tc.shape, tc.opts)
		rows = append(rows, report)
		guards = append(guards, guard)
		if err != nil {
			return rows, guards, err
		}
	}
	return rows, guards, nil
}

func selectedQueryRow(cfg config, name string) bool {
	return len(cfg.queryRows) == 0 || cfg.queryRows[name]
}

func runTextQueryRow(col *collections.Collection, cfg config, name, query string, operator collections.TextSearchOperator, shape string) (queryReport, guardrailResult, error) {
	opts := collections.TextSearchOptions{IndexName: textIndexName, Query: query, Operator: operator, TopK: cfg.topK, ResultMode: collections.TextSearchResultModeScoreOnly, CandidateLimit: cfg.rows, MaxPostingsScanned: maxInt(cfg.rows*4, cfg.topK)}
	warm, err := col.SearchText(opts)
	if err != nil {
		queryErr := fmt.Errorf("warm %s: %w", name, err)
		row := failedTextQueryRow(cfg, name, shape, warm, nil, queryErr)
		guard := textFailureGuard(name, warm.Stats, queryErr)
		if cfg.allowGuardrailFails {
			return row, guard, nil
		}
		return row, guard, queryErr
	}
	if len(warm.Results) == 0 {
		queryErr := fmt.Errorf("warm %s returned no results", name)
		row := failedTextQueryRow(cfg, name, shape, warm, nil, queryErr)
		guard := textFailureGuard(name, warm.Stats, queryErr)
		if cfg.allowGuardrailFails {
			return row, guard, nil
		}
		return row, guard, queryErr
	}
	guard := textGuardrail(name, warm.Stats)
	durations := make([]int64, 0, cfg.queries)
	var last collections.TextSearchResponse
	for i := 0; i < cfg.queries; i++ {
		start := time.Now()
		got, err := col.SearchText(opts)
		elapsed := time.Since(start).Nanoseconds()
		if err != nil {
			queryErr := fmt.Errorf("%s query %d: %w", name, i, err)
			row := failedTextQueryRow(cfg, name, shape, got, durations, queryErr)
			guard := textFailureGuard(name, got.Stats, queryErr)
			if cfg.allowGuardrailFails {
				return row, guard, nil
			}
			return row, guard, queryErr
		}
		durations = append(durations, elapsed)
		last = got
		if g := textGuardrail(name, got.Stats); !g.OK {
			guard = g
		}
	}
	lat := summarizeLatency(durations)
	stats := last.Stats
	return queryReport{
		Name: name, Modality: "text", QueryShape: shape, Boundary: "warm no-document text-v2 score-only search", Rows: cfg.rows,
		TopK: cfg.topK, CandidateBudget: cfg.rows, Samples: len(durations), Results: len(last.Results), Latency: lat, RawLatencyNS: durations, OpsPerSec: opsPerSec(lat.MeanNS),
		TextStats: &stats, GuardrailOK: guard.OK, GuardrailFailure: guard.Failure,
	}, guard, nil
}

func failedTextQueryRow(cfg config, name, shape string, response collections.TextSearchResponse, durations []int64, err error) queryReport {
	stats := response.Stats
	guard := textFailureGuard(name, response.Stats, err)
	lat := summarizeLatency(durations)
	return queryReport{Name: name, Modality: "text", QueryShape: shape, Boundary: "warm no-document text-v2 score-only search", Rows: cfg.rows, TopK: cfg.topK, CandidateBudget: cfg.rows, Samples: len(durations), Results: len(response.Results), Latency: lat, RawLatencyNS: durations, OpsPerSec: opsPerSec(lat.MeanNS), TextStats: &stats, GuardrailOK: false, GuardrailFailure: guard.Failure}
}

func textFailureGuard(name string, stats collections.TextSearchStats, err error) guardrailResult {
	guard := textGuardrail(name, stats)
	if guard.OK {
		return guardrailResult{Name: name, OK: false, Failure: err.Error()}
	}
	if err != nil && !strings.Contains(guard.Failure, err.Error()) {
		guard.Failure += "; error=" + err.Error()
	}
	return guard
}

func runHybridQueryRow(col *collections.Collection, cfg config, name, shape string, opts collections.HybridSearchOptions) (queryReport, guardrailResult, error) {
	warm, err := col.SearchHybrid(opts)
	if err != nil {
		warmErr := fmt.Errorf("warm %s: %w", name, err)
		if err := profiledWarmupError(cfg, warmErr); err != nil {
			return queryReport{}, guardrailResult{}, err
		}
		row := failedHybridQueryRow(cfg, name, shape, warm, nil, warmErr)
		guard := hybridFailureGuard(name, warm.Stats, warmErr)
		if cfg.allowGuardrailFails {
			return row, guard, nil
		}
		return row, guard, warmErr
	}
	if len(warm.Results) == 0 {
		warmErr := fmt.Errorf("warm %s returned no results", name)
		if err := profiledWarmupError(cfg, warmErr); err != nil {
			return queryReport{}, guardrailResult{}, err
		}
		row := failedHybridQueryRow(cfg, name, shape, warm, nil, warmErr)
		guard := guardrailResult{Name: name, OK: false, Failure: warmErr.Error()}
		if cfg.allowGuardrailFails {
			return row, guard, nil
		}
		return row, guard, warmErr
	}
	guard := hybridGuardrail(name, warm.Stats)
	stopProfile, err := startQueryProfiles(cfg)
	if err != nil {
		return queryReport{}, guardrailResult{}, err
	}
	durations, last, guard, allocations, sampleErr := runProfiledHybridSamples(col, cfg, name, opts, guard)
	profileErr := stopProfile()
	if profileErr != nil {
		profileErr = fmt.Errorf("write %s profile: %w", name, profileErr)
		row := withAllocationSummary(failedHybridQueryRow(cfg, name, shape, last, durations, profileErr), cfg, allocations)
		guard = hybridFailureGuard(name, last.Stats, profileErr)
		return row, guard, profileErr
	}
	if sampleErr != nil {
		row := withAllocationSummary(failedHybridQueryRow(cfg, name, shape, last, durations, sampleErr), cfg, allocations)
		guard = hybridFailureGuard(name, last.Stats, sampleErr)
		if cfg.allowGuardrailFails {
			return row, guard, nil
		}
		return row, guard, sampleErr
	}
	lat := summarizeLatency(durations)
	stats := last.Stats
	row := queryReport{
		Name: name, Modality: "hybrid", QueryShape: shape, Boundary: "warm no-document hybrid candidate generation/fusion", Rows: cfg.rows,
		TopK: cfg.topK, CandidateBudget: cfg.candidateLimit, Samples: len(durations), Results: len(last.Results), Latency: lat, RawLatencyNS: durations, OpsPerSec: opsPerSec(lat.MeanNS),
		HybridStats: &stats, GuardrailOK: guard.OK, GuardrailFailure: guard.Failure,
	}
	return withAllocationSummary(row, cfg, allocations), guard, nil
}

func runProfiledHybridSamples(col *collections.Collection, cfg config, name string, opts collections.HybridSearchOptions, guard guardrailResult) ([]int64, collections.HybridSearchResponse, guardrailResult, allocationSummary, error) {
	durations := make([]int64, 0, cfg.queries)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	var last collections.HybridSearchResponse
	for i := 0; i < cfg.queries; i++ {
		start := time.Now()
		got, err := col.SearchHybrid(opts)
		elapsed := time.Since(start).Nanoseconds()
		last = got
		if err != nil {
			runtime.ReadMemStats(&after)
			return durations, last, guard, allocationSummary{Bytes: after.TotalAlloc - before.TotalAlloc, Objects: after.Mallocs - before.Mallocs}, fmt.Errorf("%s query %d: %w", name, i, err)
		}
		durations = append(durations, elapsed)
		if g := hybridGuardrail(name, got.Stats); !g.OK {
			guard = g
		}
	}
	runtime.ReadMemStats(&after)
	return durations, last, guard, allocationSummary{Bytes: after.TotalAlloc - before.TotalAlloc, Objects: after.Mallocs - before.Mallocs}, nil
}

func withAllocationSummary(row queryReport, cfg config, allocations allocationSummary) queryReport {
	if cfg.allocProfile == "" {
		return row
	}
	row.AllocBytes = allocations.Bytes
	row.AllocObjects = allocations.Objects
	if row.Samples > 0 {
		row.BytesPerOp = float64(allocations.Bytes) / float64(row.Samples)
		row.AllocsPerOp = float64(allocations.Objects) / float64(row.Samples)
	}
	return row
}

func requireExactMemProfileRate(allocProfile bool, memProfileRate int) error {
	if allocProfile && memProfileRate != 1 {
		return fmt.Errorf("-alloc-profile requires runtime.MemProfileRate == 1 at process startup; launch with GODEBUG=memprofilerate=1")
	}
	return nil
}

func createProfileFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
}

func startQueryProfiles(cfg config) (func() error, error) {
	return startQueryProfilesAtMemProfileRate(cfg, runtime.MemProfileRate)
}

func startQueryProfilesAtMemProfileRate(cfg config, memProfileRate int) (func() error, error) {
	if cfg.cpuProfile == "" && cfg.allocProfile == "" {
		return func() error { return nil }, nil
	}
	if err := requireExactMemProfileRate(cfg.allocProfile != "", memProfileRate); err != nil {
		return nil, err
	}
	writeProfile := func(path string) error {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		f, err := createProfileFile(path)
		if err != nil {
			return err
		}
		writeErr := pprof.Lookup("allocs").WriteTo(f, 0)
		closeErr := f.Close()
		if writeErr != nil {
			return writeErr
		}
		return closeErr
	}
	baseProfile := cfg.allocBaseProfile
	if baseProfile == "" && cfg.allocProfile != "" {
		baseProfile = cfg.allocProfile + ".base"
	}
	if baseProfile != "" {
		runtime.GC()
		if err := writeProfile(baseProfile); err != nil {
			return nil, fmt.Errorf("write allocation baseline profile: %w", err)
		}
	}
	writeAllocsAfter := func() error {
		if cfg.allocProfile == "" {
			return nil
		}
		// Flush the current allocation cycle after the timed helper returns so
		// allocs includes its recent samples without putting GC in that stack.
		runtime.GC()
		return writeProfile(cfg.allocProfile)
	}
	if cfg.cpuProfile == "" {
		var once sync.Once
		var stopErr error
		return func() error { once.Do(func() { stopErr = writeAllocsAfter() }); return stopErr }, nil
	}
	if err := os.MkdirAll(filepath.Dir(cfg.cpuProfile), 0o755); err != nil {
		return nil, err
	}
	f, err := createProfileFile(cfg.cpuProfile)
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	var once sync.Once
	var stopErr error
	return func() error {
		once.Do(func() {
			pprof.StopCPUProfile()
			stopErr = finalizeQueryProfiles(f.Close, writeAllocsAfter)
		})
		return stopErr
	}, nil
}

func finalizeQueryProfiles(closeCPU, writeAllocs func() error) error {
	return errors.Join(closeCPU(), writeAllocs())
}

func profiledWarmupError(cfg config, warmErr error) error {
	if warmErr == nil || (cfg.cpuProfile == "" && cfg.allocProfile == "") {
		return nil
	}
	return fmt.Errorf("%w; cannot produce requested profile without successful warm-up", warmErr)
}

func failedHybridQueryRow(cfg config, name, shape string, response collections.HybridSearchResponse, durations []int64, err error) queryReport {
	stats := response.Stats
	guard := hybridFailureGuard(name, response.Stats, err)
	lat := summarizeLatency(durations)
	return queryReport{Name: name, Modality: "hybrid", QueryShape: shape, Boundary: "warm no-document hybrid candidate generation/fusion", Rows: cfg.rows, TopK: cfg.topK, CandidateBudget: cfg.candidateLimit, Samples: len(durations), Results: len(response.Results), Latency: lat, RawLatencyNS: durations, OpsPerSec: opsPerSec(lat.MeanNS), HybridStats: &stats, GuardrailOK: false, GuardrailFailure: guard.Failure}
}

func hybridFailureGuard(name string, stats collections.HybridSearchStats, err error) guardrailResult {
	guard := hybridGuardrail(name, stats)
	if guard.OK {
		return guardrailResult{Name: name, OK: false, Failure: err.Error()}
	}
	if err != nil && !strings.Contains(guard.Failure, err.Error()) {
		guard.Failure = guard.Failure + "; error=" + err.Error()
	}
	return guard
}

func runReopenProbe(fixture scaleFixture, cfg config) (reopenReport, scaleFixture, error) {
	startTotal := time.Now()
	closeStart := time.Now()
	if err := fixture.db.Close(); err != nil {
		return reopenReport{}, fixture, fmt.Errorf("close for reopen: %w", err)
	}
	fixture.db = nil
	closeSeconds := secondsSince(closeStart)
	openStart := time.Now()
	db, err := backenddb.Open(backenddb.Options{Dir: fixture.dir, DisableBackgroundPrune: true})
	if err != nil {
		return reopenReport{}, fixture, fmt.Errorf("reopen db: %w", err)
	}
	openSeconds := secondsSince(openStart)
	openColStart := time.Now()
	col, err := collections.NewCollectionManager(db).OpenCollection(collectionName)
	if err != nil {
		_ = db.Close()
		return reopenReport{}, fixture, fmt.Errorf("reopen collection: %w", err)
	}
	openCollectionSeconds := secondsSince(openColStart)
	probeStart := time.Now()
	resp, err := col.SearchText(collections.TextSearchOptions{IndexName: textIndexName, Query: "refund", TopK: cfg.topK, ResultMode: collections.TextSearchResultModeScoreOnly, CandidateLimit: cfg.rows, MaxPostingsScanned: maxInt(cfg.rows*4, cfg.topK)})
	if err != nil {
		_ = db.Close()
		return reopenReport{}, fixture, fmt.Errorf("reopen text probe: %w", err)
	}
	if len(resp.Results) == 0 || !textGuardrail("reopen_text_probe", resp.Stats).OK {
		_ = db.Close()
		return reopenReport{}, fixture, fmt.Errorf("reopen text probe failed guardrail stats=%+v results=%d", resp.Stats, len(resp.Results))
	}
	stats, err := col.TextIndexStorageStats(textIndexName)
	if err != nil {
		_ = db.Close()
		return reopenReport{}, fixture, fmt.Errorf("reopen text storage stats: %w", err)
	}
	var vectorStatus *collections.VectorIndexStatus
	if cfg.includeVector {
		status, err := col.VectorIndexStatus(vectorIndexName)
		if err != nil {
			_ = db.Close()
			return reopenReport{}, fixture, fmt.Errorf("reopen vector status: %w", err)
		}
		vectorStatus = &status
		if status.State != collections.VectorIndexStateColumnGraphLoaded || !status.Loaded || status.RebuildNeeded {
			_ = db.Close()
			return reopenReport{}, fixture, fmt.Errorf("reopen vector status not serveable: state=%s loaded=%v rebuild_needed=%v", status.State, status.Loaded, status.RebuildNeeded)
		}
		vectorProbe, err := col.SearchHybrid(collections.HybridSearchOptions{
			TopK:       cfg.topK,
			ResultMode: collections.HybridResultModeScoreOnly,
			Text:       &collections.HybridTextQuery{IndexName: textIndexName, Query: "refund policy", CandidateLimit: cfg.candidateLimit},
			Vector:     &collections.HybridVectorQuery{IndexName: vectorIndexName, Query: queryVector(cfg.dims), CandidateLimit: cfg.candidateLimit, EfSearch: cfg.efSearch, QueryMode: collections.VectorIndexQueryModeExact},
		})
		if err != nil {
			_ = db.Close()
			return reopenReport{}, fixture, fmt.Errorf("reopen hybrid vector guardrail probe: %w", err)
		}
		if len(vectorProbe.Results) == 0 || !hybridGuardrail("reopen_hybrid_vector_probe", vectorProbe.Stats).OK {
			_ = db.Close()
			return reopenReport{}, fixture, fmt.Errorf("reopen hybrid vector guardrail failed stats=%+v results=%d", vectorProbe.Stats, len(vectorProbe.Results))
		}
	}
	probeSeconds := secondsSince(probeStart)
	bytes, err := dirSize(fixture.dir)
	if err != nil {
		_ = db.Close()
		return reopenReport{}, fixture, err
	}
	fixture.db = db
	fixture.col = col
	return reopenReport{CloseSeconds: closeSeconds, OpenSeconds: openSeconds, OpenCollectionSeconds: openCollectionSeconds, ProbeSeconds: probeSeconds, TotalSeconds: time.Since(startTotal).Seconds(), TextStorage: stats, VectorStatus: vectorStatus, StorageBytes: bytes}, fixture, nil
}

func runConcurrentProbe(col *collections.Collection, cfg config) (concurrentReport, guardrailResult, error) {
	totalQueries := maxInt(cfg.queries, cfg.readers)
	durationCh := make(chan int64, totalQueries)
	errCh := make(chan error, cfg.readers+1)
	statsCh := make(chan collections.TextSearchStats, totalQueries)
	start := time.Now()
	var wg sync.WaitGroup
	var next int
	var nextMu sync.Mutex
	opts := collections.TextSearchOptions{IndexName: textIndexName, Query: "refund", TopK: cfg.topK, ResultMode: collections.TextSearchResultModeScoreOnly, CandidateLimit: cfg.rows + cfg.concurrentWrites, MaxPostingsScanned: maxInt((cfg.rows+cfg.concurrentWrites)*4, cfg.topK)}
	for worker := 0; worker < cfg.readers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				nextMu.Lock()
				idx := next
				next++
				nextMu.Unlock()
				if idx >= totalQueries {
					return
				}
				qStart := time.Now()
				got, err := col.SearchText(opts)
				elapsed := time.Since(qStart).Nanoseconds()
				if err != nil {
					errCh <- err
					return
				}
				if len(got.Results) == 0 {
					errCh <- errors.New("concurrent search returned no results")
					return
				}
				if g := textGuardrail("concurrent_text_search", got.Stats); !g.OK {
					errCh <- errors.New(g.Failure)
					return
				}
				durationCh <- elapsed
				statsCh <- got.Stats
			}
		}()
	}
	writerStart := time.Now()
	ids, docs := makeScaleBatch(cfg.rows+1_000_000_000, cfg.concurrentWrites, cfg.dims, "concurrent")
	_, writeErr := col.InsertBatch(ids, docs)
	writerSeconds := secondsSince(writerStart)
	if writeErr != nil {
		errCh <- fmt.Errorf("concurrent writer InsertBatch: %w", writeErr)
	}
	wg.Wait()
	close(durationCh)
	close(statsCh)
	close(errCh)
	var errs []string
	for err := range errCh {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	var durations []int64
	for duration := range durationCh {
		durations = append(durations, duration)
	}
	var lastStats *collections.TextSearchStats
	for st := range statsCh {
		copied := st
		lastStats = &copied
	}
	lat := summarizeLatency(durations)
	guard := guardrailResult{Name: "concurrent_search_write_sanity", OK: len(errs) == 0}
	if len(errs) != 0 {
		guard.Failure = strings.Join(errs, "; ")
	}
	elapsed := time.Since(start).Seconds()
	return concurrentReport{Readers: cfg.readers, Queries: totalQueries, Writes: cfg.concurrentWrites, SearchLatency: lat, ThroughputOpsPerSec: float64(totalQueries) / nonZero(elapsed), WriterSeconds: writerSeconds, TotalSeconds: elapsed, Errors: errs, LastTextStats: lastStats, GuardrailOK: guard.OK, GuardrailFailure: guard.Failure}, guard, nil
}

func runMaintenanceProbe(cfg config) (maintenanceReport, error) {
	dir := filepath.Join(cfg.outDir, "maintenance_db")
	if err := prepareEmptyDir(dir); err != nil {
		return maintenanceReport{}, err
	}
	if !cfg.keepDB {
		defer os.RemoveAll(dir)
	}
	db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return maintenanceReport{}, fmt.Errorf("open maintenance db: %w", err)
	}
	defer db.Close()
	mgr := collections.NewCollectionManager(db)
	meta := primaryCollectionMeta(config{dims: cfg.dims, includeVector: false})
	if _, err := mgr.CreateCollection(meta); err != nil {
		return maintenanceReport{}, fmt.Errorf("create maintenance collection: %w", err)
	}
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		return maintenanceReport{}, fmt.Errorf("open maintenance collection: %w", err)
	}
	for offset := 0; offset < cfg.rows; offset += cfg.batchSize {
		count := minInt(cfg.batchSize, cfg.rows-offset)
		ids, docs := makeScaleBatch(offset, count, cfg.dims, "maintenance")
		if _, err := col.InsertBatch(ids, docs); err != nil {
			return maintenanceReport{}, fmt.Errorf("maintenance insert batch offset %d: %w", offset, err)
		}
	}
	if err := col.Flush(); err != nil {
		return maintenanceReport{}, fmt.Errorf("maintenance flush: %w", err)
	}
	if err := db.Checkpoint(); err != nil {
		return maintenanceReport{}, fmt.Errorf("maintenance setup checkpoint: %w", err)
	}

	updates := minInt(cfg.maintenanceUpdates, cfg.rows)
	deletes := minInt(cfg.maintenanceDeletes, maxInt(cfg.rows-updates, 0))
	maintenance := maintenanceReport{Updates: updates, Deletes: deletes}
	updateStart := time.Now()
	for i := 0; i < updates; i++ {
		id := scaleDocID(i)
		replacement := scaleDocument(i, cfg.dims, "maintenance-updated")
		updated, changed, err := col.Update(id, func([]byte) ([]byte, bool, error) {
			return replacement, true, nil
		})
		if err != nil {
			return maintenanceReport{}, fmt.Errorf("maintenance update %s: %w", id, err)
		}
		if !updated || !changed {
			return maintenanceReport{}, fmt.Errorf("maintenance update %s updated=%v changed=%v", id, updated, changed)
		}
	}
	maintenance.UpdateSeconds = secondsSince(updateStart)
	deleteStart := time.Now()
	if deletes > 0 {
		ids := make([][]byte, deletes)
		for i := 0; i < deletes; i++ {
			ids[i] = scaleDocID(updates + i)
		}
		deleted, err := col.DeleteBatch(ids)
		if err != nil {
			return maintenanceReport{}, fmt.Errorf("maintenance delete batch: %w", err)
		}
		if deleted != deletes {
			return maintenanceReport{}, fmt.Errorf("maintenance delete batch deleted=%d want %d", deleted, deletes)
		}
	}
	maintenance.DeleteSeconds = secondsSince(deleteStart)
	rewriteStart := time.Now()
	stats, err := col.RewriteTextIndex(textIndexName, collections.TextIndexRewriteOptions{})
	if err != nil {
		return maintenanceReport{}, fmt.Errorf("rewrite text index: %w", err)
	}
	maintenance.RewriteSeconds = secondsSince(rewriteStart)
	checkpointStart := time.Now()
	if err := db.Checkpoint(); err != nil {
		return maintenanceReport{}, fmt.Errorf("checkpoint after rewrite: %w", err)
	}
	maintenance.CheckpointSeconds = secondsSince(checkpointStart)
	storage, err := col.TextIndexStorageStats(textIndexName)
	if err != nil {
		return maintenanceReport{}, fmt.Errorf("text storage after rewrite: %w", err)
	}
	maintenance.Stats = stats
	maintenance.TextStorageAfter = storage
	bytes, err := dirSize(dir)
	if err != nil {
		return maintenanceReport{}, err
	}
	maintenance.StorageBytesAfter = bytes
	maintenance.PostconditionOK = true
	if storage.V2DeletedDocs != 0 {
		maintenance.PostconditionOK = false
		maintenance.PostconditionFailure = fmt.Sprintf("deleted docs remain after rewrite: %d", storage.V2DeletedDocs)
	}
	if storage.V2RewriteMergeState == "" {
		maintenance.PostconditionOK = false
		maintenance.PostconditionFailure = "empty rewrite merge state"
	}
	probe, err := col.SearchText(collections.TextSearchOptions{IndexName: textIndexName, Query: "refund", TopK: cfg.topK, ResultMode: collections.TextSearchResultModeScoreOnly, CandidateLimit: cfg.rows, MaxPostingsScanned: maxInt(cfg.rows*4, cfg.topK)})
	if err != nil {
		maintenance.PostconditionOK = false
		maintenance.PostconditionFailure = "post-rewrite search failed: " + err.Error()
	} else if guard := textGuardrail("post_rewrite_text_probe", probe.Stats); !guard.OK || len(probe.Results) == 0 {
		maintenance.PostconditionOK = false
		if guard.Failure != "" {
			maintenance.PostconditionFailure = guard.Failure
		} else {
			maintenance.PostconditionFailure = "post-rewrite search returned no results"
		}
	}
	return maintenance, nil
}

func runBackfillProbe(cfg config) (backfillReport, error) {
	dir := filepath.Join(cfg.outDir, "backfill_db")
	if err := prepareEmptyDir(dir); err != nil {
		return backfillReport{}, err
	}
	if !cfg.keepDB {
		defer os.RemoveAll(dir)
	}
	start := time.Now()
	db, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		return backfillReport{}, fmt.Errorf("open backfill db: %w", err)
	}
	defer db.Close()
	mgr := collections.NewCollectionManager(db)
	meta := &collections.CollectionMeta{Name: collectionName, Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}, Indexes: []collections.IndexDefinition{{Name: tenantIndexName, Field: "tenant", ValueType: collections.IndexValueString}}}
	if _, err := mgr.CreateCollection(meta); err != nil {
		return backfillReport{}, fmt.Errorf("create backfill collection: %w", err)
	}
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		return backfillReport{}, fmt.Errorf("open backfill collection: %w", err)
	}
	var genElapsed, insertElapsed time.Duration
	for offset := 0; offset < cfg.backfillRows; offset += cfg.batchSize {
		count := minInt(cfg.batchSize, cfg.backfillRows-offset)
		genStart := time.Now()
		ids, docs := makeScaleBatch(offset, count, cfg.dims, "backfill")
		genElapsed += time.Since(genStart)
		insertStart := time.Now()
		if _, err := col.InsertBatch(ids, docs); err != nil {
			return backfillReport{}, fmt.Errorf("backfill insert batch offset %d: %w", offset, err)
		}
		insertElapsed += time.Since(insertStart)
	}
	flushStart := time.Now()
	if err := col.Flush(); err != nil {
		return backfillReport{}, fmt.Errorf("backfill pre-index flush: %w", err)
	}
	flushSeconds := secondsSince(flushStart)
	backfillStart := time.Now()
	_, stats, err := col.CreateTextIndex(textDefinition())
	if err != nil {
		return backfillReport{}, fmt.Errorf("CreateTextIndex backfill: %w", err)
	}
	backfillSeconds := secondsSince(backfillStart)
	checkpointStart := time.Now()
	if err := db.Checkpoint(); err != nil {
		return backfillReport{}, fmt.Errorf("backfill checkpoint: %w", err)
	}
	checkpointSeconds := secondsSince(checkpointStart)
	storage, err := col.TextIndexStorageStats(textIndexName)
	if err != nil {
		return backfillReport{}, fmt.Errorf("backfill text storage stats: %w", err)
	}
	bytes, err := dirSize(dir)
	if err != nil {
		return backfillReport{}, err
	}
	total := time.Since(start).Seconds()
	return backfillReport{Rows: cfg.backfillRows, GenerationSeconds: genElapsed.Seconds(), InsertSeconds: insertElapsed.Seconds(), FlushSeconds: flushSeconds, BackfillSeconds: backfillSeconds, CheckpointSeconds: checkpointSeconds, TotalSeconds: total, RowsPerSecond: float64(cfg.backfillRows) / nonZero(total), Stats: stats, TextStorage: storage, StorageBytes: bytes, StorageBytesPerDoc: float64(bytes) / float64(cfg.backfillRows)}, nil
}

func textGuardrail(name string, stats collections.TextSearchStats) guardrailResult {
	var failures []string
	if stats.DocumentsFetched != 0 {
		failures = append(failures, fmt.Sprintf("docs_fetched=%d", stats.DocumentsFetched))
	}
	if stats.FullDocumentScanFallbacks != 0 {
		failures = append(failures, fmt.Sprintf("full_doc_fallbacks=%d", stats.FullDocumentScanFallbacks))
	}
	if stats.FailClosed != 0 {
		failures = append(failures, fmt.Sprintf("fail_closed=%d reason=%s", stats.FailClosed, stats.FailClosedReason))
	}
	if stats.TextStateLookups != 0 {
		failures = append(failures, fmt.Sprintf("text_state_lookups=%d", stats.TextStateLookups))
	}
	if stats.TextMatchDetailsBuilt != 0 {
		failures = append(failures, fmt.Sprintf("text_match_details=%d", stats.TextMatchDetailsBuilt))
	}
	return guardrailFromFailures(name, failures)
}

func hybridGuardrail(name string, stats collections.HybridSearchStats) guardrailResult {
	var failures []string
	if stats.DocumentsFetched != 0 {
		failures = append(failures, fmt.Sprintf("docs_fetched=%d", stats.DocumentsFetched))
	}
	if stats.FullDocumentScanFallbacks != 0 {
		failures = append(failures, fmt.Sprintf("full_doc_fallbacks=%d", stats.FullDocumentScanFallbacks))
	}
	if stats.FailClosed != 0 {
		failures = append(failures, fmt.Sprintf("fail_closed=%d reason=%s", stats.FailClosed, stats.FailClosedReason))
	}
	if stats.TextStateLookups != 0 {
		failures = append(failures, fmt.Sprintf("text_state_lookups=%d", stats.TextStateLookups))
	}
	if stats.TextMatchDetailsBuilt != 0 {
		failures = append(failures, fmt.Sprintf("text_match_details=%d", stats.TextMatchDetailsBuilt))
	}
	return guardrailFromFailures(name, failures)
}

func guardrailFromFailures(name string, failures []string) guardrailResult {
	if len(failures) == 0 {
		return guardrailResult{Name: name, OK: true}
	}
	return guardrailResult{Name: name, OK: false, Failure: strings.Join(failures, ", ")}
}

func failOnGuardrails(guards []guardrailResult, allow bool) error {
	if allow {
		return nil
	}
	for _, guard := range guards {
		if !guard.OK {
			return fmt.Errorf("guardrail %s failed: %s", guard.Name, guard.Failure)
		}
	}
	return nil
}

func summarizeLatency(values []int64) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	copyValues := append([]int64(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool { return copyValues[i] < copyValues[j] })
	var sum int64
	for _, value := range copyValues {
		sum += value
	}
	return latencySummary{MinNS: copyValues[0], P50NS: percentile(copyValues, 50), P95NS: percentile(copyValues, 95), P99NS: percentile(copyValues, 99), MaxNS: copyValues[len(copyValues)-1], MeanNS: float64(sum) / float64(len(copyValues))}
}

func percentile(sorted []int64, pct int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := int(math.Ceil(float64(len(sorted)*pct)/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func rankBottlenecks(rep report) []bottleneckRow {
	type candidate struct {
		name      string
		metric    string
		value     float64
		unit      string
		sortValue float64
		followUp  string
	}
	var candidates []candidate
	candidates = append(candidates, candidate{name: "fixture_load", metric: "total_seconds", value: rep.Load.TotalSeconds, unit: "s", sortValue: rep.Load.TotalSeconds, followUp: "Investigate write/index build batching, text-v2 append block density, and vector rebuild split if load dominates scale runs."})
	if rep.Load.VectorRebuildSeconds > 0 {
		candidates = append(candidates, candidate{name: "vector_rebuild", metric: "seconds", value: rep.Load.VectorRebuildSeconds, unit: "s", sortValue: rep.Load.VectorRebuildSeconds, followUp: "If vector rebuild dominates, isolate column_graph rebuild scheduling from text-v2 scale evidence."})
	}
	if rep.Backfill != nil {
		candidates = append(candidates, candidate{name: "text_backfill", metric: "seconds", value: rep.Backfill.BackfillSeconds, unit: "s", sortValue: rep.Backfill.BackfillSeconds, followUp: "Feed #2732 maintenance policy with backfill/rewrite budget evidence."})
	}
	if rep.Maintenance != nil {
		candidates = append(candidates, candidate{name: "text_rewrite", metric: "seconds", value: rep.Maintenance.RewriteSeconds, unit: "s", sortValue: rep.Maintenance.RewriteSeconds, followUp: "Use rewrite cost and stale purge counts to size #2732 bounded maintenance triggers."})
	}
	for _, row := range rep.Queries {
		p95Seconds := float64(row.Latency.P95NS) / float64(time.Second)
		candidates = append(candidates, candidate{name: row.Name, metric: "p95_ns", value: float64(row.Latency.P95NS), unit: "ns", sortValue: p95Seconds, followUp: "Profile this retrieval row first if it is on the target production query mix."})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].sortValue > candidates[j].sortValue })
	limit := minInt(5, len(candidates))
	out := make([]bottleneckRow, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, bottleneckRow{Rank: i + 1, Name: candidates[i].name, Metric: candidates[i].metric, Value: candidates[i].value, Unit: candidates[i].unit, FollowUp: candidates[i].followUp})
	}
	return out
}

func storageSnapshotFromText(label string, docs int, bytes int64, stats collections.TextIndexStorageStats, vectorStatus *collections.VectorIndexStatus) storageSnapshot {
	snap := storageSnapshot{Label: label, Bytes: bytes, TextEncodedBytes: stats.EncodedBytes, V2PostingBlocks: stats.V2PostingBlocks, V2LiveDocuments: stats.V2LiveDocuments, V2DeletedDocs: stats.V2DeletedDocs}
	if docs > 0 {
		docsDivisor := float64(docs)
		snap.BytesPerDoc = float64(bytes) / docsDivisor
		snap.TextBytesPerDoc = float64(stats.EncodedBytes) / docsDivisor
		snap.TextDocIDBytesPerDoc = float64(stats.V2DocIDBytes) / docsDivisor
		snap.TextDocMapBytesPerDoc = float64(stats.V2DocMapBytes) / docsDivisor
		snap.TextPostingBlockBytesPerDoc = float64(stats.V2PostingBlockBytes) / docsDivisor
		snap.TextNormBlockBytesPerDoc = float64(stats.V2NormBlockBytes) / docsDivisor
		snap.TextPositionBytesPerDoc = float64(stats.V2PositionBytes) / docsDivisor
		snap.TextTermStatsBytesPerDoc = float64(stats.V2TermStatsBytes) / docsDivisor
		snap.TextStatusFormatBytesPerDoc = float64(stats.V2StatusFormatBytes) / docsDivisor
	}
	if vectorStatus != nil {
		snap.VectorNativeBytes = vectorStatusBytes(vectorStatus)
	}
	return snap
}

func vectorStatusBytes(status *collections.VectorIndexStatus) int64 {
	if status == nil {
		return 0
	}
	if status.NativeRootBytes != 0 {
		return status.NativeRootBytes
	}
	return status.Stats.BytesDisk
}

func writeReports(rep report) error {
	if err := os.MkdirAll(rep.Artifacts.OutDir, 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(rep.Artifacts.JSONReport, append(payload, '\n')); err != nil {
		return err
	}
	return atomicWriteFile(rep.Artifacts.Markdown, []byte(renderMarkdown(rep)))
}

func atomicWriteFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o644); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func renderMarkdown(rep report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# TreeDB text-v2/hybrid scale report\n\n")
	fmt.Fprintf(&b, "- schema: `%s`\n", rep.SchemaVersion)
	fmt.Fprintf(&b, "- generated: `%s`\n", rep.GeneratedAt)
	fmt.Fprintf(&b, "- branch/commit: `%s` / `%s`\n", rep.Context.Branch, rep.Context.Commit)
	fmt.Fprintf(&b, "- base: `%s` / `%s`\n", rep.Context.BaseRef, rep.Context.BaseSHA)
	fmt.Fprintf(&b, "- rows: `%d`, dims: `%d`, batch: `%d`, queries/row: `%d`\n", rep.Config.Rows, rep.Config.Dims, rep.Config.BatchSize, rep.Config.Queries)
	fmt.Fprintf(&b, "- phases: selected `%s`; completed `%s`; status **%s**\n", strings.Join(rep.SelectedPhases, ","), strings.Join(rep.CompletedPhases, ","), reportStatus(rep.Complete))
	fmt.Fprintf(&b, "- db dir: `%s` (kept=%v)\n\n", rep.Artifacts.DBDir, rep.Artifacts.DBKept)

	fmt.Fprintf(&b, "## Load/storage\n\n")
	fmt.Fprintf(&b, "| phase | seconds | rows/s | storage bytes | bytes/doc | text bytes/doc | vector native bytes |\n")
	fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	vectorBytes := int64(0)
	if rep.Load.VectorStatus != nil {
		vectorBytes = vectorStatusBytes(rep.Load.VectorStatus)
	}
	fmt.Fprintf(&b, "| load | %.3f | %.1f | %d | %.1f | %.1f | %d |\n", rep.Load.TotalSeconds, rep.Load.RowsPerSecond, rep.Load.StorageBytesAfterLoad, rep.Load.StorageBytesPerDoc, float64(rep.Load.TextStorage.EncodedBytes)/float64(maxInt(rep.Load.Rows, 1)), vectorBytes)
	if rep.Backfill != nil {
		fmt.Fprintf(&b, "| backfill fixture | %.3f | %.1f | %d | %.1f | %.1f | 0 |\n", rep.Backfill.TotalSeconds, rep.Backfill.RowsPerSecond, rep.Backfill.StorageBytes, rep.Backfill.StorageBytesPerDoc, float64(rep.Backfill.TextStorage.EncodedBytes)/float64(maxInt(rep.Backfill.Rows, 1)))
	}
	fmt.Fprintf(&b, "\nLoad breakdown: generation `%.3fs`, insert `%.3fs`, flush `%.3fs`, vector rebuild `%.3fs`, checkpoint `%.3fs`.\n\n", rep.Load.GenerationSeconds, rep.Load.InsertSeconds, rep.Load.FlushSeconds, rep.Load.VectorRebuildSeconds, rep.Load.CheckpointSeconds)

	if len(rep.StorageSnapshots) != 0 {
		fmt.Fprintf(&b, "### Text-v2 lane bytes/doc\n\n")
		fmt.Fprintf(&b, "| snapshot | docid | docmap | postings | norms | positions | terms | status/format |\n")
		fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		for _, snap := range rep.StorageSnapshots {
			fmt.Fprintf(&b, "| `%s` | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f | %.1f |\n", snap.Label, snap.TextDocIDBytesPerDoc, snap.TextDocMapBytesPerDoc, snap.TextPostingBlockBytesPerDoc, snap.TextNormBlockBytesPerDoc, snap.TextPositionBytesPerDoc, snap.TextTermStatsBytesPerDoc, snap.TextStatusFormatBytesPerDoc)
		}
		fmt.Fprintf(&b, "\n")
	}

	if len(rep.Queries) != 0 {
		fmt.Fprintf(&b, "## Retrieval latency\n\n")
		fmt.Fprintf(&b, "| row | modality | boundary | p50 | p95 | p99 | mean | ops/sec | results | guardrail | key counters |\n")
		fmt.Fprintf(&b, "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
		for _, row := range rep.Queries {
			fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s | %.1f | %d | %s | %s |\n", row.Name, row.Modality, markdownTable(row.Boundary), formatNS(row.Latency.P50NS), formatNS(row.Latency.P95NS), formatNS(row.Latency.P99NS), formatNS(int64(row.Latency.MeanNS)), row.OpsPerSec, row.Results, guardrailLabel(row.GuardrailOK, row.GuardrailFailure), markdownTable(queryCounters(row)))
		}
		fmt.Fprintf(&b, "\n")
	}

	if rep.Reopen != nil {
		fmt.Fprintf(&b, "## Reopen\n\n")
		fmt.Fprintf(&b, "Close `%.3fs`, open `%.3fs`, open collection `%.3fs`, probe `%.3fs`, total `%.3fs`.\n\n", rep.Reopen.CloseSeconds, rep.Reopen.OpenSeconds, rep.Reopen.OpenCollectionSeconds, rep.Reopen.ProbeSeconds, rep.Reopen.TotalSeconds)
	}
	if rep.Concurrent != nil {
		fmt.Fprintf(&b, "## Concurrent serving/write sanity\n\n")
		fmt.Fprintf(&b, "Readers `%d`, queries `%d`, writes `%d`, throughput `%.1f ops/s`, p50 `%s`, p95 `%s`, p99 `%s`, writer `%.3fs`, guardrail %s.\n\n", rep.Concurrent.Readers, rep.Concurrent.Queries, rep.Concurrent.Writes, rep.Concurrent.ThroughputOpsPerSec, formatNS(rep.Concurrent.SearchLatency.P50NS), formatNS(rep.Concurrent.SearchLatency.P95NS), formatNS(rep.Concurrent.SearchLatency.P99NS), rep.Concurrent.WriterSeconds, guardrailLabel(rep.Concurrent.GuardrailOK, rep.Concurrent.GuardrailFailure))
	}
	if rep.Maintenance != nil {
		fmt.Fprintf(&b, "## Maintenance/rewrite\n\n")
		fmt.Fprintf(&b, "Updates `%d` in `%.3fs`, deletes `%d` in `%.3fs`, rewrite `%.3fs`, checkpoint `%.3fs`. Rewrite read `%d` blocks, wrote `%d`, deleted `%d`, purged stale postings `%d`; postcondition %s.\n\n", rep.Maintenance.Updates, rep.Maintenance.UpdateSeconds, rep.Maintenance.Deletes, rep.Maintenance.DeleteSeconds, rep.Maintenance.RewriteSeconds, rep.Maintenance.CheckpointSeconds, rep.Maintenance.Stats.PostingBlocksRead, rep.Maintenance.Stats.PostingBlocksWritten, rep.Maintenance.Stats.PostingBlocksDeleted, rep.Maintenance.Stats.StalePostingsPurged, guardrailLabel(rep.Maintenance.PostconditionOK, rep.Maintenance.PostconditionFailure))
	}
	if len(rep.Bottlenecks) != 0 {
		fmt.Fprintf(&b, "## Ranked bottlenecks / follow-ups\n\n")
		fmt.Fprintf(&b, "| rank | row | metric | value | follow-up |\n| ---: | --- | --- | ---: | --- |\n")
		for _, row := range rep.Bottlenecks {
			fmt.Fprintf(&b, "| %d | `%s` | `%s` | %.3f %s | %s |\n", row.Rank, row.Name, row.Metric, row.Value, row.Unit, markdownTable(row.FollowUp))
		}
		fmt.Fprintf(&b, "\n")
	}
	if len(rep.Guardrails) != 0 {
		fmt.Fprintf(&b, "## Guardrails\n\n")
		for _, guard := range rep.Guardrails {
			fmt.Fprintf(&b, "- `%s`: %s\n", guard.Name, guardrailLabel(guard.OK, guard.Failure))
		}
		fmt.Fprintf(&b, "\n")
	}
	if len(rep.Caveats) != 0 {
		fmt.Fprintf(&b, "## Caveats\n\n")
		for _, caveat := range rep.Caveats {
			fmt.Fprintf(&b, "- %s\n", caveat)
		}
	}
	return b.String()
}

func reportStatus(complete bool) string {
	if complete {
		return "COMPLETE"
	}
	return "INCOMPLETE (partial evidence; not a completed qualification)"
}

func queryCounters(row queryReport) string {
	if row.TextStats != nil {
		st := row.TextStats
		return fmt.Sprintf("docs_fetched=%d, fail_closed=%d, postings=%d, blocks_visited=%d, blocks_skipped=%d, scored=%d", st.DocumentsFetched, st.FailClosed, st.TextPostingsScanned, st.TextPostingBlocksVisited, st.TextPostingBlocksSkipped, st.TextCandidatesScored)
	}
	if row.HybridStats != nil {
		st := row.HybridStats
		return fmt.Sprintf("docs_fetched=%d, fail_closed=%d, text_budget=%d/%d, vector_budget=%d/%d, text_candidates=%d, vector_candidates=%d, scalar_prefilter=%d, fused=%d, budget_policy=%s, budget_stop=%s, budget_fallback=%s", st.DocumentsFetched, st.FailClosed, st.TextCandidateBudgetEffective, st.TextCandidatesRequested, st.VectorCandidateBudgetEffective, st.VectorCandidatesRequested, st.TextCandidatesReturned, st.VectorCandidatesReturned, st.ScalarPrefilterIDs, st.CandidatesFused, st.CandidateBudgetPolicy, st.CandidateBudgetStopReason, st.CandidateBudgetFallbackReason)
	}
	return ""
}

func guardrailLabel(ok bool, failure string) string {
	if ok {
		return "PASS"
	}
	if failure == "" {
		return "FAIL"
	}
	return "FAIL: `" + markdownTable(failure) + "`"
}

func markdownTable(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

func formatNS(ns int64) string {
	if ns >= int64(time.Second) {
		return fmt.Sprintf("%.3fs", float64(ns)/float64(time.Second))
	}
	if ns >= int64(time.Millisecond) {
		return fmt.Sprintf("%.3fms", float64(ns)/float64(time.Millisecond))
	}
	if ns >= int64(time.Microsecond) {
		return fmt.Sprintf("%.3fµs", float64(ns)/float64(time.Microsecond))
	}
	return fmt.Sprintf("%dns", ns)
}

func captureContext(cfg config) reportContext {
	executable, err := os.Executable()
	if err != nil {
		executable = ""
	}
	buildInfo, _ := debug.ReadBuildInfo()
	binaryState, revision, vcsClean, vcsStatus := invocationProvenance(executable, buildInfo)
	return reportContext{
		// Repository and branch are intentionally left unset: this process may be
		// a standalone binary invoked outside a checkout. VCS provenance comes
		// only from the invoked binary's embedded build metadata.
		Commit:      revision,
		BaseRef:     cfg.baseRef,
		BaseSHA:     cfg.baseSHA,
		Go:          runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
		CPU:         cpuLabel(),
		NCPU:        runtime.NumCPU(),
		Uptime:      strings.TrimSpace(runCmd("uptime")),
		Command:     "process_argv=" + strings.Join(os.Args, " "),
		VCSClean:    vcsClean,
		VCSStatus:   vcsStatus,
		BinaryState: binaryState,
		Corpus:      "deterministic synthetic customer-support corpus v1",
		Cache:       "warm in-process retrieval after fixture load/reopen",
		Durability:  "TreeDB default durability; checkpoint after load",
		NoisePolicy: "warmup excluded; raw per-query samples summarized in-process; no null rows accepted",
	}
}

func invocationProvenance(executable string, info *debug.BuildInfo) (binaryState, revision string, vcsClean bool, vcsStatus string) {
	if executable == "" {
		binaryState = "unknown (os.Executable unavailable)"
	} else {
		binaryState = "executable=" + executable
	}
	if info == nil {
		return binaryState + "; build metadata unavailable", "", false, "unknown (build metadata unavailable)"
	}
	mainPath := info.Main.Path
	if mainPath == "" {
		mainPath = "unknown"
	}
	goVersion := info.GoVersion
	if goVersion == "" {
		goVersion = "unknown"
	}
	revision, modified := buildSetting(info, "vcs.revision"), buildSetting(info, "vcs.modified")
	if revision == "" || modified == "" {
		return fmt.Sprintf("%s; build_main=%s; build_go=%s; vcs=unknown", binaryState, mainPath, goVersion), "", false, "unknown (incomplete embedded VCS metadata)"
	}
	if modified == "true" {
		return fmt.Sprintf("%s; build_main=%s; build_go=%s; vcs_revision=%s; vcs_modified=true", binaryState, mainPath, goVersion, revision), revision, false, "dirty (embedded build metadata)"
	}
	if modified == "false" {
		return fmt.Sprintf("%s; build_main=%s; build_go=%s; vcs_revision=%s; vcs_modified=false", binaryState, mainPath, goVersion, revision), revision, true, "clean (embedded build metadata)"
	}
	return fmt.Sprintf("%s; build_main=%s; build_go=%s; vcs_revision=%s; vcs_modified=%s", binaryState, mainPath, goVersion, revision, modified), revision, false, "unknown (unrecognized embedded vcs.modified)"
}

func buildSetting(info *debug.BuildInfo, key string) string {
	for _, setting := range info.Settings {
		if setting.Key == key {
			return setting.Value
		}
	}
	return ""
}

func runCmd(name string, args ...string) string {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return string(bytes.TrimSpace(out))
}

func cpuLabel() string {
	if runtime.GOOS == "darwin" {
		return strings.TrimSpace(runCmd("sysctl", "-n", "machdep.cpu.brand_string"))
	}
	if raw, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "model name") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					return strings.TrimSpace(parts[1])
				}
			}
		}
	}
	return ""
}

func dirSize(root string) (int64, error) {
	var total int64
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
		total += info.Size()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("walk storage dir %q: %w", root, err)
	}
	return total, nil
}

func secondsSince(start time.Time) float64 { return time.Since(start).Seconds() }

func opsPerSec(meanNS float64) float64 {
	if meanNS <= 0 {
		return 0
	}
	return float64(time.Second) / meanNS
}

func nonZero(v float64) float64 {
	if v == 0 {
		return 1
	}
	return v
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
