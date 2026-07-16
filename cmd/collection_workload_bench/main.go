package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/cmd/internal/treedbstats"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	defaultCollectionName = "users"
	defaultDocuments      = 16000
	defaultBatchSize      = 16000
	defaultReads          = 50000
	defaultRangeReads     = 10000
	defaultUpdates        = 10000
	defaultDeletes        = 1000

	treedbBenchDirMarkerName = ".collection-workload-bench"
	treedbBenchDirMarkerBody = "created by cmd/collection_workload_bench\n"
)

type config struct {
	TreeDBDir             string
	KeepTreeDBDir         bool
	Documents             int
	BatchSize             int
	Formats               []collections.DocumentFormat
	IndexCounts           []int
	ReadStates            []readState
	Reads                 int
	RangeReads            int
	Updates               int
	Deletes               int
	ReaderSweep           []int
	OutputFormat          string
	TreeDBProfile         treedb.Profile
	DataRootStorage       collections.RootStoragePolicy
	IndexStateRootStorage collections.RootStoragePolicy
	IndexRootStorage      collections.RootStoragePolicy
}

type readState string

const (
	readStateBuffered     readState = "buffered"
	readStateFlushed      readState = "flushed"
	readStateCheckpointed readState = "checkpointed"
)

type result struct {
	BenchmarkFamily       string           `json:"benchmark_family"`
	TreeDBProfile         string           `json:"treedb_profile"`
	Documents             int              `json:"documents"`
	BatchSize             int              `json:"batch_size"`
	Reads                 int              `json:"reads"`
	RangeReads            int              `json:"range_reads"`
	Updates               int              `json:"updates"`
	Deletes               int              `json:"deletes"`
	ReaderSweep           []int            `json:"reader_sweep"`
	Rows                  []rowResult      `json:"rows"`
	StartedAt             string           `json:"started_at"`
	FinishedAt            string           `json:"finished_at"`
	DurationMS            float64          `json:"duration_ms"`
	TreeDBDataRootStorage string           `json:"treedb_data_root_storage,omitempty"`
	TreeDBIndexStorage    string           `json:"treedb_index_root_storage,omitempty"`
	TreeDBIndexStateRoot  string           `json:"treedb_index_state_root_storage,omitempty"`
	Notes                 []string         `json:"notes,omitempty"`
	Errors                []benchmarkError `json:"errors,omitempty"`
}

type benchmarkError struct {
	Format    string `json:"format"`
	Indexes   int    `json:"indexes"`
	ReadState string `json:"read_state"`
	Error     string `json:"error"`
}

type rowResult struct {
	Format          string                 `json:"format"`
	Indexes         int                    `json:"indexes"`
	ReadState       string                 `json:"read_state"`
	IndexNames      []string               `json:"index_names"`
	Load            phaseResult            `json:"load_insert_many"`
	StateTransition phaseResult            `json:"state_transition"`
	Phases          []phaseResult          `json:"phases"`
	FinalStats      map[string]string      `json:"final_treedb_stats,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

type phaseResult struct {
	Name             string             `json:"name"`
	Operations       int64              `json:"operations"`
	DriverCalls      int64              `json:"driver_calls,omitempty"`
	NSPerOp          float64            `json:"ns_per_op"`
	OpsPerSec        float64            `json:"ops_per_sec"`
	DurationMS       float64            `json:"duration_ms"`
	Skipped          bool               `json:"skipped,omitempty"`
	SkipReason       string             `json:"skip_reason,omitempty"`
	TreeDBStatsDelta map[string]string  `json:"treedb_stats_delta,omitempty"`
	TreeDBMetrics    map[string]float64 `json:"treedb_metrics,omitempty"`
}

type benchTarget struct {
	dir       string
	removeDir bool
	db        *backenddb.DB
	manager   *collections.CollectionManager
	col       *collections.Collection
	cleanup   func() error
	fixture   *fixture
	meta      collections.CollectionMeta
}

type fixture struct {
	ids     [][]byte
	idText  []string
	emails  []string
	cities  []string
	ages    []int64
	docs    [][]byte
	format  collections.DocumentFormat
	indexes int
	encoder collections.TemplateV1Encoder
}

type phaseFunc func() (int64, int64, error)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := runAndWrite(os.Stdout, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runAndWrite(w io.Writer, cfg config) error {
	res, err := run(cfg)
	writeErr := writeResult(w, cfg.OutputFormat, res)
	if err != nil || writeErr != nil {
		return errors.Join(err, writeErr)
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		Documents:             defaultDocuments,
		BatchSize:             defaultBatchSize,
		Reads:                 defaultReads,
		RangeReads:            defaultRangeReads,
		Updates:               defaultUpdates,
		Deletes:               defaultDeletes,
		OutputFormat:          "text",
		TreeDBProfile:         treedb.ProfileBenchUnsafe,
		Formats:               []collections.DocumentFormat{collections.DocumentFormatJSON, collections.DocumentFormatTemplateV1, collections.DocumentFormatBSON},
		IndexCounts:           []int{0, 1, 2},
		ReadStates:            []readState{readStateBuffered, readStateFlushed, readStateCheckpointed},
		ReaderSweep:           []int{1, 2, 4, 8, 16, 32},
		DataRootStorage:       collections.RootStorageDefault,
		IndexStateRootStorage: collections.RootStorageDefault,
		IndexRootStorage:      collections.RootStorageDefault,
	}
	var formats, indexes, readStates, readerSweep string
	fs := flag.NewFlagSet("collection_workload_bench", flag.ContinueOnError)
	fs.StringVar(&cfg.TreeDBDir, "treedb-dir", "", "TreeDB directory; empty uses a temporary directory per matrix row")
	fs.BoolVar(&cfg.KeepTreeDBDir, "keep-treedb-dir", false, "keep temporary TreeDB directories after the run")
	fs.IntVar(&cfg.Documents, "documents", cfg.Documents, "documents loaded before each row")
	fs.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "documents per InsertBatch during load")
	fs.StringVar(&formats, "formats", "json,template-v1,bson", "comma-separated storage formats: json, template-v1|collections-v1, bson")
	fs.StringVar(&indexes, "indexes", "0,1,2", "comma-separated native index-count shapes: 0, 1=email, 2=email+age, 3=email+age+city")
	fs.StringVar(&readStates, "read-states", "buffered,flushed,checkpointed", "comma-separated read states: buffered, flushed, checkpointed")
	fs.IntVar(&cfg.Reads, "reads", cfg.Reads, "per-phase point-read/query operations")
	fs.IntVar(&cfg.RangeReads, "range-reads", cfg.RangeReads, "per-phase range query operations")
	fs.IntVar(&cfg.Updates, "updates", cfg.Updates, "native update operations")
	fs.IntVar(&cfg.Deletes, "deletes", cfg.Deletes, "native delete operations")
	fs.StringVar(&readerSweep, "reader-sweep", "1,2,4,8,16,32", "comma-separated reader/writer fanout values")
	profile := string(cfg.TreeDBProfile)
	fs.StringVar(&profile, "treedb-profile", profile, "TreeDB profile: "+treedb.BenchmarkProfileFlagHelp)
	dataRootStorage := string(cfg.DataRootStorage)
	indexStateRootStorage := string(cfg.IndexStateRootStorage)
	indexRootStorage := string(cfg.IndexRootStorage)
	fs.StringVar(&dataRootStorage, "treedb-data-root-storage", dataRootStorage, "collection data root storage: default, fast, compressed")
	fs.StringVar(&indexStateRootStorage, "treedb-index-state-root-storage", indexStateRootStorage, "collection index-state root storage: default, fast, compressed")
	fs.StringVar(&indexRootStorage, "treedb-index-root-storage", indexRootStorage, "secondary index root storage: default, fast, compressed")
	fs.StringVar(&cfg.OutputFormat, "format", cfg.OutputFormat, "output format: text or json")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	var err error
	cfg.Formats, err = parseFormats(formats)
	if err != nil {
		return config{}, err
	}
	cfg.IndexCounts, err = parseIndexCounts(indexes)
	if err != nil {
		return config{}, err
	}
	cfg.ReadStates, err = parseReadStates(readStates)
	if err != nil {
		return config{}, err
	}
	cfg.ReaderSweep, err = parsePositiveInts(readerSweep, "reader-sweep")
	if err != nil {
		return config{}, err
	}
	cfg.TreeDBProfile, err = parseProfile(profile)
	if err != nil {
		return config{}, err
	}
	cfg.DataRootStorage, err = parseRootStorage(dataRootStorage)
	if err != nil {
		return config{}, fmt.Errorf("treedb-data-root-storage: %w", err)
	}
	cfg.IndexStateRootStorage, err = parseRootStorage(indexStateRootStorage)
	if err != nil {
		return config{}, fmt.Errorf("treedb-index-state-root-storage: %w", err)
	}
	cfg.IndexRootStorage, err = parseRootStorage(indexRootStorage)
	if err != nil {
		return config{}, fmt.Errorf("treedb-index-root-storage: %w", err)
	}
	if cfg.Documents <= 0 {
		return config{}, errors.New("documents must be positive")
	}
	if cfg.BatchSize <= 0 {
		return config{}, errors.New("batch-size must be positive")
	}
	if cfg.Reads < 0 || cfg.RangeReads < 0 || cfg.Updates < 0 || cfg.Deletes < 0 {
		return config{}, errors.New("reads, range-reads, updates, and deletes must be non-negative")
	}
	if cfg.Deletes > cfg.Documents {
		return config{}, errors.New("deletes cannot exceed documents")
	}
	if cfg.OutputFormat != "text" && cfg.OutputFormat != "json" {
		return config{}, fmt.Errorf("unknown format %q", cfg.OutputFormat)
	}
	return cfg, nil
}

func parseFormats(raw string) ([]collections.DocumentFormat, error) {
	parts := splitCSV(raw)
	if len(parts) == 0 {
		return nil, errors.New("formats cannot be empty")
	}
	out := make([]collections.DocumentFormat, 0, len(parts))
	seen := make(map[collections.DocumentFormat]struct{}, len(parts))
	for _, part := range parts {
		var format collections.DocumentFormat
		switch strings.ToLower(part) {
		case "json":
			format = collections.DocumentFormatJSON
		case "", "template-v1", "collections-v1":
			format = collections.DocumentFormatTemplateV1
		case "bson":
			format = collections.DocumentFormatBSON
		default:
			return nil, fmt.Errorf("unknown format %q", part)
		}
		if _, ok := seen[format]; ok {
			return nil, fmt.Errorf("formats contains duplicate value %q", format)
		}
		seen[format] = struct{}{}
		out = append(out, format)
	}
	return out, nil
}

func parseIndexCounts(raw string) ([]int, error) {
	out, err := parsePositiveOrZeroInts(raw, "indexes")
	if err != nil {
		return nil, err
	}
	for _, n := range out {
		if n > 3 {
			return nil, fmt.Errorf("indexes=%d unsupported by native workload harness; max is 3", n)
		}
	}
	if err := rejectDuplicateInts(out, "indexes"); err != nil {
		return nil, err
	}
	return out, nil
}

func parseReadStates(raw string) ([]readState, error) {
	parts := splitCSV(raw)
	if len(parts) == 0 {
		return nil, errors.New("read-states cannot be empty")
	}
	out := make([]readState, 0, len(parts))
	seen := make(map[readState]struct{}, len(parts))
	for _, part := range parts {
		var state readState
		switch strings.ToLower(part) {
		case string(readStateBuffered):
			state = readStateBuffered
		case string(readStateFlushed):
			state = readStateFlushed
		case string(readStateCheckpointed):
			state = readStateCheckpointed
		default:
			return nil, fmt.Errorf("unknown read-state %q", part)
		}
		if _, ok := seen[state]; ok {
			return nil, fmt.Errorf("read-states contains duplicate value %q", state)
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	return out, nil
}

func parsePositiveInts(raw, label string) ([]int, error) {
	out, err := parsePositiveOrZeroInts(raw, label)
	if err != nil {
		return nil, err
	}
	for _, n := range out {
		if n <= 0 {
			return nil, fmt.Errorf("%s values must be positive", label)
		}
	}
	return out, rejectDuplicateInts(out, label)
}

func rejectDuplicateInts(values []int, label string) error {
	seen := make(map[int]struct{}, len(values))
	for _, n := range values {
		if _, ok := seen[n]; ok {
			return fmt.Errorf("%s contains duplicate value %d", label, n)
		}
		seen[n] = struct{}{}
	}
	return nil
}

func parsePositiveOrZeroInts(raw, label string) ([]int, error) {
	parts := splitCSV(raw)
	if len(parts) == 0 {
		return nil, fmt.Errorf("%s cannot be empty", label)
	}
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("%s contains invalid non-negative integer %q", label, part)
		}
		out = append(out, n)
	}
	return out, nil
}

func parseProfile(raw string) (treedb.Profile, error) {
	profile, ok := treedb.ParseBenchmarkProfile(raw, treedb.ProfileBenchUnsafe)
	if !ok {
		return "", fmt.Errorf("unsupported -treedb-profile %q; allowed: %s", raw, treedb.BenchmarkProfileFlagHelp)
	}
	return profile, nil
}

func parseRootStorage(raw string) (collections.RootStoragePolicy, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "default":
		return collections.RootStorageDefault, nil
	case string(collections.RootStorageFast):
		return collections.RootStorageFast, nil
	case string(collections.RootStorageCompressed):
		return collections.RootStorageCompressed, nil
	default:
		return "", fmt.Errorf("unknown root storage policy %q", raw)
	}
}

func splitCSV(raw string) []string {
	fields := strings.Split(raw, ",")
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func run(cfg config) (result, error) {
	started := time.Now()
	res := result{
		BenchmarkFamily:       "native_collections",
		TreeDBProfile:         string(cfg.TreeDBProfile),
		Documents:             cfg.Documents,
		BatchSize:             cfg.BatchSize,
		Reads:                 cfg.Reads,
		RangeReads:            cfg.RangeReads,
		Updates:               cfg.Updates,
		Deletes:               cfg.Deletes,
		ReaderSweep:           append([]int(nil), cfg.ReaderSweep...),
		StartedAt:             started.Format(time.RFC3339Nano),
		TreeDBDataRootStorage: string(cfg.DataRootStorage),
		TreeDBIndexStorage:    string(cfg.IndexRootStorage),
		TreeDBIndexStateRoot:  string(cfg.IndexStateRootStorage),
		Notes: []string{
			"Native collection benchmark: no Mongo driver, gateway, wire protocol, cursor materialization, or BSON _id primary-key encoding.",
			"Index-count shapes are native: 0=primary only, 1=email, 2=email+age, 3=email+age+city.",
			"age_range_scan_limit_10 uses the collection primary scan API and filters materialized document age values.",
		},
	}
	for _, format := range cfg.Formats {
		for _, indexes := range cfg.IndexCounts {
			for _, state := range cfg.ReadStates {
				row, err := runRow(cfg, format, indexes, state)
				if err != nil {
					res.Errors = append(res.Errors, benchmarkError{
						Format:    string(format),
						Indexes:   indexes,
						ReadState: string(state),
						Error:     err.Error(),
					})
					finishResult(&res, started)
					return res, err
				}
				res.Rows = append(res.Rows, row)
			}
		}
	}
	finishResult(&res, started)
	return res, nil
}

func finishResult(res *result, started time.Time) {
	if res == nil {
		return
	}
	res.FinishedAt = time.Now().Format(time.RFC3339Nano)
	res.DurationMS = float64(time.Since(started).Nanoseconds()) / 1e6
}

func runRow(cfg config, format collections.DocumentFormat, indexes int, state readState) (rowResult, error) {
	target, err := openTarget(cfg, format, indexes)
	if err != nil {
		return rowResult{}, err
	}
	defer func() { _ = target.close() }()

	row := rowResult{
		Format:     string(format),
		Indexes:    indexes,
		ReadState:  string(state),
		IndexNames: indexNames(indexes),
		Metadata: map[string]interface{}{
			"native_document_ids":  true,
			"mongo_gateway":        false,
			"mongo_wire":           false,
			"mongo_driver":         false,
			"bson_id_key_encoding": false,
		},
	}
	row.Load, err = timedPhase(target, "load_insert_many", func() (int64, int64, error) {
		return loadFixture(target, cfg.BatchSize)
	})
	if err != nil {
		return rowResult{}, err
	}
	row.StateTransition, err = timedPhase(target, "read_state_"+string(state), func() (int64, int64, error) {
		return applyReadState(target, state)
	})
	if err != nil {
		return rowResult{}, err
	}

	readPhases, err := runReadPhases(target, cfg, indexes, state)
	if err != nil {
		return rowResult{}, err
	}
	row.Phases = append(row.Phases, readPhases...)
	if cfg.Updates > 0 {
		updateWorkload, err := prepareUpdateSet(target, cfg.Updates)
		if err != nil {
			return rowResult{}, err
		}
		phase, err := timedPhase(target, "id_update_set", func() (int64, int64, error) {
			return runUpdateSet(target, updateWorkload, 1)
		})
		if err != nil {
			return rowResult{}, err
		}
		row.Phases = append(row.Phases, phase)
		for _, writers := range cfg.ReaderSweep {
			phase, err := timedPhase(target, fmt.Sprintf("concurrent_id_update_set_w%d", writers), func() (int64, int64, error) {
				return runUpdateSet(target, updateWorkload, writers)
			})
			if err != nil {
				return rowResult{}, err
			}
			row.Phases = append(row.Phases, phase)
		}
	}
	if cfg.Deletes > 0 {
		phase, err := timedPhase(target, "id_delete_one", func() (int64, int64, error) {
			return runDeletes(target, cfg.Deletes)
		})
		if err != nil {
			return rowResult{}, err
		}
		row.Phases = append(row.Phases, phase)
	}
	row.FinalStats = collectStats(target)
	return row, nil
}

func runReadPhases(target *benchTarget, cfg config, indexes int, state readState) ([]phaseResult, error) {
	var phases []phaseResult
	appendPhase := func(phase phaseResult, err error) error {
		if err != nil {
			return fmt.Errorf("%s: %w", phase.Name, err)
		}
		phases = append(phases, phase)
		return nil
	}
	if cfg.Reads > 0 {
		phase, err := timedPhase(target, "id_find_one", func() (int64, int64, error) {
			return runIDReads(target, cfg.Reads, 1)
		})
		if err := appendPhase(phase, err); err != nil {
			return nil, err
		}
		for _, readers := range cfg.ReaderSweep {
			phase, err := timedPhase(target, fmt.Sprintf("concurrent_id_find_one_r%d", readers), func() (int64, int64, error) {
				return runIDReads(target, cfg.Reads, readers)
			})
			if err := appendPhase(phase, err); err != nil {
				return nil, err
			}
		}
		if indexes >= 1 {
			phase, err = timedPhase(target, "email_find_one", func() (int64, int64, error) {
				return runEmailReads(target, cfg.Reads, 1)
			})
			if err := appendPhase(phase, err); err != nil {
				return nil, err
			}
			for _, readers := range cfg.ReaderSweep {
				phase, err := timedPhase(target, fmt.Sprintf("concurrent_email_find_one_r%d", readers), func() (int64, int64, error) {
					return runEmailReads(target, cfg.Reads, readers)
				})
				if err := appendPhase(phase, err); err != nil {
					return nil, err
				}
			}
		} else {
			phases = append(phases, skippedPhase("email_find_one", "email index is absent for indexes_0"))
		}
	}
	if cfg.RangeReads > 0 {
		if indexes >= 2 {
			phase, err := timedPhase(target, "age_range_indexed_limit_10", func() (int64, int64, error) {
				return runAgeIndexedRangeReads(target, cfg.RangeReads, 1)
			})
			if err := appendPhase(phase, err); err != nil {
				return nil, err
			}
			for _, readers := range cfg.ReaderSweep {
				phase, err := timedPhase(target, fmt.Sprintf("concurrent_age_range_indexed_limit_10_r%d", readers), func() (int64, int64, error) {
					return runAgeIndexedRangeReads(target, cfg.RangeReads, readers)
				})
				if err := appendPhase(phase, err); err != nil {
					return nil, err
				}
			}
		} else {
			phases = append(phases, skippedPhase("age_range_indexed_limit_10", "age index is absent until indexes_2"))
		}
		if state == readStateBuffered {
			phases = append(phases, skippedPhase("age_range_scan_limit_10", "primary scan API flushes buffered writes before scanning"))
		} else {
			phase, err := timedPhase(target, "age_range_scan_limit_10", func() (int64, int64, error) {
				return runAgeScanRangeReads(target, cfg.RangeReads, 1)
			})
			if err := appendPhase(phase, err); err != nil {
				return nil, err
			}
			for _, readers := range cfg.ReaderSweep {
				phase, err := timedPhase(target, fmt.Sprintf("concurrent_age_range_scan_limit_10_r%d", readers), func() (int64, int64, error) {
					return runAgeScanRangeReads(target, cfg.RangeReads, readers)
				})
				if err := appendPhase(phase, err); err != nil {
					return nil, err
				}
			}
		}
	}
	return phases, nil
}

func skippedPhase(name, reason string) phaseResult {
	return phaseResult{Name: name, Skipped: true, SkipReason: reason}
}

func openTarget(cfg config, format collections.DocumentFormat, indexes int) (*benchTarget, error) {
	dir := cfg.TreeDBDir
	removeDir := false
	if dir == "" {
		tmp, err := os.MkdirTemp("", "collection-workload-bench-*")
		if err != nil {
			return nil, err
		}
		dir = tmp
		removeDir = !cfg.KeepTreeDBDir
		if err := writeTreeDBBenchDirMarker(dir); err != nil {
			if removeDir {
				_ = os.RemoveAll(dir)
			}
			return nil, err
		}
	} else if err := resetTreeDBDir(dir); err != nil {
		return nil, err
	}
	opts := treedb.OptionsForBenchmark(cfg.TreeDBProfile, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	open := treedb.OpenBackend
	if opts.IndexOuterLeavesInValueLog {
		open = treedb.OpenBackendWithCachedLeafLog
	}
	db, cleanup, err := open(opts)
	if err != nil {
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	manager := collections.NewCollectionManager(db)
	meta := collections.CollectionMeta{
		Name: defaultCollectionName,
		Options: collections.CollectionOptions{
			DocumentFormat:          format,
			DataRootStoragePolicy:   cfg.DataRootStorage,
			IndexStateStoragePolicy: cfg.IndexStateRootStorage,
		},
		Indexes: indexDefinitions(indexes, cfg.IndexRootStorage),
	}
	if _, err := manager.CreateCollection(&meta); err != nil {
		_ = cleanup()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	col, err := manager.OpenCollection(defaultCollectionName)
	if err != nil {
		_ = cleanup()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	fixture, err := buildFixture(cfg.Documents, format, indexes)
	if err != nil {
		_ = cleanup()
		if removeDir {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	return &benchTarget{
		dir:       dir,
		removeDir: removeDir,
		db:        db,
		manager:   manager,
		col:       col,
		cleanup:   cleanup,
		fixture:   fixture,
		meta:      meta,
	}, nil
}

func (t *benchTarget) close() error {
	if t == nil {
		return nil
	}
	var err error
	if t.cleanup != nil {
		err = t.cleanup()
		t.cleanup = nil
	}
	if t.removeDir {
		if rmErr := os.RemoveAll(t.dir); err == nil {
			err = rmErr
		}
	}
	return err
}

func resetTreeDBDir(dir string) error {
	clean := filepath.Clean(strings.TrimSpace(dir))
	if clean == "" || clean == "." || clean == ".." {
		return fmt.Errorf("unsafe treedb-dir %q", dir)
	}
	abs, err := filepath.Abs(clean)
	if err != nil {
		return err
	}
	if abs == string(os.PathSeparator) || filepath.Dir(abs) == string(os.PathSeparator) {
		return fmt.Errorf("unsafe treedb-dir %q", dir)
	}
	info, err := os.Stat(abs)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := os.MkdirAll(abs, 0o700); err != nil {
			return err
		}
		return writeTreeDBBenchDirMarker(abs)
	}
	if !info.IsDir() {
		return fmt.Errorf("treedb-dir %q is not a directory", dir)
	}
	if err := requireResettableTreeDBBenchDir(abs); err != nil {
		return err
	}
	if err := os.RemoveAll(abs); err != nil {
		return err
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return err
	}
	return writeTreeDBBenchDirMarker(abs)
}

func requireResettableTreeDBBenchDir(dir string) error {
	if _, err := os.ReadDir(dir); err != nil {
		return err
	}
	if hasTreeDBBenchDirMarker(dir) {
		return nil
	}
	return fmt.Errorf("refusing to delete treedb-dir %q without %s marker", dir, treedbBenchDirMarkerName)
}

func hasTreeDBBenchDirMarker(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, treedbBenchDirMarkerName))
	if err != nil {
		return false
	}
	return string(data) == treedbBenchDirMarkerBody
}

func writeTreeDBBenchDirMarker(dir string) error {
	return os.WriteFile(filepath.Join(dir, treedbBenchDirMarkerName), []byte(treedbBenchDirMarkerBody), 0o600)
}

func indexDefinitions(count int, storage collections.RootStoragePolicy) []collections.IndexDefinition {
	defs := make([]collections.IndexDefinition, 0, count)
	if count >= 1 {
		defs = append(defs, collections.IndexDefinition{Name: "email", Field: "email", ValueType: collections.IndexValueString, Unique: true, StoragePolicy: storage})
	}
	if count >= 2 {
		defs = append(defs, collections.IndexDefinition{Name: "age", Field: "age", ValueType: collections.IndexValueInt64, StoragePolicy: storage})
	}
	if count >= 3 {
		defs = append(defs, collections.IndexDefinition{Name: "city", Field: "city", ValueType: collections.IndexValueString, StoragePolicy: storage})
	}
	return defs
}

func indexNames(count int) []string {
	defs := indexDefinitions(count, collections.RootStorageDefault)
	out := make([]string, 0, len(defs))
	for _, def := range defs {
		out = append(out, def.Name)
	}
	return out
}

func buildFixture(documents int, format collections.DocumentFormat, indexes int) (*fixture, error) {
	f := &fixture{
		ids:     make([][]byte, documents),
		idText:  make([]string, documents),
		emails:  make([]string, documents),
		cities:  make([]string, documents),
		ages:    make([]int64, documents),
		docs:    make([][]byte, documents),
		format:  format,
		indexes: indexes,
	}
	for i := 0; i < documents; i++ {
		idText := nativeDocumentIDText(i)
		f.idText[i] = idText
		f.ids[i] = []byte(idText)
		f.emails[i] = nativeEmail(i)
		f.cities[i] = nativeCity(i)
		f.ages[i] = nativeAge(i)
		doc, err := f.encodeDocument(i, false, 0)
		if err != nil {
			return nil, err
		}
		f.docs[i] = doc
	}
	return f, nil
}

func (f *fixture) encodeDocument(i int, updated bool, updateSeq int) ([]byte, error) {
	id := f.idText[i]
	email := f.emails[i]
	city := f.cities[i]
	age := f.ages[i]
	active := i%2 == 0
	score := float64(i%1000) / 10.0
	rank := int32(i % 1000)
	switch f.format {
	case collections.DocumentFormatJSON:
		return []byte(fmt.Sprintf(
			`{"id":%q,"email":%q,"city":%q,"age":%d,"active":%t,"score":%.1f,"tag0":%q,"tag1":%q,"rank":%d,"bio":%q,"updated":%t,"update_seq":%d}`,
			id, email, city, age, active, score, city, fmt.Sprintf("bucket-%02d", i%32), rank, nativeBio(), updated, updateSeq,
		)), nil
	case collections.DocumentFormatTemplateV1:
		return f.encoder.EncodeDocument(
			[]string{"id", "email", "city", "age", "active", "score", "tag0", "tag1", "rank", "bio", "updated", "update_seq"},
			[]any{id, email, city, age, active, score, city, fmt.Sprintf("bucket-%02d", i%32), int64(rank), nativeBio(), updated, int64(updateSeq)},
		)
	case collections.DocumentFormatBSON:
		return bson.Marshal(bson.D{
			{Key: "id", Value: id},
			{Key: "email", Value: email},
			{Key: "city", Value: city},
			{Key: "age", Value: age},
			{Key: "active", Value: active},
			{Key: "score", Value: score},
			{Key: "tag0", Value: city},
			{Key: "tag1", Value: fmt.Sprintf("bucket-%02d", i%32)},
			{Key: "rank", Value: rank},
			{Key: "bio", Value: nativeBio()},
			{Key: "updated", Value: updated},
			{Key: "update_seq", Value: int64(updateSeq)},
		})
	default:
		return nil, fmt.Errorf("unsupported document format %q", f.format)
	}
}

func nativeDocumentIDText(i int) string {
	return fmt.Sprintf("doc-%012d", i)
}

func nativeEmail(i int) string {
	return fmt.Sprintf("user%012d@example.test", i)
}

func nativeCity(i int) string {
	return nativeCities[i%len(nativeCities)]
}

func nativeAge(i int) int64 {
	return int64(18 + (i % 67))
}

func nativeBio() string {
	return "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
}

var nativeCities = [...]string{"hnl", "sfo", "nyc", "lon", "sin", "ber", "tyo", "syd"}

func benchmarkDocumentOrdinal(operation int, stride uint64, documentCount int) int {
	return int((uint64(operation) * stride) % uint64(documentCount))
}

func rangeReadMinAge(operation int, documentCount int) int64 {
	if documentCount <= 0 {
		return 20
	}
	ageSpan := documentCount
	if ageSpan > 67 {
		ageSpan = 67
	}
	maxAge := int64(18 + ageSpan - 1)
	base := int64(20)
	if maxAge < base {
		return maxAge
	}
	span := maxAge - base + 1
	if span > 40 {
		span = 40
	}
	return base + int64(operation%int(span))
}

func loadFixture(target *benchTarget, batchSize int) (int64, int64, error) {
	docs := target.fixture.docs
	ids := target.fixture.ids
	for start := 0; start < len(docs); start += batchSize {
		end := start + batchSize
		if end > len(docs) {
			end = len(docs)
		}
		if _, err := target.col.InsertBatch(ids[start:end], docs[start:end]); err != nil {
			return 0, 0, err
		}
	}
	return int64(len(docs)), int64((len(docs) + batchSize - 1) / batchSize), nil
}

func applyReadState(target *benchTarget, state readState) (int64, int64, error) {
	switch state {
	case readStateBuffered:
		return 1, 1, nil
	case readStateFlushed:
		if err := target.manager.FlushAll(); err != nil {
			return 0, 0, err
		}
		return 1, 1, nil
	case readStateCheckpointed:
		if err := target.manager.FlushAll(); err != nil {
			return 0, 0, err
		}
		if err := target.db.Checkpoint(); err != nil {
			return 0, 0, err
		}
		return 1, 1, nil
	default:
		return 0, 0, fmt.Errorf("unknown read-state %q", state)
	}
}

func runIDReads(target *benchTarget, total, workers int) (int64, int64, error) {
	buffers := make([][]byte, workerBufferCount(workers))
	return runParallel(total, workers, func(op int, worker int) error {
		id := target.fixture.ids[benchmarkDocumentOrdinal(op, 17, len(target.fixture.ids))]
		buf := buffers[worker]
		value, found, err := target.col.GetInto(id, buf[:0])
		buffers[worker] = value
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("missing id %q", id)
		}
		return nil
	})
}

func runEmailReads(target *benchTarget, total, workers int) (int64, int64, error) {
	buffers := make([][]byte, workerBufferCount(workers))
	return runParallel(total, workers, func(op int, worker int) error {
		ordinal := benchmarkDocumentOrdinal(op, 31, len(target.fixture.ids))
		ids, _, err := target.col.FindByIndexValueLimit("email", target.fixture.emails[ordinal], 1)
		if err != nil {
			return err
		}
		if len(ids) != 1 {
			return fmt.Errorf("email lookup returned %d ids", len(ids))
		}
		buf := buffers[worker]
		value, found, err := target.col.GetInto(ids[0], buf[:0])
		buffers[worker] = value
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("email lookup id %q missing", ids[0])
		}
		return nil
	})
}

func runAgeIndexedRangeReads(target *benchTarget, total, workers int) (int64, int64, error) {
	buffers := make([][]byte, workerBufferCount(workers))
	return runParallel(total, workers, func(op int, worker int) error {
		minAge := rangeReadMinAge(op, len(target.fixture.ids))
		ids, _, err := target.col.FindByIndexRange("age", collections.IndexRangeOptions{
			Lower: collections.IndexRangeBound{Value: minAge, Inclusive: true},
			Upper: collections.IndexRangeBound{Unbounded: true},
			Limit: 10,
		})
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return fmt.Errorf("age indexed range returned no ids")
		}
		buf := buffers[worker]
		for _, id := range ids {
			value, found, err := target.col.GetInto(id, buf[:0])
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("range lookup id %q missing", id)
			}
			buf = value
		}
		buffers[worker] = buf
		return nil
	})
}

func runAgeScanRangeReads(target *benchTarget, total, workers int) (int64, int64, error) {
	docs := len(target.fixture.ids)
	materializers := make([]*collections.StoredDocumentJSONMaterializer, workerBufferCount(workers))
	defer func() {
		for _, materializer := range materializers {
			_ = materializer.Close()
		}
	}()
	return runParallel(total, workers, func(op int, worker int) error {
		minAge := rangeReadMinAge(op, len(target.fixture.ids))
		foundCount := 0
		materializer := materializers[worker]
		_, err := target.col.ScanDocumentsFunc(docs, func(record collections.DocumentRecord) (bool, error) {
			if materializer == nil && target.fixture.format == collections.DocumentFormatTemplateV1 {
				var err error
				materializer, err = target.col.NewStoredDocumentJSONMaterializer()
				if err != nil {
					return false, err
				}
				materializers[worker] = materializer
			}
			age, err := storedDocumentAge(target, materializer, record.Document)
			if err != nil {
				return false, err
			}
			if age >= minAge {
				foundCount++
			}
			return foundCount < 10, nil
		})
		if err != nil {
			return err
		}
		if foundCount == 0 {
			return fmt.Errorf("age scan found no documents")
		}
		return nil
	})
}

func storedDocumentAge(target *benchTarget, materializer *collections.StoredDocumentJSONMaterializer, document []byte) (int64, error) {
	if target == nil || target.fixture == nil {
		return 0, errors.New("missing benchmark fixture")
	}
	switch target.fixture.format {
	case collections.DocumentFormatBSON:
		value := bson.Raw(document).Lookup("age")
		if age, ok := value.Int64OK(); ok {
			return age, nil
		}
		if age, ok := value.Int32OK(); ok {
			return int64(age), nil
		}
		return 0, errors.New("BSON document missing numeric age")
	case collections.DocumentFormatJSON:
		return jsonDocumentAge(document)
	default:
		if materializer == nil {
			return 0, errors.New("missing document materializer")
		}
		jsonDoc, err := materializer.StoredDocumentJSON(document)
		if err != nil {
			return 0, err
		}
		return jsonDocumentAge(jsonDoc)
	}
}

func jsonDocumentAge(document []byte) (int64, error) {
	var decoded struct {
		Age int64 `json:"age"`
	}
	if err := json.Unmarshal(document, &decoded); err != nil {
		return 0, err
	}
	return decoded.Age, nil
}

func workerBufferCount(workers int) int {
	if workers <= 1 {
		return 1
	}
	return workers
}

type updateSetWorkload struct {
	ordinals     []int
	replacements [][]byte
}

func prepareUpdateSet(target *benchTarget, total int) (updateSetWorkload, error) {
	workload := updateSetWorkload{
		ordinals:     make([]int, total),
		replacements: make([][]byte, total),
	}
	for i := 0; i < total; i++ {
		ordinal := benchmarkDocumentOrdinal(i, 37, len(target.fixture.ids))
		workload.ordinals[i] = ordinal
		doc, err := target.fixture.encodeDocument(ordinal, true, i)
		if err != nil {
			return updateSetWorkload{}, err
		}
		workload.replacements[i] = doc
	}
	return workload, nil
}

func runUpdateSet(target *benchTarget, workload updateSetWorkload, workers int) (int64, int64, error) {
	total := len(workload.replacements)
	return runParallel(total, workers, func(op int, worker int) error {
		id := target.fixture.ids[workload.ordinals[op]]
		replacement := workload.replacements[op]
		_, _, err := target.col.Update(id, func(current []byte) ([]byte, bool, error) {
			if current == nil {
				return nil, false, fmt.Errorf("update id %q missing", id)
			}
			return replacement, true, nil
		})
		return err
	})
}

func runDeletes(target *benchTarget, total int) (int64, int64, error) {
	for i := 0; i < total; i++ {
		if err := target.col.Delete(target.fixture.ids[i]); err != nil {
			return int64(i), int64(i), err
		}
	}
	return int64(total), int64(total), nil
}

func runParallel(total, workers int, fn func(op int, worker int) error) (int64, int64, error) {
	if total <= 0 {
		return 0, 0, nil
	}
	if workers <= 1 {
		var completed int64
		for i := 0; i < total; i++ {
			if err := fn(i, 0); err != nil {
				return completed, completed, err
			}
			completed++
		}
		return int64(total), int64(total), nil
	}
	var next atomic.Int64
	var completed atomic.Int64
	var stopped atomic.Bool
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if stopped.Load() {
					return
				}
				op := int(next.Add(1) - 1)
				if op >= total {
					return
				}
				if err := fn(op, worker); err != nil {
					stopped.Store(true)
					select {
					case errCh <- err:
					default:
					}
					return
				}
				completed.Add(1)
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		n := completed.Load()
		return n, n, err
	default:
		return int64(total), int64(total), nil
	}
}

func timedPhase(target *benchTarget, name string, fn phaseFunc) (phaseResult, error) {
	before := collectStats(target)
	start := time.Now()
	ops, calls, err := fn()
	elapsed := time.Since(start)
	after := collectStats(target)
	phase := phaseResult{
		Name:        name,
		Operations:  ops,
		DriverCalls: calls,
		DurationMS:  float64(elapsed.Nanoseconds()) / 1e6,
	}
	if ops > 0 {
		phase.NSPerOp = float64(elapsed.Nanoseconds()) / float64(ops)
		phase.OpsPerSec = float64(ops) / elapsed.Seconds()
	}
	attachStats(&phase, before, after)
	return phase, err
}

func collectStats(target *benchTarget) map[string]string {
	if target == nil {
		return nil
	}
	stats := make(map[string]string)
	if target.db != nil {
		for key, value := range target.db.Stats() {
			stats[key] = value
		}
	}
	if target.manager != nil {
		for key, value := range target.manager.Stats() {
			stats[key] = value
		}
	}
	return treedbstats.Selected(stats)
}

func attachStats(phase *phaseResult, before, after map[string]string) {
	delta, numeric := statsDelta(before, after)
	if len(delta) > 0 {
		phase.TreeDBStatsDelta = delta
	}
	if phase.Operations > 0 && len(numeric) > 0 {
		metrics := make(map[string]float64)
		for key, value := range numeric {
			if strings.HasSuffix(key, "_ns_total") {
				metrics[strings.TrimSuffix(key, "_total")+"/op"] = value / float64(phase.Operations)
			}
		}
		if len(metrics) > 0 {
			phase.TreeDBMetrics = metrics
		}
	}
}

func statsDelta(before, after map[string]string) (map[string]string, map[string]float64) {
	if len(after) == 0 {
		return nil, nil
	}
	out := make(map[string]string)
	numeric := make(map[string]float64)
	for key, afterValue := range after {
		beforeNumber := 0.0
		beforeHadNumber := false
		if beforeValue, ok := before[key]; ok {
			if parsed, err := strconv.ParseFloat(beforeValue, 64); err == nil {
				beforeNumber = parsed
				beforeHadNumber = true
			}
		}
		parsedAfter, err := strconv.ParseFloat(afterValue, 64)
		if err != nil {
			continue
		}
		if !beforeHadNumber {
			beforeNumber = 0
		}
		delta := parsedAfter - beforeNumber
		if delta == 0 {
			continue
		}
		out[key] = strconv.FormatFloat(delta, 'f', -1, 64)
		numeric[key] = delta
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, numeric
}

func writeResult(w io.Writer, format string, res result) error {
	switch format {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	case "text":
		return writeTextResult(w, res)
	default:
		return fmt.Errorf("unknown output format %q", format)
	}
}

func writeTextResult(w io.Writer, res result) error {
	fmt.Fprintf(w, "Native collection workload benchmark\n")
	fmt.Fprintf(w, "profile=%s documents=%d batch_size=%d reads=%d range_reads=%d updates=%d deletes=%d\n\n",
		res.TreeDBProfile, res.Documents, res.BatchSize, res.Reads, res.RangeReads, res.Updates, res.Deletes)
	for _, row := range res.Rows {
		fmt.Fprintf(w, "%s indexes_%d read_state=%s indexes=%s\n", row.Format, row.Indexes, row.ReadState, strings.Join(row.IndexNames, ","))
		writePhaseText(w, row.Load)
		writePhaseText(w, row.StateTransition)
		for _, phase := range row.Phases {
			writePhaseText(w, phase)
		}
		fmt.Fprintln(w)
	}
	if len(res.Errors) > 0 {
		fmt.Fprintln(w, "errors:")
		for _, err := range res.Errors {
			fmt.Fprintf(w, "  %s indexes_%d %s: %s\n", err.Format, err.Indexes, err.ReadState, err.Error)
		}
	}
	return nil
}

func writePhaseText(w io.Writer, phase phaseResult) {
	if phase.Skipped {
		fmt.Fprintf(w, "  %-44s skipped (%s)\n", phase.Name, phase.SkipReason)
		return
	}
	fmt.Fprintf(w, "  %-44s %12.1f ns/op %12.0f ops/sec ops=%d duration=%.2fms\n",
		phase.Name, phase.NSPerOp, phase.OpsPerSec, phase.Operations, phase.DurationMS)
}

func sortedPhaseNames(row rowResult) []string {
	names := make([]string, 0, len(row.Phases)+2)
	names = append(names, row.Load.Name, row.StateTransition.Name)
	for _, phase := range row.Phases {
		names = append(names, phase.Name)
	}
	sort.Strings(names)
	return names
}
