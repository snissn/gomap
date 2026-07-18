package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

const (
	modeDocument             = "document"
	modeTypedRow             = "typed-row"
	modeTypedColumn          = "typed-column"
	modeHybridDocumentRow    = "hybrid-document-row"
	modeHybridDocumentCol    = "hybrid-document-column"
	modeHybridRowColumn      = "hybrid-row-column"
	workloadInsert           = "insert"
	workloadPointGet         = "point-get"
	workloadRangeFilter      = "range-filter"
	workloadRangeAggregate   = "range-aggregate"
	workloadFullAggregate    = "full-aggregate"
	workloadMixedRead        = "mixed-read"
	workloadReopenRead       = "reopen-read"
	collectionName           = "demo_events"
	indexTimeUS              = "time_us"
	defaultRows              = 1000
	defaultBatchSize         = 1000
	defaultPayloadBytes      = 128
	defaultStringCardinality = 16
	defaultExtraFields       = 2
	defaultSeed              = int64(1)
)

type config struct {
	Mode              string
	Workload          string
	Rows              int
	BatchSize         int
	PayloadBytes      int
	Int64Distribution string
	StringCardinality int
	ExtraFields       int
	Dir               string
	KeepDir           bool
	Seed              int64
	ReadIntegrity     string
	Checkpoint        bool
	Reopen            bool
	ProfileDir        string
	Preset            string
}

type fixtureRow struct {
	ID      []byte
	Doc     []byte
	TimeUS  int64
	Amount  int64
	Kind    string
	Payload string
}

type storedDoc struct {
	TimeUS int64  `json:"time_us"`
	Amount int64  `json:"amount"`
	Kind   string `json:"kind"`
}

type aggregate struct {
	Count int64   `json:"count"`
	Sum   int64   `json:"sum"`
	Avg   float64 `json:"avg"`
}

type materializationSummary struct {
	DocumentMaterializations int `json:"document_materializations"`
	RowMaterializations      int `json:"row_materializations"`
}

type diagnosticSummary struct {
	Fallback             bool   `json:"fallback,omitempty"`
	FallbackReason       string `json:"fallback_reason,omitempty"`
	RowsScanned          int    `json:"rows_scanned,omitempty"`
	RowsMatched          int    `json:"rows_matched,omitempty"`
	PartsConsidered      int    `json:"parts_considered,omitempty"`
	PartsPruned          int    `json:"parts_pruned,omitempty"`
	BlocksDecoded        int    `json:"blocks_decoded,omitempty"`
	MappedBytes          uint64 `json:"mapped_bytes,omitempty"`
	DecodedBytes         uint64 `json:"decoded_bytes,omitempty"`
	HeapCopyBytes        uint64 `json:"heap_copy_bytes,omitempty"`
	DecodedMetadataBytes uint64 `json:"decoded_metadata_bytes,omitempty"`
	ReadIntegrity        string `json:"read_integrity,omitempty"`
}

type summary struct {
	Mode              string                 `json:"mode"`
	Workload          string                 `json:"workload"`
	Rows              int                    `json:"rows"`
	BatchSize         int                    `json:"batch_size"`
	PayloadBytes      int                    `json:"payload_bytes"`
	Int64Distribution string                 `json:"int64_distribution"`
	StringCardinality int                    `json:"string_cardinality"`
	ExtraFields       int                    `json:"extra_fields"`
	SetupMS           float64                `json:"setup_ms"`
	QueryMS           float64                `json:"query_ms"`
	Ops               int64                  `json:"ops"`
	OpsSec            float64                `json:"ops_sec"`
	RowsSec           float64                `json:"rows_sec"`
	Matches           int64                  `json:"matches"`
	Aggregate         aggregate              `json:"aggregate"`
	Materialization   materializationSummary `json:"materialization"`
	Diagnostics       diagnosticSummary      `json:"diagnostics"`
	DBDir             string                 `json:"db_dir"`
	ProfileDir        string                 `json:"profile_dir,omitempty"`
	KeptDir           bool                   `json:"kept_dir"`
}

func defaultConfig() config {
	return config{
		Mode:              modeDocument,
		Workload:          workloadRangeAggregate,
		Rows:              defaultRows,
		BatchSize:         defaultBatchSize,
		PayloadBytes:      defaultPayloadBytes,
		Int64Distribution: "linear",
		StringCardinality: defaultStringCardinality,
		ExtraFields:       defaultExtraFields,
		Seed:              defaultSeed,
		ReadIntegrity:     string(collections.ColumnAssetReadIntegrityVerify),
	}
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	out, err := runDemo(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	printSummary(os.Stdout, out)
}

func parseConfig(args []string) (config, error) {
	defs := defaultConfig()
	var parsed config
	parsed = defs
	fs := flag.NewFlagSet("treedb_collection_demo", flag.ContinueOnError)
	fs.SetOutput(new(bytes.Buffer))
	fs.StringVar(&parsed.Mode, "mode", defs.Mode, "storage mode: document|typed-row|typed-column|hybrid-document-row|hybrid-document-column|hybrid-row-column")
	fs.StringVar(&parsed.Workload, "workload", defs.Workload, "workload: insert|point-get|range-filter|range-aggregate|full-aggregate|mixed-read|reopen-read")
	fs.IntVar(&parsed.Rows, "rows", defs.Rows, "fixture row count")
	fs.IntVar(&parsed.BatchSize, "batch-size", defs.BatchSize, "InsertBatch size")
	fs.IntVar(&parsed.PayloadBytes, "payload-bytes", defs.PayloadBytes, "bytes in retained payload string")
	fs.StringVar(&parsed.Int64Distribution, "int64-distribution", defs.Int64Distribution, "linear|modulo|random|descending")
	fs.IntVar(&parsed.StringCardinality, "string-cardinality", defs.StringCardinality, "distinct kind strings")
	fs.IntVar(&parsed.ExtraFields, "extra-fields", defs.ExtraFields, "extra retained JSON fields")
	fs.StringVar(&parsed.Dir, "dir", defs.Dir, "database directory; a temp dir is used when empty")
	fs.BoolVar(&parsed.KeepDir, "keep-dir", defs.KeepDir, "keep database directory after exit")
	fs.Int64Var(&parsed.Seed, "seed", defs.Seed, "deterministic fixture seed")
	fs.StringVar(&parsed.ReadIntegrity, "read-integrity", defs.ReadIntegrity, "typed-column read integrity: verify|cached_verify|skip_checksums")
	fs.BoolVar(&parsed.Checkpoint, "checkpoint", defs.Checkpoint, "checkpoint after setup before query")
	fs.BoolVar(&parsed.Reopen, "reopen", defs.Reopen, "checkpoint, close, and reopen before query")
	fs.StringVar(&parsed.ProfileDir, "profile-dir", defs.ProfileDir, "emit cpu.pprof, allocs.pprof, summary.json, summary.md")
	fs.StringVar(&parsed.Preset, "preset", defs.Preset, "preset: document-app|event-analytics|schema-aware|hybrid-product|perf-engineer")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	visited := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { visited[f.Name] = true })

	cfg := defs
	if parsed.Preset != "" {
		var err error
		cfg, err = configWithPreset(cfg, parsed.Preset)
		if err != nil {
			return config{}, err
		}
	}
	if visited["mode"] {
		cfg.Mode = parsed.Mode
	}
	if visited["workload"] {
		cfg.Workload = parsed.Workload
	}
	if visited["rows"] {
		cfg.Rows = parsed.Rows
	}
	if visited["batch-size"] {
		cfg.BatchSize = parsed.BatchSize
	}
	if visited["payload-bytes"] {
		cfg.PayloadBytes = parsed.PayloadBytes
	}
	if visited["int64-distribution"] {
		cfg.Int64Distribution = parsed.Int64Distribution
	}
	if visited["string-cardinality"] {
		cfg.StringCardinality = parsed.StringCardinality
	}
	if visited["extra-fields"] {
		cfg.ExtraFields = parsed.ExtraFields
	}
	if visited["dir"] {
		cfg.Dir = parsed.Dir
	}
	if visited["keep-dir"] {
		cfg.KeepDir = parsed.KeepDir
	}
	if visited["seed"] {
		cfg.Seed = parsed.Seed
	}
	if visited["read-integrity"] {
		cfg.ReadIntegrity = parsed.ReadIntegrity
	}
	if visited["checkpoint"] {
		cfg.Checkpoint = parsed.Checkpoint
	}
	if visited["reopen"] {
		cfg.Reopen = parsed.Reopen
	}
	if visited["profile-dir"] {
		cfg.ProfileDir = parsed.ProfileDir
	}
	cfg.Preset = parsed.Preset
	return validateConfig(cfg)
}

func configWithPreset(cfg config, preset string) (config, error) {
	switch preset {
	case "document-app":
		cfg.Mode = modeDocument
		cfg.Workload = workloadPointGet
		cfg.Rows = 5000
		cfg.PayloadBytes = 512
		cfg.ExtraFields = 4
	case "event-analytics":
		cfg.Mode = modeTypedColumn
		cfg.Workload = workloadRangeAggregate
		cfg.Rows = 10000
		cfg.PayloadBytes = 64
		cfg.ExtraFields = 1
	case "schema-aware":
		cfg.Mode = modeTypedRow
		cfg.Workload = workloadPointGet
		cfg.Rows = 5000
		cfg.PayloadBytes = 128
	case "hybrid-product":
		cfg.Mode = modeHybridDocumentCol
		cfg.Workload = workloadMixedRead
		cfg.Rows = 5000
		cfg.PayloadBytes = 512
		cfg.ExtraFields = 6
	case "perf-engineer":
		cfg.Mode = modeTypedColumn
		cfg.Workload = workloadRangeAggregate
		cfg.Rows = 50000
		cfg.BatchSize = 5000
		cfg.PayloadBytes = 32
	case "":
	default:
		return cfg, fmt.Errorf("unknown preset %q", preset)
	}
	return cfg, nil
}

func validateConfig(cfg config) (config, error) {
	switch cfg.Mode {
	case modeDocument, modeTypedRow, modeTypedColumn, modeHybridDocumentRow, modeHybridDocumentCol, modeHybridRowColumn:
	default:
		return cfg, fmt.Errorf("unknown mode %q", cfg.Mode)
	}
	switch cfg.Workload {
	case workloadInsert, workloadPointGet, workloadRangeFilter, workloadRangeAggregate, workloadFullAggregate, workloadMixedRead, workloadReopenRead:
	default:
		return cfg, fmt.Errorf("unknown workload %q", cfg.Workload)
	}
	if cfg.Rows <= 0 {
		return cfg, errors.New("rows must be positive")
	}
	if cfg.BatchSize <= 0 {
		return cfg, errors.New("batch-size must be positive")
	}
	if cfg.PayloadBytes < 0 {
		return cfg, errors.New("payload-bytes cannot be negative")
	}
	if cfg.StringCardinality <= 0 {
		return cfg, errors.New("string-cardinality must be positive")
	}
	if cfg.ExtraFields < 0 {
		return cfg, errors.New("extra-fields cannot be negative")
	}
	switch cfg.Int64Distribution {
	case "linear", "modulo", "random", "descending":
	default:
		return cfg, fmt.Errorf("unknown int64-distribution %q", cfg.Int64Distribution)
	}
	switch cfg.ReadIntegrity {
	case string(collections.ColumnAssetReadIntegrityVerify), string(collections.ColumnAssetReadIntegrityCachedVerify), string(collections.ColumnAssetReadIntegritySkipChecksums):
	default:
		return cfg, fmt.Errorf("unknown read-integrity %q", cfg.ReadIntegrity)
	}
	return cfg, nil
}

func runDemo(cfg config) (summary, error) {
	dir, kept, dirCleanup, err := prepareDemoDir(cfg.Dir, cfg.KeepDir)
	if err != nil {
		return summary{}, err
	}
	cfg.Dir = dir
	defer dirCleanup()

	prof, err := startProfiles(cfg.ProfileDir)
	if err != nil {
		return summary{}, err
	}
	defer prof.stop()

	rows := generateFixtureRows(cfg)
	backend, backendCleanup, err := openDemoBackend(cfg.Dir)
	if err != nil {
		return summary{}, err
	}
	cleanupCalled := false
	closeBackend := func() error {
		if cleanupCalled {
			return nil
		}
		cleanupCalled = true
		return backendCleanup()
	}
	defer closeBackend()

	mgr := collections.NewCollectionManager(backend)
	if _, err := mgr.CreateCollection(collectionMetaForMode(cfg.Mode)); err != nil {
		return summary{}, err
	}
	col, err := mgr.OpenCollection(collectionName)
	if err != nil {
		return summary{}, err
	}

	setupStart := time.Now()
	if err := insertRows(col, rows, cfg.BatchSize); err != nil {
		return summary{}, err
	}
	if err := mgr.FlushAll(); err != nil {
		return summary{}, err
	}
	setupDur := time.Since(setupStart)

	needReopen := cfg.Reopen || cfg.Workload == workloadReopenRead
	if cfg.Checkpoint || needReopen {
		if err := backend.Checkpoint(); err != nil {
			return summary{}, err
		}
	}
	if needReopen {
		if err := closeBackend(); err != nil {
			return summary{}, err
		}
		backend, backendCleanup, err = openDemoBackend(cfg.Dir)
		if err != nil {
			return summary{}, err
		}
		cleanupCalled = false
		mgr = collections.NewCollectionManager(backend)
		col, err = mgr.OpenCollection(collectionName)
		if err != nil {
			return summary{}, err
		}
	}

	queryStart := time.Now()
	result, err := executeWorkload(col, rows, cfg)
	if err != nil {
		return summary{}, err
	}
	queryDur := time.Since(queryStart)

	out := summary{
		Mode: cfg.Mode, Workload: cfg.Workload, Rows: cfg.Rows, BatchSize: cfg.BatchSize,
		PayloadBytes: cfg.PayloadBytes, Int64Distribution: cfg.Int64Distribution,
		StringCardinality: cfg.StringCardinality, ExtraFields: cfg.ExtraFields,
		SetupMS: durationMS(setupDur), QueryMS: durationMS(queryDur), Ops: result.Ops,
		Matches: result.Matches, Aggregate: result.Aggregate, Materialization: result.Materialization,
		Diagnostics: result.Diagnostics, DBDir: cfg.Dir, ProfileDir: cfg.ProfileDir, KeptDir: kept,
	}
	if cfg.Workload == workloadInsert {
		if setupDur > 0 {
			out.OpsSec = float64(result.Ops) / setupDur.Seconds()
			out.RowsSec = float64(cfg.Rows) / setupDur.Seconds()
		}
	} else {
		if queryDur > 0 && result.Ops > 0 {
			out.OpsSec = float64(result.Ops) / queryDur.Seconds()
		}
		if queryDur > 0 {
			out.RowsSec = float64(cfg.Rows) / queryDur.Seconds()
		}
	}
	if err := prof.writeSummary(out); err != nil {
		return summary{}, err
	}
	return out, nil
}

type profileRun struct {
	dir string
	cpu *os.File
}

func startProfiles(dir string) (*profileRun, error) {
	p := &profileRun{dir: dir}
	if dir == "" {
		return p, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		return nil, err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	p.cpu = f
	return p, nil
}

func (p *profileRun) stop() {
	if p == nil || p.cpu == nil {
		return
	}
	pprof.StopCPUProfile()
	_ = p.cpu.Close()
	p.cpu = nil
}

func (p *profileRun) writeSummary(out summary) error {
	if p == nil || p.dir == "" {
		return nil
	}
	p.stop()
	runtime.GC()
	allocs, err := os.Create(filepath.Join(p.dir, "allocs.pprof"))
	if err != nil {
		return err
	}
	if err := pprof.Lookup("allocs").WriteTo(allocs, 0); err != nil {
		_ = allocs.Close()
		return err
	}
	if err := allocs.Close(); err != nil {
		return err
	}
	jsonBytes, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(p.dir, "summary.json"), append(jsonBytes, '\n'), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(p.dir, "summary.md"), []byte(markdownSummary(out)), 0o644)
}

func prepareDemoDir(dir string, keep bool) (string, bool, func(), error) {
	if strings.TrimSpace(dir) == "" {
		tmp, err := os.MkdirTemp("", "treedb_collection_demo_")
		if err != nil {
			return "", false, nil, err
		}
		if keep {
			return tmp, true, func() {}, nil
		}
		return tmp, false, func() { _ = os.RemoveAll(tmp) }, nil
	}
	abs, err := validateFreshDemoDir(dir)
	if err != nil {
		return "", false, nil, err
	}
	if err := ensureFreshDemoDir(abs); err != nil {
		return "", false, nil, err
	}
	return abs, true, func() {}, nil
}

func validateFreshDemoDir(dir string) (string, error) {
	rawInput := strings.TrimSpace(dir)
	if rawInput == "" || demoDirHasParentTraversal(rawInput) {
		return "", fmt.Errorf("refusing unsafe demo directory %q", dir)
	}
	cleanInput := filepath.Clean(rawInput)
	if cleanInput == "." || cleanInput == ".." {
		return "", fmt.Errorf("refusing unsafe demo directory %q", dir)
	}
	abs, err := filepath.Abs(cleanInput)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	root := filepath.VolumeName(abs) + string(os.PathSeparator)
	if abs == root || abs == filepath.Clean(os.TempDir()) {
		return "", fmt.Errorf("refusing unsafe demo directory %q", dir)
	}
	return abs, nil
}

func demoDirHasParentTraversal(path string) bool {
	for _, part := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return true
		}
	}
	return false
}

func ensureFreshDemoDir(abs string) error {
	info, err := os.Stat(abs)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("-dir %q exists and is not a directory", abs)
		}
		entries, err := os.ReadDir(abs)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("-dir %q already exists and is not empty; choose a fresh directory", abs)
		}
		return nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(abs, 0o755)
	}
	return err
}

func openDemoBackend(dir string) (*backenddb.DB, func() error, error) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
	opts.DisableBackgroundPrune = true
	return treedb.OpenBackendWithCachedLeafLog(opts)
}

func collectionMetaForMode(mode string) *collections.CollectionMeta {
	meta := &collections.CollectionMeta{Name: collectionName}
	if mode == modeDocument {
		meta.Indexes = []collections.IndexDefinition{{Name: indexTimeUS, Field: "time_us", ValueType: collections.IndexValueInt64}}
	}
	cfg := &collections.ColumnStoreConfig{Enabled: true}
	column := func(name, path string, vt collections.ColumnStoreValueType, owner collections.TypedStorageFieldOwner, dict bool) collections.ColumnStoreColumn {
		return collections.ColumnStoreColumn{Name: name, Path: path, ValueType: vt, Owner: owner, Dictionary: dict}
	}
	switch mode {
	case modeDocument:
		return meta
	case modeTypedRow:
		cfg.Columns = []collections.ColumnStoreColumn{
			column("time_us", "time_us", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerRowAsset, false),
			column("amount", "amount", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerRowAsset, false),
			column("kind", "kind", collections.ColumnStoreValueString, collections.TypedStorageOwnerRowAsset, true),
		}
	case modeTypedColumn:
		cfg.Columns = []collections.ColumnStoreColumn{
			column("time_us", "time_us", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerColumnPart, false),
			column("amount", "amount", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerColumnPart, false),
			column("kind", "kind", collections.ColumnStoreValueString, collections.TypedStorageOwnerRowAsset, true),
		}
	case modeHybridDocumentRow:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadFull
		cfg.Columns = []collections.ColumnStoreColumn{
			column("time_us", "time_us", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerRowAsset, false),
			column("amount", "amount", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerRowAsset, false),
			column("kind", "kind", collections.ColumnStoreValueString, collections.TypedStorageOwnerRowAsset, true),
		}
	case modeHybridDocumentCol:
		cfg.RetainedPayload = collections.ColumnRetainedPayloadFull
		cfg.Columns = []collections.ColumnStoreColumn{
			column("time_us", "time_us", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerColumnPart, false),
			column("amount", "amount", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerColumnPart, false),
			column("kind", "kind", collections.ColumnStoreValueString, collections.TypedStorageOwnerRowAsset, true),
		}
	case modeHybridRowColumn:
		cfg.Columns = []collections.ColumnStoreColumn{
			column("time_us", "time_us", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerColumnPart, false),
			column("amount", "amount", collections.ColumnStoreValueInt64, collections.TypedStorageOwnerRowAsset, false),
			column("kind", "kind", collections.ColumnStoreValueString, collections.TypedStorageOwnerRowAsset, true),
		}
	}
	cfg.SortKey = []collections.ColumnSortKey{{Column: "time_us"}}
	cfg.AggregateMetadata = []collections.ColumnAggregateMetadata{{Name: "rows", Kind: collections.ColumnAggregateCount}}
	meta.Options.ColumnStore = cfg
	return meta
}

func generateFixtureRows(cfg config) []fixtureRow {
	rng := rand.New(rand.NewSource(cfg.Seed))
	rows := make([]fixtureRow, cfg.Rows)
	for i := range rows {
		timeUS := int64(i)
		switch cfg.Int64Distribution {
		case "modulo":
			timeUS = int64(i % max(1, cfg.Rows/10))
		case "random":
			timeUS = rng.Int63n(int64(max(1, cfg.Rows*2)))
		case "descending":
			timeUS = int64(cfg.Rows - i - 1)
		}
		amount := int64((i*17)%1000) - 100
		kind := fmt.Sprintf("k%03d", i%cfg.StringCardinality)
		payload := deterministicPayload(cfg.PayloadBytes, cfg.Seed, i)
		id := []byte(fmt.Sprintf("doc%012d", i))
		rows[i] = fixtureRow{ID: id, TimeUS: timeUS, Amount: amount, Kind: kind, Payload: payload}
		rows[i].Doc = encodeFixtureDoc(rows[i], cfg.ExtraFields)
	}
	return rows
}

func deterministicPayload(n int, seed int64, row int) string {
	if n <= 0 {
		return ""
	}
	alphabet := "abcdefghijklmnopqrstuvwxyz0123456789"
	var b strings.Builder
	b.Grow(n)
	start := positiveModulo(seed+int64(row*13), len(alphabet))
	for b.Len() < n {
		b.WriteByte(alphabet[(start+b.Len())%len(alphabet)])
	}
	return b.String()
}

func positiveModulo(value int64, mod int) int {
	if mod <= 0 {
		return 0
	}
	out := value % int64(mod)
	if out < 0 {
		out += int64(mod)
	}
	return int(out)
}

func encodeFixtureDoc(row fixtureRow, extraFields int) []byte {
	doc := map[string]any{
		"time_us": row.TimeUS,
		"amount":  row.Amount,
		"kind":    row.Kind,
		"payload": row.Payload,
	}
	for i := 0; i < extraFields; i++ {
		doc[fmt.Sprintf("extra_%d", i)] = row.TimeUS + int64(i)
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		panic(fmt.Sprintf("encode deterministic fixture document: %v", err))
	}
	return encoded
}

func insertRows(col *collections.Collection, rows []fixtureRow, batchSize int) error {
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		ids := make([][]byte, end-start)
		docs := make([][]byte, end-start)
		for i := start; i < end; i++ {
			ids[i-start] = rows[i].ID
			docs[i-start] = rows[i].Doc
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			return err
		}
	}
	return nil
}

type workloadResult struct {
	Ops             int64
	Matches         int64
	Aggregate       aggregate
	Materialization materializationSummary
	Diagnostics     diagnosticSummary
}

func executeWorkload(col *collections.Collection, rows []fixtureRow, cfg config) (workloadResult, error) {
	switch cfg.Workload {
	case workloadInsert:
		return workloadResult{Ops: int64(len(rows))}, nil
	case workloadPointGet:
		return runPointGet(col, rows)
	case workloadRangeFilter:
		return runRangeFilter(col, rows, cfg)
	case workloadRangeAggregate:
		return runRangeAggregate(col, rows, cfg, false)
	case workloadFullAggregate:
		return runRangeAggregate(col, rows, cfg, true)
	case workloadMixedRead:
		pg, err := runPointGet(col, rows[:min(len(rows), 128)])
		if err != nil {
			return workloadResult{}, err
		}
		agg, err := runRangeAggregate(col, rows, cfg, false)
		if err != nil {
			return workloadResult{}, err
		}
		agg.Ops += pg.Ops
		agg.Materialization.DocumentMaterializations += pg.Materialization.DocumentMaterializations
		return agg, nil
	case workloadReopenRead:
		return runRangeAggregate(col, rows, cfg, false)
	default:
		return workloadResult{}, fmt.Errorf("unknown workload %q", cfg.Workload)
	}
}

func runPointGet(col *collections.Collection, rows []fixtureRow) (workloadResult, error) {
	var out workloadResult
	for _, row := range rows {
		doc, err := col.Get(row.ID)
		if err != nil {
			return out, err
		}
		if doc == nil {
			return out, fmt.Errorf("missing document %s", row.ID)
		}
		out.Ops++
		out.Matches++
		out.Materialization.DocumentMaterializations++
	}
	return out, nil
}

func runRangeFilter(col *collections.Collection, rows []fixtureRow, cfg config) (workloadResult, error) {
	low, high := queryRange(rows, false)
	if modeHasTypedStorage(cfg.Mode) {
		res, err := col.RunTypedColumnInt64PredicateScan(collections.TypedColumnInt64PredicateScanRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: low, High: high, ColumnAssetReadIntegrity: collections.ColumnAssetReadIntegrity(cfg.ReadIntegrity)})
		if err != nil {
			return workloadResult{}, err
		}
		return workloadResult{Ops: 1, Matches: int64(len(res.Rows)), Diagnostics: diagnosticsFrom(res.Diagnostics), Materialization: materializationFrom(res.Diagnostics)}, nil
	}
	ids, _, err := col.FindByIndexRange(indexTimeUS, collections.IndexRangeOptions{Lower: collections.IndexRangeBound{Value: low, Inclusive: true}, Upper: collections.IndexRangeBound{Value: high, Inclusive: true}, Limit: len(rows) + 1})
	if err != nil {
		return workloadResult{}, err
	}
	return workloadResult{Ops: 1, Matches: int64(len(ids))}, nil
}

func runRangeAggregate(col *collections.Collection, rows []fixtureRow, cfg config, full bool) (workloadResult, error) {
	low, high := queryRange(rows, full)
	if modeHasTypedStorage(cfg.Mode) {
		res, err := col.RunTypedColumnInt64PredicateAggregate(collections.TypedColumnInt64PredicateAggregateRequest{Column: "time_us", Kind: collections.TypedColumnInt64PredicateRange, Low: low, High: high, ColumnAssetReadIntegrity: collections.ColumnAssetReadIntegrity(cfg.ReadIntegrity)})
		if err != nil {
			return workloadResult{}, err
		}
		diag := diagnosticsFrom(res.Diagnostics)
		return workloadResult{Ops: 1, Matches: res.Count, Aggregate: aggregate{Count: res.Count, Sum: res.Sum, Avg: res.Avg}, Diagnostics: diag, Materialization: materializationFrom(res.Diagnostics)}, nil
	}
	return scanDocumentAggregate(col, len(rows), low, high)
}

func scanDocumentAggregate(col *collections.Collection, maxDocs int, low, high int64) (workloadResult, error) {
	var out workloadResult
	_, err := col.ScanDocumentsFunc(maxDocs, func(record collections.DocumentRecord) (bool, error) {
		out.Materialization.DocumentMaterializations++
		var doc storedDoc
		if err := json.Unmarshal(record.Document, &doc); err != nil {
			return false, err
		}
		if doc.TimeUS >= low && doc.TimeUS <= high {
			out.Matches++
			out.Aggregate.Count++
			out.Aggregate.Sum += doc.TimeUS
		}
		return true, nil
	})
	if err != nil {
		return out, err
	}
	out.Ops = 1
	if out.Aggregate.Count > 0 {
		out.Aggregate.Avg = float64(out.Aggregate.Sum) / float64(out.Aggregate.Count)
	}
	return out, nil
}

func queryRange(rows []fixtureRow, full bool) (int64, int64) {
	if full {
		return math.MinInt64, math.MaxInt64
	}
	vals := make([]int64, len(rows))
	for i := range rows {
		vals[i] = rows[i].TimeUS
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[len(vals)/4], vals[(len(vals)*3)/4]
}

func modeHasTypedStorage(mode string) bool { return mode != modeDocument }

func diagnosticsFrom(d collections.TypedColumnInt64PredicateScanDiagnostics) diagnosticSummary {
	// TypedColumnInt64PredicateScanDiagnostics exposes decoded metadata and decoded
	// candidate payload bytes separately. The demo's decoded_bytes output is the
	// total decoded byte budget for the query.
	return diagnosticSummary{
		Fallback:             d.Fallback,
		FallbackReason:       d.FallbackReason,
		RowsScanned:          d.RowsScanned,
		RowsMatched:          d.RowsMatched,
		PartsConsidered:      d.PartsConsidered,
		PartsPruned:          d.PartsPruned,
		BlocksDecoded:        d.BlocksDecoded,
		MappedBytes:          d.MappedBytes,
		DecodedBytes:         d.DecodedMetadataBytes + d.DecodedHeapCopyBytes,
		HeapCopyBytes:        d.HeapCopyBytes,
		DecodedMetadataBytes: d.DecodedMetadataBytes,
		ReadIntegrity:        d.ColumnAssetReadIntegrity,
	}
}

func materializationFrom(d collections.TypedColumnInt64PredicateScanDiagnostics) materializationSummary {
	return materializationSummary{DocumentMaterializations: d.DocumentMaterializations, RowMaterializations: d.RowMaterializations}
}

func durationMS(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func printSummary(w interface{ Write([]byte) (int, error) }, s summary) {
	fmt.Fprintf(w, "mode=%s workload=%s rows=%d\n", s.Mode, s.Workload, s.Rows)
	fmt.Fprintf(w, "setup_ms=%.3f query_ms=%.3f ops_sec=%.2f rows_sec=%.2f\n", s.SetupMS, s.QueryMS, s.OpsSec, s.RowsSec)
	fmt.Fprintf(w, "matches=%d count=%d sum=%d avg=%.3f\n", s.Matches, s.Aggregate.Count, s.Aggregate.Sum, s.Aggregate.Avg)
	fmt.Fprintf(w, "document_materializations=%d row_materializations=%d mapped_bytes=%d decoded_bytes=%d\n", s.Materialization.DocumentMaterializations, s.Materialization.RowMaterializations, s.Diagnostics.MappedBytes, s.Diagnostics.DecodedBytes)
	fmt.Fprintf(w, "db_dir=%s\n", s.DBDir)
	if s.ProfileDir != "" {
		fmt.Fprintf(w, "profile_dir=%s\n", s.ProfileDir)
	}
}

func markdownSummary(s summary) string {
	return fmt.Sprintf("# TreeDB collection demo\n\n- mode: `%s`\n- workload: `%s`\n- rows: `%d`\n- setup_ms: `%.3f`\n- query_ms: `%.3f`\n- matches: `%d`\n- count/sum/avg: `%d` / `%d` / `%.3f`\n- db_dir: `%s`\n", s.Mode, s.Workload, s.Rows, s.SetupMS, s.QueryMS, s.Matches, s.Aggregate.Count, s.Aggregate.Sum, s.Aggregate.Avg, s.DBDir)
}
