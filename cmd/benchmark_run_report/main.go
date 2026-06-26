package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type config struct {
	RunRoot string
	OutPath string
	Title   string
}

type rawEngineRun struct {
	Name       string
	Profile    string
	Checkpoint string
	Results    map[string]float64
	IndexDB    string
	LeafVLog   string
}

type collectionRow struct {
	ConfigName         string
	Engine             string
	Format             string
	Shape              string
	IndexCount         int
	DocumentCount      int
	BenchmarkName      string
	Phase              string
	MaintenanceMode    string
	TotalBytes         float64
	BytesPerDoc        float64
	DocsPerSec         float64
	NsPerDoc           float64
	BatchSize          int
	BenchmarkTimed     bool
	MeasurementKind    string
	MeasurementNote    string
	SourceArtifact     string
	MaintenanceStats   map[string]float64
	Extra              map[string]string
	ProductionEvidence *collectionProductionEvidence
	Source             string
}

type collectionProductionEvidence struct {
	ProducerRoute                      string   `json:"producer_route"`
	ProducerRouteCandidateOps          *float64 `json:"producer_route_candidate_ops"`
	ProducerRouteUsedOps               *float64 `json:"producer_route_used_ops"`
	ProducerRouteFallbacks             *float64 `json:"producer_route_fallbacks"`
	StoragePolicy                      string   `json:"storage_policy"`
	GOMAXPROCS                         *int     `json:"gomaxprocs"`
	PhysicalCores                      *int     `json:"physical_cores"`
	FlushAdmissionEffectiveConcurrency *int     `json:"flush_admission_effective_concurrency"`
	FlushAdmissionAdmitted             *bool    `json:"flush_admission_admitted"`
	FlushAdmissionSpanNative           *bool    `json:"flush_admission_span_native"`
	FlushAdmissionBacklogCoalescing    *bool    `json:"flush_admission_backlog_coalescing"`
	FlushSpanCandidateOps              *float64 `json:"flush_span_candidate_ops"`
	FlushSpanUsedOps                   *float64 `json:"flush_span_used_ops"`
	FlushSpanFallbacks                 *float64 `json:"flush_span_fallbacks"`
	OrderedRootSpanFallbacks           *float64 `json:"ordered_root_span_fallbacks"`
}

type collectionComparison struct {
	Name              string
	TreeDBConfig      string
	TreeDBPhase       string
	SQLiteConfig      string
	SQLitePhase       string
	TreeDBBytesPerDoc float64
	SQLiteBytesPerDoc float64
	SmallerRatio      float64
	ComparisonBasis   string
	Source            string
}

type collectionGuardrailCheck struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

type mongoSummaryRow struct {
	Documents               int
	SecondaryIndexes        int
	RangeIndex              bool
	RangeMode               string
	TreeDBConfig            string
	MongoConfig             string
	Phase                   string
	TreeDBOpsSec            float64
	TreeDBSampledOpsSec     float64
	TreeDBSampledNsPerOp    float64
	MongoOpsSec             float64
	MongoSampledOpsSec      float64
	MongoSampledNsPerOp     float64
	TreeDBToMongoRatio      float64
	TreeDBToMongoSampled    float64
	TreeDBP50US             float64
	TreeDBP95US             float64
	TreeDBP99US             float64
	MongoP50US              float64
	MongoP95US              float64
	MongoP99US              float64
	TreeDBDiskSnapshot      string
	TreeDBDiskBytes         float64
	TreeDBPhysicalBytes     float64
	MongoDBStatsDataSize    float64
	MongoDBStatsTotalSize   float64
	MongoPhysicalBytes      float64
	TreeDBToMongoTotalRatio float64
	TreeDBToMongoPhysRatio  float64
	BatchSize               int
	InsertProducers         int
	EffectiveProducers      int
	DriverCalls             int
	LoadBatchCount          int
	Source                  string
}

type matrixRow struct {
	Target           string
	Config           string
	Documents        int
	SecondaryIndexes int
	RawJSON          string
	PhysicalBytes    int64
}

type benchmarkResult struct {
	Target           string        `json:"target"`
	Documents        int           `json:"documents"`
	SecondaryIndexes int           `json:"secondary_indexes"`
	RangeIndex       bool          `json:"range_index"`
	ClientMode       string        `json:"client_mode,omitempty"`
	TreeDBProfile    string        `json:"treedb_profile,omitempty"`
	DocumentFormat   string        `json:"treedb_document_format,omitempty"`
	Phases           []phaseResult `json:"phases"`
}

type phaseResult struct {
	Name                string         `json:"name"`
	Operations          int            `json:"operations"`
	DriverCalls         int            `json:"driver_calls"`
	DurationMillis      float64        `json:"duration_ms"`
	OpsPerSecond        float64        `json:"ops_per_sec"`
	SampledOpsPerSecond float64        `json:"sampled_ops_per_sec,omitempty"`
	SampledNsPerOp      float64        `json:"sampled_ns_per_op,omitempty"`
	LatencyMicros       latencySummary `json:"latency_micros"`
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type loadModeRow struct {
	Indexes       int
	Target        string
	Config        string
	OpsPerSec     float64
	PhysicalBytes int64
	RawJSON       string
}

type gitIdentity struct {
	Label string
	Hash  string
}

type commandLogEntry struct {
	Command     string
	ExitStatus  int
	DurationSec int
	Warning     string
	Complete    bool
}

type runMetadata map[string]string

type artifactSectionStatus struct {
	Name     string
	Artifact string
	Status   string
	Required bool
	Detail   string
}

type reportData struct {
	Config                config
	GeneratedAt           time.Time
	Git                   []gitIdentity
	Commands              []commandLogEntry
	RunMetadata           runMetadata
	ArtifactSections      []artifactSectionStatus
	RawEngine             []rawEngineRun
	Collections           []collectionRow
	CollectionComparisons []collectionComparison
	MongoFullSweep        []mongoSummaryRow
	MongoLoadModes        []loadModeRow
	MongoLoadScaling      []mongoSummaryRow
	MongoScaling          map[int][]mongoSummaryRow
	Profiles              profileReportData
	Warnings              []string
}

type profileReportData struct {
	Benchprof   []benchprofInsightSummary
	Collections []collectionProfileSummary
	Mongo       []mongoProfileSummary
}

type benchprofInsightSummary struct {
	Source        string
	ProfilesDir   string
	OpsSource     string
	Insights      []string
	Warnings      []string
	Targets       []profileInvestigationTarget
	CPUProfiles   int
	AllocProfiles int
	BlockProfiles int
	MutexProfiles int
}

type profileInvestigationTarget struct {
	DBTag    string  `json:"db_tag"`
	Test     string  `json:"test"`
	Category string  `json:"category"`
	Function string  `json:"function"`
	FlatPct  float64 `json:"flat_pct"`
	Why      string  `json:"why"`
	File     string  `json:"file"`
	Line     int     `json:"line"`
}

type collectionProfileSummary struct {
	Source           string
	ProfileDir       string
	Cell             string
	ExecutionPath    string
	Engine           string
	DocumentFormat   string
	StoragePolicy    string
	BenchmarkPattern string
	Benchtime        string
	Count            int
	DurationMillis   float64
	Artifacts        []collectionProfileArtifact
	RunError         string
}

type collectionProfileArtifact struct {
	Phase         string
	CPUProfile    string
	AllocsProfile string
	BlockProfile  string
	MutexProfile  string
	Output        string
	Error         string
}

type mongoProfileSummary struct {
	Source           string
	ProfileDir       string
	Target           string
	ClientMode       string
	DocumentFormat   string
	TreeDBProfile    string
	Documents        int
	SecondaryIndexes int
	HasResult        bool
	Artifacts        []mongoProfileArtifact
	RunError         string
	ResultError      string
}

type mongoProfileArtifact struct {
	Phase          string
	Prefix         string
	DurationMillis float64
	OpsPerSecond   float64
	P95US          float64
	CPUProfile     string
	AllocsProfile  string
	BlockProfile   string
	MutexProfile   string
	Trace          string
	Error          string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "benchmark_run_report: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	data, err := loadReportData(cfg)
	if err != nil {
		return err
	}
	doc := renderHTML(data)
	if err := os.MkdirAll(filepath.Dir(cfg.OutPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(cfg.OutPath, []byte(doc), 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote deep benchmark report: %s\n", cfg.OutPath)
	return nil
}

func parseConfig(args []string) (config, error) {
	cfg := config{Title: "TreeDB Benchmark Run Report"}
	fs := flag.NewFlagSet("benchmark_run_report", flag.ContinueOnError)
	fs.StringVar(&cfg.RunRoot, "run-root", "", "Root directory containing canonical benchmark run artifacts")
	fs.StringVar(&cfg.OutPath, "out", "", "HTML report output path; defaults to <run-root>/deep_report.html")
	fs.StringVar(&cfg.Title, "title", cfg.Title, "Report title")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.RunRoot == "" {
		return config{}, errors.New("-run-root is required")
	}
	abs, err := filepath.Abs(cfg.RunRoot)
	if err != nil {
		return config{}, err
	}
	cfg.RunRoot = abs
	info, err := os.Stat(cfg.RunRoot)
	if err != nil {
		return config{}, fmt.Errorf("-run-root %q: %w", cfg.RunRoot, err)
	}
	if !info.IsDir() {
		return config{}, fmt.Errorf("-run-root %q is not a directory", cfg.RunRoot)
	}
	if _, err := os.ReadDir(cfg.RunRoot); err != nil {
		return config{}, fmt.Errorf("-run-root %q is not readable: %w", cfg.RunRoot, err)
	}
	if cfg.OutPath == "" {
		cfg.OutPath = filepath.Join(cfg.RunRoot, "deep_report.html")
	}
	return cfg, nil
}

func loadReportData(cfg config) (reportData, error) {
	data := reportData{
		Config:       cfg,
		GeneratedAt:  time.Now().UTC(),
		Git:          loadGitIdentity(cfg.RunRoot),
		RunMetadata:  loadRunMetadata(filepath.Join(cfg.RunRoot, "RUNBOOK.md")),
		MongoScaling: make(map[int][]mongoSummaryRow),
	}
	if commands, warnings := loadCommandLog(filepath.Join(cfg.RunRoot, "commands.log")); len(commands) > 0 || len(warnings) > 0 {
		data.Commands = commands
		data.Warnings = append(data.Warnings, warnings...)
	}
	if rows, warnings := loadRawEngine(filepath.Join(cfg.RunRoot, "raw_engine_full_matrix")); len(rows) > 0 || len(warnings) > 0 {
		data.RawEngine = rows
		data.Warnings = append(data.Warnings, warnings...)
	}
	if rows, comps, warnings := loadCollections(filepath.Join(cfg.RunRoot, "collections_sqlite_canonical_1m")); len(rows) > 0 || len(comps) > 0 || len(warnings) > 0 {
		data.Collections = rows
		data.CollectionComparisons = comps
		data.Warnings = append(data.Warnings, warnings...)
	}
	if rows, err := readMongoSummary(filepath.Join(cfg.RunRoot, "mongo_gateway_full_sweep_1m_expanded", "summary.tsv")); err == nil {
		data.MongoFullSweep = rows
	} else if !errors.Is(err, os.ErrNotExist) {
		data.Warnings = append(data.Warnings, err.Error())
	}
	if rows, warnings, err := loadMongoLoadModes(filepath.Join(cfg.RunRoot, "mongo_client_mode_load_matrix_1m")); err == nil {
		data.MongoLoadModes = rows
		data.Warnings = append(data.Warnings, warnings...)
	} else if !errors.Is(err, os.ErrNotExist) {
		data.Warnings = append(data.Warnings, err.Error())
	}
	if rows, warnings := loadMongoLoadScaling(filepath.Join(cfg.RunRoot, "mongo_gateway_load_scaling_1m")); len(rows) > 0 || len(warnings) > 0 {
		data.MongoLoadScaling = rows
		data.Warnings = append(data.Warnings, warnings...)
	}
	if scaling, warnings := loadMongoScaling(filepath.Join(cfg.RunRoot, "mongo_gateway_reader_writer_scaling_1m")); len(scaling) > 0 || len(warnings) > 0 {
		data.MongoScaling = scaling
		data.Warnings = append(data.Warnings, warnings...)
	}
	profiles, warnings := loadProfiles(cfg.RunRoot)
	data.Profiles = profiles
	data.Warnings = append(data.Warnings, warnings...)
	data.ArtifactSections = summarizeArtifactSections(cfg.RunRoot, data)
	return data, nil
}

func loadGitIdentity(runRoot string) []gitIdentity {
	path := filepath.Join(runRoot, "HEAD.txt")
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []gitIdentity
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		out = append(out, gitIdentity{Label: key, Hash: value})
	}
	return out
}

func loadRunMetadata(path string) runMetadata {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	meta := make(runMetadata)
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "- ") {
			continue
		}
		key, value, ok := strings.Cut(strings.TrimSpace(strings.TrimPrefix(line, "- ")), ":")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" {
			meta[key] = value
		}
	}
	return meta
}

func (m runMetadata) boolValue(key string) bool {
	if m == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(m[key])) {
	case "1", "true", "yes":
		return true
	default:
		return false
	}
}

func loadCommandLog(path string) ([]commandLogEntry, []string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, []string{fmt.Sprintf("commands log: %v", err)}
	}
	var entries []commandLogEntry
	var warnings []string
	var current *commandLogEntry
	flush := func(final bool) {
		if current == nil {
			return
		}
		if !current.Complete {
			if !final {
				warnings = append(warnings, "commands log command without exit status: "+current.Command)
			}
			current = nil
			return
		}
		entries = append(entries, *current)
		current = nil
	}
	for lineIdx, line := range strings.Split(string(raw), "\n") {
		lineNo := lineIdx + 1
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		switch {
		case strings.HasPrefix(line, "command:"):
			flush(false)
			current = &commandLogEntry{Command: strings.TrimSpace(strings.TrimPrefix(line, "command:"))}
		case strings.HasPrefix(line, "exit_status:"):
			if current == nil {
				warnings = append(warnings, fmt.Sprintf("commands log line %d has exit status without command", lineNo))
				continue
			}
			status, duration, err := parseCommandExitLine(line)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("commands log line %d: %v", lineNo, err))
				continue
			}
			current.ExitStatus = status
			current.DurationSec = duration
			current.Complete = true
		case strings.HasPrefix(line, "warning:"):
			warning := strings.TrimSpace(strings.TrimPrefix(line, "warning:"))
			if current == nil {
				warnings = append(warnings, "commands log warning without command: "+warning)
				continue
			}
			current.Warning = warning
		default:
			warnings = append(warnings, fmt.Sprintf("commands log line %d is unrecognized: %s", lineNo, line))
		}
	}
	flush(true)
	return entries, warnings
}

type artifactSectionExpectation struct {
	Name      string
	Artifact  string
	SkipKeys  []string
	Optional  bool
	HasData   func(reportData) bool
	Exists    func(string) bool
	SkipNote  string
	Missing   string
	Partial   string
	Available string
}

func summarizeArtifactSections(runRoot string, data reportData) []artifactSectionStatus {
	if len(data.RunMetadata) == 0 {
		return nil
	}
	expectations := []artifactSectionExpectation{
		{
			Name:      "Raw TreeDB engine",
			Artifact:  "raw_engine_full_matrix/",
			SkipKeys:  []string{"skip_raw"},
			HasData:   func(data reportData) bool { return len(data.RawEngine) > 0 },
			Available: "raw engine rows loaded",
		},
		{
			Name:      "Collections vs SQLite",
			Artifact:  "collections_sqlite_canonical_1m/",
			SkipKeys:  []string{"skip_collections"},
			HasData:   func(data reportData) bool { return len(data.Collections) > 0 || len(data.CollectionComparisons) > 0 },
			Available: "collection rows or comparisons loaded",
		},
		{
			Name:      "Mongo API full sweep",
			Artifact:  "mongo_gateway_full_sweep_1m_expanded/summary.tsv",
			SkipKeys:  []string{"skip_mongo"},
			HasData:   func(data reportData) bool { return len(data.MongoFullSweep) > 0 },
			Available: "full-sweep summary rows loaded",
		},
		{
			Name:      "Mongo client-mode load matrix",
			Artifact:  "mongo_client_mode_load_matrix_1m/matrix.tsv",
			SkipKeys:  []string{"skip_mongo", "skip_load_modes"},
			HasData:   func(data reportData) bool { return len(data.MongoLoadModes) > 0 },
			Available: "client-mode load rows loaded",
		},
		{
			Name:      "Mongo InsertMany producer scaling",
			Artifact:  "mongo_gateway_load_scaling_1m/summary.tsv",
			SkipKeys:  []string{"skip_mongo", "skip_load_scaling"},
			HasData:   func(data reportData) bool { return len(data.MongoLoadScaling) > 0 },
			Available: "producer-scaling rows loaded",
		},
		{
			Name:      "Mongo reader/writer scaling",
			Artifact:  "mongo_gateway_reader_writer_scaling_1m/",
			SkipKeys:  []string{"skip_mongo", "skip_scaling"},
			HasData:   func(data reportData) bool { return len(data.MongoScaling) > 0 },
			Available: "reader/writer scaling rows loaded",
		},
		{
			Name:      "Profiling artifacts",
			Artifact:  "profile manifests under raw, collection, and Mongo artifacts",
			Optional:  true,
			HasData:   func(data reportData) bool { return hasProfileArtifacts(data.Profiles) },
			Exists:    func(runRoot string) bool { return hasProfileArtifactDirs(runRoot) },
			Missing:   "optional profile manifests are absent",
			Partial:   "profile directories exist but no manifests were loaded",
			Available: "profile manifests loaded",
		},
	}

	out := make([]artifactSectionStatus, 0, len(expectations))
	for _, expectation := range expectations {
		required := !expectation.Optional
		if sectionSkipped(data.RunMetadata, expectation.SkipKeys) {
			detail := expectation.SkipNote
			if detail == "" {
				detail = "skip flag set in RUNBOOK.md"
			}
			out = append(out, artifactSectionStatus{
				Name:     expectation.Name,
				Artifact: expectation.Artifact,
				Status:   "skipped",
				Required: false,
				Detail:   detail,
			})
			continue
		}
		exists := artifactExists(runRoot, expectation.Artifact)
		if !exists && !strings.HasSuffix(expectation.Artifact, "/") {
			exists = artifactContainerExists(runRoot, expectation.Artifact)
		}
		if expectation.Exists != nil {
			exists = expectation.Exists(runRoot)
		}
		if expectation.HasData(data) {
			detail := expectation.Available
			if detail == "" {
				detail = "artifact rows loaded"
			}
			out = append(out, artifactSectionStatus{
				Name:     expectation.Name,
				Artifact: expectation.Artifact,
				Status:   "present",
				Required: required,
				Detail:   detail,
			})
			continue
		}
		status := "missing required"
		detail := expectation.Missing
		if expectation.Optional {
			status = "missing optional"
			if detail == "" {
				detail = "optional artifact is absent"
			}
		} else if detail == "" {
			detail = "expected artifact is absent"
		}
		if exists {
			status = "partial"
			detail = expectation.Partial
			if detail == "" {
				detail = "artifact exists but no reportable rows were loaded"
			}
		}
		out = append(out, artifactSectionStatus{
			Name:     expectation.Name,
			Artifact: expectation.Artifact,
			Status:   status,
			Required: required,
			Detail:   detail,
		})
	}
	return out
}

func sectionSkipped(meta runMetadata, keys []string) bool {
	for _, key := range keys {
		if meta.boolValue(key) {
			return true
		}
	}
	return false
}

func artifactExists(runRoot, rel string) bool {
	rel = strings.TrimSuffix(rel, "/")
	_, err := os.Stat(filepath.Join(runRoot, filepath.FromSlash(rel)))
	return err == nil
}

func artifactContainerExists(runRoot, rel string) bool {
	dir := filepath.Dir(filepath.FromSlash(rel))
	if dir == "." || dir == string(os.PathSeparator) {
		return false
	}
	info, err := os.Stat(filepath.Join(runRoot, dir))
	return err == nil && info.IsDir()
}

func hasProfileArtifacts(profiles profileReportData) bool {
	return len(profiles.Benchprof) > 0 || len(profiles.Collections) > 0 || len(profiles.Mongo) > 0
}

func hasProfileArtifactDirs(runRoot string) bool {
	for _, rel := range []string{
		"raw_engine_full_matrix",
		"collections_sqlite_canonical_1m",
		"mongo_gateway_full_sweep_1m_expanded/profiles",
		"mongo_client_mode_load_matrix_1m/profiles",
	} {
		found := false
		root := filepath.Join(runRoot, filepath.FromSlash(rel))
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil || !entry.IsDir() {
				return nil
			}
			if entry.Name() == "profiles" {
				found = true
				return filepath.SkipDir
			}
			return nil
		})
		if found {
			return true
		}
	}
	return false
}

func parseCommandExitLine(line string) (int, int, error) {
	fields := strings.Fields(line)
	if len(fields) < 4 || fields[0] != "exit_status:" || fields[2] != "duration_sec:" {
		return 0, 0, fmt.Errorf("malformed exit line %q", line)
	}
	status, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0, 0, fmt.Errorf("parse exit status %q: %w", fields[1], err)
	}
	duration, err := strconv.Atoi(fields[3])
	if err != nil {
		return 0, 0, fmt.Errorf("parse duration_sec %q: %w", fields[3], err)
	}
	return status, duration, nil
}

func loadRawEngine(dir string) ([]rawEngineRun, []string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, []string{fmt.Sprintf("raw engine: %v", err)}
	}
	var out []rawEngineRun
	var warnings []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		runDir := filepath.Join(dir, entry.Name())
		path := filepath.Join(runDir, "benchprof_results.json")
		raw, err := os.ReadFile(path)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		var parsed struct {
			Runs []struct {
				Profile string                        `json:"profile"`
				Results map[string]map[string]float64 `json:"results"`
			} `json:"runs"`
		}
		if err := json.Unmarshal(raw, &parsed); err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		if len(parsed.Runs) == 0 {
			warnings = append(warnings, fmt.Sprintf("%s: no runs", path))
			continue
		}
		result := make(map[string]float64)
		for test, engines := range parsed.Runs[0].Results {
			if v, ok := engines["TreeDB"]; ok {
				result[test] = v
			}
		}
		indexDB, leafVLog := rawDiskFromLog(filepath.Join(runDir, "unified-bench.log"))
		out = append(out, rawEngineRun{
			Name:       entry.Name(),
			Profile:    parsed.Runs[0].Profile,
			Checkpoint: checkpointLabel(entry.Name()),
			Results:    result,
			IndexDB:    indexDB,
			LeafVLog:   leafVLog,
		})
	}
	sort.Slice(out, func(i, j int) bool { return rawEngineOrder(out[i]) < rawEngineOrder(out[j]) })
	return out, warnings
}

func rawDiskFromLog(path string) (string, string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", ""
	}
	text := string(raw)
	return afterPrefix(text, "maindb/index.db: "), afterPrefix(text, "maindb/leaf_vlog: total=")
}

func afterPrefix(text, prefix string) string {
	idx := strings.LastIndex(text, prefix)
	if idx < 0 {
		return ""
	}
	rest := text[idx+len(prefix):]
	if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
		rest = rest[:nl]
	}
	return strings.TrimSpace(rest)
}

func checkpointLabel(name string) string {
	if strings.Contains(name, "no_checkpoint") {
		return "no checkpoint between tests"
	}
	if strings.Contains(name, "checkpoint_between") {
		return "checkpoint between tests"
	}
	return ""
}

func rawEngineOrder(run rawEngineRun) string {
	profileOrder := "2"
	if run.Profile == "wal_on_fast" {
		profileOrder = "0"
	} else if run.Profile == "fast" {
		profileOrder = "1"
	}
	checkpointOrder := "1"
	if strings.Contains(run.Checkpoint, "between") && !strings.Contains(run.Checkpoint, "no checkpoint") {
		checkpointOrder = "0"
	}
	return profileOrder + checkpointOrder + run.Name
}

func loadCollections(dir string) ([]collectionRow, []collectionComparison, []string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil, nil
		}
		return nil, nil, []string{fmt.Sprintf("collections: %v", err)}
	}
	var rows []collectionRow
	var comps []collectionComparison
	var warnings []string
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "indexes_") {
			continue
		}
		dirIndex := collectionIndexFromDirectory(entry.Name())
		path := filepath.Join(dir, entry.Name(), "benchmark_results.json")
		raw, err := os.ReadFile(path)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		var parsed struct {
			Results         []collectionRow            `json:"results"`
			Comparisons     []collectionComparison     `json:"comparisons"`
			GuardrailChecks []collectionGuardrailCheck `json:"guardrail_checks"`
			Checks          []collectionGuardrailCheck `json:"checks"`
		}
		if err := json.Unmarshal(raw, &parsed); err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		parsed.Checks = append(parsed.GuardrailChecks, parsed.Checks...)
		var compactWarnings []string
		parsed.Comparisons, compactWarnings = filterCollectionComparisonsForCompactedEvidence(path, dirIndex, parsed.Results, parsed.Comparisons, parsed.Checks)
		warnings = append(warnings, compactWarnings...)
		for i := range parsed.Results {
			parsed.Results[i].Source = filepath.ToSlash(filepath.Join(entry.Name(), "benchmark_results.json"))
		}
		for i := range parsed.Comparisons {
			parsed.Comparisons[i].Source = filepath.ToSlash(filepath.Join(entry.Name(), "benchmark_results.json"))
		}
		rows = append(rows, parsed.Results...)
		comps = append(comps, parsed.Comparisons...)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].IndexCount != rows[j].IndexCount {
			return rows[i].IndexCount < rows[j].IndexCount
		}
		if rows[i].ConfigName != rows[j].ConfigName {
			return rows[i].ConfigName < rows[j].ConfigName
		}
		return rows[i].Phase < rows[j].Phase
	})
	sort.Slice(comps, func(i, j int) bool {
		if comps[i].TreeDBConfig != comps[j].TreeDBConfig {
			return comps[i].TreeDBConfig < comps[j].TreeDBConfig
		}
		return comps[i].Name < comps[j].Name
	})
	return rows, comps, warnings
}

func filterCollectionComparisonsForCompactedEvidence(path string, dirIndex int, rows []collectionRow, comps []collectionComparison, checks []collectionGuardrailCheck) ([]collectionComparison, []string) {
	if len(comps) == 0 {
		return comps, nil
	}
	if collectionChecksBlockCompactedClaims(checks) {
		return nil, []string{fmt.Sprintf("%s: exhaustive_compact did not complete; suppressing TreeDB compacted-size comparisons from this source", path)}
	}

	filtered := make([]collectionComparison, 0, len(comps))
	suppressed := 0
	for _, comp := range comps {
		if collectionComparisonHasCompactedProductionEvidence(rows, comp, dirIndex) {
			filtered = append(filtered, comp)
			continue
		}
		suppressed++
	}
	if suppressed == 0 {
		return filtered, nil
	}
	return filtered, []string{fmt.Sprintf("%s: no positive exhaustive_compact row and matching production evidence for %d TreeDB comparison(s) at the compared config/index; suppressing unsupported TreeDB compacted-size comparisons from this source", path, suppressed)}
}

func collectionChecksBlockCompactedClaims(checks []collectionGuardrailCheck) bool {
	for _, check := range checks {
		if check.Code == "phase.exhaustive_compact.failed" {
			return true
		}
	}
	return false
}

func collectionComparisonHasCompactedProductionEvidence(rows []collectionRow, comp collectionComparison, dirIndex int) bool {
	compIndex := collectionIndexFromConfig(comp.TreeDBConfig)
	hasExhaustive := false
	hasProduction := false
	for _, row := range rows {
		if row.Phase == "exhaustive_compact" &&
			row.Shape == "collection" &&
			strings.HasPrefix(row.ConfigName, "treedb_") &&
			row.ConfigName == comp.TreeDBConfig &&
			row.BytesPerDoc > 0 &&
			(compIndex < 0 || row.IndexCount == compIndex) &&
			(dirIndex < 0 || row.IndexCount == dirIndex) {
			hasExhaustive = true
		}
		if row.Phase == "post_insert" &&
			row.Shape == "collection" &&
			row.ConfigName == comp.TreeDBConfig &&
			(compIndex < 0 || row.IndexCount == compIndex) &&
			(dirIndex < 0 || row.IndexCount == dirIndex) &&
			collectionRowHasProductionEvidence(row) {
			hasProduction = true
		}
	}
	return hasExhaustive && hasProduction
}

func collectionRowHasProductionEvidence(row collectionRow) bool {
	ev := row.ProductionEvidence
	if ev == nil {
		return false
	}
	return strings.TrimSpace(ev.StoragePolicy) != "" &&
		ev.GOMAXPROCS != nil &&
		ev.FlushAdmissionEffectiveConcurrency != nil &&
		ev.FlushAdmissionAdmitted != nil &&
		ev.FlushAdmissionSpanNative != nil &&
		ev.FlushAdmissionBacklogCoalescing != nil &&
		ev.FlushSpanFallbacks != nil &&
		ev.OrderedRootSpanFallbacks != nil &&
		collectionRowHasProducerPathEvidence(row)
}

func collectionRowHasProducerPathEvidence(row collectionRow) bool {
	ev := row.ProductionEvidence
	if ev == nil {
		return false
	}
	if strings.TrimSpace(ev.ProducerRoute) != "" &&
		collectionFloatPtrPositive(ev.ProducerRouteCandidateOps) &&
		collectionFloatPtrPositive(ev.ProducerRouteUsedOps) {
		return true
	}
	return row.IndexCount == 0 &&
		collectionFloatPtrPositive(ev.FlushSpanCandidateOps) &&
		collectionFloatPtrPositive(ev.FlushSpanUsedOps)
}

func collectionFloatPtrPositive(v *float64) bool {
	return v != nil && *v > 0
}

func collectionIndexFromDirectory(name string) int {
	if !strings.HasPrefix(name, "indexes_") {
		return -1
	}
	v, err := strconv.Atoi(strings.TrimPrefix(name, "indexes_"))
	if err != nil {
		return -1
	}
	return v
}

func (r *collectionRow) UnmarshalJSON(raw []byte) error {
	type alias struct {
		ConfigName         string                        `json:"config_name"`
		Engine             string                        `json:"engine"`
		Format             string                        `json:"format"`
		Shape              string                        `json:"shape"`
		IndexCount         int                           `json:"index_count"`
		DocumentCount      int                           `json:"document_count"`
		BenchmarkName      string                        `json:"benchmark_name"`
		Phase              string                        `json:"phase"`
		MaintenanceMode    string                        `json:"maintenance_mode"`
		TotalBytes         float64                       `json:"total_bytes"`
		BytesPerDoc        float64                       `json:"bytes_per_doc"`
		DocsPerSec         float64                       `json:"docs_per_sec"`
		NsPerDoc           float64                       `json:"ns_per_doc"`
		BatchSize          int                           `json:"batch_size"`
		BenchmarkTimed     bool                          `json:"benchmark_timed"`
		MeasurementKind    string                        `json:"measurement_kind"`
		MeasurementNote    string                        `json:"measurement_note"`
		SourceArtifact     string                        `json:"source_artifact"`
		MaintenanceStats   map[string]float64            `json:"maintenance_stats"`
		Extra              map[string]string             `json:"extra"`
		ProductionEvidence *collectionProductionEvidence `json:"production_evidence"`
	}
	var a alias
	if err := json.Unmarshal(raw, &a); err != nil {
		return err
	}
	r.ConfigName = a.ConfigName
	r.Engine = a.Engine
	r.Format = a.Format
	r.Shape = a.Shape
	r.IndexCount = a.IndexCount
	r.DocumentCount = a.DocumentCount
	r.BenchmarkName = a.BenchmarkName
	r.Phase = a.Phase
	r.MaintenanceMode = a.MaintenanceMode
	r.TotalBytes = a.TotalBytes
	r.BytesPerDoc = a.BytesPerDoc
	r.DocsPerSec = a.DocsPerSec
	r.NsPerDoc = a.NsPerDoc
	r.BatchSize = a.BatchSize
	r.BenchmarkTimed = a.BenchmarkTimed
	r.MeasurementKind = a.MeasurementKind
	r.MeasurementNote = a.MeasurementNote
	r.SourceArtifact = a.SourceArtifact
	r.MaintenanceStats = a.MaintenanceStats
	r.Extra = a.Extra
	r.ProductionEvidence = a.ProductionEvidence
	return nil
}

func (r *collectionComparison) UnmarshalJSON(raw []byte) error {
	type alias struct {
		Name              string  `json:"comparison_name"`
		TreeDBConfig      string  `json:"treedb_config_name"`
		TreeDBPhase       string  `json:"treedb_phase"`
		SQLiteConfig      string  `json:"sqlite_config_name"`
		SQLitePhase       string  `json:"sqlite_phase"`
		TreeDBBytesPerDoc float64 `json:"treedb_bytes_per_doc"`
		SQLiteBytesPerDoc float64 `json:"sqlite_bytes_per_doc"`
		SmallerRatio      float64 `json:"smaller_ratio"`
		ComparisonBasis   string  `json:"comparison_basis"`
	}
	var a alias
	if err := json.Unmarshal(raw, &a); err != nil {
		return err
	}
	r.Name = a.Name
	r.TreeDBConfig = a.TreeDBConfig
	r.TreeDBPhase = a.TreeDBPhase
	r.SQLiteConfig = a.SQLiteConfig
	r.SQLitePhase = a.SQLitePhase
	r.TreeDBBytesPerDoc = a.TreeDBBytesPerDoc
	r.SQLiteBytesPerDoc = a.SQLiteBytesPerDoc
	r.SmallerRatio = a.SmallerRatio
	r.ComparisonBasis = a.ComparisonBasis
	return nil
}

func readMongoSummary(path string) ([]mongoSummaryRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s: empty summary", path)
	}
	header, err := tsvHeader(path, records[0], []string{
		"documents",
		"secondary_indexes",
		"range_index",
		"range_mode",
		"treedb_config",
		"mongo_config",
		"phase",
		"treedb_ops_sec",
		"treedb_sampled_ops_sec",
		"treedb_sampled_ns_per_op",
		"mongo_ops_sec",
		"mongo_sampled_ops_sec",
		"mongo_sampled_ns_per_op",
		"treedb_to_mongo_ops_ratio",
		"treedb_to_mongo_sampled_ops_ratio",
		"treedb_p50_us",
		"mongo_p50_us",
		"treedb_p95_us",
		"mongo_p95_us",
		"treedb_p99_us",
		"mongo_p99_us",
		"treedb_disk_snapshot",
		"treedb_disk_bytes",
		"treedb_physical_bytes",
		"mongo_dbstats_data_size_bytes",
		"mongo_dbstats_total_size_bytes",
		"mongo_physical_bytes",
		"treedb_to_mongo_dbstats_total_ratio",
		"treedb_to_mongo_physical_ratio",
	})
	if err != nil {
		return nil, err
	}
	var rows []mongoSummaryRow
	for rowIdx, rec := range records[1:] {
		line := rowIdx + 2
		var parseErr error
		setErr := func(err error) {
			if err != nil && parseErr == nil {
				parseErr = err
			}
		}
		get := func(name string) string {
			v, err := tsvField(path, header, rec, line, name)
			setErr(err)
			return v
		}
		intField := func(name string) int {
			v, err := parseIntColumn(path, line, name, get(name))
			setErr(err)
			return v
		}
		floatField := func(name string) float64 {
			v, err := parseFloatColumn(path, line, name, get(name))
			setErr(err)
			return v
		}
		optionalIntField := func(name string) int {
			if _, ok := header[normalizeTSVHeader(name)]; !ok {
				return 0
			}
			v, err := tsvField(path, header, rec, line, name)
			setErr(err)
			if v == "" {
				return 0
			}
			parsed, err := parseIntColumn(path, line, name, v)
			setErr(err)
			return parsed
		}
		boolField := func(name string) bool {
			v, err := parseBoolColumn(path, line, name, get(name))
			setErr(err)
			return v
		}
		row := mongoSummaryRow{
			Documents:               intField("documents"),
			SecondaryIndexes:        intField("secondary_indexes"),
			RangeIndex:              boolField("range_index"),
			RangeMode:               get("range_mode"),
			TreeDBConfig:            get("treedb_config"),
			MongoConfig:             get("mongo_config"),
			Phase:                   get("phase"),
			TreeDBOpsSec:            floatField("treedb_ops_sec"),
			TreeDBSampledOpsSec:     floatField("treedb_sampled_ops_sec"),
			TreeDBSampledNsPerOp:    floatField("treedb_sampled_ns_per_op"),
			MongoOpsSec:             floatField("mongo_ops_sec"),
			MongoSampledOpsSec:      floatField("mongo_sampled_ops_sec"),
			MongoSampledNsPerOp:     floatField("mongo_sampled_ns_per_op"),
			TreeDBToMongoRatio:      floatField("treedb_to_mongo_ops_ratio"),
			TreeDBToMongoSampled:    floatField("treedb_to_mongo_sampled_ops_ratio"),
			TreeDBP50US:             floatField("treedb_p50_us"),
			TreeDBP95US:             floatField("treedb_p95_us"),
			TreeDBP99US:             floatField("treedb_p99_us"),
			MongoP50US:              floatField("mongo_p50_us"),
			MongoP95US:              floatField("mongo_p95_us"),
			MongoP99US:              floatField("mongo_p99_us"),
			TreeDBDiskSnapshot:      get("treedb_disk_snapshot"),
			TreeDBDiskBytes:         floatField("treedb_disk_bytes"),
			TreeDBPhysicalBytes:     floatField("treedb_physical_bytes"),
			MongoDBStatsDataSize:    floatField("mongo_dbstats_data_size_bytes"),
			MongoDBStatsTotalSize:   floatField("mongo_dbstats_total_size_bytes"),
			MongoPhysicalBytes:      floatField("mongo_physical_bytes"),
			TreeDBToMongoTotalRatio: floatField("treedb_to_mongo_dbstats_total_ratio"),
			TreeDBToMongoPhysRatio:  floatField("treedb_to_mongo_physical_ratio"),
			BatchSize:               optionalIntField("batch_size"),
			InsertProducers:         optionalIntField("insert_producers"),
			EffectiveProducers:      optionalIntField("effective_producers"),
			DriverCalls:             optionalIntField("driver_calls"),
			LoadBatchCount:          optionalIntField("load_batch_count"),
			Source:                  filepath.Base(filepath.Dir(path)),
		}
		if parseErr != nil {
			return nil, parseErr
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func loadMongoLoadModes(dir string) ([]loadModeRow, []string, error) {
	matrixPath := filepath.Join(dir, "matrix.tsv")
	rows, err := readMatrix(matrixPath)
	if err != nil {
		return nil, nil, err
	}
	var out []loadModeRow
	var warnings []string
	for _, row := range rows {
		rawPath, err := resolveRunArtifactPath(dir, row.RawJSON)
		if err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		result, err := readBenchmarkResult(rawPath)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", rawPath, err))
			continue
		}
		phase := findPhase(result.Phases, "load_insert_many")
		if phase.Name == "" {
			continue
		}
		out = append(out, loadModeRow{
			Indexes:       row.SecondaryIndexes,
			Target:        row.Target,
			Config:        row.Config,
			OpsPerSec:     phase.OpsPerSecond,
			PhysicalBytes: row.PhysicalBytes,
			RawJSON:       row.RawJSON,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Indexes != out[j].Indexes {
			return out[i].Indexes < out[j].Indexes
		}
		if out[i].Target != out[j].Target {
			return out[i].Target < out[j].Target
		}
		return out[i].Config < out[j].Config
	})
	return out, warnings, nil
}

func loadMongoLoadScaling(dir string) ([]mongoSummaryRow, []string) {
	path := filepath.Join(dir, "summary.tsv")
	rows, err := readMongoSummary(path)
	if err == nil {
		return rows, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		if _, statErr := os.Stat(dir); statErr == nil {
			return nil, []string{fmt.Sprintf("mongo load scaling: %s is missing summary.tsv", filepath.Base(dir))}
		}
		return nil, nil
	}
	return nil, []string{err.Error()}
}

func resolveRunArtifactPath(baseDir, relPath string) (string, error) {
	if strings.TrimSpace(relPath) == "" {
		return "", fmt.Errorf("empty artifact path under %s", baseDir)
	}
	if filepath.IsAbs(relPath) {
		return "", fmt.Errorf("%s: artifact path must be relative to %s", relPath, baseDir)
	}
	baseAbs, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}
	pathAbs := filepath.Join(baseAbs, filepath.Clean(relPath))
	relative, err := filepath.Rel(baseAbs, pathAbs)
	if err != nil {
		return "", err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("%s: artifact path escapes %s", relPath, baseDir)
	}
	return pathAbs, nil
}

func loadMongoScaling(dir string) (map[int][]mongoSummaryRow, []string) {
	out := make(map[int][]mongoSummaryRow)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, nil
		}
		return out, []string{fmt.Sprintf("mongo scaling: %v", err)}
	}
	var warnings []string
	for _, entry := range entries {
		idx, ok := indexDirCount(entry.Name())
		if !entry.IsDir() || !ok {
			continue
		}
		path := filepath.Join(dir, entry.Name(), "summary.tsv")
		rows, err := readMongoSummary(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				warnings = append(warnings, fmt.Sprintf("mongo scaling: %s is missing summary.tsv", filepath.Join(entry.Name(), "summary.tsv")))
			} else {
				warnings = append(warnings, err.Error())
			}
			continue
		}
		out[idx] = rows
	}
	return out, warnings
}

func indexDirCount(name string) (int, bool) {
	value, ok := strings.CutPrefix(name, "indexes_")
	if !ok || value == "" {
		return 0, false
	}
	idx, err := strconv.Atoi(value)
	if err != nil || idx < 0 {
		return 0, false
	}
	return idx, true
}

func loadProfiles(runRoot string) (profileReportData, []string) {
	var out profileReportData
	var warnings []string

	for _, path := range walkProfileFiles(filepath.Join(runRoot, "raw_engine_full_matrix"), "insights.json", &warnings) {
		item, err := readBenchprofInsightSummary(runRoot, path)
		if err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		out.Benchprof = append(out.Benchprof, item)
	}
	for _, path := range walkProfileFiles(filepath.Join(runRoot, "collections_sqlite_canonical_1m"), "collection_profile_manifest.json", &warnings) {
		item, err := readCollectionProfileSummary(runRoot, path)
		if err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		out.Collections = append(out.Collections, item)
	}
	for _, root := range []string{
		filepath.Join(runRoot, "mongo_gateway_full_sweep_1m_expanded", "profiles"),
		filepath.Join(runRoot, "mongo_client_mode_load_matrix_1m", "profiles"),
	} {
		for _, path := range walkProfileFiles(root, "profile_manifest.json", &warnings) {
			item, err := readMongoProfileSummary(runRoot, path)
			if err != nil {
				warnings = append(warnings, err.Error())
				continue
			}
			out.Mongo = append(out.Mongo, item)
		}
	}

	sort.Slice(out.Benchprof, func(i, j int) bool { return out.Benchprof[i].Source < out.Benchprof[j].Source })
	sort.Slice(out.Collections, func(i, j int) bool { return out.Collections[i].Source < out.Collections[j].Source })
	sort.Slice(out.Mongo, func(i, j int) bool { return out.Mongo[i].Source < out.Mongo[j].Source })
	return out, warnings
}

func walkProfileFiles(root, filename string, warnings *[]string) []string {
	info, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		*warnings = append(*warnings, fmt.Sprintf("profiles: %s: %v", root, err))
		return nil
	}
	if !info.IsDir() {
		return nil
	}
	var paths []string
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			*warnings = append(*warnings, fmt.Sprintf("profiles: %s: %v", path, err))
			return nil
		}
		if !entry.IsDir() && entry.Name() == filename {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		*warnings = append(*warnings, fmt.Sprintf("profiles: %s: %v", root, err))
	}
	sort.Strings(paths)
	return paths
}

func readBenchprofInsightSummary(runRoot, path string) (benchprofInsightSummary, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return benchprofInsightSummary{}, err
	}
	var parsed struct {
		ProfilesDir          string                       `json:"profiles_dir"`
		OpsSource            string                       `json:"ops_source"`
		Insights             []string                     `json:"insights"`
		Warnings             []string                     `json:"warnings"`
		InvestigationTargets []profileInvestigationTarget `json:"investigation_targets"`
		CPUProfiles          []json.RawMessage            `json:"cpu_profiles"`
		AllocSpace           []json.RawMessage            `json:"alloc_space_profiles"`
		AllocObjects         []json.RawMessage            `json:"alloc_object_profiles"`
		BlockProfiles        []json.RawMessage            `json:"block_profiles"`
		MutexProfiles        []json.RawMessage            `json:"mutex_profiles"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return benchprofInsightSummary{}, fmt.Errorf("%s: %w", path, err)
	}
	return benchprofInsightSummary{
		Source:        relArtifact(runRoot, path),
		ProfilesDir:   parsed.ProfilesDir,
		OpsSource:     parsed.OpsSource,
		Insights:      parsed.Insights,
		Warnings:      parsed.Warnings,
		Targets:       parsed.InvestigationTargets,
		CPUProfiles:   len(parsed.CPUProfiles),
		AllocProfiles: len(parsed.AllocSpace) + len(parsed.AllocObjects),
		BlockProfiles: len(parsed.BlockProfiles),
		MutexProfiles: len(parsed.MutexProfiles),
	}, nil
}

func readCollectionProfileSummary(runRoot, path string) (collectionProfileSummary, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return collectionProfileSummary{}, err
	}
	var manifest struct {
		ProfileDir       string  `json:"profile_dir"`
		Cell             string  `json:"cell"`
		ExecutionPath    string  `json:"execution_path"`
		Engine           string  `json:"engine"`
		DocumentFormat   string  `json:"document_format"`
		StoragePolicy    string  `json:"storage_policy"`
		BenchmarkPattern string  `json:"benchmark_pattern"`
		Benchtime        string  `json:"benchtime"`
		Count            int     `json:"count"`
		DurationMillis   float64 `json:"duration_ms"`
		RunError         string  `json:"run_error"`
		Artifacts        []struct {
			Phase         string `json:"phase"`
			CPUProfile    string `json:"cpu_profile"`
			AllocsProfile string `json:"allocs_profile"`
			BlockProfile  string `json:"block_profile"`
			MutexProfile  string `json:"mutex_profile"`
			Output        string `json:"output"`
			Error         string `json:"error"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return collectionProfileSummary{}, fmt.Errorf("%s: %w", path, err)
	}
	item := collectionProfileSummary{
		Source:           relArtifact(runRoot, path),
		ProfileDir:       manifest.ProfileDir,
		Cell:             manifest.Cell,
		ExecutionPath:    manifest.ExecutionPath,
		Engine:           manifest.Engine,
		DocumentFormat:   manifest.DocumentFormat,
		StoragePolicy:    manifest.StoragePolicy,
		BenchmarkPattern: manifest.BenchmarkPattern,
		Benchtime:        manifest.Benchtime,
		Count:            manifest.Count,
		DurationMillis:   manifest.DurationMillis,
		RunError:         manifest.RunError,
	}
	for _, artifact := range manifest.Artifacts {
		item.Artifacts = append(item.Artifacts, collectionProfileArtifact{
			Phase:         artifact.Phase,
			CPUProfile:    artifact.CPUProfile,
			AllocsProfile: artifact.AllocsProfile,
			BlockProfile:  artifact.BlockProfile,
			MutexProfile:  artifact.MutexProfile,
			Output:        artifact.Output,
			Error:         artifact.Error,
		})
	}
	return item, nil
}

func readMongoProfileSummary(runRoot, path string) (mongoProfileSummary, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return mongoProfileSummary{}, err
	}
	var manifest struct {
		ProfileDir string `json:"profile_dir"`
		RunError   string `json:"run_error"`
		Artifacts  []struct {
			Phase          string  `json:"phase"`
			Prefix         string  `json:"prefix"`
			DurationMillis float64 `json:"duration_ms"`
			CPUProfile     string  `json:"cpu_profile"`
			AllocsProfile  string  `json:"allocs_profile"`
			BlockProfile   string  `json:"block_profile"`
			MutexProfile   string  `json:"mutex_profile"`
			Trace          string  `json:"trace"`
			Error          string  `json:"error"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return mongoProfileSummary{}, fmt.Errorf("%s: %w", path, err)
	}
	result, resultErr := readBenchmarkResult(filepath.Join(filepath.Dir(path), "benchmark_result.json"))
	phaseByName := make(map[string]phaseResult, len(result.Phases))
	for _, phase := range result.Phases {
		phaseByName[phase.Name] = phase
	}
	item := mongoProfileSummary{
		Source:           relArtifact(runRoot, path),
		ProfileDir:       manifest.ProfileDir,
		Target:           result.Target,
		ClientMode:       result.ClientMode,
		DocumentFormat:   result.DocumentFormat,
		TreeDBProfile:    result.TreeDBProfile,
		Documents:        result.Documents,
		SecondaryIndexes: result.SecondaryIndexes,
		HasResult:        resultErr == nil,
		RunError:         manifest.RunError,
	}
	if resultErr != nil {
		item.ResultError = resultErr.Error()
	}
	for _, artifact := range manifest.Artifacts {
		phase := phaseByName[artifact.Phase]
		item.Artifacts = append(item.Artifacts, mongoProfileArtifact{
			Phase:          artifact.Phase,
			Prefix:         artifact.Prefix,
			DurationMillis: artifact.DurationMillis,
			OpsPerSecond:   phase.OpsPerSecond,
			P95US:          phase.LatencyMicros.P95,
			CPUProfile:     artifact.CPUProfile,
			AllocsProfile:  artifact.AllocsProfile,
			BlockProfile:   artifact.BlockProfile,
			MutexProfile:   artifact.MutexProfile,
			Trace:          artifact.Trace,
			Error:          artifact.Error,
		})
	}
	return item, nil
}

func relArtifact(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

func readMatrix(path string) ([]matrixRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("%s: empty matrix", path)
	}
	header, err := tsvHeader(path, records[0], []string{"target", "config", "documents", "secondary_indexes", "raw_json", "physical_bytes"})
	if err != nil {
		return nil, err
	}
	var rows []matrixRow
	for rowIdx, rec := range records[1:] {
		line := rowIdx + 2
		var parseErr error
		setErr := func(err error) {
			if err != nil && parseErr == nil {
				parseErr = err
			}
		}
		get := func(name string) string {
			v, err := tsvField(path, header, rec, line, name)
			setErr(err)
			return v
		}
		documents, err := parseIntColumn(path, line, "documents", get("documents"))
		setErr(err)
		secondaryIndexes, err := parseIntColumn(path, line, "secondary_indexes", get("secondary_indexes"))
		setErr(err)
		physicalBytes, err := parseInt64Column(path, line, "physical_bytes", get("physical_bytes"))
		setErr(err)
		target := get("target")
		config := get("config")
		rawJSON := get("raw_json")
		if parseErr != nil {
			return nil, parseErr
		}
		rows = append(rows, matrixRow{
			Target:           target,
			Config:           config,
			Documents:        documents,
			SecondaryIndexes: secondaryIndexes,
			RawJSON:          rawJSON,
			PhysicalBytes:    physicalBytes,
		})
	}
	return rows, nil
}

func readBenchmarkResult(path string) (benchmarkResult, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return benchmarkResult{}, err
	}
	var result benchmarkResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return benchmarkResult{}, fmt.Errorf("%s: %w", path, err)
	}
	return result, nil
}

func findPhase(phases []phaseResult, name string) phaseResult {
	for _, phase := range phases {
		if phase.Name == name {
			return phase
		}
	}
	return phaseResult{}
}

func renderHTML(data reportData) string {
	var b strings.Builder
	b.WriteString("<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n")
	b.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n")
	b.WriteString("<title>" + esc(data.Config.Title) + "</title>\n")
	b.WriteString(reportCSS())
	b.WriteString("</head>\n<body>\n")
	b.WriteString("<header><div><p class=\"eyebrow\">TreeDB benchmark evidence</p><h1>" + esc(data.Config.Title) + "</h1>")
	b.WriteString("<p class=\"subtle\">Generated " + esc(data.GeneratedAt.Format(time.RFC3339)) + " from <code>" + esc(data.Config.RunRoot) + "</code>.</p></div>")
	if len(data.Git) > 0 {
		b.WriteString("<p class=\"subtle\">Git identity:")
		for _, item := range data.Git {
			b.WriteString(" <code>" + esc(item.Label) + "=" + esc(item.Hash) + "</code>")
		}
		b.WriteString("</p>")
	}
	b.WriteString(reportNav(data))
	b.WriteString("</header>\n")
	renderRunStatus(&b, data.Commands, data.ArtifactSections)
	if len(data.Warnings) > 0 {
		b.WriteString("<section class=\"warn\"><h2>Warnings</h2><ul>")
		for _, warning := range data.Warnings {
			b.WriteString("<li>" + esc(warning) + "</li>")
		}
		b.WriteString("</ul></section>\n")
	}
	renderMongoFullSweep(&b, data.MongoFullSweep)
	renderMongoLoadScaling(&b, data.MongoLoadScaling)
	renderMongoLoadModes(&b, data.MongoLoadModes)
	renderMongoScaling(&b, data.MongoScaling)
	renderCollections(&b, data.Collections, data.CollectionComparisons)
	renderRawEngine(&b, data.RawEngine)
	renderProfiles(&b, data.Profiles)
	b.WriteString("</body>\n</html>\n")
	return b.String()
}

func reportNav(data reportData) string {
	type navItem struct {
		Href  string
		Label string
	}
	var items []navItem
	if len(data.Commands) > 0 || len(data.ArtifactSections) > 0 {
		items = append(items, navItem{Href: "#run-status", Label: "Run status"})
	}
	if len(data.MongoFullSweep) > 0 {
		items = append(items, navItem{Href: "#mongo-full", Label: "Mongo API sweep"})
	}
	if hasMongoLoadScalingRows(data.MongoLoadScaling) {
		items = append(items, navItem{Href: "#mongo-load-scaling", Label: "Load scaling"})
	}
	if len(data.MongoLoadModes) > 0 {
		items = append(items, navItem{Href: "#mongo-load", Label: "Load modes"})
	}
	if len(data.MongoScaling) > 0 {
		items = append(items, navItem{Href: "#scaling", Label: "Scaling"})
	}
	if len(data.Collections) > 0 || len(data.CollectionComparisons) > 0 {
		items = append(items, navItem{Href: "#collections", Label: "Collections"})
	}
	if len(data.RawEngine) > 0 {
		items = append(items, navItem{Href: "#raw-engine", Label: "Raw engine"})
	}
	if len(data.Profiles.Benchprof) > 0 || len(data.Profiles.Collections) > 0 || len(data.Profiles.Mongo) > 0 {
		items = append(items, navItem{Href: "#profiles", Label: "Profiles"})
	}
	if len(items) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("<nav>")
	for _, item := range items {
		b.WriteString("<a href=\"" + esc(item.Href) + "\">" + esc(item.Label) + "</a>")
	}
	b.WriteString("</nav>")
	return b.String()
}

func renderRunStatus(b *strings.Builder, commands []commandLogEntry, sections []artifactSectionStatus) {
	if len(commands) == 0 && len(sections) == 0 {
		return
	}
	failed := commandFailureCount(commands)
	blockedSections := blockingArtifactSectionCount(sections)
	skippedSections := skippedArtifactSectionCount(sections)
	classAttr := ""
	if failed > 0 {
		classAttr = " class=\"warn\""
	}
	if blockedSections > 0 {
		classAttr = " class=\"warn\""
	}
	status := runStatusText(len(commands), failed, blockedSections, skippedSections)
	b.WriteString("<section id=\"run-status\"" + classAttr + "><h2>Run Status</h2>")
	b.WriteString("<p class=\"subtle\">" + esc(status) + "</p>")
	if len(sections) > 0 {
		var body [][]string
		for _, section := range sections {
			required := "yes"
			if !section.Required {
				required = "no"
			}
			body = append(body, []string{
				section.Name,
				section.Status,
				required,
				section.Artifact,
				section.Detail,
			})
		}
		b.WriteString("<h3>Artifact Sections</h3>")
		writeTable(b, []string{"section", "status", "required", "artifact", "detail"}, body, nil)
	}
	if len(commands) > 0 {
		var body [][]string
		for i, command := range commands {
			body = append(body, []string{
				strconv.Itoa(i + 1),
				strconv.Itoa(command.ExitStatus),
				formatOptionalInt(command.DurationSec),
				emptyDash(command.Warning),
				command.Command,
			})
		}
		b.WriteString("<h3>Recorded Commands</h3>")
		writeTable(b, []string{"#", "exit status", "duration sec", "warning", "command"}, body, numericColumns(0, 1, 2))
	}
	b.WriteString("</section>\n")
}

func runStatusText(commandCount, failedCommands, blockedSections, skippedSections int) string {
	var sentences []string
	if commandCount > 0 {
		if failedCommands > 0 {
			sentences = append(sentences, fmt.Sprintf("Partial run: %d of %d recorded commands exited nonzero.", failedCommands, commandCount))
		} else if blockedSections > 0 || skippedSections > 0 {
			sentences = append(sentences, fmt.Sprintf("Partial run: all %d recorded commands exited 0.", commandCount))
		} else {
			sentences = append(sentences, fmt.Sprintf("Complete run: all %d recorded commands exited 0.", commandCount))
		}
	} else {
		if blockedSections > 0 || skippedSections > 0 {
			sentences = append(sentences, "Partial run: no commands.log was available for command-level status.")
		} else {
			sentences = append(sentences, "Complete run: no commands.log was available, but all expected artifact sections are present.")
		}
	}
	if blockedSections > 0 {
		sentences = append(sentences, fmt.Sprintf("%d expected artifact section(s) are missing or partial.", blockedSections))
	}
	if skippedSections > 0 {
		sentences = append(sentences, fmt.Sprintf("%d artifact section(s) were intentionally skipped.", skippedSections))
	}
	return strings.Join(sentences, " ")
}

func commandFailureCount(commands []commandLogEntry) int {
	var failed int
	for _, command := range commands {
		if command.ExitStatus != 0 {
			failed++
		}
	}
	return failed
}

func blockingArtifactSectionCount(sections []artifactSectionStatus) int {
	var blocked int
	for _, section := range sections {
		if section.Required && (section.Status == "missing required" || section.Status == "partial") {
			blocked++
		}
	}
	return blocked
}

func skippedArtifactSectionCount(sections []artifactSectionStatus) int {
	var skipped int
	for _, section := range sections {
		if section.Status == "skipped" {
			skipped++
		}
	}
	return skipped
}

func reportCSS() string {
	return `<style>
:root { color-scheme: light; --ink:#18212b; --muted:#617083; --line:#d9e1ea; --bg:#f6f8fb; --panel:#fff; --blue:#2867c7; --green:#1f8a5b; --orange:#bd6a21; --red:#b94242; --page-gutter:18px; --content-max:1680px; }
* { box-sizing: border-box; }
body { margin:0; font:14px/1.45 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; color:var(--ink); background:var(--bg); }
header { padding:28px max(var(--page-gutter), calc((100vw - var(--content-max)) / 2)) 20px; background:var(--panel); border-bottom:1px solid var(--line); position:sticky; top:0; z-index:4; }
h1 { margin:2px 0 8px; font-size:28px; letter-spacing:0; }
h2 { margin:0 0 12px; font-size:22px; }
h3 { margin:18px 0 10px; font-size:16px; }
section { width:calc(100% - 36px); max-width:var(--content-max); margin:var(--page-gutter) auto; padding:20px 24px; background:var(--panel); border:1px solid var(--line); border-radius:8px; }
nav { display:flex; flex-wrap:wrap; gap:8px; margin-top:12px; }
nav a { color:var(--blue); text-decoration:none; border:1px solid var(--line); border-radius:999px; padding:5px 10px; background:#fbfdff; }
code { background:#eef3f8; border:1px solid #dbe4ee; border-radius:4px; padding:1px 4px; }
.eyebrow { margin:0; text-transform:uppercase; letter-spacing:.08em; color:var(--muted); font-size:12px; font-weight:700; }
.subtle { color:var(--muted); margin:4px 0; }
.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:14px; }
.metric { border:1px solid var(--line); border-radius:8px; padding:14px; background:#fbfdff; }
.metric .label { color:var(--muted); font-size:12px; }
.metric .value { font-size:24px; font-weight:700; margin-top:4px; }
.summary-strip { display:grid; margin:10px 0 18px; border:1px solid var(--line); border-radius:6px; background:#fff; overflow:hidden; }
.summary-row { display:grid; grid-template-columns:minmax(100px,1.1fr) repeat(5,minmax(112px,1fr)); gap:10px; align-items:center; padding:8px 10px; border-top:1px solid var(--line); background:#fff; }
.summary-row:first-child { border-top:0; }
.summary-workload { color:#344256; font-weight:600; white-space:nowrap; }
.summary-cell { min-width:0; }
.summary-cell .label { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.04em; }
.summary-cell .value { color:var(--ink); font-size:13px; font-weight:600; font-variant-numeric:tabular-nums; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.chart-group { margin-top:24px; padding-top:18px; border-top:1px solid var(--line); }
.chart-group:first-of-type { margin-top:14px; padding-top:0; border-top:0; }
.chart-group h3 { margin:0 0 4px; font-size:18px; }
.chart-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:18px; margin:14px 0 22px; align-items:start; }
.chart { border:1px solid var(--line); border-radius:8px; padding:12px; overflow:hidden; background:#fff; min-width:0; }
.chart-head { display:flex; justify-content:space-between; align-items:flex-start; gap:12px; margin-bottom:8px; }
.chart-title { font-weight:700; margin-bottom:8px; }
.chart-head .chart-title { margin-bottom:0; }
.chart svg { display:block; width:100%; height:auto; max-width:100%; }
.table-wrap { overflow:auto; border:1px solid var(--line); border-radius:8px; margin:10px 0 18px; }
table { border-collapse:separate; border-spacing:0; width:100%; min-width:900px; }
th, td { padding:7px 9px; border-bottom:1px solid var(--line); vertical-align:top; white-space:nowrap; }
th { position:sticky; top:0; z-index:1; background:#f0f4f8; text-align:left; font-size:12px; color:#344256; }
td.num, th.num { text-align:right; font-variant-numeric:tabular-nums; }
tr:hover td { background:#fafcff; }
details { margin:12px 0; }
summary { cursor:pointer; color:var(--blue); font-weight:700; }
.warn { border-color:#e6c36a; background:#fff8e1; }
.legend { display:flex; gap:12px; flex-wrap:wrap; font-size:12px; color:var(--muted); justify-content:flex-end; text-align:right; }
.chart-head .legend { max-width:72%; }
.swatch { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:-1px; }
@media (max-width: 980px) { :root { --page-gutter:10px; } section { width:calc(100% - 20px); } .chart-grid { grid-template-columns:1fr; } .summary-row { grid-template-columns:1fr 1fr; } .summary-workload { grid-column:1 / -1; } header { position:static; } }
</style>
`
}

func renderRawEngine(b *strings.Builder, rows []rawEngineRun) {
	if len(rows) == 0 {
		return
	}
	b.WriteString("<section id=\"raw-engine\"><h2>Raw TreeDB Engine</h2>")
	tests := collectRawTests(rows)
	b.WriteString("<p class=\"subtle\">Full default `unified-bench` matrix by profile/checkpoint mode. Tests are grouped along one shared chart, with the four requested variants shown side by side on a common ops/sec axis.</p>")
	renderRawEngineCharts(b, rows, tests)
	var headers []string
	headers = append(headers, "test")
	for _, row := range rows {
		headers = append(headers, row.Profile+" / "+row.Checkpoint)
	}
	var body [][]string
	for _, test := range tests {
		line := []string{test}
		for _, row := range rows {
			if v, ok := row.Results[test]; ok {
				line = append(line, fmtOps(v))
			} else {
				line = append(line, "-")
			}
		}
		body = append(body, line)
	}
	b.WriteString("<details><summary>All raw engine throughput rows</summary>")
	writeTable(b, headers, body, numericColumns(1, len(headers)-1))
	b.WriteString("</details>")
	var diskRows [][]string
	for _, row := range rows {
		diskRows = append(diskRows, []string{row.Profile, row.Checkpoint, emptyDash(row.IndexDB), emptyDash(row.LeafVLog)})
	}
	b.WriteString("<details><summary>Raw engine disk rows</summary>")
	writeTable(b, []string{"profile", "checkpoint mode", "index.db", "leaf_vlog"}, diskRows, map[int]bool{})
	b.WriteString("</details>")
	b.WriteString("</section>\n")
}

func renderRawEngineCharts(b *strings.Builder, rows []rawEngineRun, tests []string) {
	b.WriteString(rawEngineGroupedBarChart("Raw engine throughput by test", rows, tests, rawEnginePresentVariants(rows, rawEngineVariants())))
}

func renderProfiles(b *strings.Builder, data profileReportData) {
	if len(data.Benchprof) == 0 && len(data.Collections) == 0 && len(data.Mongo) == 0 {
		return
	}
	b.WriteString("<section id=\"profiles\"><h2>Profiling Follow-Up</h2>")
	b.WriteString("<p class=\"subtle\">Profile panels are generated from precomputed `benchprof` insights, Collections/SQLite pprof manifests, and Mongo gateway profile manifests. Report rendering does not rerun pprof; it links phase artifacts back to the benchmark bundle.</p>")
	if len(data.Benchprof) > 0 {
		b.WriteString("<div class=\"chart-group\"><h3>Raw Engine Benchprof Insights</h3>")
		for _, item := range data.Benchprof {
			b.WriteString("<h4>" + esc(item.Source) + "</h4>")
			b.WriteString("<div class=\"summary-strip\"><div class=\"summary-row\">")
			b.WriteString("<div class=\"summary-workload\">profiles</div>")
			writeSummaryCell(b, "CPU", strconv.Itoa(item.CPUProfiles))
			writeSummaryCell(b, "alloc", strconv.Itoa(item.AllocProfiles))
			writeSummaryCell(b, "block", strconv.Itoa(item.BlockProfiles))
			writeSummaryCell(b, "mutex", strconv.Itoa(item.MutexProfiles))
			writeSummaryCell(b, "targets", strconv.Itoa(len(item.Targets)))
			b.WriteString("</div></div>")
			if len(item.Insights) > 0 {
				b.WriteString("<ul>")
				for _, insight := range firstStrings(item.Insights, 5) {
					b.WriteString("<li>" + esc(insight) + "</li>")
				}
				b.WriteString("</ul>")
			}
			if len(item.Targets) > 0 {
				var body [][]string
				for _, target := range firstTargets(item.Targets, 10) {
					ref := target.File
					if target.Line > 0 {
						ref += ":" + strconv.Itoa(target.Line)
					}
					body = append(body, []string{target.Test, target.DBTag, target.Category, target.Function, fmtFloat(target.FlatPct, 2), ref, target.Why})
				}
				writeTable(b, []string{"test", "db", "category", "function", "flat %", "reference", "why"}, body, numericColumns(4))
			}
		}
		b.WriteString("</div>")
	}
	if len(data.Collections) > 0 {
		b.WriteString("<div class=\"chart-group\"><h3>Collections vs SQLite Profile Manifests</h3>")
		b.WriteString("<p class=\"subtle\">These pprof captures are separate from the timed collection benchmark pass, so they are for attribution rather than the canonical throughput number. Artifact filenames are relative to the profile directory.</p>")
		var body [][]string
		for _, item := range data.Collections {
			profileDir := filepath.ToSlash(filepath.Dir(item.Source))
			if len(item.Artifacts) == 0 {
				body = append(body, []string{
					item.Source,
					profileDir,
					emptyDash(item.Cell),
					emptyDash(item.Engine),
					emptyDash(item.DocumentFormat),
					emptyDash(item.Benchtime),
					strconv.Itoa(item.Count),
					fmtFloat(item.DurationMillis, 1),
					"(run)",
					"-",
					"-",
					"-",
					"-",
					"-",
					firstNonEmpty(item.RunError, "no profile artifacts recorded"),
				})
				continue
			}
			for _, artifact := range item.Artifacts {
				body = append(body, []string{
					item.Source,
					profileDir,
					emptyDash(item.Cell),
					emptyDash(item.Engine),
					emptyDash(item.DocumentFormat),
					emptyDash(item.Benchtime),
					strconv.Itoa(item.Count),
					fmtFloat(item.DurationMillis, 1),
					emptyDash(artifact.Phase),
					emptyDash(artifact.CPUProfile),
					emptyDash(artifact.AllocsProfile),
					emptyDash(artifact.BlockProfile),
					emptyDash(artifact.MutexProfile),
					emptyDash(artifact.Output),
					emptyDash(firstNonEmpty(artifact.Error, item.RunError)),
				})
			}
		}
		writeTable(b, []string{"manifest", "profile dir", "cell", "engine", "format", "benchtime", "count", "profile pass ms", "phase", "cpu", "allocs", "block", "mutex", "output", "error"}, body, numericColumns(6, 7))
		b.WriteString("</div>")
	}
	if len(data.Mongo) > 0 {
		b.WriteString("<div class=\"chart-group\"><h3>Mongo Gateway Profile Manifests</h3>")
		var body [][]string
		for _, item := range data.Mongo {
			if len(item.Artifacts) == 0 {
				body = append(body, []string{
					item.Source,
					emptyDash(item.ClientMode),
					emptyDash(item.DocumentFormat),
					profileIndexesLabel(item),
					"(run)",
					"-",
					"-",
					"-",
					"-",
					"-",
					"-",
					"-",
					firstNonEmpty(item.RunError, item.ResultError, "no profile artifacts recorded"),
				})
				continue
			}
			for _, artifact := range item.Artifacts {
				body = append(body, []string{
					item.Source,
					emptyDash(item.ClientMode),
					emptyDash(item.DocumentFormat),
					profileIndexesLabel(item),
					artifact.Phase,
					fmtOps(artifact.OpsPerSecond),
					fmtFloat(artifact.P95US, 1),
					fmtFloat(artifact.DurationMillis, 1),
					emptyDash(artifact.CPUProfile),
					emptyDash(artifact.AllocsProfile),
					emptyDash(artifact.BlockProfile),
					emptyDash(artifact.MutexProfile),
					emptyDash(firstNonEmpty(artifact.Error, item.RunError, item.ResultError)),
				})
			}
		}
		writeTable(b, []string{"manifest", "client mode", "format", "indexes", "phase", "ops/sec", "p95 us", "profile ms", "cpu", "allocs", "block", "mutex", "error"}, body, numericColumns(3, 5, 6, 7))
		b.WriteString("</div>")
	}
	b.WriteString("</section>\n")
}

func firstStrings(values []string, limit int) []string {
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}

func firstTargets(values []profileInvestigationTarget, limit int) []profileInvestigationTarget {
	if len(values) <= limit {
		return values
	}
	return values[:limit]
}

type rawEngineVariant struct {
	Profile    string
	Checkpoint string
	Label      string
	Color      string
}

func rawEngineVariants() []rawEngineVariant {
	return []rawEngineVariant{
		{Profile: "wal_on_fast", Checkpoint: "checkpoint between tests", Label: "wal_on_fast / checkpoint between tests", Color: "#2867c7"},
		{Profile: "wal_on_fast", Checkpoint: "no checkpoint between tests", Label: "wal_on_fast / no checkpoint between tests", Color: "#6b7fd7"},
		{Profile: "fast", Checkpoint: "checkpoint between tests", Label: "fast / checkpoint between tests", Color: "#1f8a5b"},
		{Profile: "fast", Checkpoint: "no checkpoint between tests", Label: "fast / no checkpoint between tests", Color: "#bd6a21"},
	}
}

func rawEnginePresentVariants(rows []rawEngineRun, variants []rawEngineVariant) []rawEngineVariant {
	present := make(map[string]bool)
	for _, row := range rows {
		present[row.Profile+"\x00"+row.Checkpoint] = true
	}
	var out []rawEngineVariant
	for _, variant := range variants {
		if present[variant.Profile+"\x00"+variant.Checkpoint] {
			out = append(out, variant)
		}
	}
	return out
}

func rawEngineResult(rows []rawEngineRun, profile, checkpoint, test string) (float64, bool) {
	for _, row := range rows {
		if row.Profile == profile && row.Checkpoint == checkpoint {
			v, ok := row.Results[test]
			return v, ok
		}
	}
	return 0, false
}

func collectRawTests(rows []rawEngineRun) []string {
	seen := make(map[string]bool)
	for _, row := range rows {
		for test := range row.Results {
			seen[test] = true
		}
	}
	tests := make([]string, 0, len(seen))
	for test := range seen {
		tests = append(tests, test)
	}
	sort.Slice(tests, func(i, j int) bool {
		return rawTestOrder(tests[i]) < rawTestOrder(tests[j])
	})
	return tests
}

func rawTestOrder(test string) string {
	order := map[string]int{
		"sequential_write":                      0,
		"random_write":                          1,
		"dataset_write_random":                  2,
		"dataset_write_sorted":                  3,
		"batch_write":                           4,
		"batch_write_steady":                    5,
		"batch_random":                          6,
		"batch_delete":                          7,
		"batch_small_seq":                       8,
		"random_delete":                         9,
		"random_read":                           10,
		"random_read_parallel":                  11,
		"random_read_parallel_acquire_snapshot": 12,
		"random_read_batch":                     13,
		"full_scan":                             14,
		"prefix_scan":                           15,
	}
	if idx, ok := order[test]; ok {
		return fmt.Sprintf("%03d_%s", idx, test)
	}
	return "999_" + test
}

func renderCollections(b *strings.Builder, rows []collectionRow, comps []collectionComparison) {
	if len(rows) == 0 && len(comps) == 0 {
		return
	}
	b.WriteString("<section id=\"collections\"><h2>Collections vs SQLite</h2>")
	b.WriteString("<p class=\"subtle\">TreeDB collection formats and SQLite baselines, including post-insert throughput, storage lifecycle, compacted-state ratios, and maintenance evidence. Raw canonical rows stay collapsed below.</p>")
	if len(rows) > 0 {
		writeCollectionSummary(b, rows, comps)
		if productionRows := collectionProductionEvidenceRows(rows); len(productionRows) > 0 {
			b.WriteString("<details open><summary>TreeDB production evidence</summary>")
			writeTable(b, []string{"config", "phase", "route", "route used ops", "fallbacks", "GOMAXPROCS", "physical cores", "effective concurrency", "admitted", "span-native", "backlog", "storage"}, productionRows, numericColumns(3, 4, 5, 6, 7))
			b.WriteString("</details>")
		}
		b.WriteString("<div class=\"chart-group\"><h3>Index-count overview</h3>")
		b.WriteString("<div class=\"chart-grid\">")
		docsRows := collectionDocsRows(rows)
		if len(docsRows.Categories) > 0 {
			b.WriteString(compactVerticalBarChart("Post-insert throughput by index count", docsRows.Categories, "index count", docsRows.Series, "docs/sec"))
		}
		postDiskRows := collectionPostInsertDiskRows(rows)
		if len(postDiskRows.Categories) > 0 {
			b.WriteString(compactVerticalBarChart("Post-insert bytes/doc by index count", postDiskRows.Categories, "index count", postDiskRows.Series, "B/doc"))
		}
		diskRows := collectionDiskRows(rows)
		if len(diskRows.Categories) > 0 {
			b.WriteString(compactVerticalBarChart("Compacted bytes/doc by index count", diskRows.Categories, "index count", diskRows.Series, "B/doc"))
		}
		ratioRows := collectionComparisonRatioRows(comps)
		if len(ratioRows.Categories) > 0 {
			b.WriteString(compactVerticalBarChart("Compacted size: SQLite bytes/doc divided by TreeDB bytes/doc", ratioRows.Categories, "index count", ratioRows.Series, "ratio"))
		}
		b.WriteString("</div></div>")
		if len(ratioRows.Categories) > 0 {
			b.WriteString("<p class=\"subtle\">For compacted-size ratio charts, higher means TreeDB is smaller; 1.00x means equal bytes/doc.</p>")
		}
		for _, idx := range sortedCollectionIndexes(rows) {
			indexRows := collectionRowsForIndex(rows, idx)
			if len(indexRows) == 0 {
				continue
			}
			b.WriteString("<div class=\"chart-group\"><h3>" + esc(indexCountTitleLabel(idx)) + ": Storage Lifecycle</h3>")
			b.WriteString("<p class=\"subtle\">Lifecycle charts compare post-insert storage with TreeDB maintenance phases and SQLite VACUUM where those rows are present.</p>")
			lifecycle := collectionLifecycleRows(indexRows)
			if len(lifecycle.Categories) > 0 {
				b.WriteString("<div class=\"chart-grid\">")
				b.WriteString(compactVerticalBarChart("Bytes/doc by lifecycle phase, "+indexCountLowerLabel(idx), lifecycle.Categories, "phase", lifecycle.Series, "B/doc"))
				maintenance := collectionMaintenanceRows(indexRows)
				if len(maintenance) > 0 {
					b.WriteString(collectionMaintenanceTableChart("Maintenance evidence, "+indexCountLowerLabel(idx), maintenance))
				}
				b.WriteString("</div>")
			}
			b.WriteString("</div>")
		}
		b.WriteString("<details><summary>Collection highlight rows</summary>")
		renderCollectionHighlight(b, rows)
		b.WriteString("</details>")
	}
	if len(comps) > 0 {
		b.WriteString("<details><summary>Compacted-state comparisons</summary>")
		var body [][]string
		for _, c := range comps {
			body = append(body, []string{c.TreeDBConfig, c.TreeDBPhase, c.SQLiteConfig, c.SQLitePhase, fmtFloat(c.TreeDBBytesPerDoc, 1), fmtFloat(c.SQLiteBytesPerDoc, 1), fmtRatio(c.SmallerRatio), c.ComparisonBasis})
		}
		writeTable(b, []string{"TreeDB config", "TreeDB phase", "SQLite config", "SQLite phase", "TreeDB B/doc", "SQLite B/doc", "SQLite/TreeDB size ratio", "basis"}, body, numericColumns(4, 5, 6))
		b.WriteString("</details>")
	}
	if len(rows) > 0 {
		b.WriteString("<details><summary>All collection result rows</summary>")
		var body [][]string
		for _, r := range rows {
			body = append(body, []string{r.ConfigName, r.Engine, r.Format, strconv.Itoa(r.IndexCount), r.Phase, r.MaintenanceMode, fmtOps(r.DocsPerSec), fmtFloat(r.BytesPerDoc, 1), fmtBytes(r.TotalBytes), r.MeasurementKind, r.MeasurementNote})
		}
		writeTable(b, []string{"config", "engine", "format", "indexes", "phase", "maintenance", "docs/sec", "B/doc", "total bytes", "kind", "note"}, body, numericColumns(3, 6, 7, 8))
		b.WriteString("</details>")
	}
	b.WriteString("</section>\n")
}

type collectionChartRows struct {
	Categories []string
	Series     []chartSeries
}

func collectionDocsRows(rows []collectionRow) collectionChartRows {
	return collectionChart(rows, "docs")
}

func collectionDiskRows(rows []collectionRow) collectionChartRows {
	return collectionChart(rows, "bytes")
}

func collectionPostInsertDiskRows(rows []collectionRow) collectionChartRows {
	return collectionChart(rows, "post_bytes")
}

func collectionComparisonRatioRows(comps []collectionComparison) collectionChartRows {
	if len(comps) == 0 {
		return collectionChartRows{}
	}
	values := make(map[string][]float64)
	labelSet := make(map[string]bool)
	for _, comp := range comps {
		if comp.SmallerRatio <= 0 || !collectionComparisonIsClaimableCompacted(comp) {
			continue
		}
		label := collectionComparisonLabel(comp)
		labelSet[label] = true
	}
	labels := make([]string, 0, len(labelSet))
	for label := range labelSet {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	labelPos := make(map[string]int, len(labels))
	for i, label := range labels {
		labelPos[label] = i
	}
	for _, comp := range comps {
		if comp.SmallerRatio <= 0 || !collectionComparisonIsClaimableCompacted(comp) {
			continue
		}
		key := displayConfigWithoutIndex(comp.TreeDBConfig) + " vs " + displayConfigWithoutIndex(comp.SQLiteConfig)
		if _, ok := values[key]; !ok {
			values[key] = make([]float64, len(labels))
		}
		pos := labelPos[collectionComparisonLabel(comp)]
		if comp.SmallerRatio > values[key][pos] {
			values[key][pos] = comp.SmallerRatio
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var series []chartSeries
	for _, key := range keys {
		series = append(series, chartSeries{Name: key, Values: values[key], Color: collectionSeriesColor(key)})
	}
	return collectionChartRows{Categories: labels, Series: nonZeroSeries(series)}
}

func collectionComparisonIsClaimableCompacted(comp collectionComparison) bool {
	return comp.TreeDBPhase == "exhaustive_compact" && comp.SQLitePhase == "sqlite_vacuum"
}

func collectionComparisonLabel(comp collectionComparison) string {
	idx := collectionIndexFromConfig(comp.TreeDBConfig)
	if idx >= 0 {
		return indexCountLabel(idx)
	}
	return comp.Name
}

func collectionIndexFromConfig(config string) int {
	if config == "" {
		return -1
	}
	if pos := strings.LastIndex(config, "_indexes_"); pos >= 0 {
		v, err := strconv.Atoi(config[pos+len("_indexes_"):])
		if err != nil {
			return -1
		}
		return v
	}
	pos := strings.LastIndex(config, "_indexes")
	if pos < 0 {
		return -1
	}
	prefix := config[:pos]
	start := strings.LastIndex(prefix, "_")
	if start < 0 || start+1 >= len(prefix) {
		return -1
	}
	v, err := strconv.Atoi(prefix[start+1:])
	if err != nil {
		return -1
	}
	return v
}

func writeCollectionSummary(b *strings.Builder, rows []collectionRow, comps []collectionComparison) {
	fast, hasFast := bestCollectionRow(rows, func(row collectionRow) bool {
		return row.Phase == "post_insert" && row.DocsPerSec > 0
	}, func(a, b collectionRow) bool {
		return a.DocsPerSec > b.DocsPerSec
	})
	small, hasSmall := bestCollectionRow(rows, func(row collectionRow) bool {
		return collectionEngineFamily(row) == "TreeDB" && row.BytesPerDoc > 0 && collectionChartPhaseMatches("TreeDB", "bytes", row.Phase)
	}, func(a, b collectionRow) bool {
		return a.BytesPerDoc < b.BytesPerDoc
	})
	bestComp, hasComp := bestCollectionComparison(comps)
	b.WriteString("<h4>Collection summary</h4><div class=\"summary-strip\">")
	if hasFast {
		b.WriteString("<div class=\"summary-row\"><div class=\"summary-workload\">fastest insert</div>")
		writeSummaryCell(b, "config", displayConfig(fast.ConfigName))
		writeSummaryCell(b, "indexes", indexCountLabel(fast.IndexCount))
		writeSummaryCell(b, "docs/sec", fmtOps(fast.DocsPerSec))
		writeSummaryCell(b, "format", displayFormat(fast.Format))
		writeSummaryCell(b, "phase", fast.Phase)
		b.WriteString("</div>")
	}
	if hasSmall {
		b.WriteString("<div class=\"summary-row\"><div class=\"summary-workload\">smallest TreeDB compacted</div>")
		writeSummaryCell(b, "config", displayConfig(small.ConfigName))
		writeSummaryCell(b, "indexes", indexCountLabel(small.IndexCount))
		writeSummaryCell(b, "B/doc", fmtFloat(small.BytesPerDoc, 1))
		writeSummaryCell(b, "phase", small.Phase)
		writeSummaryCell(b, "total", fmtBytes(small.TotalBytes))
		b.WriteString("</div>")
	}
	if hasComp {
		b.WriteString("<div class=\"summary-row\"><div class=\"summary-workload\">best SQLite/TreeDB ratio</div>")
		writeSummaryCell(b, "TreeDB", displayConfig(bestComp.TreeDBConfig))
		writeSummaryCell(b, "SQLite", displayConfig(bestComp.SQLiteConfig))
		writeSummaryCell(b, "ratio", fmtRatio(bestComp.SmallerRatio))
		writeSummaryCell(b, "TreeDB B/doc", fmtFloat(bestComp.TreeDBBytesPerDoc, 1))
		writeSummaryCell(b, "SQLite B/doc", fmtFloat(bestComp.SQLiteBytesPerDoc, 1))
		b.WriteString("</div>")
	}
	b.WriteString("</div>")
}

func bestCollectionRow(rows []collectionRow, include func(collectionRow) bool, better func(collectionRow, collectionRow) bool) (collectionRow, bool) {
	var best collectionRow
	var ok bool
	for _, row := range rows {
		if !include(row) {
			continue
		}
		if !ok || better(row, best) {
			best = row
			ok = true
		}
	}
	return best, ok
}

func bestCollectionComparison(comps []collectionComparison) (collectionComparison, bool) {
	var best collectionComparison
	var ok bool
	for _, comp := range comps {
		if comp.SmallerRatio <= 0 || !collectionComparisonIsClaimableCompacted(comp) {
			continue
		}
		if !ok || comp.SmallerRatio > best.SmallerRatio {
			best = comp
			ok = true
		}
	}
	return best, ok
}

func collectionRowsForIndex(rows []collectionRow, idx int) []collectionRow {
	out := make([]collectionRow, 0)
	for _, row := range rows {
		if row.IndexCount == idx {
			out = append(out, row)
		}
	}
	return out
}

func collectionProductionEvidenceRows(rows []collectionRow) [][]string {
	var out [][]string
	for _, row := range rows {
		if row.ProductionEvidence == nil || !strings.HasPrefix(row.ConfigName, "treedb_") {
			continue
		}
		ev := row.ProductionEvidence
		out = append(out, []string{
			displayConfig(row.ConfigName),
			row.Phase,
			ev.ProducerRoute,
			fmtFloatPtr(ev.ProducerRouteUsedOps, 0),
			fmtFloatPtr(ev.ProducerRouteFallbacks, 0),
			fmtIntPtr(ev.GOMAXPROCS),
			fmtIntPtr(ev.PhysicalCores),
			fmtIntPtr(ev.FlushAdmissionEffectiveConcurrency),
			fmtBoolPtr(ev.FlushAdmissionAdmitted),
			fmtBoolPtr(ev.FlushAdmissionSpanNative),
			fmtBoolPtr(ev.FlushAdmissionBacklogCoalescing),
			ev.StoragePolicy,
		})
	}
	return out
}

func collectionLifecycleRows(rows []collectionRow) collectionChartRows {
	phases := []string{"post_insert", "online_one_pass_maintenance", "offline_compact", "offline_rewrite", "exhaustive_compact", "full_leafgen_pack_gc", "sqlite_vacuum"}
	phaseLabels := []string{"post insert", "online maint", "offline compact", "offline rewrite", "exhaustive", "leafgen GC", "SQLite VACUUM"}
	values := make(map[string][]float64)
	phasePresent := make([]bool, len(phases))
	for _, row := range rows {
		if row.BytesPerDoc <= 0 {
			continue
		}
		pos := indexOf(phases, row.Phase)
		if pos < 0 {
			continue
		}
		phasePresent[pos] = true
		key := displayConfig(row.ConfigName)
		if _, ok := values[key]; !ok {
			values[key] = make([]float64, len(phases))
		}
		values[key][pos] = row.BytesPerDoc
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var series []chartSeries
	for _, key := range keys {
		series = append(series, chartSeries{Name: key, Values: values[key], Color: collectionSeriesColor(key)})
	}
	return compactCollectionChartRows(collectionChartRows{Categories: phaseLabels, Series: nonZeroSeries(series)}, phasePresent)
}

func compactCollectionChartRows(rows collectionChartRows, present []bool) collectionChartRows {
	if len(rows.Categories) == 0 || len(present) != len(rows.Categories) {
		return rows
	}
	positions := make([]int, 0, len(rows.Categories))
	categories := make([]string, 0, len(rows.Categories))
	for i, ok := range present {
		if ok {
			positions = append(positions, i)
			categories = append(categories, rows.Categories[i])
		}
	}
	if len(positions) == len(rows.Categories) {
		return rows
	}
	series := make([]chartSeries, 0, len(rows.Series))
	for _, s := range rows.Series {
		values := make([]float64, len(positions))
		for i, pos := range positions {
			if pos < len(s.Values) {
				values[i] = s.Values[pos]
			}
		}
		series = append(series, chartSeries{Name: s.Name, Values: values, Color: s.Color})
	}
	return collectionChartRows{Categories: categories, Series: nonZeroSeries(series)}
}

func indexOf(values []string, value string) int {
	for i, v := range values {
		if v == value {
			return i
		}
	}
	return -1
}

func collectionMaintenanceRows(rows []collectionRow) [][]string {
	var out [][]string
	addBytes := func(row collectionRow, metric string, value float64) {
		if value > 0 {
			out = append(out, []string{displayConfig(row.ConfigName), row.Phase, metric, fmtBytes(value)})
		}
	}
	addFloat := func(row collectionRow, metric string, value float64) {
		if value > 0 {
			out = append(out, []string{displayConfig(row.ConfigName), row.Phase, metric, fmtFloat(value, 1)})
		}
	}
	addText := func(row collectionRow, metric, value string) {
		if strings.TrimSpace(value) != "" {
			out = append(out, []string{displayConfig(row.ConfigName), row.Phase, metric, value})
		}
	}
	for _, row := range rows {
		if len(row.MaintenanceStats) == 0 && len(row.Extra) == 0 {
			continue
		}
		switch row.Phase {
		case "full_leafgen_pack_gc":
			addBytes(row, "leafgen candidate bytes", row.MaintenanceStats["leafgen_candidate_bytes_total"])
			addBytes(row, "leafgen live bytes", row.MaintenanceStats["leafgen_candidate_bytes_live"])
			addBytes(row, "leafgen dead bytes", row.MaintenanceStats["leafgen_candidate_bytes_dead"])
			addBytes(row, "leafgen expected reclaim", row.MaintenanceStats["leafgen_expected_reclaim_bytes"])
			addBytes(row, "index vacuum before", row.MaintenanceStats["index_vacuum_disk_bytes_before"])
			addBytes(row, "index vacuum after", row.MaintenanceStats["index_vacuum_disk_bytes_after"])
			addFloat(row, "index vacuum seconds", row.MaintenanceStats["index_vacuum_seconds"])
		case "sqlite_vacuum":
			addBytes(row, "VACUUM bytes before", row.MaintenanceStats["sqlite_vacuum_disk_total_bytes_before"])
			addBytes(row, "VACUUM bytes after", row.MaintenanceStats["sqlite_vacuum_disk_total_bytes_after"])
			addBytes(row, "VACUUM bytes delta", row.MaintenanceStats["sqlite_vacuum_disk_total_bytes_delta"])
			addFloat(row, "VACUUM ops/sec", maintenanceStat(row.MaintenanceStats, "sqlite_vacuum_ops/sec", "sqlite_vacuum_ops_per_sec"))
			addText(row, "VACUUM bytes before", row.Extra["sqlite_vacuum_bytes_before"])
			addText(row, "VACUUM bytes after", row.Extra["sqlite_vacuum_bytes_after"])
			addText(row, "VACUUM bytes delta", row.Extra["sqlite_vacuum_bytes_delta"])
		case "offline_rewrite", "online_one_pass_maintenance":
			addBytes(row, "total bytes", row.TotalBytes)
			addFloat(row, "bytes/doc", row.BytesPerDoc)
			addText(row, "index DB bytes", row.Extra["index_db_bytes"])
			addText(row, "leaf vlog bytes", row.Extra["leaf_vlog_bytes"])
		}
	}
	return out
}

func maintenanceStat(stats map[string]float64, names ...string) float64 {
	for _, name := range names {
		if value := stats[name]; value != 0 {
			return value
		}
	}
	return 0
}

func collectionMaintenanceTableChart(title string, rows [][]string) string {
	var b strings.Builder
	b.WriteString("<div class=\"chart\"><div class=\"chart-title\">" + esc(chartLabel(title)) + "</div>")
	writeTable(&b, []string{"config", "phase", "metric", "value"}, rows, nil)
	b.WriteString("</div>")
	return b.String()
}

func collectionChart(rows []collectionRow, kind string) collectionChartRows {
	indexes := sortedCollectionIndexes(rows)
	categories := make([]string, 0, len(indexes))
	indexPos := make(map[int]int, len(indexes))
	for pos, idx := range indexes {
		categories = append(categories, indexCountLabel(idx))
		indexPos[idx] = pos
	}
	values := make(map[string][]float64)
	priorities := make(map[string][]int)
	labels := make(map[string]string)
	orders := make(map[string]string)
	for _, row := range rows {
		if row.Shape != "collection" {
			continue
		}
		family := collectionEngineFamily(row)
		if family == "" {
			continue
		}
		if !collectionChartPhaseMatches(family, kind, row.Phase) {
			continue
		}
		pos, ok := indexPos[row.IndexCount]
		if !ok {
			continue
		}
		key := family + "\x00" + canonicalFormat(row.Format)
		if _, ok := values[key]; !ok {
			values[key] = make([]float64, len(indexes))
			priorities[key] = make([]int, len(indexes))
			labels[key] = collectionSeriesLabel(family, row.Format, kind)
			orders[key] = collectionSeriesOrder(family, row.Format)
		}
		value, priority, ok := collectionChartMetric(row, kind)
		if !ok {
			continue
		}
		if kind == "bytes" || kind == "post_bytes" {
			if values[key][pos] == 0 || value < values[key][pos] {
				values[key][pos] = value
			}
			continue
		}
		if priority >= priorities[key][pos] {
			values[key][pos] = value
			priorities[key][pos] = priority
		}
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if orders[keys[i]] != orders[keys[j]] {
			return orders[keys[i]] < orders[keys[j]]
		}
		return labels[keys[i]] < labels[keys[j]]
	})
	var series []chartSeries
	for _, key := range keys {
		series = append(series, chartSeries{Name: labels[key], Values: values[key], Color: collectionSeriesColor(key)})
	}
	return collectionChartRows{Categories: categories, Series: nonZeroSeries(series)}
}

func collectionChartMetric(row collectionRow, kind string) (float64, int, bool) {
	if kind == "bytes" || kind == "post_bytes" {
		if row.BytesPerDoc <= 0 {
			return 0, 0, false
		}
		return row.BytesPerDoc, 1, true
	}
	if row.DocsPerSec <= 0 {
		return 0, 0, false
	}
	return row.DocsPerSec, collectionDocsPriority(row), true
}

func collectionDocsPriority(row collectionRow) int {
	switch row.MeasurementKind {
	case "go_benchmark":
		return 3
	case "fixture_wall_timed":
		return 2
	default:
		return 1
	}
}

func collectionEngineFamily(row collectionRow) string {
	engine := strings.ToLower(row.Engine)
	config := strings.ToLower(row.ConfigName)
	switch {
	case strings.HasPrefix(config, "treedb_") || strings.HasPrefix(engine, "treedb"):
		return "TreeDB"
	case strings.HasPrefix(config, "sqlite_") || strings.HasPrefix(engine, "sqlite"):
		return "SQLite"
	default:
		return ""
	}
}

func collectionChartPhase(family, kind string) (string, bool) {
	if kind == "docs" {
		return "post_insert", true
	}
	if kind == "post_bytes" {
		return "post_insert", true
	}
	if kind != "bytes" {
		return "", false
	}
	if family == "TreeDB" {
		return "exhaustive_compact", true
	}
	if family == "SQLite" {
		return "sqlite_vacuum", true
	}
	return "", false
}

func collectionChartPhaseMatches(family, kind, phase string) bool {
	if kind == "bytes" {
		if family == "TreeDB" {
			return phase == "exhaustive_compact"
		}
		if family == "SQLite" {
			return phase == "sqlite_vacuum"
		}
		return false
	}
	want, ok := collectionChartPhase(family, kind)
	if ok {
		return phase == want
	}
	return false
}

func collectionSeriesLabel(family, format, kind string) string {
	label := family + " " + displayFormat(format)
	if kind == "bytes" {
		if family == "TreeDB" {
			label += " compacted"
		} else if family == "SQLite" {
			label += " VACUUM"
		}
	}
	if kind == "post_bytes" {
		label += " post-insert"
	}
	return label
}

func displayConfig(config string) string {
	value := strings.TrimSpace(config)
	value = strings.TrimPrefix(value, "treedb_")
	value = strings.TrimPrefix(value, "sqlite_")
	value = strings.ReplaceAll(value, "_collection", "")
	value = displayConfigIndexSuffix(value)
	value = strings.ReplaceAll(value, "_", " ")
	return chartLabel(value)
}

func displayConfigWithoutIndex(config string) string {
	value := strings.TrimSpace(config)
	value = strings.TrimPrefix(value, "treedb_")
	value = strings.TrimPrefix(value, "sqlite_")
	value = strings.ReplaceAll(value, "_collection", "")
	value = strings.TrimSpace(stripConfigIndexSuffix(value))
	value = strings.TrimSuffix(value, "_")
	value = strings.ReplaceAll(value, "_", " ")
	return chartLabel(value)
}

func displayConfigIndexSuffix(config string) string {
	idx := collectionIndexFromConfig(config)
	if idx < 0 {
		return config
	}
	for _, suffix := range configIndexSuffixes(idx) {
		if strings.HasSuffix(config, suffix) {
			return strings.TrimSuffix(config, suffix) + fmt.Sprintf(" / i%d", idx)
		}
	}
	return config
}

func stripConfigIndexSuffix(config string) string {
	idx := collectionIndexFromConfig(config)
	if idx < 0 {
		return config
	}
	for _, suffix := range configIndexSuffixes(idx) {
		if strings.HasSuffix(config, suffix) {
			return strings.TrimSuffix(config, suffix)
		}
	}
	return config
}

func configIndexSuffixes(idx int) []string {
	return []string{
		fmt.Sprintf("_%d_indexes", idx),
		fmt.Sprintf("_indexes_%d", idx),
	}
}

func displayFormat(format string) string {
	switch canonicalFormat(format) {
	case "bson":
		return "BSON"
	case "json":
		return "JSON"
	case "native-columns":
		return "native"
	case "template-v1":
		return "template-v1"
	case "":
		return "unknown"
	default:
		return chartLabel(strings.ReplaceAll(canonicalFormat(format), "-", " "))
	}
}

func canonicalFormat(format string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(format), "_", "-"))
}

func collectionSeriesOrder(family, format string) string {
	familyOrder := "2"
	if family == "TreeDB" {
		familyOrder = "0"
	} else if family == "SQLite" {
		familyOrder = "1"
	}
	formatOrder := map[string]string{
		"template-v1":    "00",
		"bson":           "01",
		"json":           "02",
		"native-columns": "03",
		"native":         "03",
		"extended-json":  "04",
		"canonical-json": "05",
	}
	order, ok := formatOrder[canonicalFormat(format)]
	if !ok {
		order = "99_" + canonicalFormat(format)
	}
	return familyOrder + "_" + order
}

func collectionSeriesColor(key string) string {
	parts := strings.Split(key, "\x00")
	if len(parts) >= 2 {
		switch parts[0] + "\x00" + parts[1] {
		case "TreeDB\x00template-v1":
			return "#2867c7"
		case "TreeDB\x00bson":
			return "#6b7fd7"
		case "TreeDB\x00json":
			return "#bd6a21"
		case "SQLite\x00native-columns":
			return "#1f8a5b"
		case "SQLite\x00json":
			return "#b94242"
		}
	}
	palette := []string{"#2867c7", "#6b7fd7", "#bd6a21", "#1f8a5b", "#b94242", "#7c5a2f", "#6c6f7d", "#8a4f9e"}
	sum := 0
	for _, r := range key {
		sum += int(r)
	}
	return palette[sum%len(palette)]
}

func nonZeroSeries(series []chartSeries) []chartSeries {
	out := make([]chartSeries, 0, len(series))
	for _, s := range series {
		for _, v := range s.Values {
			if v > 0 {
				out = append(out, s)
				break
			}
		}
	}
	return out
}

func renderCollectionHighlight(b *strings.Builder, rows []collectionRow) {
	docsRows := collectionDocsRows(rows)
	diskRows := collectionDiskRows(rows)
	headers := []string{"indexes"}
	for _, series := range docsRows.Series {
		headers = append(headers, series.Name+" docs/s")
	}
	for _, series := range diskRows.Series {
		headers = append(headers, series.Name+" B/doc")
	}
	var body [][]string
	for idx, label := range docsRows.Categories {
		row := []string{label}
		for _, series := range docsRows.Series {
			row = append(row, fmtOps(series.Values[idx]))
		}
		for _, series := range diskRows.Series {
			if idx < len(series.Values) {
				row = append(row, fmtFloat(series.Values[idx], 1))
			} else {
				row = append(row, "-")
			}
		}
		body = append(body, row)
	}
	numeric := make(map[int]bool)
	for i := 1; i < len(headers); i++ {
		numeric[i] = true
	}
	writeTable(b, headers, body, numeric)
}

func sortedCollectionIndexes(rows []collectionRow) []int {
	seen := make(map[int]bool)
	for _, row := range rows {
		seen[row.IndexCount] = true
	}
	var indexes []int
	for idx := range seen {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	return indexes
}

func collectionValue(rows []collectionRow, idx int, config, phase, value string) float64 {
	best := 0.0
	for _, row := range rows {
		if row.IndexCount == idx && row.ConfigName == config && row.Phase == phase {
			if value == "docs" {
				if row.DocsPerSec > 0 {
					if row.MeasurementKind == "go_benchmark" {
						return row.DocsPerSec
					}
					if best == 0 {
						best = row.DocsPerSec
					}
				}
				continue
			}
			return row.BytesPerDoc
		}
	}
	return best
}

func renderMongoFullSweep(b *strings.Builder, rows []mongoSummaryRow) {
	if len(rows) == 0 {
		return
	}
	b.WriteString("<section id=\"mongo-full\"><h2>Mongo API Full Sweep</h2>")
	b.WriteString("<p class=\"subtle\">Full BSON `driver-command-raw` TreeDB-vs-MongoDB phase comparison. Headline charts and summaries are shown first; raw `summary.tsv` rows are collapsed below.</p>")
	b.WriteString("<div class=\"chart-group\"><h3>Index-count overview</h3>")
	b.WriteString("<p class=\"subtle\">These two charts show the headline insert throughput and physical disk footprint as secondary indexes are added.</p>")
	b.WriteString(fullSweepLoadNote(rows))
	b.WriteString("<div class=\"chart-grid\">")
	indexes := sortedMongoIndexes(rows)
	loadRows := mongoRowsForPhase(rows, "load_insert_many")
	if len(loadRows) > 0 {
		indexLabels := mongoRowIndexLabels(loadRows)
		b.WriteString(lineChart("Insert throughput vs secondary indexes", indexLabels, "secondary indexes", "docs/sec", []chartSeries{
			{Name: "TreeDB", Values: mongoRowOps(loadRows, "tree"), Color: "#2867c7"},
			{Name: "MongoDB", Values: mongoRowOps(loadRows, "mongo"), Color: "#1f8a5b"},
		}, "docs/sec"))
		b.WriteString(lineChart("Storage footprint vs secondary indexes", indexLabels, "secondary indexes", "storage bytes", []chartSeries{
			{Name: "TreeDB", Values: mongoRowDisk(loadRows, "tree"), Color: "#2867c7"},
			{Name: "MongoDB", Values: mongoRowDisk(loadRows, "mongo"), Color: "#1f8a5b"},
		}, "bytes"))
		b.WriteString("<p class=\"subtle\"><strong>Storage basis:</strong> " + esc(mongoStorageBasisText(loadRows)) + "</p>")
		writeMongoIndexRetentionTable(b, loadRows)
	}
	b.WriteString("</div></div>")
	for _, idx := range indexes {
		indexLabel := indexCountTitleLabel(idx)
		throughputRows := mongoOperationThroughputRows(rows, idx, true)
		b.WriteString("<div class=\"chart-group\"><h3>" + esc(indexLabel) + ": Mongo API Scaling</h3>")
		b.WriteString("<p class=\"subtle\">These are the actual concurrency sweeps. Single threaded client rows are one-client request/response probes; higher client counts show where each operation saturates.</p>")
		if scope, ok := mongoPrimaryScope(rows, idx); ok {
			b.WriteString("<p class=\"subtle\"><strong>Scope:</strong> " + esc(mongoScopeText(scope)) + "</p>")
		}
		b.WriteString("<div class=\"chart-grid\">")
		for _, spec := range mongoConcurrentReadSweepSpecs() {
			readerCounts := mongoSweepCountsInPrimaryScope(rows, idx, spec.Prefix)
			if len(readerCounts) == 0 {
				continue
			}
			b.WriteString(lineChart(spec.Title, countLabels(readerCounts), "client count", "ops/sec", []chartSeries{
				{Name: "TreeDB", Values: mongoSweep(rows, idx, spec.Prefix, "tree", readerCounts), Color: "#2867c7"},
				{Name: "MongoDB", Values: mongoSweep(rows, idx, spec.Prefix, "mongo", readerCounts), Color: "#1f8a5b"},
			}, "ops/sec"))
		}
		writerCounts := mongoSweepCountsInPrimaryScope(rows, idx, "concurrent_id_update_set_w")
		if len(writerCounts) > 0 {
			b.WriteString(lineChart("_id update one: throughput vs writer clients", countLabels(writerCounts), "client count", "ops/sec", []chartSeries{
				{Name: "TreeDB", Values: mongoSweep(rows, idx, "concurrent_id_update_set_w", "tree", writerCounts), Color: "#2867c7"},
				{Name: "MongoDB", Values: mongoSweep(rows, idx, "concurrent_id_update_set_w", "mongo", writerCounts), Color: "#1f8a5b"},
			}, "ops/sec"))
		}
		b.WriteString("</div>")
		if len(throughputRows) > 0 {
			writeMongoThroughputSummary(b, throughputRows)
		}
		indexRows := mongoRowsForIndex(rows, idx)
		if len(indexRows) > 0 {
			b.WriteString("<details><summary>Raw full-sweep TSV rows for " + esc(indexCountLowerLabel(idx)) + "</summary>")
			writeMongoSummaryTable(b, indexRows)
			b.WriteString("</details>")
		}
		b.WriteString("</div>")
	}
	b.WriteString("<details><summary>All raw full-sweep TSV rows</summary>")
	writeMongoSummaryTable(b, rows)
	b.WriteString("</details></section>\n")
}

func renderMongoLoadModes(b *strings.Builder, rows []loadModeRow) {
	if len(rows) == 0 {
		return
	}
	b.WriteString("<section id=\"mongo-load\"><h2>Load-Only Client-Mode Matrix</h2>")
	b.WriteString("<p class=\"subtle\">Insert-only matrix. This section uses raw JSON directly so every MongoDB and TreeDB client mode has its own throughput row.</p>")
	b.WriteString("<p class=\"subtle\">Use this section for pure ingest client-path comparisons. It is intentionally separate from the full-sweep load chart, which inherits the broader read/range/update sweep setup.</p>")
	b.WriteString("<p class=\"subtle\">Driver, driver-command, driver-command-raw, and driver-unack modes are comparable Mongo-compatible client paths and render as paired TreeDB/MongoDB bars. Client modes marked with <strong>*</strong> are TreeDB-only ceiling probes explained below.</p>")
	b.WriteString("<p class=\"subtle\"><strong>Load-mode storage basis:</strong> " + esc(loadModeStorageBasisText(rows)) + "</p>")
	b.WriteString("<div class=\"chart-grid\">")
	for _, idx := range sortedLoadModeIndexes(rows) {
		cats, tree, mongo := loadModeChartRows(rows, idx)
		if len(cats) == 0 {
			continue
		}
		b.WriteString(loadModeBarChart(fmt.Sprintf("Load throughput by client mode, %d indexes", idx), cats, tree, mongo, "docs/sec", "Docs/Sec"))
		diskCats, treeDisk, mongoDisk := loadModeDiskChartRows(rows, idx)
		if len(diskCats) > 0 {
			b.WriteString(loadModeBarChart(fmt.Sprintf("Physical storage by client mode, %d indexes", idx), diskCats, treeDisk, mongoDisk, "bytes", "Physical Bytes"))
		}
	}
	b.WriteString("</div>")
	b.WriteString("<p class=\"subtle\"><strong>* TreeDB-only modes:</strong> <code>direct</code> calls the collection API directly in the selected TreeDB document format. <code>raw-wire-tcp</code> still sends Mongo OP_MSG bytes over loopback TCP to the TreeDB gateway while bypassing the MongoDB Go driver. <code>raw-wire</code> removes the socket too and drives the same raw command/gateway path in process. These modes are TreeDB-only ceiling probes, so each renders as one centered TreeDB bar instead of a paired MongoDB bar.</p>")
	var body [][]string
	for _, row := range rows {
		body = append(body, []string{strconv.Itoa(row.Indexes), row.Target, row.Config, fmtOps(row.OpsPerSec), loadModePhysicalBytesCell(row), row.RawJSON})
	}
	b.WriteString("<details><summary>Raw load-mode rows</summary>")
	writeTable(b, []string{"indexes", "target", "config", "load docs/sec", "physical bytes", "raw JSON"}, body, numericColumns(0, 3, 4))
	b.WriteString("</details>")
	b.WriteString("</section>\n")
}

func renderMongoLoadScaling(b *strings.Builder, rows []mongoSummaryRow) {
	loadRows := mongoLoadScalingRows(rows)
	if len(loadRows) == 0 {
		return
	}
	b.WriteString("<section id=\"mongo-load-scaling\"><h2>Mongo API InsertMany Producer Scaling</h2>")
	b.WriteString("<p class=\"subtle\">Load-only BSON `driver-command-raw` TreeDB-vs-MongoDB sweep. This section varies InsertMany producer count and is separate from reader/writer operation scaling.</p>")
	if metadata := mongoLoadScalingScopeText(loadRows); metadata != "" {
		b.WriteString("<p class=\"subtle\"><strong>Scope:</strong> " + esc(metadata) + "</p>")
	}
	b.WriteString("<div class=\"chart-grid\">")
	for _, idx := range sortedMongoIndexes(loadRows) {
		counts := mongoLoadProducerCounts(loadRows, idx)
		if len(counts) == 0 {
			continue
		}
		b.WriteString(lineChart("Insert throughput vs insert producers, "+indexCountLabel(idx), countLabels(counts), "insert producers", "docs/sec", []chartSeries{
			{Name: "TreeDB", Values: mongoLoadProducerOps(loadRows, idx, counts, "tree"), Color: "#2867c7"},
			{Name: "MongoDB", Values: mongoLoadProducerOps(loadRows, idx, counts, "mongo"), Color: "#1f8a5b"},
		}, "docs/sec"))
	}
	b.WriteString("</div>")
	writeMongoLoadScalingSummaryTable(b, loadRows)
	b.WriteString("<details><summary>Raw load-scaling TSV rows</summary>")
	writeMongoSummaryTable(b, loadRows)
	b.WriteString("</details></section>\n")
}

func mongoLoadScalingRows(rows []mongoSummaryRow) []mongoSummaryRow {
	var out []mongoSummaryRow
	for _, row := range rows {
		if row.Phase == "load_insert_many" {
			out = append(out, row)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].SecondaryIndexes != out[j].SecondaryIndexes {
			return out[i].SecondaryIndexes < out[j].SecondaryIndexes
		}
		if mongoLoadProducerCount(out[i]) != mongoLoadProducerCount(out[j]) {
			return mongoLoadProducerCount(out[i]) < mongoLoadProducerCount(out[j])
		}
		return out[i].TreeDBConfig < out[j].TreeDBConfig
	})
	return out
}

func hasMongoLoadScalingRows(rows []mongoSummaryRow) bool {
	for _, row := range rows {
		if row.Phase == "load_insert_many" {
			return true
		}
	}
	return false
}

func mongoLoadScalingScopeText(rows []mongoSummaryRow) string {
	if len(rows) == 0 {
		return ""
	}
	first := rows[0]
	for _, row := range rows[1:] {
		if row.Documents != first.Documents || row.BatchSize != first.BatchSize || row.TreeDBConfig != first.TreeDBConfig || row.MongoConfig != first.MongoConfig {
			return "mixed document, batch, or client-mode metadata; see raw load-scaling rows"
		}
	}
	parts := []string{fmt.Sprintf("%s docs", commaInt(int64(first.Documents)))}
	if first.BatchSize > 0 {
		parts = append(parts, fmt.Sprintf("batch size %s", commaInt(int64(first.BatchSize))))
	}
	if batches := mongoLoadBatchCount(first); batches > 0 {
		parts = append(parts, fmt.Sprintf("%s load batches", commaInt(int64(batches))))
	}
	if first.TreeDBConfig != "" {
		parts = append(parts, "TreeDB "+first.TreeDBConfig)
	}
	if first.MongoConfig != "" {
		parts = append(parts, "MongoDB "+first.MongoConfig)
	}
	return strings.Join(parts, ", ")
}

func mongoLoadProducerCounts(rows []mongoSummaryRow, idx int) []int {
	seen := make(map[int]bool)
	for _, row := range rows {
		if row.SecondaryIndexes != idx {
			continue
		}
		count := mongoLoadProducerCount(row)
		if count > 0 {
			seen[count] = true
		}
	}
	var out []int
	for count := range seen {
		out = append(out, count)
	}
	sort.Ints(out)
	return out
}

func mongoLoadProducerOps(rows []mongoSummaryRow, idx int, counts []int, side string) []float64 {
	out := make([]float64, 0, len(counts))
	for _, count := range counts {
		row, ok := mongoLoadProducerRow(rows, idx, count)
		if !ok {
			out = append(out, 0)
			continue
		}
		if side == "mongo" {
			out = append(out, row.MongoOpsSec)
		} else {
			out = append(out, row.TreeDBOpsSec)
		}
	}
	return out
}

func mongoLoadProducerRow(rows []mongoSummaryRow, idx, count int) (mongoSummaryRow, bool) {
	for _, row := range rows {
		if row.SecondaryIndexes == idx && mongoLoadProducerCount(row) == count {
			return row, true
		}
	}
	return mongoSummaryRow{}, false
}

func mongoLoadProducerCount(row mongoSummaryRow) int {
	if row.InsertProducers > 0 {
		return row.InsertProducers
	}
	return mongoEffectiveLoadProducers(row)
}

func writeMongoLoadScalingSummaryTable(b *strings.Builder, rows []mongoSummaryRow) {
	var body [][]string
	for _, row := range rows {
		body = append(body, []string{
			strconv.Itoa(row.SecondaryIndexes),
			formatOptionalInt(mongoLoadProducerCount(row)),
			formatOptionalInt(mongoEffectiveLoadProducers(row)),
			formatOptionalInt(mongoLoadBatchCount(row)),
			fmtOps(row.TreeDBOpsSec),
			fmtOps(row.MongoOpsSec),
			fmtRatio(row.TreeDBToMongoRatio),
		})
	}
	b.WriteString("<h3>Producer scaling summary</h3>")
	b.WriteString("<p class=\"subtle\">Requested producers are used on the chart axis. Effective producers show capped runs when requested producers exceed the number of load batches.</p>")
	writeTable(b, []string{"indexes", "requested producers", "effective producers", "load batches", "TreeDB docs/sec", "MongoDB docs/sec", "TreeDB/MongoDB"}, body, numericColumns(0, 1, 2, 3, 4, 5, 6))
}

func fullSweepLoadNote(rows []mongoSummaryRow) string {
	note := "The load chart in this section is the load phase of the broader full sweep, not the pure ingest client-mode matrix. Use it when interpreting the read/range/update sweep as a whole; use the load-only matrix below when comparing client paths."
	if metadata := mongoLoadMetadataText(rows); metadata != "" {
		note += " " + metadata
	}
	for _, row := range rows {
		if row.Phase == "load_insert_many" && row.SecondaryIndexes == 0 && row.RangeIndex {
			note += " This run has range_index=true, so even the displayed 0-secondary-index load cell maintains the additional age_1 range index during insert."
			break
		}
	}
	return "<p class=\"subtle\"><strong>Load interpretation:</strong> " + esc(note) + "</p>"
}

func mongoLoadMetadataText(rows []mongoSummaryRow) string {
	loadRows := mongoRowsForPhase(rows, "load_insert_many")
	if len(loadRows) == 0 {
		return ""
	}
	first, ok := firstMongoLoadRowWithMetadata(loadRows)
	if !ok {
		return "Load metadata is unavailable in this summary; regenerate with a newer compare report to show batch size and producer counts."
	}
	for _, row := range loadRows {
		if !mongoLoadHasMetadata(row) || !sameMongoLoadMetadata(first, row) {
			return "Load rows use mixed or incomplete document, batch, or producer metadata; see the raw full-sweep TSV rows."
		}
	}
	parts := []string{fmt.Sprintf("%s docs", commaInt(int64(first.Documents)))}
	if first.BatchSize > 0 {
		parts = append(parts, fmt.Sprintf("batch size %s", commaInt(int64(first.BatchSize))))
	}
	requested := first.InsertProducers
	effective := mongoEffectiveLoadProducers(first)
	batches := mongoLoadBatchCount(first)
	switch {
	case requested > 0 && effective > 0 && requested != effective:
		if batches > 0 {
			parts = append(parts, fmt.Sprintf("requested insert producers %d, effective %d (capped by %d load batches)", requested, effective, batches))
		} else {
			parts = append(parts, fmt.Sprintf("requested insert producers %d, effective %d", requested, effective))
		}
	case requested > 0:
		parts = append(parts, fmt.Sprintf("insert producers %d", requested))
	case effective > 0:
		parts = append(parts, fmt.Sprintf("effective insert producers %d", effective))
	}
	if first.DriverCalls > 0 {
		parts = append(parts, fmt.Sprintf("driver calls %s", commaInt(int64(first.DriverCalls))))
	}
	return "Measured load metadata: " + strings.Join(parts, ", ") + "."
}

func firstMongoLoadRowWithMetadata(rows []mongoSummaryRow) (mongoSummaryRow, bool) {
	for _, row := range rows {
		if mongoLoadHasMetadata(row) {
			return row, true
		}
	}
	return mongoSummaryRow{}, false
}

func mongoLoadHasMetadata(row mongoSummaryRow) bool {
	return row.BatchSize > 0 || row.InsertProducers > 0 || row.EffectiveProducers > 0 || row.DriverCalls > 0 || row.LoadBatchCount > 0
}

func sameMongoLoadMetadata(a, b mongoSummaryRow) bool {
	return a.Documents == b.Documents &&
		a.BatchSize == b.BatchSize &&
		a.InsertProducers == b.InsertProducers &&
		mongoEffectiveLoadProducers(a) == mongoEffectiveLoadProducers(b) &&
		mongoLoadBatchCount(a) == mongoLoadBatchCount(b)
}

func mongoEffectiveLoadProducers(row mongoSummaryRow) int {
	effective := row.EffectiveProducers
	if effective <= 0 {
		effective = row.InsertProducers
	}
	if effective <= 0 {
		return 0
	}
	batches := mongoLoadBatchCount(row)
	if batches > 0 && effective > batches {
		return batches
	}
	return effective
}

func mongoLoadBatchCount(row mongoSummaryRow) int {
	if row.LoadBatchCount > 0 {
		return row.LoadBatchCount
	}
	if row.Documents <= 0 || row.BatchSize <= 0 {
		return 0
	}
	return (row.Documents + row.BatchSize - 1) / row.BatchSize
}

func renderMongoScaling(b *strings.Builder, byIndex map[int][]mongoSummaryRow) {
	if len(byIndex) == 0 {
		return
	}
	b.WriteString("<section id=\"scaling\"><h2>Mongo API Reader/Writer Scaling</h2>")
	b.WriteString("<p class=\"subtle\">Each scaling cell performs a fresh load before the measured concurrent phase. Writer and reader counts are explicit.</p>")
	indexes := make([]int, 0, len(byIndex))
	for idx := range byIndex {
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	for _, idx := range indexes {
		rows := byIndex[idx]
		indexLabel := indexCountLabel(idx)
		b.WriteString("<div class=\"chart-group\"><h3>" + esc(indexLabel) + ": throughput vs client count</h3>")
		b.WriteString("<p class=\"subtle\">Fresh " + esc(scalingDocumentLabel(rows)) + " load per cell, then a focused writer or reader concurrency sweep for this index count.</p>")
		b.WriteString("<div class=\"chart-grid\">")
		writerCounts := mongoSweepCounts(rows, idx, "concurrent_id_update_set_w")
		if len(writerCounts) > 0 {
			b.WriteString(lineChart("_id update one: throughput vs writer client count, "+indexLabel, countLabels(writerCounts), "client count", "ops/sec", []chartSeries{
				{Name: "TreeDB", Values: mongoSweepAny(rows, idx, "concurrent_id_update_set_w", "tree", writerCounts), Color: "#2867c7"},
				{Name: "MongoDB", Values: mongoSweepAny(rows, idx, "concurrent_id_update_set_w", "mongo", writerCounts), Color: "#1f8a5b"},
			}, "ops/sec"))
		}
		for _, spec := range mongoConcurrentReadSweepSpecs() {
			readerCounts := mongoSweepCounts(rows, idx, spec.Prefix)
			if len(readerCounts) == 0 {
				continue
			}
			b.WriteString(lineChart(spec.Title+", "+indexLabel, countLabels(readerCounts), "client count", "ops/sec", []chartSeries{
				{Name: "TreeDB", Values: mongoSweepAny(rows, idx, spec.Prefix, "tree", readerCounts), Color: "#2867c7"},
				{Name: "MongoDB", Values: mongoSweepAny(rows, idx, spec.Prefix, "mongo", readerCounts), Color: "#1f8a5b"},
			}, "ops/sec"))
		}
		b.WriteString("</div>")
		summaryRows := mongoOperationThroughputRows(rows, idx, false)
		if len(summaryRows) > 0 {
			writeMongoThroughputSummary(b, summaryRows)
		}
		b.WriteString("</div>")
	}
	for _, idx := range indexes {
		b.WriteString("<details><summary>Raw scaling TSV rows, " + esc(indexCountLowerLabel(idx)) + "</summary>")
		writeMongoSummaryTable(b, byIndex[idx])
		b.WriteString("</details>")
	}
	b.WriteString("</section>\n")
}

func scalingDocumentLabel(rows []mongoSummaryRow) string {
	for _, row := range rows {
		if row.Documents > 0 {
			return commaInt(int64(row.Documents)) + "-document"
		}
	}
	return "per-cell"
}

func writeMongoSummaryTable(b *strings.Builder, rows []mongoSummaryRow) {
	var body [][]string
	for _, row := range rows {
		body = append(body, []string{
			strconv.Itoa(row.Documents),
			strconv.Itoa(row.SecondaryIndexes),
			strconv.FormatBool(row.RangeIndex),
			emptyDash(row.RangeMode),
			row.TreeDBConfig,
			emptyDash(row.MongoConfig),
			row.Phase,
			formatOptionalInt(row.BatchSize),
			formatOptionalInt(row.InsertProducers),
			formatOptionalInt(mongoEffectiveLoadProducers(row)),
			formatOptionalInt(row.DriverCalls),
			formatOptionalInt(mongoLoadBatchCount(row)),
			fmtOps(row.TreeDBOpsSec),
			fmtOps(row.MongoOpsSec),
			fmtRatio(row.TreeDBToMongoRatio),
			fmtFloat(row.TreeDBP95US, 0),
			fmtFloat(row.MongoP95US, 0),
			emptyDash(row.TreeDBDiskSnapshot),
			fmtBytes(row.TreeDBDiskBytes),
			fmtBytes(row.TreeDBPhysicalBytes),
			fmtBytes(row.MongoDBStatsTotalSize),
			fmtBytes(row.MongoPhysicalBytes),
			row.Source,
		})
	}
	writeTable(b, []string{"docs", "indexes", "range index", "range mode", "TreeDB config", "Mongo config", "phase", "batch size", "insert producers", "effective producers", "driver calls", "load batches", "TreeDB ops/s", "Mongo ops/s", "ratio", "TreeDB p95 us", "Mongo p95 us", "TreeDB disk snapshot", "TreeDB logical bytes", "TreeDB physical", "Mongo dbStats total", "Mongo physical", "source"}, body, numericColumns(0, 1, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 19, 20, 21))
}

func formatOptionalInt(value int) string {
	if value <= 0 {
		return "n/a"
	}
	return commaInt(int64(value))
}

func sortedMongoIndexes(rows []mongoSummaryRow) []int {
	seen := make(map[int]bool)
	for _, row := range rows {
		seen[row.SecondaryIndexes] = true
	}
	var out []int
	for idx := range seen {
		out = append(out, idx)
	}
	sort.Ints(out)
	return out
}

func mongoRowsForIndex(rows []mongoSummaryRow, idx int) []mongoSummaryRow {
	var out []mongoSummaryRow
	for _, row := range rows {
		if row.SecondaryIndexes == idx {
			out = append(out, row)
		}
	}
	return out
}

func indexNumberLabels(indexes []int) []string {
	out := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		out = append(out, strconv.Itoa(idx))
	}
	return out
}

func mongoRowsForPhase(rows []mongoSummaryRow, phase string) []mongoSummaryRow {
	var out []mongoSummaryRow
	for _, idx := range sortedMongoIndexes(rows) {
		row, ok := mongoRow(rows, idx, phase)
		if !ok {
			continue
		}
		out = append(out, row)
	}
	return out
}

type mongoThroughputOperationSpec struct {
	Label         string
	SinglePhases  []string
	SweepPrefixes []string
	CountUnit     string
}

type mongoThroughputOperationRow struct {
	Label          string
	Single         mongoSummaryRow
	HasSingle      bool
	RatioBest      mongoSummaryRow
	RatioBestCount int
	HasRatioBest   bool
	TreePeak       mongoSummaryRow
	TreePeakCount  int
	HasTreePeak    bool
	MongoPeak      mongoSummaryRow
	MongoPeakCount int
	HasMongoPeak   bool
	CountUnit      string
}

func mongoOperationThroughputRows(rows []mongoSummaryRow, idx int, primaryScopeOnly bool) []mongoThroughputOperationRow {
	specs := []mongoThroughputOperationSpec{
		{
			Label:         "_id read",
			SinglePhases:  []string{"id_find_one"},
			SweepPrefixes: []string{"concurrent_id_find_one_r"},
			CountUnit:     "reader",
		},
		{
			Label:         "email read",
			SinglePhases:  []string{"email_find_one"},
			SweepPrefixes: []string{"concurrent_email_find_one_r"},
			CountUnit:     "reader",
		},
		{
			Label:         "range read",
			SinglePhases:  []string{"age_range_indexed_limit_10", "age_range_scan_limit_10"},
			SweepPrefixes: []string{"concurrent_age_range_indexed_limit_10_r", "concurrent_age_range_scan_limit_10_r"},
			CountUnit:     "reader",
		},
		{
			Label:         "update",
			SinglePhases:  []string{"id_update_set"},
			SweepPrefixes: []string{"concurrent_id_update_set_w"},
			CountUnit:     "writer",
		},
	}
	out := make([]mongoThroughputOperationRow, 0, len(specs))
	for _, spec := range specs {
		item := mongoThroughputOperationRow{Label: spec.Label, CountUnit: spec.CountUnit}
		scope, hasScope := mongoPrimaryScope(rows, idx)
		include := func(row mongoSummaryRow) bool {
			return !primaryScopeOnly || !hasScope || mongoSameScope(row, scope)
		}
		item.Single, item.HasSingle = mongoFirstPhaseRowFiltered(rows, idx, spec.SinglePhases, include)
		item.RatioBest, item.RatioBestCount, item.HasRatioBest = mongoBestSweepRowByRatioFiltered(rows, idx, spec.SweepPrefixes, include)
		item.TreePeak, item.TreePeakCount, item.HasTreePeak = mongoBestSweepRowBySide(rows, idx, spec.SweepPrefixes, "tree", include)
		item.MongoPeak, item.MongoPeakCount, item.HasMongoPeak = mongoBestSweepRowBySide(rows, idx, spec.SweepPrefixes, "mongo", include)
		if !item.HasSingle && !item.HasRatioBest && !item.HasTreePeak && !item.HasMongoPeak {
			continue
		}
		out = append(out, item)
	}
	return out
}

func mongoFirstPhaseRow(rows []mongoSummaryRow, idx int, phases []string) (mongoSummaryRow, bool) {
	return mongoFirstPhaseRowFiltered(rows, idx, phases, nil)
}

func mongoFirstPhaseRowFiltered(rows []mongoSummaryRow, idx int, phases []string, include func(mongoSummaryRow) bool) (mongoSummaryRow, bool) {
	for _, phase := range phases {
		for _, row := range rows {
			if row.SecondaryIndexes != idx || row.Phase != phase {
				continue
			}
			if include != nil && !include(row) {
				continue
			}
			return row, true
		}
	}
	return mongoSummaryRow{}, false
}

func writeMongoThroughputSummary(b *strings.Builder, rows []mongoThroughputOperationRow) {
	var body [][]string
	for _, row := range rows {
		body = append(body, []string{
			row.Label,
			mongoSingleOps(row),
			mongoPeakOps(row, "tree"),
			mongoPeakCountLabel(row, "tree"),
			mongoPeakOps(row, "mongo"),
			mongoPeakCountLabel(row, "mongo"),
			mongoBestRatio(row),
			mongoRatioCountLabel(row),
			mongoRatioOps(row, "tree"),
			mongoRatioOps(row, "mongo"),
			mongoScaleUpRatio(row),
		})
	}
	if len(body) == 0 {
		return
	}
	b.WriteString("<h4>Throughput summary</h4>")
	b.WriteString("<p class=\"subtle\">Peak columns show best observed throughput in the saturated sweep. The ratio columns show the largest same-client TreeDB/MongoDB ratio and the client count where that ratio occurred.</p>")
	writeTable(b, []string{"operation", "single-threaded TreeDB", "peak TreeDB", "TreeDB clients", "peak MongoDB", "MongoDB clients", "largest TreeDB/MongoDB", "ratio clients", "TreeDB at ratio", "MongoDB at ratio", "TreeDB scale-up"}, body, numericColumns(1, 2, 4, 6, 8, 9, 10))
}

func writeSummaryCell(b *strings.Builder, label, value string) {
	b.WriteString("<div class=\"summary-cell\"><div class=\"label\">" + esc(label) + "</div><div class=\"value\">" + esc(value) + "</div></div>")
}

func mongoRatioOps(row mongoThroughputOperationRow, side string) string {
	if !row.HasRatioBest {
		return "-"
	}
	if side == "mongo" {
		return fmtOps(row.RatioBest.MongoOpsSec)
	}
	return fmtOps(row.RatioBest.TreeDBOpsSec)
}

func mongoSingleOps(row mongoThroughputOperationRow) string {
	if !row.HasSingle {
		return "-"
	}
	return fmtOps(row.Single.TreeDBOpsSec)
}

func mongoPeakOps(row mongoThroughputOperationRow, side string) string {
	if side == "mongo" {
		if !row.HasMongoPeak {
			return "-"
		}
		return fmtOps(row.MongoPeak.MongoOpsSec)
	}
	if !row.HasTreePeak {
		return "-"
	}
	return fmtOps(row.TreePeak.TreeDBOpsSec)
}

func mongoPeakCountLabel(row mongoThroughputOperationRow, side string) string {
	if side == "mongo" {
		return mongoBestCountLabel(row.MongoPeakCount, row.CountUnit, row.HasMongoPeak)
	}
	return mongoBestCountLabel(row.TreePeakCount, row.CountUnit, row.HasTreePeak)
}

func mongoBestRatio(row mongoThroughputOperationRow) string {
	if !row.HasRatioBest || row.RatioBest.MongoOpsSec <= 0 {
		return "-"
	}
	return fmtRatio(row.RatioBest.TreeDBOpsSec / row.RatioBest.MongoOpsSec)
}

func mongoRatioCountLabel(row mongoThroughputOperationRow) string {
	if !row.HasRatioBest {
		return "-"
	}
	return mongoBestCountLabel(row.RatioBestCount, row.CountUnit, true)
}

func mongoScaleUpRatio(row mongoThroughputOperationRow) string {
	if !row.HasSingle || !row.HasRatioBest || row.Single.TreeDBOpsSec <= 0 {
		return "-"
	}
	return fmtRatio(row.RatioBest.TreeDBOpsSec / row.Single.TreeDBOpsSec)
}

func mongoBestCountLabel(count int, unit string, ok bool) string {
	if !ok || count <= 0 {
		return "-"
	}
	if count == 1 {
		return "1 " + unit
	}
	return fmt.Sprintf("%d %ss", count, unit)
}

type mongoReadSweepSpec struct {
	Prefix string
	Title  string
}

func mongoConcurrentReadSweepSpecs() []mongoReadSweepSpec {
	return []mongoReadSweepSpec{
		{Prefix: "concurrent_id_find_one_r", Title: "_id find one: throughput vs reader clients"},
		{Prefix: "concurrent_email_find_one_r", Title: "email find one: throughput vs reader clients"},
		{Prefix: "concurrent_age_range_indexed_limit_10_r", Title: "age range limit 10: throughput vs reader clients"},
		{Prefix: "concurrent_age_range_scan_limit_10_r", Title: "age range scan limit 10: throughput vs reader clients"},
	}
}

func mongoRowIndexLabels(rows []mongoSummaryRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, strconv.Itoa(row.SecondaryIndexes))
	}
	return out
}

func mongoRowOps(rows []mongoSummaryRow, side string) []float64 {
	out := make([]float64, 0, len(rows))
	for _, row := range rows {
		if side == "mongo" {
			out = append(out, row.MongoOpsSec)
		} else {
			out = append(out, row.TreeDBOpsSec)
		}
	}
	return out
}

func mongoRowDisk(rows []mongoSummaryRow, side string) []float64 {
	out := make([]float64, 0, len(rows))
	for _, row := range rows {
		if side == "mongo" {
			if row.MongoPhysicalBytes > 0 {
				out = append(out, row.MongoPhysicalBytes)
			} else {
				out = append(out, row.MongoDBStatsTotalSize)
			}
		} else {
			out = append(out, row.TreeDBPhysicalBytes)
		}
	}
	return out
}

func writeMongoIndexRetentionTable(b *strings.Builder, rows []mongoSummaryRow) {
	if len(rows) == 0 {
		return
	}
	var baseline mongoSummaryRow
	var hasBaseline bool
	for _, row := range rows {
		if row.SecondaryIndexes == 0 {
			baseline = row
			hasBaseline = true
			break
		}
	}
	if !hasBaseline {
		return
	}
	var body [][]string
	for _, row := range rows {
		body = append(body, []string{
			strconv.Itoa(row.SecondaryIndexes),
			fmtOps(row.TreeDBOpsSec),
			fmtOps(row.MongoOpsSec),
			fmtPercentRatio(ratioOrZero(row.TreeDBOpsSec, baseline.TreeDBOpsSec)),
			fmtPercentRatio(ratioOrZero(row.MongoOpsSec, baseline.MongoOpsSec)),
			fmtRatio(row.TreeDBToMongoRatio),
			fmtRatio(row.TreeDBToMongoPhysRatio),
		})
	}
	b.WriteString("<h3>Index throughput retention</h3>")
	b.WriteString("<p class=\"subtle\">Retention is each load throughput divided by the 0-secondary-index load throughput for the same database.</p>")
	writeTable(b, []string{"secondary indexes", "TreeDB docs/sec", "MongoDB docs/sec", "TreeDB retained", "MongoDB retained", "TreeDB/MongoDB throughput", "TreeDB/MongoDB physical bytes"}, body, numericColumns(0, 1, 2, 3, 4, 5, 6))
}

func mongoStorageBasisText(rows []mongoSummaryRow) string {
	treeBasis := "TreeDB physical bytes from summary.tsv"
	if len(rows) > 0 {
		snapshots := make(map[string]bool)
		for _, row := range rows {
			snapshot := strings.TrimSpace(row.TreeDBDiskSnapshot)
			if snapshot != "" {
				snapshots[snapshot] = true
			}
		}
		if len(snapshots) == 1 {
			for snapshot := range snapshots {
				treeBasis = "TreeDB physical bytes from " + snapshot + " snapshot"
				if snapshot == "maintenance" {
					treeBasis = "TreeDB physical bytes after maintenance"
				}
			}
		}
	}
	mongoPhysical := 0
	mongoDBStats := 0
	for _, row := range rows {
		if row.MongoPhysicalBytes > 0 {
			mongoPhysical++
		} else if row.MongoDBStatsTotalSize > 0 {
			mongoDBStats++
		}
	}
	mongoBasis := "MongoDB storage bytes from summary.tsv"
	switch {
	case mongoPhysical > 0 && mongoDBStats == 0:
		mongoBasis = "MongoDB measured data-directory bytes"
	case mongoPhysical == 0 && mongoDBStats > 0:
		mongoBasis = "MongoDB dbStats.totalSize from this run"
	case mongoPhysical > 0 && mongoDBStats > 0:
		mongoBasis = "MongoDB mixed measured data-directory bytes and dbStats.totalSize; see raw rows"
	}
	return treeBasis + "; " + mongoBasis + "."
}

func mongoRow(rows []mongoSummaryRow, idx int, phase string) (mongoSummaryRow, bool) {
	scope, hasScope := mongoPrimaryScope(rows, idx)
	var fallback mongoSummaryRow
	var hasFallback bool
	for _, row := range rows {
		if row.SecondaryIndexes != idx || row.Phase != phase {
			continue
		}
		if !hasFallback {
			fallback = row
			hasFallback = true
		}
		if !hasScope || mongoSameScope(row, scope) {
			return row, true
		}
	}
	if hasFallback {
		return fallback, true
	}
	return mongoSummaryRow{}, false
}

func mongoPrimaryScope(rows []mongoSummaryRow, idx int) (mongoSummaryRow, bool) {
	var candidates []mongoSummaryRow
	for _, row := range rows {
		if row.SecondaryIndexes == idx && row.Phase == "load_insert_many" {
			candidates = append(candidates, row)
		}
	}
	if len(candidates) == 0 {
		return mongoSummaryRow{}, false
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Documents != candidates[j].Documents {
			return candidates[i].Documents > candidates[j].Documents
		}
		if candidates[i].TreeDBConfig != candidates[j].TreeDBConfig {
			return candidates[i].TreeDBConfig < candidates[j].TreeDBConfig
		}
		if candidates[i].MongoConfig != candidates[j].MongoConfig {
			return candidates[i].MongoConfig < candidates[j].MongoConfig
		}
		if candidates[i].RangeIndex != candidates[j].RangeIndex {
			return !candidates[i].RangeIndex && candidates[j].RangeIndex
		}
		return candidates[i].RangeMode < candidates[j].RangeMode
	})
	return candidates[0], true
}

func mongoSameScope(row, scope mongoSummaryRow) bool {
	// RangeMode describes the read phase variant; load rows often leave it empty.
	rangeModeMatches := scope.RangeMode == "" || row.RangeMode == "" || row.RangeMode == scope.RangeMode
	return row.Documents == scope.Documents &&
		row.TreeDBConfig == scope.TreeDBConfig &&
		row.MongoConfig == scope.MongoConfig &&
		row.RangeIndex == scope.RangeIndex &&
		rangeModeMatches
}

func mongoScopeText(scope mongoSummaryRow) string {
	parts := []string{
		fmt.Sprintf("%s docs", commaInt(int64(scope.Documents))),
		"TreeDB " + emptyDash(scope.TreeDBConfig),
		"MongoDB " + emptyDash(scope.MongoConfig),
		"range index=" + strconv.FormatBool(scope.RangeIndex),
	}
	if strings.TrimSpace(scope.RangeMode) != "" {
		parts = append(parts, "range mode="+scope.RangeMode)
	}
	return strings.Join(parts, ", ")
}

func mongoSweepCounts(rows []mongoSummaryRow, idx int, prefix string) []int {
	return mongoSweepCountsFiltered(rows, idx, prefix, nil)
}

func mongoSweepCountsInPrimaryScope(rows []mongoSummaryRow, idx int, prefix string) []int {
	scope, hasScope := mongoPrimaryScope(rows, idx)
	if !hasScope {
		return mongoSweepCounts(rows, idx, prefix)
	}
	return mongoSweepCountsFiltered(rows, idx, prefix, func(row mongoSummaryRow) bool {
		return mongoSameScope(row, scope)
	})
}

func mongoBestSweepRowByRatio(rows []mongoSummaryRow, idx int, prefixes []string) (mongoSummaryRow, int, bool) {
	return mongoBestSweepRowByRatioFiltered(rows, idx, prefixes, nil)
}

func mongoBestSweepRowByRatioFiltered(rows []mongoSummaryRow, idx int, prefixes []string, include func(mongoSummaryRow) bool) (mongoSummaryRow, int, bool) {
	var best mongoSummaryRow
	var bestCount int
	var bestRatio float64
	var ok bool
	for _, row := range rows {
		prefix, hasPrefix := mongoSweepPrefix(row.Phase, prefixes)
		if row.SecondaryIndexes != idx || !hasPrefix {
			continue
		}
		if include != nil && !include(row) {
			continue
		}
		if row.TreeDBOpsSec <= 0 || row.MongoOpsSec <= 0 {
			continue
		}
		count, err := strconv.Atoi(strings.TrimPrefix(row.Phase, prefix))
		if err != nil || count <= 0 {
			continue
		}
		ratio := row.TreeDBOpsSec / row.MongoOpsSec
		if !ok || ratio > bestRatio || (ratio == bestRatio && count > bestCount) {
			best = row
			bestCount = count
			bestRatio = ratio
			ok = true
		}
	}
	return best, bestCount, ok
}

func mongoBestSweepRowBySide(rows []mongoSummaryRow, idx int, prefixes []string, side string, include func(mongoSummaryRow) bool) (mongoSummaryRow, int, bool) {
	var best mongoSummaryRow
	var bestCount int
	var bestOps float64
	var ok bool
	for _, row := range rows {
		prefix, hasPrefix := mongoSweepPrefix(row.Phase, prefixes)
		if row.SecondaryIndexes != idx || !hasPrefix {
			continue
		}
		if include != nil && !include(row) {
			continue
		}
		count, err := strconv.Atoi(strings.TrimPrefix(row.Phase, prefix))
		if err != nil || count <= 0 {
			continue
		}
		ops := row.TreeDBOpsSec
		if side == "mongo" {
			ops = row.MongoOpsSec
		}
		if ops <= 0 {
			continue
		}
		if !ok || ops > bestOps || (ops == bestOps && count > bestCount) {
			best = row
			bestCount = count
			bestOps = ops
			ok = true
		}
	}
	return best, bestCount, ok
}

func mongoSweepPrefix(phase string, prefixes []string) (string, bool) {
	for _, prefix := range prefixes {
		if strings.HasPrefix(phase, prefix) {
			return prefix, true
		}
	}
	return "", false
}

func mongoSweepCountsFiltered(rows []mongoSummaryRow, idx int, prefix string, include func(mongoSummaryRow) bool) []int {
	seen := make(map[int]bool)
	for _, row := range rows {
		if row.SecondaryIndexes != idx || !strings.HasPrefix(row.Phase, prefix) {
			continue
		}
		if include != nil && !include(row) {
			continue
		}
		count, err := strconv.Atoi(strings.TrimPrefix(row.Phase, prefix))
		if err != nil || count <= 0 {
			continue
		}
		seen[count] = true
	}
	out := make([]int, 0, len(seen))
	for count := range seen {
		out = append(out, count)
	}
	sort.Ints(out)
	return out
}

func countLabels(counts []int) []string {
	out := make([]string, 0, len(counts))
	for _, count := range counts {
		out = append(out, strconv.Itoa(count))
	}
	return out
}

func mongoRowAny(rows []mongoSummaryRow, idx int, phase string) (mongoSummaryRow, bool) {
	for _, row := range rows {
		if row.SecondaryIndexes == idx && row.Phase == phase {
			return row, true
		}
	}
	return mongoSummaryRow{}, false
}

func mongoSweep(rows []mongoSummaryRow, idx int, prefix, side string, counts []int) []float64 {
	return mongoSweepWithLookup(rows, idx, prefix, side, counts, mongoRow)
}

func mongoSweepAny(rows []mongoSummaryRow, idx int, prefix, side string, counts []int) []float64 {
	return mongoSweepWithLookup(rows, idx, prefix, side, counts, mongoRowAny)
}

func mongoSweepWithLookup(rows []mongoSummaryRow, idx int, prefix, side string, counts []int, lookup func([]mongoSummaryRow, int, string) (mongoSummaryRow, bool)) []float64 {
	var out []float64
	for _, count := range counts {
		row, ok := lookup(rows, idx, prefix+strconv.Itoa(count))
		if !ok {
			out = append(out, math.NaN())
			continue
		}
		if side == "mongo" {
			out = append(out, row.MongoOpsSec)
		} else {
			out = append(out, row.TreeDBOpsSec)
		}
	}
	return out
}

func sortedLoadModeIndexes(rows []loadModeRow) []int {
	seen := make(map[int]bool)
	for _, row := range rows {
		seen[row.Indexes] = true
	}
	var out []int
	for idx := range seen {
		out = append(out, idx)
	}
	sort.Ints(out)
	return out
}

func loadModeChartRows(rows []loadModeRow, idx int) ([]string, []float64, []float64) {
	return loadModeValueRows(rows, idx, func(row loadModeRow) float64 { return row.OpsPerSec })
}

func loadModeDiskChartRows(rows []loadModeRow, idx int) ([]string, []float64, []float64) {
	return loadModeValueRows(rows, idx, func(row loadModeRow) float64 { return float64(row.PhysicalBytes) })
}

func loadModeStorageBasisText(rows []loadModeRow) string {
	if loadModeHasUnavailableMongoPhysicalBytes(rows) {
		return "TreeDB physical_bytes from matrix.tsv; MongoDB physical_bytes unavailable in this matrix and omitted from storage bars."
	}
	if loadModeHasMongoPhysicalBytes(rows) {
		return "TreeDB and MongoDB physical_bytes from matrix.tsv."
	}
	return "TreeDB physical_bytes from matrix.tsv; no MongoDB physical storage row is available in this matrix."
}

func loadModeHasMongoPhysicalBytes(rows []loadModeRow) bool {
	for _, row := range rows {
		if row.Target == "mongo" && row.PhysicalBytes > 0 {
			return true
		}
	}
	return false
}

func loadModeHasUnavailableMongoPhysicalBytes(rows []loadModeRow) bool {
	for _, row := range rows {
		if row.Target == "mongo" && row.PhysicalBytes == 0 {
			return true
		}
	}
	return false
}

func loadModePhysicalBytesCell(row loadModeRow) string {
	if row.Target == "mongo" && row.PhysicalBytes == 0 {
		return "unavailable"
	}
	return fmtBytes(float64(row.PhysicalBytes))
}

func loadModeValueRows(rows []loadModeRow, idx int, value func(loadModeRow) float64) ([]string, []float64, []float64) {
	type pair struct {
		TreeDB float64
		Mongo  float64
	}
	pairs := make(map[string]pair)
	for _, row := range rows {
		if row.Indexes != idx {
			continue
		}
		mode := loadModeLabel(row)
		pair := pairs[mode]
		switch row.Target {
		case "treedb":
			pair.TreeDB = value(row)
		case "mongo":
			pair.Mongo = value(row)
		}
		pairs[mode] = pair
	}
	var labels []string
	for mode := range pairs {
		labels = append(labels, mode)
	}
	sort.Slice(labels, func(i, j int) bool {
		return loadModeOrder(labels[i]) < loadModeOrder(labels[j])
	})
	tree := make([]float64, 0, len(labels))
	mongo := make([]float64, 0, len(labels))
	for _, mode := range labels {
		pair := pairs[mode]
		tree = append(tree, pair.TreeDB)
		mongo = append(mongo, pair.Mongo)
	}
	return labels, tree, mongo
}

func isTreeDBOnlyLoadMode(mode string) bool {
	return loadModeKind(mode) != ""
}

func loadModeDisplayLabel(mode string) string {
	if isTreeDBOnlyLoadMode(mode) {
		return mode + " *"
	}
	return mode
}

func loadModeLabel(row loadModeRow) string {
	config := row.Config
	if config == row.Target {
		config = row.Target + "_driver"
	}
	config = strings.TrimPrefix(config, row.Target+"_")
	config = strings.TrimPrefix(config, "bson_")
	return "BSON " + config
}

func loadModeOrder(mode string) string {
	order := map[string]int{
		"BSON driver":             0,
		"BSON driver_command":     1,
		"BSON driver_command_raw": 2,
		"BSON driver_unack":       3,
		"BSON direct":             4,
		"BSON raw_wire_tcp":       5,
		"BSON raw_wire":           6,
	}
	if idx, ok := order[mode]; ok {
		return fmt.Sprintf("%02d_%s", idx, mode)
	}
	switch loadModeKind(mode) {
	case "direct":
		return "04_" + mode
	case "raw_wire_tcp":
		return "05_" + mode
	case "raw_wire":
		return "06_" + mode
	}
	return "99_" + mode
}

func loadModeKind(mode string) string {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	normalized = strings.TrimPrefix(normalized, "bson ")
	switch {
	case normalized == "direct" || strings.HasSuffix(normalized, "_direct"):
		return "direct"
	case normalized == "raw_wire_tcp" || strings.HasSuffix(normalized, "_raw_wire_tcp"):
		return "raw_wire_tcp"
	case normalized == "raw_wire" || strings.HasSuffix(normalized, "_raw_wire"):
		return "raw_wire"
	default:
		return ""
	}
}

func indexCountLabel(indexes int) string {
	if indexes == 1 {
		return "1 index"
	}
	return fmt.Sprintf("%d indexes", indexes)
}

func indexCountTitleLabel(indexes int) string {
	if indexes == 1 {
		return "1 Index"
	}
	return fmt.Sprintf("%d Indexes", indexes)
}

func indexCountLowerLabel(indexes int) string {
	if indexes == 1 {
		return "1 index"
	}
	return fmt.Sprintf("%d indexes", indexes)
}

type chartSeries struct {
	Name   string
	Values []float64
	Color  string
}

func rawEngineGroupedBarChart(title string, rows []rawEngineRun, tests []string, variants []rawEngineVariant) string {
	if len(tests) == 0 || len(variants) == 0 {
		return ""
	}
	const width = 1600.0
	const height = 560.0
	const left = 82.0
	const top = 28.0
	const right = 26.0
	const bottom = 148.0
	plotW := width - left - right
	plotH := height - top - bottom
	axisY := top + plotH
	maxV := 0.0
	for _, test := range tests {
		for _, variant := range variants {
			if v, ok := rawEngineResult(rows, variant.Profile, variant.Checkpoint, test); ok && v > maxV {
				maxV = v
			}
		}
	}
	if maxV <= 0 {
		maxV = 1
	}
	maxV *= 1.08
	y := func(v float64) float64 {
		return top + plotH - (v/maxV)*plotH
	}
	groupW := plotW / float64(len(tests))
	barGap := math.Max(1.0, groupW*0.025)
	innerPad := math.Max(4.0, groupW*0.10)
	barW := (groupW - innerPad*2 - barGap*float64(len(variants)-1)) / float64(len(variants))
	if barW < 1 {
		barW = 1
	}
	var b strings.Builder
	b.WriteString(chartHeader(title, rawEngineVariantSeries(variants)))
	b.WriteString("<svg width=\"1600\" height=\"560\" viewBox=\"0 0 1600 560\" role=\"img\" aria-label=\"" + esc(chartLabel(title)) + ", x axis Benchmark Test, y axis Ops/Sec\">")
	for tick := 0; tick <= 4; tick++ {
		value := maxV * float64(tick) / 4
		yy := y(value)
		b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#edf2f7\"/>", left, yy, left+plotW, yy))
		b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"end\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">%s</text>", left-7, yy+4, esc(shortNumber(value))))
	}
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, axisY, left+plotW, axisY))
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, top, left, axisY))
	for i, test := range tests {
		groupX := left + float64(i)*groupW
		if i > 0 {
			b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#dbe4ee\"/>", groupX, top, groupX, axisY+8))
		}
		largestVariant := -1
		largestValue := 0.0
		for vidx, variant := range variants {
			if v, ok := rawEngineResult(rows, variant.Profile, variant.Checkpoint, test); ok && v > largestValue {
				largestValue = v
				largestVariant = vidx
			}
		}
		for vidx, variant := range variants {
			v, ok := rawEngineResult(rows, variant.Profile, variant.Checkpoint, test)
			if !ok || v <= 0 {
				continue
			}
			barH := (v / maxV) * plotH
			x := groupX + innerPad + float64(vidx)*(barW+barGap)
			yy := axisY - barH
			b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"%s\"><title>%s, %s: %s ops/sec</title></rect>", x, yy, barW, barH, esc(variant.Color), esc(test), esc(variant.Label), esc(formatChartValue(v, "ops/sec"))))
			if vidx == largestVariant && barW >= 10 && v > 0 {
				b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+barW/2, math.Max(top+10, yy-5), esc(shortNumber(v))))
			}
		}
		labelX := groupX + groupW/2
		for lineIdx, line := range chartLabelLines(test, 13, 4) {
			b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">%s</text>", labelX, axisY+24+float64(lineIdx)*15, esc(line)))
		}
	}
	b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"13\" font-weight=\"700\" fill=\"#344256\">Benchmark Test</text>", left+plotW/2, axisY+94))
	b.WriteString(fmt.Sprintf("<text transform=\"translate(16 %.1f) rotate(-90)\" text-anchor=\"middle\" font-size=\"13\" font-weight=\"700\" fill=\"#344256\">Ops/Sec</text>", top+plotH/2))
	b.WriteString("</svg>")
	b.WriteString("</div>")
	return b.String()
}

func rawEngineVariantSeries(variants []rawEngineVariant) []chartSeries {
	var out []chartSeries
	for _, variant := range variants {
		out = append(out, chartSeries{Name: variant.Label, Color: variant.Color})
	}
	return out
}

func lineChart(title string, labels []string, xAxisLabel, yAxisLabel string, series []chartSeries, unit string) string {
	const width = 680.0
	const height = 330.0
	const left = 82.0
	const top = 24.0
	const right = 20.0
	const bottom = 68.0
	plotW := width - left - right
	plotH := height - top - bottom
	maxY := 0.0
	for _, s := range series {
		for _, v := range s.Values {
			if !isFinite(v) {
				continue
			}
			if v > maxY {
				maxY = v
			}
		}
	}
	if maxY <= 0 {
		maxY = 1
	}
	maxY *= 1.08
	x := func(i int) float64 {
		if len(labels) <= 1 {
			return left + plotW/2
		}
		return left + float64(i)*plotW/float64(len(labels)-1)
	}
	y := func(v float64) float64 {
		return top + plotH - (v/maxY)*plotH
	}
	var b strings.Builder
	b.WriteString(chartHeader(title, series))
	b.WriteString("<svg width=\"680\" height=\"330\" viewBox=\"0 0 680 330\" role=\"img\" aria-label=\"" + esc(chartLabel(title)) + ", x axis " + esc(chartLabel(xAxisLabel)) + ", y axis " + esc(chartLabel(yAxisLabel)) + "\">")
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, top+plotH, left+plotW, top+plotH))
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, top, left, top+plotH))
	for i, label := range labels {
		xx := x(i)
		b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">%s</text>", xx, height-34, esc(chartLabel(label))))
	}
	for tick := 0; tick <= 4; tick++ {
		value := maxY * float64(tick) / 4
		yy := y(value)
		b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#edf2f7\"/>", left, yy, left+plotW, yy))
		b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"end\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">%s</text>", left-7, yy+4, esc(shortNumber(value))))
	}
	for _, s := range series {
		points := make([]string, 0, len(s.Values))
		flushPoints := func() {
			if len(points) >= 2 {
				b.WriteString("<polyline fill=\"none\" stroke=\"" + esc(s.Color) + "\" stroke-width=\"2.5\" points=\"" + strings.Join(points, " ") + "\"/>")
			}
			points = points[:0]
		}
		for i, v := range s.Values {
			if !isFinite(v) {
				flushPoints()
				continue
			}
			points = append(points, fmt.Sprintf("%.1f,%.1f", x(i), y(v)))
		}
		flushPoints()
		for i, v := range s.Values {
			if !isFinite(v) {
				continue
			}
			b.WriteString(fmt.Sprintf("<circle cx=\"%.1f\" cy=\"%.1f\" r=\"3.2\" fill=\"%s\"><title>%s %s: %s</title></circle>", x(i), y(v), esc(s.Color), esc(s.Name), esc(labels[i]), esc(formatChartTooltipValue(v, unit))))
		}
	}
	b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"13\" font-weight=\"700\" fill=\"#344256\">%s</text>", left+plotW/2, height-10, esc(chartLabel(xAxisLabel))))
	b.WriteString(fmt.Sprintf("<text transform=\"translate(16 %.1f) rotate(-90)\" text-anchor=\"middle\" font-size=\"13\" font-weight=\"700\" fill=\"#344256\">%s</text>", top+plotH/2, esc(chartLabel(yAxisLabel))))
	b.WriteString("</svg>")
	b.WriteString("</div>")
	return b.String()
}

func verticalBarChart(title string, categories []string, xAxisLabel string, series []chartSeries, unit string) string {
	return verticalBarChartWithSize(title, categories, xAxisLabel, series, unit, 430, 112, 11, 4)
}

func compactVerticalBarChart(title string, categories []string, xAxisLabel string, series []chartSeries, unit string) string {
	return verticalBarChartWithSize(title, categories, xAxisLabel, series, unit, 330, 86, 14, 2)
}

func loadModeBarChart(title string, categories []string, tree, mongo []float64, unit, yAxisLabel string) string {
	if len(categories) == 0 {
		return ""
	}
	maxV := 0.0
	for i := range categories {
		if i < len(tree) && tree[i] > maxV {
			maxV = tree[i]
		}
		if i < len(mongo) && mongo[i] > maxV {
			maxV = mongo[i]
		}
	}
	if maxV <= 0 {
		maxV = 1
	}
	maxV *= 1.08
	const width = 760.0
	const height = 430.0
	const left = 76.0
	const right = 18.0
	const top = 22.0
	const bottom = 112.0
	plotW := width - left - right
	plotH := height - top - bottom
	axisY := top + plotH
	groupW := plotW / float64(len(categories))
	barGap := math.Max(1.0, groupW*0.04)
	innerPad := math.Max(3.0, groupW*0.16)
	pairedBarW := (groupW - innerPad*2 - barGap) / 2
	if pairedBarW < 1 {
		pairedBarW = 1
	}
	soloBarW := pairedBarW
	y := func(v float64) float64 {
		return top + plotH - (v/maxV)*plotH
	}
	var b strings.Builder
	b.WriteString(chartHeader(title, []chartSeries{
		{Name: "TreeDB", Color: "#2867c7"},
		{Name: "MongoDB", Color: "#1f8a5b"},
	}))
	b.WriteString(fmt.Sprintf("<svg width=\"760\" height=\"430\" viewBox=\"0 0 760 430\" role=\"img\" aria-label=\"%s, x axis Client Mode, y axis %s\">", esc(chartLabel(title)), esc(chartLabel(yAxisLabel))))
	for tick := 0; tick <= 4; tick++ {
		value := maxV * float64(tick) / 4
		yy := y(value)
		b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#edf2f7\"/>", left, yy, left+plotW, yy))
		b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"end\" font-size=\"11\" font-weight=\"700\" fill=\"#344256\">%s</text>", left-7, yy+4, esc(shortNumber(value))))
	}
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, axisY, left+plotW, axisY))
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, top, left, axisY))
	for i, cat := range categories {
		groupX := left + float64(i)*groupW
		treeDBOnlyMode := isTreeDBOnlyLoadMode(cat)
		if treeDBOnlyMode {
			if i < len(tree) && tree[i] > 0 {
				v := tree[i]
				barH := (v / maxV) * plotH
				x := groupX + (groupW-soloBarW)/2
				yy := axisY - barH
				b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"#2867c7\"><title>%s TreeDB-only ceiling: %s</title></rect>", x, yy, soloBarW, barH, esc(cat), esc(formatChartTooltipValue(v, unit))))
				if soloBarW >= 10 {
					b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+soloBarW/2, math.Max(top+10, yy-5), esc(formatChartValue(v, unit))))
				}
			}
		} else {
			if i < len(tree) && tree[i] > 0 {
				v := tree[i]
				barH := (v / maxV) * plotH
				x := groupX + innerPad
				yy := axisY - barH
				b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"#2867c7\"><title>%s TreeDB: %s</title></rect>", x, yy, pairedBarW, barH, esc(cat), esc(formatChartTooltipValue(v, unit))))
				if pairedBarW >= 10 {
					b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+pairedBarW/2, math.Max(top+10, yy-5), esc(formatChartValue(v, unit))))
				}
			}
			if i < len(mongo) && mongo[i] > 0 {
				v := mongo[i]
				barH := (v / maxV) * plotH
				x := groupX + innerPad + pairedBarW + barGap
				yy := axisY - barH
				b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"#1f8a5b\"><title>%s MongoDB: %s</title></rect>", x, yy, pairedBarW, barH, esc(cat), esc(formatChartTooltipValue(v, unit))))
				if pairedBarW >= 10 {
					b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+pairedBarW/2, math.Max(top+10, yy-5), esc(formatChartValue(v, unit))))
				}
			}
		}
		labelX := groupX + groupW/2
		for lineIdx, line := range chartLabelLines(loadModeDisplayLabel(cat), 11, 4) {
			b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"11\" font-weight=\"700\" fill=\"#344256\">%s</text>", labelX, axisY+20+float64(lineIdx)*14, esc(line)))
		}
	}
	b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">Client Mode</text>", left+plotW/2, height-8))
	b.WriteString(fmt.Sprintf("<text transform=\"translate(16 %.1f) rotate(-90)\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">%s</text>", top+plotH/2, esc(chartLabel(yAxisLabel))))
	b.WriteString("</svg>")
	b.WriteString("</div>")
	return b.String()
}

func verticalBarChartWithSize(title string, categories []string, xAxisLabel string, series []chartSeries, unit string, height, bottom float64, labelMaxChars, labelMaxLines int) string {
	if len(categories) == 0 {
		return ""
	}
	maxV := 0.0
	for _, s := range series {
		for _, v := range s.Values {
			if v > maxV {
				maxV = v
			}
		}
	}
	if maxV <= 0 {
		maxV = 1
	}
	maxV *= 1.08
	width := 760.0
	left := 76.0
	right := 18.0
	top := 22.0
	plotW := width - left - right
	plotH := height - top - bottom
	axisY := top + plotH
	groupW := plotW / float64(len(categories))
	barGap := math.Max(1.0, groupW*0.04)
	innerPad := math.Max(3.0, groupW*0.16)
	barW := (groupW - innerPad*2 - barGap*float64(len(series)-1)) / float64(len(series))
	if barW < 1 {
		barW = 1
	}
	y := func(v float64) float64 {
		return top + plotH - (v/maxV)*plotH
	}
	var b strings.Builder
	b.WriteString(chartHeader(title, series))
	b.WriteString(fmt.Sprintf("<svg width=\"760\" height=\"%.0f\" viewBox=\"0 0 760 %.0f\" role=\"img\" aria-label=\"%s, x axis %s, y axis %s\">", height, height, esc(chartLabel(title)), esc(chartLabel(xAxisLabel)), esc(chartLabel(unit))))
	for tick := 0; tick <= 4; tick++ {
		value := maxV * float64(tick) / 4
		yy := y(value)
		b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#edf2f7\"/>", left, yy, left+plotW, yy))
		b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"end\" font-size=\"11\" font-weight=\"700\" fill=\"#344256\">%s</text>", left-7, yy+4, esc(shortNumber(value))))
	}
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, axisY, left+plotW, axisY))
	b.WriteString(fmt.Sprintf("<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#748399\" stroke-width=\"2\"/>", left, top, left, axisY))
	for i, cat := range categories {
		groupX := left + float64(i)*groupW
		for sidx, s := range series {
			if i >= len(s.Values) {
				continue
			}
			v := s.Values[i]
			if v <= 0 {
				continue
			}
			barH := (v / maxV) * plotH
			x := groupX + innerPad + float64(sidx)*(barW+barGap)
			yy := axisY - barH
			b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"%s\"><title>%s %s: %s</title></rect>", x, yy, barW, barH, esc(s.Color), esc(cat), esc(s.Name), esc(formatChartTooltipValue(v, unit))))
			if barW >= 18 && v > 0 {
				b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+barW/2, math.Max(top+10, yy-5), esc(formatChartValue(v, unit))))
			}
		}
		labelX := groupX + groupW/2
		for lineIdx, line := range chartLabelLines(cat, labelMaxChars, labelMaxLines) {
			b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"11\" font-weight=\"700\" fill=\"#344256\">%s</text>", labelX, axisY+20+float64(lineIdx)*14, esc(line)))
		}
	}
	b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">%s</text>", left+plotW/2, height-8, esc(chartLabel(xAxisLabel))))
	b.WriteString(fmt.Sprintf("<text transform=\"translate(16 %.1f) rotate(-90)\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">%s</text>", top+plotH/2, esc(chartLabel(unit))))
	b.WriteString("</svg>")
	b.WriteString("</div>")
	return b.String()
}

func chartHeader(title string, series []chartSeries) string {
	return "<div class=\"chart\"><div class=\"chart-head\"><div class=\"chart-title\">" + esc(chartLabel(title)) + "</div>" + legend(series) + "</div>"
}

func legend(series []chartSeries) string {
	var b strings.Builder
	b.WriteString("<div class=\"legend\">")
	for _, s := range series {
		b.WriteString("<span><span class=\"swatch\" style=\"background:" + esc(s.Color) + "\"></span>" + esc(chartLabel(s.Name)) + "</span>")
	}
	b.WriteString("</div>")
	return b.String()
}

func writeTable(b *strings.Builder, headers []string, rows [][]string, numeric map[int]bool) {
	b.WriteString("<div class=\"table-wrap\"><table><thead><tr>")
	for i, h := range headers {
		cls := ""
		if numeric[i] {
			cls = " class=\"num\""
		}
		b.WriteString("<th" + cls + ">" + esc(h) + "</th>")
	}
	b.WriteString("</tr></thead><tbody>")
	for _, row := range rows {
		b.WriteString("<tr>")
		for i, value := range row {
			cls := ""
			if numeric[i] {
				cls = " class=\"num\""
			}
			b.WriteString("<td" + cls + ">" + esc(value) + "</td>")
		}
		b.WriteString("</tr>")
	}
	b.WriteString("</tbody></table></div>")
}

func numericColumns(indexes ...int) map[int]bool {
	out := make(map[int]bool)
	if len(indexes) == 2 && indexes[0] <= indexes[1] {
		for i := indexes[0]; i <= indexes[1]; i++ {
			out[i] = true
		}
		return out
	}
	for _, idx := range indexes {
		out[idx] = true
	}
	return out
}

func fmtOps(v float64) string {
	if v == 0 {
		return "-"
	}
	return commaInt(int64(math.Round(v)))
}

func fmtFloat(v float64, decimals int) string {
	if v == 0 {
		return "-"
	}
	return commaFloat(v, decimals)
}

func fmtFloatPtr(v *float64, decimals int) string {
	if v == nil {
		return "-"
	}
	return fmtFloat(*v, decimals)
}

func fmtIntPtr(v *int) string {
	if v == nil {
		return "-"
	}
	return strconv.Itoa(*v)
}

func fmtBoolPtr(v *bool) string {
	if v == nil {
		return "-"
	}
	return strconv.FormatBool(*v)
}

func fmtRatio(v float64) string {
	if v == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fx", v)
}

func fmtPercentRatio(v float64, ok bool) string {
	if !ok {
		return "-"
	}
	return fmt.Sprintf("%.1f%%", v*100)
}

func ratioOrZero(numerator, denominator float64) (float64, bool) {
	if denominator == 0 {
		return 0, false
	}
	return numerator / denominator, true
}

func fmtBytes(v float64) string {
	if v == 0 {
		return "-"
	}
	if v >= 1024*1024*1024 {
		return fmt.Sprintf("%.2f GiB", v/(1024*1024*1024))
	}
	if v >= 1024*1024 {
		return fmt.Sprintf("%.2f MiB", v/(1024*1024))
	}
	if v >= 1024 {
		return fmt.Sprintf("%.2f KiB", v/1024)
	}
	return fmt.Sprintf("%.0f B", v)
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func shortNumber(v float64) string {
	abs := math.Abs(v)
	switch {
	case abs >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", v/1_000_000_000)
	case abs >= 1_000_000:
		return fmt.Sprintf("%.1fM", v/1_000_000)
	case abs >= 1_000:
		return fmt.Sprintf("%.0fK", v/1_000)
	default:
		return fmt.Sprintf("%.0f", v)
	}
}

func formatChartValue(v float64, unit string) string {
	if unit == "bytes" {
		return fmtBytes(v)
	}
	return shortNumber(v)
}

func formatChartTooltipValue(v float64, unit string) string {
	formatted := formatChartValue(v, unit)
	if unit == "bytes" {
		return formatted
	}
	return formatted + " " + unit
}

func commaInt(v int64) string {
	raw := strconv.FormatInt(v, 10)
	neg := ""
	if strings.HasPrefix(raw, "-") {
		neg = "-"
		raw = strings.TrimPrefix(raw, "-")
	}
	if len(raw) <= 3 {
		return neg + raw
	}
	var parts []string
	for len(raw) > 3 {
		parts = append(parts, raw[len(raw)-3:])
		raw = raw[:len(raw)-3]
	}
	parts = append(parts, raw)
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return neg + strings.Join(parts, ",")
}

func commaFloat(v float64, decimals int) string {
	raw := fmt.Sprintf("%.*f", decimals, v)
	parts := strings.SplitN(raw, ".", 2)
	whole, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return raw
	}
	if len(parts) == 1 {
		return commaInt(whole)
	}
	return commaInt(whole) + "." + parts[1]
}

func trimLabel(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 1 {
		return s[:max]
	}
	return s[:max-1] + "..."
}

func chartLabel(s string) string {
	s = strings.ReplaceAll(s, "_", " ")
	s = strings.ReplaceAll(s, "-", " ")
	words := strings.Fields(s)
	for i, word := range words {
		words[i] = chartLabelCompound(word)
	}
	return strings.Join(words, " ")
}

func chartLabelCompound(s string) string {
	parts := strings.Split(s, "/")
	for i, part := range parts {
		parts[i] = chartLabelWord(part)
	}
	return strings.Join(parts, "/")
}

func chartLabelWord(s string) string {
	if s == "" {
		return s
	}
	prefixLen := 0
	for prefixLen < len(s) && !isASCIILetterOrDigit(s[prefixLen]) {
		prefixLen++
	}
	suffixLen := 0
	for suffixLen < len(s)-prefixLen && !isASCIILetterOrDigit(s[len(s)-1-suffixLen]) {
		suffixLen++
	}
	prefix := s[:prefixLen]
	core := s[prefixLen : len(s)-suffixLen]
	suffix := s[len(s)-suffixLen:]
	if core == "" {
		return s
	}
	lower := strings.ToLower(core)
	switch lower {
	case "b":
		core = "B"
	case "bson":
		core = "BSON"
	case "cpu":
		core = "CPU"
	case "db":
		core = "DB"
	case "doc":
		core = "Doc"
	case "docs":
		core = "Docs"
	case "gc":
		core = "GC"
	case "id":
		core = "ID"
	case "json":
		core = "JSON"
	case "mongodb":
		core = "MongoDB"
	case "ops":
		core = "Ops"
	case "sec":
		core = "Sec"
	case "sqlite":
		core = "SQLite"
	case "treedb":
		core = "TreeDB"
	case "tcp":
		core = "TCP"
	case "vlog":
		core = "VLog"
	case "wal":
		core = "WAL"
	default:
		core = strings.ToUpper(lower[:1]) + lower[1:]
	}
	return prefix + core + suffix
}

func isASCIILetterOrDigit(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func chartLabelLines(s string, maxChars, maxLines int) []string {
	words := strings.Fields(chartLabel(s))
	if len(words) == 0 {
		return nil
	}
	var lines []string
	current := ""
	for _, word := range words {
		if current == "" {
			current = word
			continue
		}
		if len(current)+1+len(word) <= maxChars {
			current += " " + word
			continue
		}
		lines = append(lines, current)
		current = word
	}
	if current != "" {
		lines = append(lines, current)
	}
	if maxLines <= 0 || len(lines) <= maxLines {
		return lines
	}
	kept := append([]string{}, lines[:maxLines]...)
	kept[maxLines-1] = trimLabel(strings.Join(lines[maxLines-1:], " "), maxChars)
	return kept
}

func emptyDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func profileIndexesLabel(item mongoProfileSummary) string {
	if !item.HasResult {
		return "-"
	}
	return strconv.Itoa(item.SecondaryIndexes)
}

func tsvHeader(path string, names []string, required []string) (map[string]int, error) {
	header := make(map[string]int, len(names))
	for i, name := range names {
		normalized := normalizeTSVHeader(name)
		if normalized == "" {
			continue
		}
		if prev, ok := header[normalized]; ok {
			return nil, fmt.Errorf("%s: duplicate TSV header %q at columns %d and %d", path, normalized, prev+1, i+1)
		}
		header[normalized] = i
	}
	for _, name := range required {
		normalized := normalizeTSVHeader(name)
		if _, ok := header[normalized]; !ok {
			return nil, fmt.Errorf("%s: missing required TSV column %q", path, name)
		}
	}
	return header, nil
}

func normalizeTSVHeader(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func tsvField(path string, header map[string]int, rec []string, line int, name string) (string, error) {
	idx, ok := header[normalizeTSVHeader(name)]
	if !ok {
		return "", fmt.Errorf("%s:%d: missing TSV column %q", path, line, name)
	}
	if idx >= len(rec) {
		return "", fmt.Errorf("%s:%d: row has %d fields, missing column %q at field %d", path, line, len(rec), name, idx+1)
	}
	return strings.TrimSpace(rec[idx]), nil
}

func parseIntColumn(path string, line int, name, value string) (int, error) {
	v, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s:%d: parse int column %q value %q: %w", path, line, name, value, err)
	}
	return v, nil
}

func parseInt64Column(path string, line int, name, value string) (int64, error) {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s:%d: parse int64 column %q value %q: %w", path, line, name, value, err)
	}
	return v, nil
}

func parseFloatColumn(path string, line int, name, value string) (float64, error) {
	if value == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s:%d: parse float column %q value %q: %w", path, line, name, value, err)
	}
	return v, nil
}

func parseBoolColumn(path string, line int, name, value string) (bool, error) {
	v, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s:%d: parse bool column %q value %q: %w", path, line, name, value, err)
	}
	return v, nil
}

func esc(s string) string {
	return html.EscapeString(s)
}
