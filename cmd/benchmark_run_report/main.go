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
	ConfigName      string
	Engine          string
	Format          string
	Shape           string
	IndexCount      int
	DocumentCount   int
	Phase           string
	MaintenanceMode string
	TotalBytes      float64
	BytesPerDoc     float64
	DocsPerSec      float64
	NsPerDoc        float64
	MeasurementKind string
	MeasurementNote string
	Source          string
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

type reportData struct {
	Config                config
	GeneratedAt           time.Time
	Git                   []gitIdentity
	RawEngine             []rawEngineRun
	Collections           []collectionRow
	CollectionComparisons []collectionComparison
	MongoFullSweep        []mongoSummaryRow
	MongoLoadModes        []loadModeRow
	MongoScaling          map[int][]mongoSummaryRow
	Warnings              []string
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
		MongoScaling: make(map[int][]mongoSummaryRow),
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
	if scaling, warnings := loadMongoScaling(filepath.Join(cfg.RunRoot, "mongo_gateway_reader_writer_scaling_1m")); len(scaling) > 0 || len(warnings) > 0 {
		data.MongoScaling = scaling
		data.Warnings = append(data.Warnings, warnings...)
	}
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
		path := filepath.Join(dir, entry.Name(), "benchmark_results.json")
		raw, err := os.ReadFile(path)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", path, err))
			continue
		}
		var parsed struct {
			Results     []collectionRow        `json:"results"`
			Comparisons []collectionComparison `json:"comparisons"`
		}
		if err := json.Unmarshal(raw, &parsed); err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", path, err))
			continue
		}
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

func (r *collectionRow) UnmarshalJSON(raw []byte) error {
	type alias struct {
		ConfigName      string  `json:"config_name"`
		Engine          string  `json:"engine"`
		Format          string  `json:"format"`
		Shape           string  `json:"shape"`
		IndexCount      int     `json:"index_count"`
		DocumentCount   int     `json:"document_count"`
		Phase           string  `json:"phase"`
		MaintenanceMode string  `json:"maintenance_mode"`
		TotalBytes      float64 `json:"total_bytes"`
		BytesPerDoc     float64 `json:"bytes_per_doc"`
		DocsPerSec      float64 `json:"docs_per_sec"`
		NsPerDoc        float64 `json:"ns_per_doc"`
		MeasurementKind string  `json:"measurement_kind"`
		MeasurementNote string  `json:"measurement_note"`
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
	r.Phase = a.Phase
	r.MaintenanceMode = a.MaintenanceMode
	r.TotalBytes = a.TotalBytes
	r.BytesPerDoc = a.BytesPerDoc
	r.DocsPerSec = a.DocsPerSec
	r.NsPerDoc = a.NsPerDoc
	r.MeasurementKind = a.MeasurementKind
	r.MeasurementNote = a.MeasurementNote
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
	if len(data.Warnings) > 0 {
		b.WriteString("<section class=\"warn\"><h2>Warnings</h2><ul>")
		for _, warning := range data.Warnings {
			b.WriteString("<li>" + esc(warning) + "</li>")
		}
		b.WriteString("</ul></section>\n")
	}
	renderMongoFullSweep(&b, data.MongoFullSweep)
	renderMongoLoadModes(&b, data.MongoLoadModes)
	renderMongoScaling(&b, data.MongoScaling)
	renderCollections(&b, data.Collections, data.CollectionComparisons)
	renderRawEngine(&b, data.RawEngine)
	b.WriteString("</body>\n</html>\n")
	return b.String()
}

func reportNav(data reportData) string {
	type navItem struct {
		Href  string
		Label string
	}
	var items []navItem
	if len(data.MongoFullSweep) > 0 {
		items = append(items, navItem{Href: "#mongo-full", Label: "Mongo API sweep"})
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
	b.WriteString("<p class=\"subtle\">Charts show post-insert throughput and compacted bytes/doc for TreeDB collection formats and SQLite baselines. Disclosure tables preserve the parsed rows and compacted-state comparisons.</p>")
	if len(rows) > 0 {
		b.WriteString("<div class=\"chart-grid\">")
		docsRows := collectionDocsRows(rows)
		if len(docsRows.Categories) > 0 {
			b.WriteString(compactVerticalBarChart("Post-insert throughput by index count", docsRows.Categories, "index count", docsRows.Series, "docs/sec"))
		}
		diskRows := collectionDiskRows(rows)
		if len(diskRows.Categories) > 0 {
			b.WriteString(compactVerticalBarChart("Compacted bytes/doc by index count", diskRows.Categories, "index count", diskRows.Series, "B/doc"))
		}
		b.WriteString("</div>")
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

func collectionChart(rows []collectionRow, kind string) collectionChartRows {
	indexes := sortedCollectionIndexes(rows)
	categories := make([]string, 0, len(indexes))
	indexPos := make(map[int]int, len(indexes))
	for pos, idx := range indexes {
		categories = append(categories, indexCountLabel(idx))
		indexPos[idx] = pos
	}
	values := make(map[string][]float64)
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
		phase, ok := collectionChartPhase(family, kind)
		if !ok || row.Phase != phase {
			continue
		}
		pos, ok := indexPos[row.IndexCount]
		if !ok {
			continue
		}
		key := family + "\x00" + canonicalFormat(row.Format)
		if _, ok := values[key]; !ok {
			values[key] = make([]float64, len(indexes))
			labels[key] = collectionSeriesLabel(family, row.Format, kind)
			orders[key] = collectionSeriesOrder(family, row.Format)
		}
		if kind == "bytes" {
			values[key][pos] = row.BytesPerDoc
		} else {
			values[key][pos] = row.DocsPerSec
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
	if kind != "bytes" {
		return "", false
	}
	if family == "TreeDB" {
		return "full_leafgen_pack_gc", true
	}
	if family == "SQLite" {
		return "sqlite_vacuum", true
	}
	return "", false
}

func collectionSeriesLabel(family, format, kind string) string {
	label := family + " " + displayFormat(format)
	if kind == "bytes" {
		if family == "TreeDB" {
			label += " leafgen"
		} else if family == "SQLite" {
			label += " VACUUM"
		}
	}
	return label
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
	parts := strings.SplitN(key, "\x00", 2)
	if len(parts) == 2 {
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
		b.WriteString(lineChart("Physical storage vs secondary indexes", indexLabels, "secondary indexes", "physical bytes", []chartSeries{
			{Name: "TreeDB", Values: mongoRowDisk(loadRows, "tree"), Color: "#2867c7"},
			{Name: "MongoDB", Values: mongoRowDisk(loadRows, "mongo"), Color: "#1f8a5b"},
		}, "bytes"))
	}
	b.WriteString("</div></div>")
	for _, idx := range indexes {
		indexLabel := indexCountTitleLabel(idx)
		throughputRows := mongoOperationThroughputRows(rows, idx)
		b.WriteString("<div class=\"chart-group\"><h3>" + esc(indexLabel) + ": Mongo API Scaling</h3>")
		b.WriteString("<p class=\"subtle\">These are the actual concurrency sweeps. Single threaded client rows are one-client request/response probes; higher client counts show where each operation saturates.</p>")
		b.WriteString("<div class=\"chart-grid\">")
		for _, spec := range mongoConcurrentReadSweepSpecs() {
			readerCounts := mongoSweepCounts(rows, idx, spec.Prefix)
			if len(readerCounts) == 0 {
				continue
			}
			b.WriteString(lineChart(spec.Title, countLabels(readerCounts), "client count", "ops/sec", []chartSeries{
				{Name: "TreeDB", Values: mongoSweepAny(rows, idx, spec.Prefix, "tree", readerCounts), Color: "#2867c7"},
				{Name: "MongoDB", Values: mongoSweepAny(rows, idx, spec.Prefix, "mongo", readerCounts), Color: "#1f8a5b"},
			}, "ops/sec"))
		}
		writerCounts := mongoSweepCounts(rows, idx, "concurrent_id_update_set_w")
		if len(writerCounts) > 0 {
			b.WriteString(lineChart("_id update one: throughput vs writer clients", countLabels(writerCounts), "client count", "ops/sec", []chartSeries{
				{Name: "TreeDB", Values: mongoSweepAny(rows, idx, "concurrent_id_update_set_w", "tree", writerCounts), Color: "#2867c7"},
				{Name: "MongoDB", Values: mongoSweepAny(rows, idx, "concurrent_id_update_set_w", "mongo", writerCounts), Color: "#1f8a5b"},
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
	b.WriteString("<p class=\"subtle\">Insert-only matrix. This section uses raw JSON directly so every MongoDB and TreeDB client mode has its own throughput and physical-disk row.</p>")
	b.WriteString("<p class=\"subtle\">Use this section for pure ingest client-path comparisons. It is intentionally separate from the full-sweep load chart, which inherits the broader read/range/update sweep setup.</p>")
	b.WriteString("<p class=\"subtle\">Driver, driver-command, driver-command-raw, and driver-unack modes are comparable Mongo-compatible client paths and render as paired TreeDB/MongoDB bars. Client modes marked with <strong>*</strong> are TreeDB-only raw-wire ceiling probes explained below.</p>")
	b.WriteString("<div class=\"chart-grid\">")
	for _, idx := range sortedLoadModeIndexes(rows) {
		cats, tree, mongo := loadModeChartRows(rows, idx)
		if len(cats) == 0 {
			continue
		}
		b.WriteString(loadModeBarChart(fmt.Sprintf("Load throughput by client mode, %d indexes", idx), cats, tree, mongo))
	}
	b.WriteString("</div>")
	b.WriteString("<p class=\"subtle\"><strong>* Raw-wire modes:</strong> <code>raw-wire-tcp</code> still sends Mongo OP_MSG bytes over loopback TCP to the TreeDB gateway while bypassing the MongoDB Go driver. <code>raw-wire</code> removes the socket too and drives the same raw command/gateway path in process. These modes are TreeDB-only ingest ceiling probes, so each renders as one centered TreeDB bar instead of a paired MongoDB bar.</p>")
	var body [][]string
	for _, row := range rows {
		body = append(body, []string{strconv.Itoa(row.Indexes), row.Target, row.Config, fmtOps(row.OpsPerSec), fmtBytes(float64(row.PhysicalBytes)), row.RawJSON})
	}
	b.WriteString("<details><summary>Raw load-mode rows</summary>")
	writeTable(b, []string{"indexes", "target", "config", "load docs/sec", "physical bytes", "raw JSON"}, body, numericColumns(0, 3, 4))
	b.WriteString("</details>")
	b.WriteString("</section>\n")
}

func fullSweepLoadNote(rows []mongoSummaryRow) string {
	note := "The load chart in this section is the load phase of the broader full sweep, not the pure ingest client-mode matrix. Use it when interpreting the read/range/update sweep as a whole; use the load-only matrix below when comparing client paths."
	for _, row := range rows {
		if row.Phase == "load_insert_many" && row.SecondaryIndexes == 0 && row.RangeIndex {
			note += " This run has range_index=true, so even the displayed 0-secondary-index load cell maintains the additional age_1 range index during insert."
			break
		}
	}
	return "<p class=\"subtle\"><strong>Load interpretation:</strong> " + esc(note) + "</p>"
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
		b.WriteString("<p class=\"subtle\">Fresh 1M-document load per cell, then a focused writer or reader concurrency sweep for this index count.</p>")
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
		summaryRows := mongoOperationThroughputRows(rows, idx)
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
	writeTable(b, []string{"docs", "indexes", "range index", "range mode", "TreeDB config", "Mongo config", "phase", "TreeDB ops/s", "Mongo ops/s", "ratio", "TreeDB p95 us", "Mongo p95 us", "TreeDB disk snapshot", "TreeDB logical bytes", "TreeDB physical", "Mongo dbStats total", "Mongo physical", "source"}, body, numericColumns(0, 1, 7, 8, 9, 10, 11, 13, 14, 15, 16))
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
	CountUnit      string
}

func mongoOperationThroughputRows(rows []mongoSummaryRow, idx int) []mongoThroughputOperationRow {
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
		item.Single, item.HasSingle = mongoFirstPhaseRow(rows, idx, spec.SinglePhases)
		item.RatioBest, item.RatioBestCount, item.HasRatioBest = mongoBestSweepRowByRatio(rows, idx, spec.SweepPrefixes)
		if !item.HasSingle && !item.HasRatioBest {
			continue
		}
		out = append(out, item)
	}
	return out
}

func mongoFirstPhaseRow(rows []mongoSummaryRow, idx int, phases []string) (mongoSummaryRow, bool) {
	for _, phase := range phases {
		row, ok := mongoRow(rows, idx, phase)
		if ok {
			return row, true
		}
	}
	return mongoSummaryRow{}, false
}

func writeMongoThroughputSummary(b *strings.Builder, rows []mongoThroughputOperationRow) {
	wrote := false
	var inner strings.Builder
	for _, row := range rows {
		if !row.HasRatioBest {
			continue
		}
		wrote = true
		inner.WriteString("<div class=\"summary-row\">")
		inner.WriteString("<div class=\"summary-workload\">" + esc(row.Label) + "</div>")
		writeSummaryCell(&inner, "best same-client ratio", mongoBestRatio(row))
		writeSummaryCell(&inner, "clients", mongoRatioCountLabel(row))
		writeSummaryCell(&inner, "TreeDB @ clients", mongoRatioOps(row, "tree"))
		writeSummaryCell(&inner, "MongoDB @ clients", mongoRatioOps(row, "mongo"))
		writeSummaryCell(&inner, "TreeDB vs single @ clients", mongoScaleUpRatio(row))
		inner.WriteString("</div>")
	}
	if !wrote {
		return
	}
	b.WriteString("<h4>Best same-client comparison</h4>")
	b.WriteString("<div class=\"summary-strip\">")
	b.WriteString(inner.String())
	b.WriteString("</div>")
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
			out = append(out, row.MongoPhysicalBytes)
		} else {
			out = append(out, row.TreeDBPhysicalBytes)
		}
	}
	return out
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
	return row.Documents == scope.Documents &&
		row.TreeDBConfig == scope.TreeDBConfig &&
		row.MongoConfig == scope.MongoConfig &&
		row.RangeIndex == scope.RangeIndex
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
			pair.TreeDB = row.OpsPerSec
		case "mongo":
			pair.Mongo = row.OpsPerSec
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

func isRawWireLoadMode(mode string) bool {
	return mode == "BSON raw_wire_tcp" || mode == "BSON raw_wire"
}

func loadModeDisplayLabel(mode string) string {
	if isRawWireLoadMode(mode) {
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
		"BSON raw_wire_tcp":       4,
		"BSON raw_wire":           5,
	}
	if idx, ok := order[mode]; ok {
		return fmt.Sprintf("%02d_%s", idx, mode)
	}
	return "99_" + mode
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
		for i, v := range s.Values {
			points = append(points, fmt.Sprintf("%.1f,%.1f", x(i), y(v)))
		}
		b.WriteString("<polyline fill=\"none\" stroke=\"" + esc(s.Color) + "\" stroke-width=\"2.5\" points=\"" + strings.Join(points, " ") + "\"/>")
		for i, v := range s.Values {
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

func loadModeBarChart(title string, categories []string, tree, mongo []float64) string {
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
	b.WriteString(fmt.Sprintf("<svg width=\"760\" height=\"430\" viewBox=\"0 0 760 430\" role=\"img\" aria-label=\"%s, x axis Client Mode, y axis Docs/Sec\">", esc(chartLabel(title))))
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
		rawMode := isRawWireLoadMode(cat)
		if rawMode {
			if i < len(tree) && tree[i] > 0 {
				v := tree[i]
				barH := (v / maxV) * plotH
				x := groupX + (groupW-soloBarW)/2
				yy := axisY - barH
				b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"#2867c7\"><title>%s TreeDB-only raw-wire ceiling: %s docs/sec</title></rect>", x, yy, soloBarW, barH, esc(cat), esc(formatChartValue(v, "docs/sec"))))
				if soloBarW >= 10 {
					b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+soloBarW/2, math.Max(top+10, yy-5), esc(formatChartValue(v, "docs/sec"))))
				}
			}
		} else {
			if i < len(tree) && tree[i] > 0 {
				v := tree[i]
				barH := (v / maxV) * plotH
				x := groupX + innerPad
				yy := axisY - barH
				b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"#2867c7\"><title>%s TreeDB: %s docs/sec</title></rect>", x, yy, pairedBarW, barH, esc(cat), esc(formatChartValue(v, "docs/sec"))))
				if pairedBarW >= 10 {
					b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+pairedBarW/2, math.Max(top+10, yy-5), esc(formatChartValue(v, "docs/sec"))))
				}
			}
			if i < len(mongo) && mongo[i] > 0 {
				v := mongo[i]
				barH := (v / maxV) * plotH
				x := groupX + innerPad + pairedBarW + barGap
				yy := axisY - barH
				b.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"2\" fill=\"#1f8a5b\"><title>%s MongoDB: %s docs/sec</title></rect>", x, yy, pairedBarW, barH, esc(cat), esc(formatChartValue(v, "docs/sec"))))
				if pairedBarW >= 10 {
					b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"10\" font-weight=\"700\" fill=\"#344256\">%s</text>", x+pairedBarW/2, math.Max(top+10, yy-5), esc(formatChartValue(v, "docs/sec"))))
				}
			}
		}
		labelX := groupX + groupW/2
		for lineIdx, line := range chartLabelLines(loadModeDisplayLabel(cat), 11, 4) {
			b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"11\" font-weight=\"700\" fill=\"#344256\">%s</text>", labelX, axisY+20+float64(lineIdx)*14, esc(line)))
		}
	}
	b.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">Client Mode</text>", left+plotW/2, height-8))
	b.WriteString(fmt.Sprintf("<text transform=\"translate(16 %.1f) rotate(-90)\" text-anchor=\"middle\" font-size=\"12\" font-weight=\"700\" fill=\"#344256\">Docs/Sec</text>", top+plotH/2))
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
			if barW >= 10 && v > 0 {
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

func fmtRatio(v float64) string {
	if v == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fx", v)
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
