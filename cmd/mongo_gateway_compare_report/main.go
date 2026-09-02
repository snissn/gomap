package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type config struct {
	MatrixPath      string
	ReportPath      string
	SummaryPath     string
	Title           string
	AllowIncomplete bool
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
	Target                     string            `json:"target"`
	MongoURI                   string            `json:"mongo_uri,omitempty"`
	TreeDBDir                  string            `json:"treedb_dir,omitempty"`
	Database                   string            `json:"database"`
	Collection                 string            `json:"collection"`
	Documents                  int               `json:"documents"`
	BatchSize                  int               `json:"batch_size"`
	InsertProducers            int               `json:"insert_producers"`
	SecondaryIndexes           int               `json:"secondary_indexes"`
	RangeIndex                 bool              `json:"range_index"`
	ClientMode                 string            `json:"client_mode,omitempty"`
	ConcurrentReadKinds        []string          `json:"concurrent_read_kinds,omitempty"`
	SkippedConcurrentReadKinds []string          `json:"skipped_concurrent_read_kinds,omitempty"`
	ConcurrentReaderSweep      []int             `json:"concurrent_reader_sweep,omitempty"`
	ConcurrentWriterSweep      []int             `json:"concurrent_writer_sweep,omitempty"`
	ConcurrentRangeReaderSweep []int             `json:"concurrent_range_reader_sweep,omitempty"`
	TreeDBDocumentFormat       string            `json:"treedb_document_format,omitempty"`
	Phases                     []phaseResult     `json:"phases"`
	ProfileDir                 string            `json:"profile_dir,omitempty"`
	ProfileManifest            string            `json:"profile_manifest,omitempty"`
	ProfileResult              string            `json:"profile_result,omitempty"`
	TreeDBDiskAfterLoad        *diskSnapshot     `json:"treedb_disk_after_load,omitempty"`
	TreeDBDiskAfterCheckpoint  *diskSnapshot     `json:"treedb_disk_after_checkpoint,omitempty"`
	TreeDBDiskAfterMaintenance *diskSnapshot     `json:"treedb_disk_after_maintenance,omitempty"`
	TreeDBStatsAfterLoad       map[string]string `json:"treedb_stats_after_load,omitempty"`
	TreeDBStatsAfterCheckpoint map[string]string `json:"treedb_stats_after_checkpoint,omitempty"`
	TreeDBStatsFinal           map[string]string `json:"treedb_stats_final,omitempty"`
	MongoDBStatsAfterLoad      map[string]any    `json:"mongodb_stats_after_load,omitempty"`
	MongoDBStatsFinal          map[string]any    `json:"mongodb_stats_final,omitempty"`
}

type phaseResult struct {
	Name                    string             `json:"name"`
	Operations              int                `json:"operations"`
	DriverCalls             int                `json:"driver_calls"`
	EffectiveProducers      int                `json:"effective_producers,omitempty"`
	DurationMillis          float64            `json:"duration_ms"`
	OpsPerSecond            float64            `json:"ops_per_sec"`
	SampledOpsPerSecond     float64            `json:"sampled_ops_per_sec,omitempty"`
	SampledNsPerOp          float64            `json:"sampled_ns_per_op,omitempty"`
	DriverAggregateMillis   float64            `json:"driver_aggregate_duration_ms,omitempty"`
	DriverMeanLatencyMicros float64            `json:"driver_mean_latency_us,omitempty"`
	LatencyMicros           latencySummary     `json:"latency_micros"`
	TreeDBStatsDelta        map[string]string  `json:"treedb_stats_delta,omitempty"`
	TreeDBMetrics           map[string]float64 `json:"treedb_metrics,omitempty"`
}

type latencySummary struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

type diskSnapshot struct {
	TotalBytes int64            `json:"total_bytes"`
	Paths      map[string]int64 `json:"paths,omitempty"`
}

type runRecord struct {
	Row            matrixRow
	Result         benchmarkResult
	RawPath        string
	DisplayRawPath string
	PhaseMap       map[string]phaseResult
}

type cellKey struct {
	Documents        int
	SecondaryIndexes int
	TreeDBConfig     string
}

type baseCellKey struct {
	Documents        int
	SecondaryIndexes int
}

type cellComparison struct {
	Key             cellKey
	TreeDB          *runRecord
	Mongo           *runRecord
	MongoAlternates []*runRecord
}

type phaseComparison struct {
	Cell        cellKey
	Name        string
	RangeIndex  bool
	MongoConfig string
	TreeDBPhase phaseResult
	MongoPhase  phaseResult
	HasTreeDB   bool
	HasMongo    bool
	Ratio       float64
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "mongo_gateway_compare_report: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	cells, err := loadComparisons(cfg.MatrixPath, cfg.AllowIncomplete)
	if err != nil {
		return err
	}
	if len(cells) == 0 {
		return errors.New("matrix has no complete comparison cells")
	}
	if err := os.MkdirAll(filepath.Dir(cfg.ReportPath), 0o755); err != nil {
		return err
	}
	report := renderReport(cfg, cells, time.Now().UTC())
	if err := os.WriteFile(cfg.ReportPath, []byte(report), 0o644); err != nil {
		return err
	}
	if cfg.SummaryPath != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.SummaryPath), 0o755); err != nil {
			return err
		}
		if err := writeSummaryTSV(cfg.SummaryPath, cells); err != nil {
			return err
		}
	}
	return nil
}

func parseConfig(args []string) (config, error) {
	cfg := config{}
	fs := flag.NewFlagSet("mongo_gateway_compare_report", flag.ContinueOnError)
	fs.StringVar(&cfg.MatrixPath, "matrix", "", "TSV matrix index from scripts/mongo_gateway_compare.sh")
	fs.StringVar(&cfg.ReportPath, "report", "", "Markdown report output path")
	fs.StringVar(&cfg.SummaryPath, "summary", "", "optional TSV summary output path")
	fs.StringVar(&cfg.Title, "title", "Mongo Gateway Benchmark Comparison", "report title")
	fs.BoolVar(&cfg.AllowIncomplete, "allow-incomplete", false, "allow cells with no MongoDB baseline rows; still requires scenario-matching MongoDB baselines when MongoDB rows are present")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.MatrixPath == "" {
		return config{}, errors.New("-matrix is required")
	}
	if cfg.ReportPath == "" {
		return config{}, errors.New("-report is required")
	}
	return cfg, nil
}

func loadComparisons(matrixPath string, allowIncomplete bool) ([]cellComparison, error) {
	rows, err := readMatrix(matrixPath)
	if err != nil {
		return nil, err
	}
	matrixDir := filepath.Dir(matrixPath)
	type groupedCell struct {
		mongos         map[string]*runRecord
		trees          map[string]*runRecord
		treeConfigKeys []string
	}
	byCell := make(map[baseCellKey]*groupedCell)
	for _, row := range rows {
		rawPath := row.RawJSON
		if !filepath.IsAbs(rawPath) {
			rawPath = filepath.Join(matrixDir, rawPath)
		}
		result, err := readBenchmarkResult(rawPath)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", rawPath, err)
		}
		if result.Target != "" && row.Target != "" && result.Target != row.Target {
			return nil, fmt.Errorf("%s target mismatch: matrix has %q, result has %q", rawPath, row.Target, result.Target)
		}
		if result.Documents != 0 && result.Documents != row.Documents {
			return nil, fmt.Errorf("%s documents mismatch: matrix has %d, result has %d", rawPath, row.Documents, result.Documents)
		}
		if result.SecondaryIndexes != row.SecondaryIndexes {
			return nil, fmt.Errorf("%s secondary_indexes mismatch: matrix has %d, result has %d", rawPath, row.SecondaryIndexes, result.SecondaryIndexes)
		}
		record := &runRecord{
			Row:            row,
			Result:         result,
			RawPath:        rawPath,
			DisplayRawPath: row.RawJSON,
			PhaseMap:       phaseMap(result.Phases),
		}
		record.Row.Config = runConfig(row, result)
		key := baseCellKey{Documents: row.Documents, SecondaryIndexes: row.SecondaryIndexes}
		cell := byCell[key]
		if cell == nil {
			cell = &groupedCell{
				mongos: make(map[string]*runRecord),
				trees:  make(map[string]*runRecord),
			}
			byCell[key] = cell
		}
		switch row.Target {
		case "treedb":
			if cell.trees[record.Row.Config] != nil {
				return nil, fmt.Errorf("duplicate treedb row for documents=%d secondary_indexes=%d config=%q", key.Documents, key.SecondaryIndexes, record.Row.Config)
			}
			cell.trees[record.Row.Config] = record
			cell.treeConfigKeys = append(cell.treeConfigKeys, record.Row.Config)
		case "mongo":
			if cell.mongos[record.Row.Config] != nil {
				return nil, fmt.Errorf("duplicate mongo row for documents=%d secondary_indexes=%d config=%q", key.Documents, key.SecondaryIndexes, record.Row.Config)
			}
			cell.mongos[record.Row.Config] = record
		default:
			return nil, fmt.Errorf("unknown target %q in matrix", row.Target)
		}
	}
	cells := make([]cellComparison, 0, len(byCell))
	for key, cell := range byCell {
		if len(cell.trees) == 0 || (len(cell.mongos) == 0 && !allowIncomplete) {
			return nil, fmt.Errorf("incomplete comparison cell documents=%d secondary_indexes=%d", key.Documents, key.SecondaryIndexes)
		}
		sort.Strings(cell.treeConfigKeys)
		mongoIndex, err := buildMongoScenarioIndex(cell.mongos)
		if err != nil {
			return nil, err
		}
		for _, config := range cell.treeConfigKeys {
			mongo, err := matchingMongoRecord(key, config, mongoIndex, allowIncomplete)
			if err != nil {
				return nil, err
			}
			cells = append(cells, cellComparison{
				Key: cellKey{
					Documents:        key.Documents,
					SecondaryIndexes: key.SecondaryIndexes,
					TreeDBConfig:     config,
				},
				TreeDB:          cell.trees[config],
				Mongo:           mongo,
				MongoAlternates: sortedMongoRecords(cell.mongos),
			})
		}
	}
	sort.Slice(cells, func(i, j int) bool {
		if cells[i].Key.Documents != cells[j].Key.Documents {
			return cells[i].Key.Documents < cells[j].Key.Documents
		}
		if cells[i].Key.SecondaryIndexes != cells[j].Key.SecondaryIndexes {
			return cells[i].Key.SecondaryIndexes < cells[j].Key.SecondaryIndexes
		}
		return cells[i].Key.TreeDBConfig < cells[j].Key.TreeDBConfig
	})
	return cells, nil
}

type mongoScenarioIndex struct {
	exact        map[string]*runRecord
	bySuffix     map[string][]*runRecord
	suffixConfig map[string][]string
}

func buildMongoScenarioIndex(mongos map[string]*runRecord) (mongoScenarioIndex, error) {
	index := mongoScenarioIndex{
		exact:        mongos,
		bySuffix:     make(map[string][]*runRecord, len(mongos)),
		suffixConfig: make(map[string][]string, len(mongos)),
	}
	for config, record := range mongos {
		scenario := parseScalingScenario(config)
		if scenario.hasMarker && !scenario.valid {
			return mongoScenarioIndex{}, fmt.Errorf("invalid scaling scenario suffix for mongo config=%q", config)
		}
		index.bySuffix[scenario.suffix] = append(index.bySuffix[scenario.suffix], record)
		index.suffixConfig[scenario.suffix] = append(index.suffixConfig[scenario.suffix], config)
	}
	for suffix := range index.suffixConfig {
		sort.Strings(index.suffixConfig[suffix])
	}
	return index, nil
}

func matchingMongoRecord(key baseCellKey, treeConfig string, mongoIndex mongoScenarioIndex, allowIncomplete bool) (*runRecord, error) {
	if len(mongoIndex.exact) == 0 {
		if allowIncomplete {
			return nil, nil
		}
		return nil, fmt.Errorf("missing mongo row for documents=%d secondary_indexes=%d config=%q", key.Documents, key.SecondaryIndexes, treeConfig)
	}
	if record := mongoIndex.exact[treeConfig]; record != nil {
		return record, nil
	}
	treeScenario := parseScalingScenario(treeConfig)
	if treeScenario.hasMarker && !treeScenario.valid {
		return nil, fmt.Errorf("invalid scaling scenario suffix for documents=%d secondary_indexes=%d config=%q available_mongo_configs=%v", key.Documents, key.SecondaryIndexes, treeConfig, sortedRunRecordKeys(mongoIndex.exact))
	}
	treeScenarioLabel := treeScenario.suffix
	if !treeScenario.hasMarker {
		treeScenarioLabel = "no scaling marker present"
	}
	matches := mongoIndex.bySuffix[treeScenario.suffix]
	candidates := mongoIndex.suffixConfig[treeScenario.suffix]
	if len(candidates) > 1 {
		record, err := mongoComparisonBaseline(matches)
		if err != nil {
			return nil, fmt.Errorf("ambiguous mongo rows for documents=%d secondary_indexes=%d config=%q tree_scenario=%q candidates=%v available_mongo_configs=%v: %w", key.Documents, key.SecondaryIndexes, treeConfig, treeScenarioLabel, candidates, sortedRunRecordKeys(mongoIndex.exact), err)
		}
		return record, nil
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	return nil, fmt.Errorf("missing mongo row for documents=%d secondary_indexes=%d config=%q tree_scenario=%q available_mongo_configs=%v", key.Documents, key.SecondaryIndexes, treeConfig, treeScenarioLabel, sortedRunRecordKeys(mongoIndex.exact))
}

func mongoComparisonBaseline(records []*runRecord) (*runRecord, error) {
	var legacy *runRecord
	var explicit *runRecord
	for _, record := range records {
		if record == nil {
			continue
		}
		if isLegacyMongoDriverBaselineConfig(record.Row.Config) {
			if legacy != nil {
				return nil, fmt.Errorf("multiple legacy MongoDB driver baseline candidates: %q and %q", legacy.Row.Config, record.Row.Config)
			}
			legacy = record
			continue
		}
		if isExplicitMongoDriverBaselineConfig(record.Row.Config) {
			if explicit != nil {
				return nil, fmt.Errorf("multiple explicit MongoDB driver baseline candidates: %q and %q", explicit.Row.Config, record.Row.Config)
			}
			explicit = record
		}
	}
	if legacy != nil {
		return legacy, nil
	}
	if explicit != nil {
		return explicit, nil
	}
	for _, record := range sortedRunRecords(records) {
		if record != nil {
			return record, nil
		}
	}
	return nil, errors.New("no MongoDB baseline candidates")
}

func sortedMongoRecords(recordsByConfig map[string]*runRecord) []*runRecord {
	records := make([]*runRecord, 0, len(recordsByConfig))
	for _, record := range recordsByConfig {
		records = append(records, record)
	}
	return sortedRunRecords(records)
}

func sortedRunRecords(records []*runRecord) []*runRecord {
	records = append([]*runRecord(nil), records...)
	sort.Slice(records, func(i, j int) bool {
		if records[i].Row.Config != records[j].Row.Config {
			return records[i].Row.Config < records[j].Row.Config
		}
		return records[i].DisplayRawPath < records[j].DisplayRawPath
	})
	return records
}

func isMongoDriverBaselineConfig(config string) bool {
	return isLegacyMongoDriverBaselineConfig(config) || isExplicitMongoDriverBaselineConfig(config)
}

func isLegacyMongoDriverBaselineConfig(config string) bool {
	if config == "mongo" {
		return true
	}
	if !strings.HasPrefix(config, "mongo_") {
		return false
	}
	if config == "mongo_driver" || strings.HasPrefix(config, "mongo_driver_") {
		return false
	}
	// Legacy Mongo rows predate explicit client-mode naming and look like
	// mongo_range_index or mongo_writers_4. They are ordinary driver baselines.
	return true
}

func isExplicitMongoDriverBaselineConfig(config string) bool {
	if config == "mongo_driver" {
		return true
	}
	for _, base := range []string{"mongo_driver", "mongo_driver_range_index"} {
		if config == base {
			return true
		}
		for _, marker := range []string{"_writers_", "_readers_"} {
			if strings.HasPrefix(config, base+marker) {
				scenario := parseScalingScenario(config)
				return scenario.hasMarker && scenario.valid
			}
		}
	}
	return false
}

func sortedRunRecordKeys(records map[string]*runRecord) []string {
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func scalingScenarioSuffix(config string) string {
	scenario := parseScalingScenario(config)
	if !scenario.valid {
		return ""
	}
	return scenario.suffix
}

type scalingScenario struct {
	suffix    string
	valid     bool
	hasMarker bool
}

func parseScalingScenario(config string) scalingScenario {
	selectedIdx := -1
	selectedMarker := ""
	for _, marker := range []string{"_writers_", "_readers_"} {
		idx := strings.LastIndex(config, marker)
		if idx > selectedIdx {
			selectedIdx = idx
			selectedMarker = marker
		}
	}
	if selectedIdx < 0 {
		return scalingScenario{valid: true}
	}
	scenario := scalingScenario{hasMarker: true}
	count := config[selectedIdx+len(selectedMarker):]
	if count == "" {
		return scenario
	}
	for i := 0; i < len(count); i++ {
		if count[i] < '0' || count[i] > '9' {
			return scenario
		}
	}
	scenario.valid = true
	scenario.suffix = config[selectedIdx+1:]
	return scenario
}

func runConfig(row matrixRow, result benchmarkResult) string {
	if row.Config != "" {
		return row.Config
	}
	if row.Target == "mongo" {
		if result.ClientMode != "" && result.ClientMode != "driver" {
			return "mongo_" + normalizeConfigName(result.ClientMode)
		}
		return "mongo"
	}
	if result.TreeDBDocumentFormat != "" {
		config := "treedb_" + normalizeConfigName(result.TreeDBDocumentFormat)
		if result.ClientMode != "" && result.ClientMode != "driver" {
			config += "_" + normalizeConfigName(result.ClientMode)
		}
		return config
	}
	return "treedb"
}

func normalizeConfigName(raw string) string {
	name := strings.ToLower(strings.TrimSpace(raw))
	name = strings.ReplaceAll(name, "-", "_")
	if name == "" {
		return "default"
	}
	return name
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
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	columns := make(map[string]int, len(header))
	for i, name := range header {
		columns[strings.TrimSpace(name)] = i
	}
	required := []string{"target", "documents", "secondary_indexes", "raw_json", "physical_bytes"}
	for _, name := range required {
		if _, ok := columns[name]; !ok {
			return nil, fmt.Errorf("matrix missing %q column", name)
		}
	}
	var rows []matrixRow
	for line := 2; ; line++ {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("matrix line %d: %w", line, err)
		}
		if len(record) == 0 || strings.HasPrefix(strings.TrimSpace(record[0]), "#") {
			continue
		}
		row, err := parseMatrixRow(columns, record)
		if err != nil {
			return nil, fmt.Errorf("matrix line %d: %w", line, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func parseMatrixRow(columns map[string]int, record []string) (matrixRow, error) {
	field := func(name string) string {
		idx := columns[name]
		if idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}
	documents, err := strconv.Atoi(field("documents"))
	if err != nil || documents <= 0 {
		return matrixRow{}, fmt.Errorf("invalid documents %q", field("documents"))
	}
	indexes, err := strconv.Atoi(field("secondary_indexes"))
	if err != nil || indexes < 0 {
		return matrixRow{}, fmt.Errorf("invalid secondary_indexes %q", field("secondary_indexes"))
	}
	physicalBytes := int64(0)
	if raw := field("physical_bytes"); raw != "" {
		physicalBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || physicalBytes < 0 {
			return matrixRow{}, fmt.Errorf("invalid physical_bytes %q", raw)
		}
	}
	row := matrixRow{
		Target:           field("target"),
		Config:           optionalField(columns, record, "config"),
		Documents:        documents,
		SecondaryIndexes: indexes,
		RawJSON:          field("raw_json"),
		PhysicalBytes:    physicalBytes,
	}
	if row.Target != "treedb" && row.Target != "mongo" {
		return matrixRow{}, fmt.Errorf("invalid target %q", row.Target)
	}
	if row.RawJSON == "" {
		return matrixRow{}, errors.New("raw_json is required")
	}
	return row, nil
}

func optionalField(columns map[string]int, record []string, name string) string {
	idx, ok := columns[name]
	if !ok || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}

func readBenchmarkResult(path string) (benchmarkResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return benchmarkResult{}, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.UseNumber()
	var result benchmarkResult
	if err := dec.Decode(&result); err != nil {
		return benchmarkResult{}, err
	}
	return result, nil
}

func phaseMap(phases []phaseResult) map[string]phaseResult {
	out := make(map[string]phaseResult, len(phases))
	for _, phase := range phases {
		out[phase.Name] = phase
	}
	return out
}

func renderReport(cfg config, cells []cellComparison, generatedAt time.Time) string {
	var b strings.Builder
	hasMongo := hasMongoCells(cells)
	fmt.Fprintf(&b, "# %s\n\n", cfg.Title)
	fmt.Fprintf(&b, "- generated_at: `%s`\n", generatedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "- matrix: `%s`\n", cfg.MatrixPath)
	fmt.Fprintf(&b, "- comparison cells: `%d`\n", len(cells))
	if hasMongo {
		fmt.Fprintf(&b, "- targets: `treedb`, `mongo`\n\n")
	} else {
		fmt.Fprintf(&b, "- targets: `treedb`\n\n")
	}
	b.WriteString("## Highlights\n\n")
	for _, line := range highlightLines(cells) {
		fmt.Fprintf(&b, "- %s\n", line)
	}
	b.WriteString("\n")
	renderDiskTable(&b, cells)
	b.WriteString("\n")
	renderMongoRowsTable(&b, cells)
	b.WriteString("\n")
	renderConcurrentReadSweepTable(&b, cells)
	renderConcurrentRangeReadSweepTable(&b, cells)
	renderWriterSweepCounterTable(&b, cells)
	renderOpsTable(&b, cells)
	b.WriteString("\n")
	b.WriteString("## Raw Inputs\n\n")
	b.WriteString("| docs | indexes | range index | config | target | raw json | profile dir |\n")
	b.WriteString("| ---: | ---: | --- | --- | --- | --- | --- |\n")
	for _, cell := range cells {
		fmt.Fprintf(&b, "| %d | %d | %t | `%s` | treedb | `%s` | %s |\n",
			cell.Key.Documents, cell.Key.SecondaryIndexes, cell.TreeDB.Result.RangeIndex, cell.Key.TreeDBConfig, cell.TreeDB.DisplayRawPath, formatOptionalCode(cell.TreeDB.Result.ProfileDir))
	}
	for _, mongo := range uniqueMongoRecords(cells) {
		fmt.Fprintf(&b, "| %d | %d | %t | `%s` | mongo | `%s` | %s |\n",
			mongo.Row.Documents, mongo.Row.SecondaryIndexes, mongo.Result.RangeIndex, mongo.Row.Config, mongo.DisplayRawPath, formatOptionalCode(mongo.Result.ProfileDir))
	}
	b.WriteString("\n")
	b.WriteString("## Notes\n\n")
	b.WriteString("- TreeDB disk bytes prefer `treedb_disk_after_maintenance.total_bytes`, then fall back to `treedb_disk_after_checkpoint.total_bytes` and `treedb_disk_after_load.total_bytes` for older runs.\n")
	b.WriteString("- TreeDB physical bytes come from the matrix runner's `du` measurement of the isolated TreeDB directory.\n")
	b.WriteString("- MongoDB `dbStats.dataSize` is uncompressed logical document size, not disk usage.\n")
	b.WriteString("- MongoDB `dbStats.totalSize` is reported separately because it can diverge sharply from the isolated data-directory `du` measurement on small WiredTiger workloads.\n")
	b.WriteString("- MongoDB physical bytes are the preferred local disk comparison when the matrix runner has an isolated data directory, such as Docker mode.\n")
	b.WriteString("- Wall ops/sec values include the full benchmark phase loop. Sampled ops/sec values isolate the timed driver/gateway call inside each phase and are useful when prebuilt fixtures are enabled.\n")
	b.WriteString("- `concurrent_id_find_one_rN` phases are an `_id` read throughput sweep over `N` concurrent readers, and are grouped in the Concurrent Read Sweep section when present.\n")
	b.WriteString("- `concurrent_email_find_one_rN` phases are an email read throughput sweep over `N` concurrent readers, and are also grouped in the Concurrent Read Sweep section when present.\n")
	b.WriteString("- `concurrent_age_range_*_rN` phases are an age range-read throughput sweep over `N` concurrent readers, and are grouped in the Concurrent Range Read Sweep section when present.\n")
	b.WriteString("- Range-query benchmark rows use explicit phase names: `age_range_indexed_limit_10` means `-range-index` created `age_1`; `age_range_scan_limit_10` means bounded scan fallback.\n")
	b.WriteString("- Main comparison tables label the MongoDB config chosen as the baseline. In multi-mode matrices, all other MongoDB rows remain visible in the Mongo Matrix Rows section.\n")
	return b.String()
}

func renderMongoRowsTable(b *strings.Builder, cells []cellComparison) {
	rows := uniqueMongoRecords(cells)
	if len(rows) == 0 {
		return
	}
	b.WriteString("## Mongo Matrix Rows\n\n")
	b.WriteString("These rows list every MongoDB matrix row, including non-baseline client-mode rows that are not used as the TreeDB comparison baseline.\n\n")
	b.WriteString("| docs | indexes | range index | config | phase | ops/sec | sampled ops/sec | p95 us | dbStats totalSize | physical du | raw json |\n")
	b.WriteString("| ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, record := range rows {
		phase, hasPhase := primaryMongoPhase(record)
		total, totalOK := mongoDBStatsTotalBytes(record.Result)
		fmt.Fprintf(b, "| %d | %d | %t | `%s` | `%s` | %s | %s | %s | %s | %s | `%s` |\n",
			record.Row.Documents,
			record.Row.SecondaryIndexes,
			record.Result.RangeIndex,
			record.Row.Config,
			phase.Name,
			formatPhaseOps(hasPhase, phase.OpsPerSecond),
			formatPhaseOps(hasPhase && phase.SampledOpsPerSecond > 0, phase.SampledOpsPerSecond),
			formatPhaseLatency(hasPhase, phase.LatencyMicros.P95),
			formatMeasuredBytes(totalOK, total),
			formatOptionalBytes(record.Row.PhysicalBytes),
			record.DisplayRawPath,
		)
	}
	b.WriteString("\n")
}

func primaryMongoPhase(record *runRecord) (phaseResult, bool) {
	if record == nil {
		return phaseResult{}, false
	}
	if phaseName := scalingPrimaryPhaseName(record.Row.Config); phaseName != "" {
		if phase, ok := record.PhaseMap[phaseName]; ok {
			return phase, true
		}
	}
	if phase, ok := record.PhaseMap["load_insert_many"]; ok {
		return phase, true
	}
	for _, phase := range record.Result.Phases {
		if phase.OpsPerSecond > 0 {
			return phase, true
		}
	}
	if len(record.Result.Phases) > 0 {
		return record.Result.Phases[0], true
	}
	return phaseResult{}, false
}

func scalingPrimaryPhaseName(config string) string {
	scenario := parseScalingScenario(config)
	if !scenario.valid || !scenario.hasMarker {
		return ""
	}
	if count, ok := strings.CutPrefix(scenario.suffix, "writers_"); ok {
		return "concurrent_id_update_set_w" + count
	}
	if count, ok := strings.CutPrefix(scenario.suffix, "readers_"); ok {
		return "concurrent_id_find_one_r" + count
	}
	return ""
}

type runRecordRawKey struct {
	baseCellKey
	Config     string
	RangeIndex bool
	RawPath    string
}

func uniqueMongoRecords(cells []cellComparison) []*runRecord {
	seen := make(map[runRecordRawKey]struct{})
	var out []*runRecord
	for _, cell := range cells {
		addMongoRecord(&out, seen, cell.Mongo)
		for _, record := range cell.MongoAlternates {
			addMongoRecord(&out, seen, record)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Row.Documents != b.Row.Documents {
			return a.Row.Documents < b.Row.Documents
		}
		if a.Row.SecondaryIndexes != b.Row.SecondaryIndexes {
			return a.Row.SecondaryIndexes < b.Row.SecondaryIndexes
		}
		if a.Result.RangeIndex != b.Result.RangeIndex {
			return !a.Result.RangeIndex && b.Result.RangeIndex
		}
		if a.Row.Config != b.Row.Config {
			return a.Row.Config < b.Row.Config
		}
		return a.DisplayRawPath < b.DisplayRawPath
	})
	return out
}

func addMongoRecord(out *[]*runRecord, seen map[runRecordRawKey]struct{}, record *runRecord) {
	if record == nil {
		return
	}
	key := runRecordRawKey{
		baseCellKey: baseCellKey{Documents: record.Row.Documents, SecondaryIndexes: record.Row.SecondaryIndexes},
		Config:      record.Row.Config,
		RangeIndex:  record.Result.RangeIndex,
		RawPath:     record.DisplayRawPath,
	}
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	*out = append(*out, record)
}

func hasMongoCells(cells []cellComparison) bool {
	for _, cell := range cells {
		if cell.Mongo != nil {
			return true
		}
	}
	return false
}

func highlightLines(cells []cellComparison) []string {
	var lines []string
	hasMongo := hasMongoCells(cells)
	phaseComparisons := allPhaseComparisons(cells)
	var bestTreeDB *phaseComparison
	var bestMongo *phaseComparison
	for i := range phaseComparisons {
		cmp := &phaseComparisons[i]
		if !cmp.HasTreeDB || !cmp.HasMongo || cmp.MongoPhase.OpsPerSecond <= 0 || cmp.TreeDBPhase.OpsPerSecond <= 0 {
			continue
		}
		if cmp.Ratio >= 1 && (bestTreeDB == nil || cmp.Ratio > bestTreeDB.Ratio) {
			bestTreeDB = cmp
		}
		if cmp.Ratio < 1 && (bestMongo == nil || cmp.Ratio < bestMongo.Ratio) {
			bestMongo = cmp
		}
	}
	if bestTreeDB != nil {
		lines = append(lines, fmt.Sprintf("Largest TreeDB ops/sec lead: `%s` at %d docs / %d indexes / `%s`, %s ops/sec vs %s ops/sec (%s TreeDB / MongoDB).",
			bestTreeDB.Name,
			bestTreeDB.Cell.Documents,
			bestTreeDB.Cell.SecondaryIndexes,
			bestTreeDB.Cell.TreeDBConfig,
			formatNumber(bestTreeDB.TreeDBPhase.OpsPerSecond),
			formatNumber(bestTreeDB.MongoPhase.OpsPerSecond),
			formatRatio(bestTreeDB.Ratio),
		))
	} else {
		if hasMongo {
			lines = append(lines, "No phase in this matrix had TreeDB ahead on ops/sec.")
		} else {
			lines = append(lines, "No MongoDB baseline rows were present; ops/sec tables are TreeDB-only.")
		}
	}
	if bestMongo != nil {
		lines = append(lines, fmt.Sprintf("Largest MongoDB ops/sec lead: `%s` at %d docs / %d indexes / `%s`, %s ops/sec vs %s ops/sec (%s TreeDB / MongoDB).",
			bestMongo.Name,
			bestMongo.Cell.Documents,
			bestMongo.Cell.SecondaryIndexes,
			bestMongo.Cell.TreeDBConfig,
			formatNumber(bestMongo.TreeDBPhase.OpsPerSecond),
			formatNumber(bestMongo.MongoPhase.OpsPerSecond),
			formatRatio(bestMongo.Ratio),
		))
	} else if hasMongo {
		lines = append(lines, "No phase in this matrix had MongoDB ahead on ops/sec.")
	}
	if cell := largestDiskCell(cells); cell != nil {
		treeBytes, treeSnapshot, treeOK := treeDBBytesSnapshot(cell.TreeDB.Result)
		treePhysical := cell.TreeDB.Row.PhysicalBytes
		treeSnapshotClause := "TreeDB disk snapshot was n/a"
		if treeOK {
			treeSnapshotClause = fmt.Sprintf("TreeDB %s snapshot used %s (%s/doc)",
				treeSnapshot,
				formatBytes(treeBytes),
				formatBytesPerDoc(treeBytes, cell.Key.Documents),
			)
		}
		if cell.Mongo == nil {
			lines = append(lines, fmt.Sprintf("Largest TreeDB disk cell: at %d docs / %d indexes / `%s`, %s, and TreeDB physical du was %s (%s/doc).",
				cell.Key.Documents,
				cell.Key.SecondaryIndexes,
				cell.Key.TreeDBConfig,
				treeSnapshotClause,
				formatOptionalBytes(treePhysical),
				formatOptionalBytesPerDoc(treePhysical, cell.Key.Documents),
			))
		} else {
			mongoData, mongoDataOK := mongoDataBytes(cell.Mongo.Result)
			mongoTotal, mongoTotalOK := mongoDBStatsTotalBytes(cell.Mongo.Result)
			mongoPhysical := cell.Mongo.Row.PhysicalBytes
			lines = append(lines, fmt.Sprintf("Largest cell disk: at %d docs / %d indexes / `%s`, %s, TreeDB physical du was %s, MongoDB dbStats dataSize was %s, MongoDB dbStats totalSize was %s, and MongoDB physical du was %s (%s/doc).",
				cell.Key.Documents,
				cell.Key.SecondaryIndexes,
				cell.Key.TreeDBConfig,
				treeSnapshotClause,
				formatOptionalBytes(treePhysical),
				formatMeasuredBytes(mongoDataOK, mongoData),
				formatMeasuredBytes(mongoTotalOK, mongoTotal),
				formatOptionalBytes(mongoPhysical),
				formatOptionalBytesPerDoc(mongoPhysical, cell.Key.Documents),
			))
		}
	}
	return lines
}

func largestDiskCell(cells []cellComparison) *cellComparison {
	if len(cells) == 0 {
		return nil
	}
	best := &cells[0]
	for i := 1; i < len(cells); i++ {
		if cellDiskScore(cells[i]) > cellDiskScore(*best) {
			best = &cells[i]
		}
	}
	return best
}

func cellDiskScore(cell cellComparison) int64 {
	var score int64
	if cell.TreeDB != nil {
		treeBytes, _ := treeDBBytes(cell.TreeDB.Result)
		if cell.TreeDB.Row.PhysicalBytes > 0 {
			score += cell.TreeDB.Row.PhysicalBytes
		} else {
			score += treeBytes
		}
	}
	if cell.Mongo != nil {
		if cell.Mongo.Row.PhysicalBytes > 0 {
			score += cell.Mongo.Row.PhysicalBytes
		} else if mongoTotal, ok := mongoDBStatsTotalBytes(cell.Mongo.Result); ok {
			score += mongoTotal
		}
	}
	return score
}

func renderDiskTable(b *strings.Builder, cells []cellComparison) {
	b.WriteString("## Disk Summary\n\n")
	b.WriteString("| docs | indexes | range index | TreeDB config | MongoDB baseline config | TreeDB snapshot | TreeDB bytes | TreeDB bytes/doc | TreeDB physical du | TreeDB physical bytes/doc | MongoDB dbStats dataSize | MongoDB dbStats totalSize | MongoDB physical du | MongoDB physical bytes/doc | TreeDB / MongoDB dbStats totalSize | TreeDB / MongoDB physical |\n")
	b.WriteString("| ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, cell := range cells {
		treeBytes, treeSnapshot, treeOK := treeDBBytesSnapshot(cell.TreeDB.Result)
		treePhysical := cell.TreeDB.Row.PhysicalBytes
		hasMongo := cell.Mongo != nil
		var mongoData, mongoTotal, mongoPhysical int64
		var mongoDataOK, mongoTotalOK bool
		if hasMongo {
			mongoData, mongoDataOK = mongoDataBytes(cell.Mongo.Result)
			mongoTotal, mongoTotalOK = mongoDBStatsTotalBytes(cell.Mongo.Result)
			mongoPhysical = cell.Mongo.Row.PhysicalBytes
		}
		fmt.Fprintf(b, "| %d | %d | %t | `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			cell.Key.Documents,
			cell.Key.SecondaryIndexes,
			cell.TreeDB.Result.RangeIndex,
			cell.Key.TreeDBConfig,
			formatRunConfig(cell.Mongo),
			treeSnapshot,
			formatMeasuredBytes(treeOK, treeBytes),
			formatMeasuredBytesPerDoc(treeOK, treeBytes, cell.Key.Documents),
			formatOptionalBytes(treePhysical),
			formatOptionalBytesPerDoc(treePhysical, cell.Key.Documents),
			formatMeasuredBytes(mongoDataOK, mongoData),
			formatMeasuredBytes(mongoTotalOK, mongoTotal),
			formatOptionalBytes(mongoPhysical),
			formatOptionalBytesPerDoc(mongoPhysical, cell.Key.Documents),
			formatMeasuredRatio(treeOK && mongoTotalOK, treeBytes, mongoTotal),
			formatRatio(safeRatio(float64(treePhysical), float64(mongoPhysical))),
		)
	}
}

func renderConcurrentReadSweepTable(b *strings.Builder, cells []cellComparison) {
	rows := concurrentReadSweepComparisons(cells)
	if len(rows) == 0 {
		return
	}
	b.WriteString("## Concurrent Read Sweep\n\n")
	b.WriteString("These rows group `concurrent_*_rN` phases as read throughput sweeps. Serial read phases remain separate single-in-flight latency phases.\n\n")
	b.WriteString("| docs | indexes | TreeDB config | MongoDB baseline config | phase | readers | TreeDB wall ops/sec | TreeDB sampled ops/sec | MongoDB wall ops/sec | MongoDB sampled ops/sec | TreeDB / MongoDB wall | TreeDB p95 us | MongoDB p95 us |\n")
	b.WriteString("| ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, cmp := range rows {
		readers, _ := concurrentReadReaders(cmp.Name)
		fmt.Fprintf(b, "| %d | %d | `%s` | %s | `%s` | %d | %s | %s | %s | %s | %s | %s | %s |\n",
			cmp.Cell.Documents,
			cmp.Cell.SecondaryIndexes,
			cmp.Cell.TreeDBConfig,
			formatConfig(cmp.MongoConfig),
			cmp.Name,
			readers,
			formatPhaseOps(cmp.HasTreeDB, cmp.TreeDBPhase.OpsPerSecond),
			formatPhaseOps(cmp.HasTreeDB && cmp.TreeDBPhase.SampledOpsPerSecond > 0, cmp.TreeDBPhase.SampledOpsPerSecond),
			formatPhaseOps(cmp.HasMongo, cmp.MongoPhase.OpsPerSecond),
			formatPhaseOps(cmp.HasMongo && cmp.MongoPhase.SampledOpsPerSecond > 0, cmp.MongoPhase.SampledOpsPerSecond),
			formatRatio(cmp.Ratio),
			formatPhaseLatency(cmp.HasTreeDB, cmp.TreeDBPhase.LatencyMicros.P95),
			formatPhaseLatency(cmp.HasMongo, cmp.MongoPhase.LatencyMicros.P95),
		)
	}
	b.WriteString("\n")
}

func renderConcurrentRangeReadSweepTable(b *strings.Builder, cells []cellComparison) {
	rows := concurrentRangeReadSweepComparisons(cells)
	if len(rows) == 0 {
		return
	}
	b.WriteString("## Concurrent Range Read Sweep\n\n")
	b.WriteString("These rows group `concurrent_age_range_*_rN` phases as one age range-read throughput sweep. Serial `age_range_*_limit_10` remains a separate single-in-flight latency phase.\n\n")
	b.WriteString("| docs | indexes | range mode | TreeDB config | MongoDB baseline config | readers | TreeDB wall ops/sec | TreeDB sampled ops/sec | MongoDB wall ops/sec | MongoDB sampled ops/sec | TreeDB / MongoDB wall | TreeDB p95 us | MongoDB p95 us |\n")
	b.WriteString("| ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, cmp := range rows {
		readers, _ := concurrentRangeReadReaders(cmp.Name)
		fmt.Fprintf(b, "| %d | %d | %s | `%s` | %s | %d | %s | %s | %s | %s | %s | %s | %s |\n",
			cmp.Cell.Documents,
			cmp.Cell.SecondaryIndexes,
			formatRangeMode(cmp.Name),
			cmp.Cell.TreeDBConfig,
			formatConfig(cmp.MongoConfig),
			readers,
			formatPhaseOps(cmp.HasTreeDB, cmp.TreeDBPhase.OpsPerSecond),
			formatPhaseOps(cmp.HasTreeDB && cmp.TreeDBPhase.SampledOpsPerSecond > 0, cmp.TreeDBPhase.SampledOpsPerSecond),
			formatPhaseOps(cmp.HasMongo, cmp.MongoPhase.OpsPerSecond),
			formatPhaseOps(cmp.HasMongo && cmp.MongoPhase.SampledOpsPerSecond > 0, cmp.MongoPhase.SampledOpsPerSecond),
			formatRatio(cmp.Ratio),
			formatPhaseLatency(cmp.HasTreeDB, cmp.TreeDBPhase.LatencyMicros.P95),
			formatPhaseLatency(cmp.HasMongo, cmp.MongoPhase.LatencyMicros.P95),
		)
	}
	b.WriteString("\n")
}

func concurrentReadSweepComparisons(cells []cellComparison) []phaseComparison {
	var out []phaseComparison
	for _, cell := range cells {
		if !cellHasConcurrentReadSweep(cell) {
			continue
		}
		var mongoPhases []phaseResult
		mongoPhaseMap := map[string]phaseResult{}
		if cell.Mongo != nil {
			mongoPhases = cell.Mongo.Result.Phases
			mongoPhaseMap = cell.Mongo.PhaseMap
		}
		for _, name := range phaseNames(cell.TreeDB.Result.Phases, mongoPhases) {
			if _, ok := concurrentRangeReadReaders(name); ok {
				continue
			}
			if _, ok := concurrentReadReaders(name); !ok {
				continue
			}
			treePhase, hasTree := cell.TreeDB.PhaseMap[name]
			mongoPhase, hasMongo := mongoPhaseMap[name]
			out = append(out, phaseComparison{
				Cell:        cell.Key,
				Name:        name,
				RangeIndex:  cell.TreeDB.Result.RangeIndex,
				MongoConfig: recordConfig(cell.Mongo),
				TreeDBPhase: treePhase,
				MongoPhase:  mongoPhase,
				HasTreeDB:   hasTree,
				HasMongo:    hasMongo,
				Ratio:       safeRatio(treePhase.OpsPerSecond, mongoPhase.OpsPerSecond),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.Cell.Documents != right.Cell.Documents {
			return left.Cell.Documents < right.Cell.Documents
		}
		if left.Cell.SecondaryIndexes != right.Cell.SecondaryIndexes {
			return left.Cell.SecondaryIndexes < right.Cell.SecondaryIndexes
		}
		if left.Cell.TreeDBConfig != right.Cell.TreeDBConfig {
			return left.Cell.TreeDBConfig < right.Cell.TreeDBConfig
		}
		leftBase := concurrentReadBase(left.Name)
		rightBase := concurrentReadBase(right.Name)
		if leftBase != rightBase {
			return leftBase < rightBase
		}
		leftReaders, _ := concurrentReadReaders(left.Name)
		rightReaders, _ := concurrentReadReaders(right.Name)
		return leftReaders < rightReaders
	})
	return out
}

func concurrentRangeReadSweepComparisons(cells []cellComparison) []phaseComparison {
	var out []phaseComparison
	for _, cell := range cells {
		if !cellHasConcurrentRangeReadSweep(cell) {
			continue
		}
		var mongoPhases []phaseResult
		mongoPhaseMap := map[string]phaseResult{}
		if cell.Mongo != nil {
			mongoPhases = cell.Mongo.Result.Phases
			mongoPhaseMap = cell.Mongo.PhaseMap
		}
		for _, name := range phaseNames(cell.TreeDB.Result.Phases, mongoPhases) {
			if _, ok := concurrentRangeReadReaders(name); !ok {
				continue
			}
			treePhase, hasTree := cell.TreeDB.PhaseMap[name]
			mongoPhase, hasMongo := mongoPhaseMap[name]
			out = append(out, phaseComparison{
				Cell:        cell.Key,
				Name:        name,
				RangeIndex:  cell.TreeDB.Result.RangeIndex,
				MongoConfig: recordConfig(cell.Mongo),
				TreeDBPhase: treePhase,
				MongoPhase:  mongoPhase,
				HasTreeDB:   hasTree,
				HasMongo:    hasMongo,
				Ratio:       safeRatio(treePhase.OpsPerSecond, mongoPhase.OpsPerSecond),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.Cell.Documents != right.Cell.Documents {
			return left.Cell.Documents < right.Cell.Documents
		}
		if left.Cell.SecondaryIndexes != right.Cell.SecondaryIndexes {
			return left.Cell.SecondaryIndexes < right.Cell.SecondaryIndexes
		}
		if left.Cell.TreeDBConfig != right.Cell.TreeDBConfig {
			return left.Cell.TreeDBConfig < right.Cell.TreeDBConfig
		}
		if leftMode, rightMode := rangeMode(left.Name), rangeMode(right.Name); leftMode != rightMode {
			return leftMode < rightMode
		}
		leftReaders, _ := concurrentRangeReadReaders(left.Name)
		rightReaders, _ := concurrentRangeReadReaders(right.Name)
		return leftReaders < rightReaders
	})
	return out
}

func cellHasConcurrentReadSweep(cell cellComparison) bool {
	if len(cell.TreeDB.Result.ConcurrentReaderSweep) > 0 {
		return true
	}
	if cell.Mongo != nil && len(cell.Mongo.Result.ConcurrentReaderSweep) > 0 {
		return true
	}
	readers := make(map[int]struct{})
	for _, phase := range cell.TreeDB.Result.Phases {
		if readerCount, ok := concurrentReadReaders(phase.Name); ok {
			readers[readerCount] = struct{}{}
		}
	}
	if cell.Mongo != nil {
		for _, phase := range cell.Mongo.Result.Phases {
			if readerCount, ok := concurrentReadReaders(phase.Name); ok {
				readers[readerCount] = struct{}{}
			}
		}
	}
	return len(readers) > 1
}

func cellHasConcurrentRangeReadSweep(cell cellComparison) bool {
	if len(cell.TreeDB.Result.ConcurrentRangeReaderSweep) > 0 {
		return true
	}
	if cell.Mongo != nil && len(cell.Mongo.Result.ConcurrentRangeReaderSweep) > 0 {
		return true
	}
	readers := make(map[int]struct{})
	for _, phase := range cell.TreeDB.Result.Phases {
		if readerCount, ok := concurrentRangeReadReaders(phase.Name); ok {
			readers[readerCount] = struct{}{}
		}
	}
	if cell.Mongo != nil {
		for _, phase := range cell.Mongo.Result.Phases {
			if readerCount, ok := concurrentRangeReadReaders(phase.Name); ok {
				readers[readerCount] = struct{}{}
			}
		}
	}
	return len(readers) > 1
}

func renderWriterSweepCounterTable(b *strings.Builder, cells []cellComparison) {
	rows := writerSweepComparisons(cells)
	if len(rows) == 0 {
		return
	}
	b.WriteString("## 0-Index Writer Sweep Counters\n\n")
	b.WriteString("These rows preserve TreeDB per-phase counter deltas for `concurrent_id_update_set_wN` phases. Values come from `phase.treedb_metrics` when present, so load counters and writer counters remain separate.\n\n")
	b.WriteString("| docs | indexes | TreeDB config | MongoDB baseline config | writers | TreeDB ops/s | MongoDB ops/s | TreeDB p95 us | MongoDB p95 us | TreeDB driver calls | MongoDB driver calls | publish calls/doc | root apply calls/doc | roots/publish | root apply ns/doc | leaf-log loads/doc | leaf-log pages written/doc | leaf-log read bytes/doc | leaf-log write bytes/doc | indexed flush calls/doc | indexed flush units/batch | indexed flush docs/batch | indexed flush root-runs/doc | root-delta entries/doc | root-delta key bytes/doc | root-delta value bytes/doc | root-delta tombstones/doc | affected primary roots/doc | affected secondary roots/doc | primary root publishes/doc | primary root delta entries/doc | primary root delta bytes/doc | primary-only coalesced docs/publish | raw JSON |\n")
	b.WriteString("| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, cmp := range rows {
		writers, _ := concurrentUpdateWriters(cmp.Name)
		cell := findCell(cells, cmp.Cell)
		row := []string{
			strconv.Itoa(cmp.Cell.Documents),
			strconv.Itoa(cmp.Cell.SecondaryIndexes),
			"`" + cmp.Cell.TreeDBConfig + "`",
			formatConfig(cmp.MongoConfig),
			strconv.Itoa(writers),
			formatPhaseOps(cmp.HasTreeDB, cmp.TreeDBPhase.OpsPerSecond),
			formatPhaseOps(cmp.HasMongo, cmp.MongoPhase.OpsPerSecond),
			formatPhaseLatency(cmp.HasTreeDB, cmp.TreeDBPhase.LatencyMicros.P95),
			formatPhaseLatency(cmp.HasMongo, cmp.MongoPhase.LatencyMicros.P95),
			formatPhaseDriverCalls(cmp.HasTreeDB, cmp.TreeDBPhase.DriverCalls),
			formatPhaseDriverCalls(cmp.HasMongo, cmp.MongoPhase.DriverCalls),
			formatPhaseMetric(cmp.TreeDBPhase, "publish_delta_group_calls/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "root_apply_calls/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "roots/publish"),
			formatPhaseMetric(cmp.TreeDBPhase, "publish_delta_group_root_apply_ns/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "leaf_log_node_loads/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "leaf_log_pages_written/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "leaf_log_read_bytes/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "leaf_log_write_bytes/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "indexed_flush_calls/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "indexed_flush_units/batch"),
			formatPhaseMetric(cmp.TreeDBPhase, "indexed_flush_docs/batch"),
			formatPhaseMetric(cmp.TreeDBPhase, "indexed_flush_root_runs/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "root_delta_plan_entries/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "root_delta_plan_key_bytes/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "root_delta_plan_value_bytes/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "root_delta_plan_tombstones/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "affected_primary_roots/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "affected_secondary_roots/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "primary_root_publishes/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "primary_root_delta_entries/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "primary_root_delta_bytes/doc"),
			formatPhaseMetric(cmp.TreeDBPhase, "primary_only_coalesced_docs/publish"),
			"`" + cell.TreeDB.DisplayRawPath + "`",
		}
		b.WriteString("| " + strings.Join(row, " | ") + " |\n")
	}
	b.WriteString("\n")
}

func writerSweepComparisons(cells []cellComparison) []phaseComparison {
	var out []phaseComparison
	for _, cell := range cells {
		if cell.Key.SecondaryIndexes != 0 {
			continue
		}
		if cell.TreeDB == nil {
			continue
		}
		var mongoPhaseMap map[string]phaseResult
		if cell.Mongo != nil {
			mongoPhaseMap = cell.Mongo.PhaseMap
		}
		for _, phase := range cell.TreeDB.Result.Phases {
			if _, ok := concurrentUpdateWriters(phase.Name); !ok {
				continue
			}
			mongoPhase, hasMongo := mongoPhaseMap[phase.Name]
			out = append(out, phaseComparison{
				Cell:        cell.Key,
				Name:        phase.Name,
				RangeIndex:  cell.TreeDB.Result.RangeIndex,
				MongoConfig: recordConfig(cell.Mongo),
				TreeDBPhase: phase,
				MongoPhase:  mongoPhase,
				HasTreeDB:   true,
				HasMongo:    hasMongo,
				Ratio:       safeRatio(phase.OpsPerSecond, mongoPhase.OpsPerSecond),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.Cell.Documents != right.Cell.Documents {
			return left.Cell.Documents < right.Cell.Documents
		}
		if left.Cell.SecondaryIndexes != right.Cell.SecondaryIndexes {
			return left.Cell.SecondaryIndexes < right.Cell.SecondaryIndexes
		}
		if left.Cell.TreeDBConfig != right.Cell.TreeDBConfig {
			return left.Cell.TreeDBConfig < right.Cell.TreeDBConfig
		}
		leftWriters, _ := concurrentUpdateWriters(left.Name)
		rightWriters, _ := concurrentUpdateWriters(right.Name)
		return leftWriters < rightWriters
	})
	return out
}

func concurrentReadReaders(name string) (int, bool) {
	base := concurrentReadBase(name)
	if base == "" {
		return 0, false
	}
	readers, err := strconv.Atoi(strings.TrimPrefix(name, base+"_r"))
	if err != nil || readers <= 0 {
		return 0, false
	}
	return readers, true
}

func concurrentReadBase(name string) string {
	for _, base := range []string{
		"concurrent_id_find_one",
		"concurrent_email_find_one",
		"concurrent_age_range_indexed_limit_10",
		"concurrent_age_range_scan_limit_10",
	} {
		if strings.HasPrefix(name, base+"_r") {
			return base
		}
	}
	return ""
}

func concurrentRangeReadReaders(name string) (int, bool) {
	if !strings.HasPrefix(name, "concurrent_") || !strings.Contains(name, "_range_") {
		return 0, false
	}
	idx := strings.LastIndex(name, "_r")
	if idx < 0 {
		return 0, false
	}
	prefix, count := name[:idx], name[idx+2:]
	if !strings.Contains(prefix, "_limit_") {
		return 0, false
	}
	readers, err := strconv.Atoi(count)
	if err != nil || readers <= 0 {
		return 0, false
	}
	return readers, true
}

func concurrentUpdateWriters(name string) (int, bool) {
	const prefix = "concurrent_id_update_set_w"
	if !strings.HasPrefix(name, prefix) {
		return 0, false
	}
	writers, err := strconv.Atoi(strings.TrimPrefix(name, prefix))
	if err != nil || writers <= 0 {
		return 0, false
	}
	return writers, true
}

func renderOpsTable(b *strings.Builder, cells []cellComparison) {
	b.WriteString("## Ops/Sec Summary\n\n")
	b.WriteString("| docs | indexes | range index | range mode | TreeDB config | MongoDB baseline config | phase | TreeDB wall ops/sec | TreeDB sampled ops/sec | MongoDB wall ops/sec | MongoDB sampled ops/sec | TreeDB / MongoDB wall | TreeDB / MongoDB sampled | TreeDB p95 us | MongoDB p95 us |\n")
	b.WriteString("| ---: | ---: | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	sweepCells := make(map[cellKey]bool, len(cells))
	rangeSweepCells := make(map[cellKey]bool, len(cells))
	for _, cell := range cells {
		if cellHasConcurrentReadSweep(cell) {
			sweepCells[cell.Key] = true
		}
		if cellHasConcurrentRangeReadSweep(cell) {
			rangeSweepCells[cell.Key] = true
		}
	}
	for _, cmp := range allPhaseComparisons(cells) {
		if sweepCells[cmp.Cell] {
			if _, ok := concurrentReadReaders(cmp.Name); ok {
				continue
			}
		}
		if rangeSweepCells[cmp.Cell] {
			if _, ok := concurrentRangeReadReaders(cmp.Name); ok {
				continue
			}
		}
		sampledRatio := safeRatio(cmp.TreeDBPhase.SampledOpsPerSecond, cmp.MongoPhase.SampledOpsPerSecond)
		fmt.Fprintf(b, "| %d | %d | %t | %s | `%s` | %s | `%s` | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			cmp.Cell.Documents,
			cmp.Cell.SecondaryIndexes,
			cmp.TreeDBRangeIndex(),
			formatRangeMode(cmp.Name),
			cmp.Cell.TreeDBConfig,
			formatConfig(cmp.MongoConfig),
			cmp.Name,
			formatPhaseOps(cmp.HasTreeDB, cmp.TreeDBPhase.OpsPerSecond),
			formatPhaseOps(cmp.HasTreeDB && cmp.TreeDBPhase.SampledOpsPerSecond > 0, cmp.TreeDBPhase.SampledOpsPerSecond),
			formatPhaseOps(cmp.HasMongo, cmp.MongoPhase.OpsPerSecond),
			formatPhaseOps(cmp.HasMongo && cmp.MongoPhase.SampledOpsPerSecond > 0, cmp.MongoPhase.SampledOpsPerSecond),
			formatRatio(cmp.Ratio),
			formatRatio(sampledRatio),
			formatPhaseLatency(cmp.HasTreeDB, cmp.TreeDBPhase.LatencyMicros.P95),
			formatPhaseLatency(cmp.HasMongo, cmp.MongoPhase.LatencyMicros.P95),
		)
	}
}

func allPhaseComparisons(cells []cellComparison) []phaseComparison {
	var out []phaseComparison
	for _, cell := range cells {
		var mongoPhases []phaseResult
		mongoPhaseMap := map[string]phaseResult{}
		if cell.Mongo != nil {
			mongoPhases = cell.Mongo.Result.Phases
			mongoPhaseMap = cell.Mongo.PhaseMap
		}
		names := phaseNames(cell.TreeDB.Result.Phases, mongoPhases)
		for _, name := range names {
			treePhase, hasTree := cell.TreeDB.PhaseMap[name]
			mongoPhase, hasMongo := mongoPhaseMap[name]
			out = append(out, phaseComparison{
				Cell:        cell.Key,
				Name:        name,
				RangeIndex:  cell.TreeDB.Result.RangeIndex,
				MongoConfig: recordConfig(cell.Mongo),
				TreeDBPhase: treePhase,
				MongoPhase:  mongoPhase,
				HasTreeDB:   hasTree,
				HasMongo:    hasMongo,
				Ratio:       safeRatio(treePhase.OpsPerSecond, mongoPhase.OpsPerSecond),
			})
		}
	}
	return out
}

func phaseNames(treePhases, mongoPhases []phaseResult) []string {
	seen := make(map[string]bool, len(treePhases)+len(mongoPhases))
	var names []string
	for _, phase := range treePhases {
		if !seen[phase.Name] {
			seen[phase.Name] = true
			names = append(names, phase.Name)
		}
	}
	for _, phase := range mongoPhases {
		if !seen[phase.Name] {
			seen[phase.Name] = true
			names = append(names, phase.Name)
		}
	}
	return names
}

func (cmp phaseComparison) TreeDBRangeIndex() bool {
	return cmp.RangeIndex
}

func rangeMode(name string) string {
	if strings.Contains(name, "_range_indexed_") {
		return "indexed"
	}
	if strings.Contains(name, "_range_scan_") || strings.Contains(name, "_range_limit_") {
		return "scan"
	}
	return ""
}

func formatRangeMode(name string) string {
	mode := rangeMode(name)
	if mode == "" {
		return "n/a"
	}
	return "`" + mode + "`"
}

func formatOptionalCode(raw string) string {
	if raw == "" {
		return "n/a"
	}
	return "`" + raw + "`"
}

func recordConfig(record *runRecord) string {
	if record == nil {
		return ""
	}
	return record.Row.Config
}

func formatRunConfig(record *runRecord) string {
	return formatConfig(recordConfig(record))
}

func formatConfig(config string) string {
	if config == "" {
		return "n/a"
	}
	return "`" + config + "`"
}

func writeSummaryTSV(path string, cells []cellComparison) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := csv.NewWriter(f)
	writer.Comma = '\t'
	header := []string{
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
		"batch_size",
		"insert_producers",
		"effective_producers",
		"driver_calls",
		"load_batch_count",
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	for _, cmp := range allPhaseComparisons(cells) {
		cell := findCell(cells, cmp.Cell)
		treeBytes, treeSnapshot, treeOK := treeDBBytesSnapshot(cell.TreeDB.Result)
		treePhysical := cell.TreeDB.Row.PhysicalBytes
		hasMongo := cell.Mongo != nil
		var mongoData, mongoTotal, mongoPhysical int64
		var mongoDataOK, mongoTotalOK bool
		if hasMongo {
			mongoData, mongoDataOK = mongoDataBytes(cell.Mongo.Result)
			mongoTotal, mongoTotalOK = mongoDBStatsTotalBytes(cell.Mongo.Result)
			mongoPhysical = cell.Mongo.Row.PhysicalBytes
		}
		sampledRatio := safeRatio(cmp.TreeDBPhase.SampledOpsPerSecond, cmp.MongoPhase.SampledOpsPerSecond)
		row := []string{
			strconv.Itoa(cmp.Cell.Documents),
			strconv.Itoa(cmp.Cell.SecondaryIndexes),
			strconv.FormatBool(cmp.TreeDBRangeIndex()),
			rangeMode(cmp.Name),
			cmp.Cell.TreeDBConfig,
			recordConfig(cell.Mongo),
			cmp.Name,
			formatRawFloat(cmp.HasTreeDB, cmp.TreeDBPhase.OpsPerSecond),
			formatRawFloat(cmp.HasTreeDB && cmp.TreeDBPhase.SampledOpsPerSecond > 0, cmp.TreeDBPhase.SampledOpsPerSecond),
			formatRawFloat(cmp.HasTreeDB && cmp.TreeDBPhase.SampledNsPerOp > 0, cmp.TreeDBPhase.SampledNsPerOp),
			formatRawFloat(cmp.HasMongo, cmp.MongoPhase.OpsPerSecond),
			formatRawFloat(cmp.HasMongo && cmp.MongoPhase.SampledOpsPerSecond > 0, cmp.MongoPhase.SampledOpsPerSecond),
			formatRawFloat(cmp.HasMongo && cmp.MongoPhase.SampledNsPerOp > 0, cmp.MongoPhase.SampledNsPerOp),
			formatRawRatio(cmp.Ratio),
			formatRawRatio(sampledRatio),
			formatRawFloat(cmp.HasTreeDB, cmp.TreeDBPhase.LatencyMicros.P50),
			formatRawFloat(cmp.HasMongo, cmp.MongoPhase.LatencyMicros.P50),
			formatRawFloat(cmp.HasTreeDB, cmp.TreeDBPhase.LatencyMicros.P95),
			formatRawFloat(cmp.HasMongo, cmp.MongoPhase.LatencyMicros.P95),
			formatRawFloat(cmp.HasTreeDB, cmp.TreeDBPhase.LatencyMicros.P99),
			formatRawFloat(cmp.HasMongo, cmp.MongoPhase.LatencyMicros.P99),
			treeSnapshot,
			formatRawInt(treeOK, treeBytes),
			strconv.FormatInt(treePhysical, 10),
			formatRawInt(mongoDataOK, mongoData),
			formatRawInt(mongoTotalOK, mongoTotal),
			formatRawInt(hasMongo, mongoPhysical),
			formatRawMeasuredRatio(treeOK && mongoTotalOK, treeBytes, mongoTotal),
			formatRawRatio(safeRatio(float64(treePhysical), float64(mongoPhysical))),
		}
		row = append(row, summaryLoadMetadataFields(cmp, cell)...)
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func summaryLoadMetadataFields(cmp phaseComparison, cell cellComparison) []string {
	if cmp.Name != "load_insert_many" {
		return []string{"", "", "", "", ""}
	}
	result, phase, ok := summaryLoadMetadataSource(cmp, cell)
	if !ok {
		return []string{"", "", "", "", ""}
	}
	loadBatches := summaryLoadBatchCount(result, phase)
	effectiveProducers := summaryEffectiveLoadProducers(result, phase)
	return []string{
		formatRawInt(result.BatchSize > 0, int64(result.BatchSize)),
		formatRawInt(result.InsertProducers > 0, int64(result.InsertProducers)),
		formatRawInt(effectiveProducers > 0, int64(effectiveProducers)),
		formatRawInt(phase.DriverCalls > 0, int64(phase.DriverCalls)),
		formatRawInt(loadBatches > 0, int64(loadBatches)),
	}
}

func summaryLoadMetadataSource(cmp phaseComparison, cell cellComparison) (benchmarkResult, phaseResult, bool) {
	if cmp.HasTreeDB {
		return cell.TreeDB.Result, cmp.TreeDBPhase, true
	}
	if cmp.HasMongo && cell.Mongo != nil {
		return cell.Mongo.Result, cmp.MongoPhase, true
	}
	return benchmarkResult{}, phaseResult{}, false
}

func summaryEffectiveLoadProducers(result benchmarkResult, phase phaseResult) int {
	effective := phase.EffectiveProducers
	batches := summaryLoadBatchCount(result, phase)
	if effective <= 0 {
		effective = result.InsertProducers
	}
	if effective <= 0 {
		return 0
	}
	if batches > 0 && effective > batches {
		return batches
	}
	return effective
}

func summaryLoadBatchCount(result benchmarkResult, phase phaseResult) int {
	if result.Documents > 0 && result.BatchSize > 0 {
		return (result.Documents + result.BatchSize - 1) / result.BatchSize
	}
	return phase.DriverCalls
}

func findCell(cells []cellComparison, key cellKey) cellComparison {
	for _, cell := range cells {
		if cell.Key == key {
			return cell
		}
	}
	return cellComparison{Key: key}
}

func treeDBBytes(result benchmarkResult) (int64, bool) {
	bytes, _, ok := treeDBBytesSnapshot(result)
	return bytes, ok
}

func treeDBBytesSnapshot(result benchmarkResult) (int64, string, bool) {
	if result.TreeDBDiskAfterMaintenance != nil {
		return result.TreeDBDiskAfterMaintenance.TotalBytes, "maintenance", true
	}
	if result.TreeDBDiskAfterCheckpoint != nil {
		return result.TreeDBDiskAfterCheckpoint.TotalBytes, "checkpoint", true
	}
	if result.TreeDBDiskAfterLoad != nil {
		return result.TreeDBDiskAfterLoad.TotalBytes, "load", true
	}
	return 0, "n/a", false
}

func mongoDataBytes(result benchmarkResult) (int64, bool) {
	if result.MongoDBStatsFinal == nil {
		return 0, false
	}
	if value, ok := numberField(result.MongoDBStatsFinal, "dataSize"); ok {
		return int64(math.Round(value)), true
	}
	return 0, false
}

func mongoDBStatsTotalBytes(result benchmarkResult) (int64, bool) {
	if result.MongoDBStatsFinal == nil {
		return 0, false
	}
	if value, ok := numberField(result.MongoDBStatsFinal, "totalSize"); ok {
		return int64(math.Round(value)), true
	}
	storage, hasStorage := numberField(result.MongoDBStatsFinal, "storageSize")
	index, hasIndex := numberField(result.MongoDBStatsFinal, "indexSize")
	if hasStorage || hasIndex {
		return int64(math.Round(storage + index)), true
	}
	return 0, false
}

func numberField(values map[string]any, name string) (float64, bool) {
	value, ok := values[name]
	if !ok {
		return 0, false
	}
	switch v := value.(type) {
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	default:
		return 0, false
	}
}

func safeRatio(numerator, denominator float64) float64 {
	if denominator <= 0 || numerator < 0 {
		return 0
	}
	return numerator / denominator
}

func formatBytes(value int64) string {
	if value <= 0 {
		return "0 B"
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	f := float64(value)
	unit := 0
	for f >= 1024 && unit < len(units)-1 {
		f /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", value, units[unit])
	}
	return fmt.Sprintf("%.2f %s", f, units[unit])
}

func formatOptionalBytes(value int64) string {
	if value <= 0 {
		return "n/a"
	}
	return formatBytes(value)
}

func formatMeasuredBytes(ok bool, value int64) string {
	if !ok {
		return "n/a"
	}
	return formatBytes(value)
}

func formatBytesPerDoc(value int64, documents int) string {
	if value <= 0 || documents <= 0 {
		return "0 B"
	}
	return fmt.Sprintf("%.1f B", float64(value)/float64(documents))
}

func formatMeasuredBytesPerDoc(ok bool, value int64, documents int) string {
	if !ok {
		return "n/a"
	}
	return formatBytesPerDoc(value, documents)
}

func formatOptionalBytesPerDoc(value int64, documents int) string {
	if value <= 0 || documents <= 0 {
		return "n/a"
	}
	return formatBytesPerDoc(value, documents)
}

func formatNumber(value float64) string {
	if value == 0 {
		return "0"
	}
	if value >= 100 {
		return fmt.Sprintf("%.0f", value)
	}
	if value >= 10 {
		return fmt.Sprintf("%.1f", value)
	}
	return fmt.Sprintf("%.2f", value)
}

func formatRatio(value float64) string {
	if value <= 0 {
		return "n/a"
	}
	if value < 0.01 {
		return "<0.01x"
	}
	return fmt.Sprintf("%.2fx", value)
}

func formatMeasuredRatio(ok bool, numerator int64, denominator int64) string {
	if !ok {
		return "n/a"
	}
	return formatRatio(safeRatio(float64(numerator), float64(denominator)))
}

func formatPhaseOps(ok bool, value float64) string {
	if !ok {
		return "n/a"
	}
	return formatNumber(value)
}

func formatPhaseLatency(ok bool, value float64) string {
	if !ok {
		return "n/a"
	}
	return formatNumber(value)
}

func formatPhaseDriverCalls(ok bool, value int) string {
	if !ok {
		return "n/a"
	}
	return fmt.Sprintf("%d", value)
}

func formatPhaseMetric(phase phaseResult, name string) string {
	value, ok := phase.TreeDBMetrics[name]
	if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
		return "n/a"
	}
	return formatNumber(value)
}

func formatRawFloat(ok bool, value float64) string {
	if !ok {
		return ""
	}
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func formatRawInt(ok bool, value int64) string {
	if !ok {
		return ""
	}
	return strconv.FormatInt(value, 10)
}

func formatRawMeasuredRatio(ok bool, numerator int64, denominator int64) string {
	if !ok {
		return ""
	}
	return formatRawRatio(safeRatio(float64(numerator), float64(denominator)))
}

func formatRawRatio(value float64) string {
	if value <= 0 {
		return ""
	}
	return strconv.FormatFloat(value, 'f', 6, 64)
}
