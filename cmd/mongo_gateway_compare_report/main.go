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
	MatrixPath  string
	ReportPath  string
	SummaryPath string
	Title       string
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
	Target                     string         `json:"target"`
	MongoURI                   string         `json:"mongo_uri,omitempty"`
	TreeDBDir                  string         `json:"treedb_dir,omitempty"`
	Database                   string         `json:"database"`
	Collection                 string         `json:"collection"`
	Documents                  int            `json:"documents"`
	SecondaryIndexes           int            `json:"secondary_indexes"`
	ClientMode                 string         `json:"client_mode,omitempty"`
	TreeDBDocumentFormat       string         `json:"treedb_document_format,omitempty"`
	Phases                     []phaseResult  `json:"phases"`
	TreeDBDiskAfterLoad        *diskSnapshot  `json:"treedb_disk_after_load,omitempty"`
	TreeDBDiskAfterCheckpoint  *diskSnapshot  `json:"treedb_disk_after_checkpoint,omitempty"`
	TreeDBDiskAfterMaintenance *diskSnapshot  `json:"treedb_disk_after_maintenance,omitempty"`
	MongoDBStatsAfterLoad      map[string]any `json:"mongodb_stats_after_load,omitempty"`
	MongoDBStatsFinal          map[string]any `json:"mongodb_stats_final,omitempty"`
}

type phaseResult struct {
	Name                    string         `json:"name"`
	Operations              int            `json:"operations"`
	DriverCalls             int            `json:"driver_calls"`
	DurationMillis          float64        `json:"duration_ms"`
	OpsPerSecond            float64        `json:"ops_per_sec"`
	SampledOpsPerSecond     float64        `json:"sampled_ops_per_sec,omitempty"`
	SampledNsPerOp          float64        `json:"sampled_ns_per_op,omitempty"`
	DriverAggregateMillis   float64        `json:"driver_aggregate_duration_ms,omitempty"`
	DriverMeanLatencyMicros float64        `json:"driver_mean_latency_us,omitempty"`
	LatencyMicros           latencySummary `json:"latency_micros"`
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
	Key    cellKey
	TreeDB *runRecord
	Mongo  *runRecord
}

type phaseComparison struct {
	Cell        cellKey
	Name        string
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
	cells, err := loadComparisons(cfg.MatrixPath)
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

func loadComparisons(matrixPath string) ([]cellComparison, error) {
	rows, err := readMatrix(matrixPath)
	if err != nil {
		return nil, err
	}
	matrixDir := filepath.Dir(matrixPath)
	type groupedCell struct {
		mongo          *runRecord
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
			cell = &groupedCell{trees: make(map[string]*runRecord)}
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
			if cell.mongo != nil {
				return nil, fmt.Errorf("duplicate mongo row for documents=%d secondary_indexes=%d", key.Documents, key.SecondaryIndexes)
			}
			cell.mongo = record
		default:
			return nil, fmt.Errorf("unknown target %q in matrix", row.Target)
		}
	}
	cells := make([]cellComparison, 0, len(byCell))
	for key, cell := range byCell {
		if len(cell.trees) == 0 || cell.mongo == nil {
			return nil, fmt.Errorf("incomplete comparison cell documents=%d secondary_indexes=%d", key.Documents, key.SecondaryIndexes)
		}
		sort.Strings(cell.treeConfigKeys)
		for _, config := range cell.treeConfigKeys {
			cells = append(cells, cellComparison{
				Key: cellKey{
					Documents:        key.Documents,
					SecondaryIndexes: key.SecondaryIndexes,
					TreeDBConfig:     config,
				},
				TreeDB: cell.trees[config],
				Mongo:  cell.mongo,
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
	fmt.Fprintf(&b, "# %s\n\n", cfg.Title)
	fmt.Fprintf(&b, "- generated_at: `%s`\n", generatedAt.Format(time.RFC3339))
	fmt.Fprintf(&b, "- matrix: `%s`\n", cfg.MatrixPath)
	fmt.Fprintf(&b, "- comparison cells: `%d`\n", len(cells))
	fmt.Fprintf(&b, "- targets: `treedb`, `mongo`\n\n")
	b.WriteString("## Highlights\n\n")
	for _, line := range highlightLines(cells) {
		fmt.Fprintf(&b, "- %s\n", line)
	}
	b.WriteString("\n")
	renderDiskTable(&b, cells)
	b.WriteString("\n")
	renderOpsTable(&b, cells)
	b.WriteString("\n")
	b.WriteString("## Raw Inputs\n\n")
	b.WriteString("| docs | indexes | config | target | raw json |\n")
	b.WriteString("| ---: | ---: | --- | --- | --- |\n")
	seenMongoRaw := make(map[baseCellKey]struct{})
	for _, cell := range cells {
		fmt.Fprintf(&b, "| %d | %d | `%s` | treedb | `%s` |\n", cell.Key.Documents, cell.Key.SecondaryIndexes, cell.Key.TreeDBConfig, cell.TreeDB.DisplayRawPath)
		mongoKey := baseCellKey{Documents: cell.Key.Documents, SecondaryIndexes: cell.Key.SecondaryIndexes}
		if _, ok := seenMongoRaw[mongoKey]; !ok {
			fmt.Fprintf(&b, "| %d | %d | `mongo` | mongo | `%s` |\n", cell.Key.Documents, cell.Key.SecondaryIndexes, cell.Mongo.DisplayRawPath)
			seenMongoRaw[mongoKey] = struct{}{}
		}
	}
	b.WriteString("\n")
	b.WriteString("## Notes\n\n")
	b.WriteString("- TreeDB disk bytes prefer `treedb_disk_after_maintenance.total_bytes`, then fall back to `treedb_disk_after_checkpoint.total_bytes` and `treedb_disk_after_load.total_bytes` for older runs.\n")
	b.WriteString("- TreeDB physical bytes come from the matrix runner's `du` measurement of the isolated TreeDB directory.\n")
	b.WriteString("- MongoDB `dbStats.dataSize` is uncompressed logical document size, not disk usage.\n")
	b.WriteString("- MongoDB `dbStats.totalSize` is reported separately because it can diverge sharply from the isolated data-directory `du` measurement on small WiredTiger workloads.\n")
	b.WriteString("- MongoDB physical bytes are the preferred local disk comparison when the matrix runner has an isolated data directory, such as Docker mode.\n")
	b.WriteString("- Wall ops/sec values include the full benchmark phase loop. Sampled ops/sec values isolate the timed driver/gateway call inside each phase and are useful when prebuilt fixtures are enabled.\n")
	return b.String()
}

func highlightLines(cells []cellComparison) []string {
	var lines []string
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
		lines = append(lines, "No phase in this matrix had TreeDB ahead on ops/sec.")
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
	} else {
		lines = append(lines, "No phase in this matrix had MongoDB ahead on ops/sec.")
	}
	if cell := largestDiskCell(cells); cell != nil {
		treeBytes, treeSnapshot, treeOK := treeDBBytesSnapshot(cell.TreeDB.Result)
		treePhysical := cell.TreeDB.Row.PhysicalBytes
		mongoData, _ := mongoDataBytes(cell.Mongo.Result)
		mongoTotal, _ := mongoDBStatsTotalBytes(cell.Mongo.Result)
		mongoPhysical := cell.Mongo.Row.PhysicalBytes
		treeSnapshotClause := "TreeDB disk snapshot was n/a"
		if treeOK {
			treeSnapshotClause = fmt.Sprintf("TreeDB %s snapshot used %s (%s/doc)",
				treeSnapshot,
				formatBytes(treeBytes),
				formatBytesPerDoc(treeBytes, cell.Key.Documents),
			)
		}
		lines = append(lines, fmt.Sprintf("Largest cell disk: at %d docs / %d indexes / `%s`, %s, TreeDB physical du was %s, MongoDB dbStats dataSize was %s, MongoDB dbStats totalSize was %s, and MongoDB physical du was %s (%s/doc).",
			cell.Key.Documents,
			cell.Key.SecondaryIndexes,
			cell.Key.TreeDBConfig,
			treeSnapshotClause,
			formatOptionalBytes(treePhysical),
			formatBytes(mongoData),
			formatBytes(mongoTotal),
			formatOptionalBytes(mongoPhysical),
			formatOptionalBytesPerDoc(mongoPhysical, cell.Key.Documents),
		))
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
		mongoTotal, _ := mongoDBStatsTotalBytes(cell.Mongo.Result)
		if cell.Mongo.Row.PhysicalBytes > 0 {
			score += cell.Mongo.Row.PhysicalBytes
		} else {
			score += mongoTotal
		}
	}
	return score
}

func renderDiskTable(b *strings.Builder, cells []cellComparison) {
	b.WriteString("## Disk Summary\n\n")
	b.WriteString("| docs | indexes | TreeDB config | TreeDB snapshot | TreeDB bytes | TreeDB bytes/doc | TreeDB physical du | TreeDB physical bytes/doc | MongoDB dbStats dataSize | MongoDB dbStats totalSize | MongoDB physical du | MongoDB physical bytes/doc | TreeDB / MongoDB dbStats totalSize | TreeDB / MongoDB physical |\n")
	b.WriteString("| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, cell := range cells {
		treeBytes, treeSnapshot, treeOK := treeDBBytesSnapshot(cell.TreeDB.Result)
		treePhysical := cell.TreeDB.Row.PhysicalBytes
		mongoData, _ := mongoDataBytes(cell.Mongo.Result)
		mongoTotal, _ := mongoDBStatsTotalBytes(cell.Mongo.Result)
		mongoPhysical := cell.Mongo.Row.PhysicalBytes
		fmt.Fprintf(b, "| %d | %d | `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			cell.Key.Documents,
			cell.Key.SecondaryIndexes,
			cell.Key.TreeDBConfig,
			treeSnapshot,
			formatMeasuredBytes(treeOK, treeBytes),
			formatMeasuredBytesPerDoc(treeOK, treeBytes, cell.Key.Documents),
			formatOptionalBytes(treePhysical),
			formatOptionalBytesPerDoc(treePhysical, cell.Key.Documents),
			formatBytes(mongoData),
			formatBytes(mongoTotal),
			formatOptionalBytes(mongoPhysical),
			formatOptionalBytesPerDoc(mongoPhysical, cell.Key.Documents),
			formatMeasuredRatio(treeOK, treeBytes, mongoTotal),
			formatRatio(safeRatio(float64(treePhysical), float64(mongoPhysical))),
		)
	}
}

func renderOpsTable(b *strings.Builder, cells []cellComparison) {
	b.WriteString("## Ops/Sec Summary\n\n")
	b.WriteString("| docs | indexes | TreeDB config | phase | TreeDB wall ops/sec | TreeDB sampled ops/sec | MongoDB wall ops/sec | MongoDB sampled ops/sec | TreeDB / MongoDB wall | TreeDB / MongoDB sampled | TreeDB p95 us | MongoDB p95 us |\n")
	b.WriteString("| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, cmp := range allPhaseComparisons(cells) {
		sampledRatio := safeRatio(cmp.TreeDBPhase.SampledOpsPerSecond, cmp.MongoPhase.SampledOpsPerSecond)
		fmt.Fprintf(b, "| %d | %d | `%s` | `%s` | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			cmp.Cell.Documents,
			cmp.Cell.SecondaryIndexes,
			cmp.Cell.TreeDBConfig,
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
		names := phaseNames(cell.TreeDB.Result.Phases, cell.Mongo.Result.Phases)
		for _, name := range names {
			treePhase, hasTree := cell.TreeDB.PhaseMap[name]
			mongoPhase, hasMongo := cell.Mongo.PhaseMap[name]
			out = append(out, phaseComparison{
				Cell:        cell.Key,
				Name:        name,
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
		"treedb_config",
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
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	for _, cmp := range allPhaseComparisons(cells) {
		cell := findCell(cells, cmp.Cell)
		treeBytes, treeSnapshot, treeOK := treeDBBytesSnapshot(cell.TreeDB.Result)
		treePhysical := cell.TreeDB.Row.PhysicalBytes
		mongoData, _ := mongoDataBytes(cell.Mongo.Result)
		mongoTotal, _ := mongoDBStatsTotalBytes(cell.Mongo.Result)
		sampledRatio := safeRatio(cmp.TreeDBPhase.SampledOpsPerSecond, cmp.MongoPhase.SampledOpsPerSecond)
		row := []string{
			strconv.Itoa(cmp.Cell.Documents),
			strconv.Itoa(cmp.Cell.SecondaryIndexes),
			cmp.Cell.TreeDBConfig,
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
			strconv.FormatInt(mongoData, 10),
			strconv.FormatInt(mongoTotal, 10),
			strconv.FormatInt(cell.Mongo.Row.PhysicalBytes, 10),
			formatRawMeasuredRatio(treeOK, treeBytes, mongoTotal),
			formatRawRatio(safeRatio(float64(treePhysical), float64(cell.Mongo.Row.PhysicalBytes))),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
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
