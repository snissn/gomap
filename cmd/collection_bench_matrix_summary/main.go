package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type config struct {
	matrixIndexPath string
	outDir          string
}

type matrixRow struct {
	Cell                   string
	Engine                 string
	DataOuterLeavesInVLog  string
	IndexOuterLeavesInVLog string
	ReportMarkdownPath     string
	ReportJSONPath         string
}

type report struct {
	Status              string `json:"status"`
	CollectionBatchSize int    `json:"collection_batch_size,omitempty"`
	Sections            []struct {
		Benchmarks []benchmarkAggregate `json:"benchmarks"`
	} `json:"sections"`
}

type benchmarkAggregate struct {
	Name            string             `json:"name"`
	MeanNsPerOp     float64            `json:"mean_ns_per_op"`
	MeanBytesPerOp  float64            `json:"mean_bytes_per_op"`
	MeanAllocsPerOp float64            `json:"mean_allocs_per_op"`
	MeanMetrics     map[string]float64 `json:"mean_metrics,omitempty"`
}

type summaryRow struct {
	matrixRow
	Benchmark           string
	NsPerOp             float64
	BytesPerOp          float64
	AllocsPerOp         float64
	CollectionBatchSize int
	KeyFallbacks        *float64
	PrefixFallbacks     *float64
}

type loadedReport struct {
	CollectionBatchSize int
	Benchmarks          map[string]benchmarkAggregate
}

type userStoryRow struct {
	summaryRow
	Story      string
	DocsPerSec float64
	MSPerBatch float64
	BatchesSec float64
}

var benchmarkOrder = []string{
	"BenchmarkCollectionOverheadIndexStateJSONExtraction",
	"BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
	"BenchmarkCollectionInsertBatchWithSecondaryIndexes",
	"BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes",
	"BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
	"BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_matrix_summary: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_matrix_summary: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("collection_bench_matrix_summary", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.matrixIndexPath, "matrix-index", "", "Path to matrix_index.tsv")
	fs.StringVar(&cfg.outDir, "out-dir", "", "Output directory for summary artifacts")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.matrixIndexPath == "" {
		return config{}, fmt.Errorf("-matrix-index is required")
	}
	if cfg.outDir == "" {
		cfg.outDir = filepath.Dir(cfg.matrixIndexPath)
	}
	return cfg, nil
}

func run(cfg config) error {
	rows, err := readMatrixIndex(cfg.matrixIndexPath)
	if err != nil {
		return err
	}
	summaryRows, err := buildSummaryRows(rows)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}
	tsvPath := filepath.Join(cfg.outDir, "collections_matrix_summary.tsv")
	if err := os.WriteFile(tsvPath, []byte(renderTSV(summaryRows)), 0o644); err != nil {
		return fmt.Errorf("write tsv: %w", err)
	}
	userStoryPath := filepath.Join(cfg.outDir, "collections_user_story_summary.tsv")
	if err := os.WriteFile(userStoryPath, []byte(renderUserStoryTSV(buildUserStoryRows(summaryRows))), 0o644); err != nil {
		return fmt.Errorf("write user story tsv: %w", err)
	}
	mdPath := filepath.Join(cfg.outDir, "collections_matrix_summary.md")
	md, err := renderMarkdown(summaryRows, cfg.outDir)
	if err != nil {
		return err
	}
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
		return fmt.Errorf("write markdown: %w", err)
	}
	fmt.Printf("wrote matrix summary tsv: %s\n", tsvPath)
	fmt.Printf("wrote user story tsv:     %s\n", userStoryPath)
	fmt.Printf("wrote matrix summary md:  %s\n", mdPath)
	return nil
}

func readMatrixIndex(path string) ([]matrixRow, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open matrix index: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read matrix index: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("matrix index is empty")
	}
	header := make(map[string]int, len(records[0]))
	for i, name := range records[0] {
		header[name] = i
	}
	required := []string{"cell", "engine", "data_outer_leaves_in_vlog", "index_outer_leaves_in_vlog", "report_md", "report_json"}
	for _, name := range required {
		if _, ok := header[name]; !ok {
			return nil, fmt.Errorf("matrix index missing %q column", name)
		}
	}

	var rows []matrixRow
	for _, record := range records[1:] {
		if len(record) == 0 || strings.TrimSpace(record[0]) == "" {
			continue
		}
		rows = append(rows, matrixRow{
			Cell:                   field(record, header["cell"]),
			Engine:                 field(record, header["engine"]),
			DataOuterLeavesInVLog:  field(record, header["data_outer_leaves_in_vlog"]),
			IndexOuterLeavesInVLog: field(record, header["index_outer_leaves_in_vlog"]),
			ReportMarkdownPath:     field(record, header["report_md"]),
			ReportJSONPath:         field(record, header["report_json"]),
		})
	}
	return rows, nil
}

func field(record []string, idx int) string {
	if idx < 0 || idx >= len(record) {
		return ""
	}
	return record[idx]
}

func buildSummaryRows(rows []matrixRow) ([]summaryRow, error) {
	out := make([]summaryRow, 0, len(rows)*len(benchmarkOrder))
	for _, row := range rows {
		report, err := loadBenchmarkReport(row.ReportJSONPath)
		if err != nil {
			return nil, err
		}
		for _, name := range expectedBenchmarkNames(row) {
			benchmark, ok := report.Benchmarks[name]
			if !ok {
				return nil, fmt.Errorf("report %s missing benchmark %q for matrix cell %q", row.ReportJSONPath, name, row.Cell)
			}
			out = append(out, summaryRow{
				matrixRow:           row,
				Benchmark:           name,
				NsPerOp:             benchmark.MeanNsPerOp,
				BytesPerOp:          benchmark.MeanBytesPerOp,
				AllocsPerOp:         benchmark.MeanAllocsPerOp,
				CollectionBatchSize: report.CollectionBatchSize,
				KeyFallbacks:        metricPtr(benchmark.MeanMetrics, "per_item_key_probe_fallback_count"),
				PrefixFallbacks:     metricPtr(benchmark.MeanMetrics, "per_item_prefix_probe_fallback_count"),
			})
		}
	}
	return out, nil
}

func expectedBenchmarkNames(row matrixRow) []string {
	if strings.HasPrefix(row.Engine, "sqlite") {
		return []string{
			"BenchmarkSQLiteInsertBatchWithSecondaryIndexes",
			"BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes",
		}
	}
	return []string{
		"BenchmarkCollectionOverheadIndexStateJSONExtraction",
		"BenchmarkCollectionOverheadPlanIndexedPrecomputedState",
		"BenchmarkCollectionInsertBatchWithSecondaryIndexes",
		"BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes",
	}
}

func loadBenchmarkReport(path string) (loadedReport, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return loadedReport{}, fmt.Errorf("read report %s: %w", path, err)
	}
	var rep report
	if err := json.Unmarshal(raw, &rep); err != nil {
		return loadedReport{}, fmt.Errorf("parse report %s: %w", path, err)
	}
	if rep.Status != "ok" {
		return loadedReport{}, fmt.Errorf("report %s status %q; matrix summary requires ok reports", path, rep.Status)
	}
	out := make(map[string]benchmarkAggregate)
	for _, section := range rep.Sections {
		for _, benchmark := range section.Benchmarks {
			out[benchmark.Name] = benchmark
		}
	}
	return loadedReport{CollectionBatchSize: rep.CollectionBatchSize, Benchmarks: out}, nil
}

func metricPtr(metrics map[string]float64, name string) *float64 {
	value, ok := metrics[name]
	if !ok {
		return nil
	}
	return &value
}

func renderTSV(rows []summaryRow) string {
	var sb strings.Builder
	sb.WriteString("cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tbenchmark\tns_per_op\tbytes_per_op\tallocs_per_op\tper_item_key_probe_fallback_count\tper_item_prefix_probe_fallback_count\treport_md\n")
	for _, row := range rows {
		sb.WriteString(row.Cell)
		sb.WriteByte('\t')
		sb.WriteString(row.Engine)
		sb.WriteByte('\t')
		sb.WriteString(row.DataOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.IndexOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.Benchmark)
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.NsPerOp))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.BytesPerOp))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.AllocsPerOp))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.KeyFallbacks))
		sb.WriteByte('\t')
		sb.WriteString(formatOptionalTSVFloat(row.PrefixFallbacks))
		sb.WriteByte('\t')
		sb.WriteString(row.ReportMarkdownPath)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func renderUserStoryTSV(rows []userStoryRow) string {
	var sb strings.Builder
	sb.WriteString("cell\tengine\tdata_outer_leaves_in_vlog\tindex_outer_leaves_in_vlog\tstory\tbenchmark\tdocs_per_batch\tdocs_per_sec\tms_per_batch\tbatches_per_sec\tns_per_doc\treport_md\n")
	for _, row := range rows {
		sb.WriteString(row.Cell)
		sb.WriteByte('\t')
		sb.WriteString(row.Engine)
		sb.WriteByte('\t')
		sb.WriteString(row.DataOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.IndexOuterLeavesInVLog)
		sb.WriteByte('\t')
		sb.WriteString(row.Story)
		sb.WriteByte('\t')
		sb.WriteString(row.Benchmark)
		sb.WriteByte('\t')
		sb.WriteString(strconv.Itoa(row.CollectionBatchSize))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.DocsPerSec))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.MSPerBatch))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.BatchesSec))
		sb.WriteByte('\t')
		sb.WriteString(formatTSVFloat(row.NsPerOp))
		sb.WriteByte('\t')
		sb.WriteString(row.ReportMarkdownPath)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func formatOptionalTSVFloat(value *float64) string {
	if value == nil {
		return "-"
	}
	return formatTSVFloat(*value)
}

func formatTSVFloat(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func renderMarkdown(rows []summaryRow, outDir string) (string, error) {
	var sb strings.Builder
	sb.WriteString("# Collections Benchmark Matrix Summary\n\n")
	userStoryRows := buildUserStoryRows(rows)
	if len(userStoryRows) > 0 {
		sb.WriteString("## User-Facing Throughput\n\n")
		sb.WriteString("Batch benchmark ops are documents, so this section reports the indexed ingest story as docs/sec, batch latency, and batches/sec. Diagnostic JSON/planner rows are separated below.\n\n")
		sb.WriteString("| Cell | Engine | Data vlog | Index vlog | Story | Docs/batch | Docs/sec | ms/batch | batches/sec | ns/doc | Report |\n")
		sb.WriteString("| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
		for _, row := range userStoryRows {
			reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
			sb.WriteString("| `")
			sb.WriteString(escapeTableCell(row.Cell))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Engine))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.DataOuterLeavesInVLog))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.IndexOuterLeavesInVLog))
			sb.WriteString("` | ")
			sb.WriteString(escapeTableCell(row.Story))
			sb.WriteString(" | ")
			sb.WriteString(formatIntWithCommas(int64(row.CollectionBatchSize)))
			sb.WriteString(" | ")
			sb.WriteString(formatThroughput(row.DocsPerSec))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.MSPerBatch))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.BatchesSec))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.NsPerOp))
			sb.WriteString(" | [report](")
			sb.WriteString(markdownLinkPath(reportPath))
			sb.WriteString(") |\n")
		}
		sb.WriteString("\n")
	}
	diagnosticRows := buildDiagnosticRows(rows)
	if len(diagnosticRows) > 0 {
		sb.WriteString("## Diagnostic Rows\n\n")
		sb.WriteString("These rows are not user stories. They isolate JSON/data extraction and non-JSON planner cost so future optimization can quarantine JSON work separately from TreeDB publish/index maintenance.\n\n")
		sb.WriteString("| Cell | Engine | Diagnostic | ns/doc | B/doc | allocs/doc | Report |\n")
		sb.WriteString("| --- | --- | --- | ---: | ---: | ---: | --- |\n")
		for _, row := range diagnosticRows {
			reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
			sb.WriteString("| `")
			sb.WriteString(escapeTableCell(row.Cell))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Engine))
			sb.WriteString("` | `")
			sb.WriteString(escapeTableCell(row.Benchmark))
			sb.WriteString("` | ")
			sb.WriteString(formatFloat(row.NsPerOp))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.BytesPerOp))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(row.AllocsPerOp))
			sb.WriteString(" | [report](")
			sb.WriteString(markdownLinkPath(reportPath))
			sb.WriteString(") |\n")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("## Raw Matrix\n\n")
	sb.WriteString("| Cell | Engine | Data vlog | Index vlog | Benchmark | ns/op | B/op | allocs/op | Key fallbacks | Prefix fallbacks | Report |\n")
	sb.WriteString("| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |\n")
	for _, row := range rows {
		reportPath := relativeReportPath(outDir, row.ReportMarkdownPath)
		sb.WriteString("| `")
		sb.WriteString(escapeTableCell(row.Cell))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.Engine))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.DataOuterLeavesInVLog))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.IndexOuterLeavesInVLog))
		sb.WriteString("` | `")
		sb.WriteString(escapeTableCell(row.Benchmark))
		sb.WriteString("` | ")
		sb.WriteString(formatFloat(row.NsPerOp))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(row.BytesPerOp))
		sb.WriteString(" | ")
		sb.WriteString(formatFloat(row.AllocsPerOp))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.KeyFallbacks))
		sb.WriteString(" | ")
		sb.WriteString(formatOptionalFloat(row.PrefixFallbacks))
		sb.WriteString(" | [report](")
		sb.WriteString(markdownLinkPath(reportPath))
		sb.WriteString(") |\n")
	}
	return sb.String(), nil
}

func relativeReportPath(outDir, reportPath string) string {
	if rel, err := filepath.Rel(outDir, reportPath); err == nil {
		return filepath.ToSlash(rel)
	}
	return filepath.ToSlash(reportPath)
}

func buildUserStoryRows(rows []summaryRow) []userStoryRow {
	out := make([]userStoryRow, 0, len(rows))
	for _, row := range rows {
		story, ok := userStoryLabel(row.Benchmark)
		if !ok || row.NsPerOp <= 0 || row.CollectionBatchSize <= 0 {
			continue
		}
		batchSize := row.CollectionBatchSize
		docsPerSec := 1e9 / row.NsPerOp
		out = append(out, userStoryRow{
			summaryRow: row,
			Story:      story,
			DocsPerSec: docsPerSec,
			MSPerBatch: float64(batchSize) * row.NsPerOp / 1e6,
			BatchesSec: docsPerSec / float64(batchSize),
		})
	}
	return out
}

func userStoryLabel(benchmark string) (string, bool) {
	switch benchmark {
	case "BenchmarkCollectionInsertBatchWithSecondaryIndexes", "BenchmarkSQLiteInsertBatchWithSecondaryIndexes":
		return "bulk indexed insert", true
	case "BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes", "BenchmarkSQLiteInsertBatchCheckpointWithSecondaryIndexes":
		return "checkpointed indexed insert", true
	default:
		return "", false
	}
}

func buildDiagnosticRows(rows []summaryRow) []summaryRow {
	out := make([]summaryRow, 0, len(rows))
	for _, row := range rows {
		if isDiagnosticBenchmark(row.Benchmark) {
			out = append(out, row)
		}
	}
	return out
}

func isDiagnosticBenchmark(benchmark string) bool {
	switch benchmark {
	case "BenchmarkCollectionOverheadIndexStateJSONExtraction", "BenchmarkCollectionOverheadPlanIndexedPrecomputedState":
		return true
	default:
		return false
	}
}

func markdownLinkPath(path string) string {
	return strings.ReplaceAll(path, " ", "%20")
}

func escapeTableCell(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}

func formatOptionalFloat(value *float64) string {
	if value == nil {
		return "-"
	}
	return formatFloat(*value)
}

func formatThroughput(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	return formatIntWithCommas(int64(math.Round(value)))
}

func formatFloat(value float64) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "-"
	}
	rounded := math.Round(value*100) / 100
	if math.Abs(rounded-math.Round(rounded)) < 0.005 {
		return formatIntWithCommas(int64(math.Round(rounded)))
	}
	return trimTrailingZeros(formatWithCommas(rounded))
}

func formatWithCommas(value float64) string {
	negative := value < 0
	if negative {
		value = -value
	}
	whole := int64(value)
	fraction := value - float64(whole)
	base := formatIntWithCommas(whole)
	if fraction == 0 {
		if negative {
			return "-" + base
		}
		return base
	}
	text := fmt.Sprintf("%s%.2f", "", fraction)
	text = strings.TrimPrefix(text, "0")
	if negative {
		return "-" + base + text
	}
	return base + text
}

func trimTrailingZeros(value string) string {
	value = strings.TrimRight(value, "0")
	value = strings.TrimRight(value, ".")
	return value
}

func formatIntWithCommas(value int64) string {
	negative := value < 0
	if negative {
		value = -value
	}
	raw := strconv.FormatInt(value, 10)
	if len(raw) <= 3 {
		if negative {
			return "-" + raw
		}
		return raw
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
	result := strings.Join(parts, ",")
	if negative {
		return "-" + result
	}
	return result
}
