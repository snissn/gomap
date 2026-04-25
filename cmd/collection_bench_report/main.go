package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yuin/goldmark"
)

type config struct {
	inputPath           string
	outDir              string
	branch              string
	commit              string
	worktree            string
	executionPath       string
	benchPattern        string
	count               int
	benchmarkEngine     string
	collectionBatchSize int
	unavailableReason   string
}

type jsonEvent struct {
	Action string `json:"Action"`
	Output string `json:"Output"`
}

type benchmarkSpec struct {
	Name        string
	Section     string
	Description string
}

type benchmarkSample struct {
	Name        string  `json:"name"`
	Iterations  int64   `json:"iterations"`
	NsPerOp     float64 `json:"ns_per_op"`
	BytesPerOp  float64 `json:"bytes_per_op"`
	AllocsPerOp float64 `json:"allocs_per_op"`
}

type benchmarkAggregate struct {
	Name            string            `json:"name"`
	Section         string            `json:"section"`
	Description     string            `json:"description"`
	Samples         int               `json:"samples"`
	MeanNsPerOp     float64           `json:"mean_ns_per_op"`
	MinNsPerOp      float64           `json:"min_ns_per_op"`
	MaxNsPerOp      float64           `json:"max_ns_per_op"`
	MeanBytesPerOp  float64           `json:"mean_bytes_per_op"`
	MeanAllocsPerOp float64           `json:"mean_allocs_per_op"`
	OpsPerSec       float64           `json:"ops_per_sec"`
	Runs            []benchmarkSample `json:"runs"`
}

type reportSection struct {
	Title      string               `json:"title"`
	Benchmarks []benchmarkAggregate `json:"benchmarks"`
}

type report struct {
	GeneratedAt         string          `json:"generated_at"`
	Status              string          `json:"status"`
	UnavailableReason   string          `json:"unavailable_reason,omitempty"`
	ExecutionPath       string          `json:"execution_path,omitempty"`
	BenchmarkEngine     string          `json:"benchmark_engine,omitempty"`
	Worktree            string          `json:"worktree,omitempty"`
	Branch              string          `json:"branch,omitempty"`
	Commit              string          `json:"commit,omitempty"`
	BenchPattern        string          `json:"bench_pattern,omitempty"`
	Count               int             `json:"count,omitempty"`
	CollectionBatchSize int             `json:"collection_batch_size,omitempty"`
	RawJSONPath         string          `json:"raw_json_path,omitempty"`
	Sections            []reportSection `json:"sections"`
}

var benchmarkSpecs = []benchmarkSpec{
	{Name: "BenchmarkCollectionInsertProvidedID", Section: "Document Path", Description: "Insert documents with caller-provided IDs into the primary collection root."},
	{Name: "BenchmarkCollectionInsertBatchProvidedID", Section: "Batch Ingest Path", Description: "Insert documents with caller-provided IDs through the collection batch API; ops/sec is documents/sec."},
	{Name: "BenchmarkCollectionGetByID", Section: "Document Path", Description: "Lookup documents by primary `_id` from the dedicated primary root."},
	{Name: "BenchmarkCollectionDeleteByID", Section: "Document Path", Description: "Delete pre-existing documents from the primary root without secondary index maintenance."},
	{Name: "BenchmarkCollectionInsertWithSecondaryIndexes", Section: "Document Path", Description: "Insert documents while maintaining both unique and non-unique secondary indexes."},
	{Name: "BenchmarkCollectionInsertBatchWithSecondaryIndexes", Section: "Batch Ingest Path", Description: "Batch insert documents while maintaining unique and non-unique secondary indexes; ops/sec is documents/sec."},
	{Name: "BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes", Section: "Batch Ingest Path", Description: "Batch insert indexed documents and force a sync boundary after each batch; ops/sec is documents/sec."},
	{Name: "BenchmarkCollectionDeleteWithSecondaryIndexes", Section: "Document Path", Description: "Delete documents while removing postings from unique and non-unique secondary indexes."},
	{Name: "BenchmarkSecondaryLookupUnique", Section: "Secondary Index Path", Description: "Resolve a unique secondary index lookup to document IDs."},
	{Name: "BenchmarkSecondaryLookupNonUnique", Section: "Secondary Index Path", Description: "Resolve a non-unique secondary index lookup that returns multiple document IDs."},
	{Name: "BenchmarkSecondaryUpsertFieldChange", Section: "Secondary Index Path", Description: "Rewrite a document so an indexed field changes and postings move to the new value."},
	{Name: "BenchmarkCollectionCreateIndexBackfillExistingDocs", Section: "Secondary Index Path", Description: "Build a new secondary index and backfill it from an existing primary collection root."},
}

var benchmarkNameRE = regexp.MustCompile(`^(Benchmark\S+)-(\d+)$`)

func main() {
	cfg := parseFlags()
	rep, err := buildReport(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: create out dir: %v\n", err)
		os.Exit(1)
	}

	jsonPath := filepath.Join(cfg.outDir, "collections_report.json")
	mdPath := filepath.Join(cfg.outDir, "collections_report.md")
	htmlPath := filepath.Join(cfg.outDir, "collections_report.html")

	payload, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: marshal json: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(jsonPath, payload, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: write json: %v\n", err)
		os.Exit(1)
	}

	md := renderMarkdown(rep)
	if err := os.WriteFile(mdPath, []byte(md), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: write markdown: %v\n", err)
		os.Exit(1)
	}

	htmlDoc, err := markdownToHTMLDoc(md)
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: render html: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(htmlPath, htmlDoc, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: write html: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("wrote report json: %s\n", jsonPath)
	fmt.Printf("wrote report md:   %s\n", mdPath)
	fmt.Printf("wrote report html: %s\n", htmlPath)
}

func parseFlags() config {
	cfg, err := parseFlagsFrom(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "collection_bench_report: %v\n", err)
		os.Exit(2)
	}
	return cfg
}

func parseFlagsFrom(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("collection_bench_report", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.inputPath, "in", "", "Path to go test -json benchmark output")
	fs.StringVar(&cfg.outDir, "out-dir", "", "Output directory for report artifacts")
	fs.StringVar(&cfg.branch, "branch", "", "Optional git branch name to include in report metadata")
	fs.StringVar(&cfg.commit, "commit", "", "Optional git commit to include in report metadata")
	fs.StringVar(&cfg.worktree, "worktree", "", "Optional worktree path to include in report metadata")
	fs.StringVar(&cfg.executionPath, "execution-path", "", "Execution-path label (oracle|native-fastpath)")
	fs.StringVar(&cfg.benchPattern, "bench-pattern", "", "Optional benchmark regex to include in report metadata")
	fs.IntVar(&cfg.count, "count", 0, "Optional benchmark count to include in report metadata")
	fs.StringVar(&cfg.benchmarkEngine, "benchmark-engine", "", "Optional benchmark engine label to include in report metadata")
	fs.IntVar(&cfg.collectionBatchSize, "collection-batch-size", 0, "Optional collection benchmark batch size to include in report metadata")
	fs.StringVar(&cfg.unavailableReason, "unavailable-reason", "", "Emit an explicit unavailable report instead of parsing benchmark input")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.outDir == "" {
		return config{}, fmt.Errorf("-out-dir is required")
	}
	if err := validateExecutionPath(cfg.executionPath); err != nil {
		return config{}, err
	}
	if cfg.unavailableReason == "" && cfg.inputPath == "" {
		return config{}, fmt.Errorf("-in is required unless -unavailable-reason is set")
	}
	return cfg, nil
}

func validateExecutionPath(path string) error {
	if path == "" {
		return fmt.Errorf("-execution-path is required; hidden or implied path labels are forbidden")
	}
	if path == "oracle" || path == "native-fastpath" {
		return nil
	}
	return fmt.Errorf("-execution-path must be oracle or native-fastpath; mixed-path labels are forbidden (got %q)", path)
}

func buildReport(cfg config) (*report, error) {
	if cfg.unavailableReason != "" {
		return &report{
			GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
			Status:              "unavailable",
			UnavailableReason:   cfg.unavailableReason,
			ExecutionPath:       cfg.executionPath,
			BenchmarkEngine:     cfg.benchmarkEngine,
			Worktree:            cfg.worktree,
			Branch:              cfg.branch,
			Commit:              cfg.commit,
			BenchPattern:        cfg.benchPattern,
			Count:               cfg.count,
			CollectionBatchSize: cfg.collectionBatchSize,
			Sections:            nil,
		}, nil
	}

	file, err := os.Open(cfg.inputPath)
	if err != nil {
		return nil, fmt.Errorf("open input: %w", err)
	}
	defer file.Close()

	samples, err := parseBenchmarkSamples(file)
	if err != nil {
		return nil, err
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("no benchmark samples found in %s", cfg.inputPath)
	}

	aggregates := aggregateSamples(samples)
	sections := buildSections(aggregates)

	return &report{
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
		Status:              "ok",
		ExecutionPath:       cfg.executionPath,
		BenchmarkEngine:     cfg.benchmarkEngine,
		Worktree:            cfg.worktree,
		Branch:              cfg.branch,
		Commit:              cfg.commit,
		BenchPattern:        cfg.benchPattern,
		Count:               cfg.count,
		CollectionBatchSize: cfg.collectionBatchSize,
		RawJSONPath:         cfg.inputPath,
		Sections:            sections,
	}, nil
}

func parseBenchmarkSamples(r io.Reader) ([]benchmarkSample, error) {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 8*1024*1024)

	var samples []benchmarkSample
	var pending strings.Builder
	for scanner.Scan() {
		line := scanner.Bytes()
		var event jsonEvent
		if err := json.Unmarshal(line, &event); err != nil {
			continue
		}
		if event.Action != "output" {
			continue
		}
		pending.WriteString(event.Output)
		text := pending.String()
		lines := strings.Split(text, "\n")
		pending.Reset()
		for idx, outputLine := range lines {
			if idx == len(lines)-1 && !strings.HasSuffix(text, "\n") {
				pending.WriteString(outputLine)
				continue
			}
			sample, ok := parseBenchmarkLine(outputLine)
			if ok {
				samples = append(samples, sample)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan benchmark json: %w", err)
	}
	if pending.Len() > 0 {
		if sample, ok := parseBenchmarkLine(pending.String()); ok {
			samples = append(samples, sample)
		}
	}
	return samples, nil
}

func parseBenchmarkLine(line string) (benchmarkSample, bool) {
	fields := strings.Fields(line)
	if len(fields) < 4 {
		return benchmarkSample{}, false
	}
	match := benchmarkNameRE.FindStringSubmatch(fields[0])
	if match == nil || fields[3] != "ns/op" {
		return benchmarkSample{}, false
	}

	iterations, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return benchmarkSample{}, false
	}
	nsPerOp, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return benchmarkSample{}, false
	}

	sample := benchmarkSample{
		Name:       match[1],
		Iterations: iterations,
		NsPerOp:    nsPerOp,
	}
	for i := 4; i+1 < len(fields); i += 2 {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			continue
		}
		switch fields[i+1] {
		case "B/op":
			sample.BytesPerOp = value
		case "allocs/op":
			sample.AllocsPerOp = value
		}
	}
	return sample, true
}

func aggregateSamples(samples []benchmarkSample) map[string]benchmarkAggregate {
	type running struct {
		spec      benchmarkSpec
		runs      []benchmarkSample
		sumNs     float64
		sumBytes  float64
		sumAllocs float64
		minNs     float64
		maxNs     float64
	}

	specByName := make(map[string]benchmarkSpec, len(benchmarkSpecs))
	for _, spec := range benchmarkSpecs {
		specByName[spec.Name] = spec
	}

	runningByName := make(map[string]*running, len(samples))
	for _, sample := range samples {
		entry := runningByName[sample.Name]
		if entry == nil {
			spec := specByName[sample.Name]
			if spec.Name == "" {
				spec = benchmarkSpec{
					Name:        sample.Name,
					Section:     "Other",
					Description: "Benchmark not yet categorized in the collections report catalog.",
				}
			}
			entry = &running{
				spec:  spec,
				minNs: math.MaxFloat64,
			}
			runningByName[sample.Name] = entry
		}
		entry.runs = append(entry.runs, sample)
		entry.sumNs += sample.NsPerOp
		entry.sumBytes += sample.BytesPerOp
		entry.sumAllocs += sample.AllocsPerOp
		if sample.NsPerOp < entry.minNs {
			entry.minNs = sample.NsPerOp
		}
		if sample.NsPerOp > entry.maxNs {
			entry.maxNs = sample.NsPerOp
		}
	}

	result := make(map[string]benchmarkAggregate, len(runningByName))
	for name, entry := range runningByName {
		sampleCount := float64(len(entry.runs))
		meanNs := entry.sumNs / sampleCount
		opsPerSec := 0.0
		if meanNs > 0 {
			opsPerSec = 1e9 / meanNs
		}
		result[name] = benchmarkAggregate{
			Name:            name,
			Section:         entry.spec.Section,
			Description:     entry.spec.Description,
			Samples:         len(entry.runs),
			MeanNsPerOp:     meanNs,
			MinNsPerOp:      entry.minNs,
			MaxNsPerOp:      entry.maxNs,
			MeanBytesPerOp:  entry.sumBytes / sampleCount,
			MeanAllocsPerOp: entry.sumAllocs / sampleCount,
			OpsPerSec:       opsPerSec,
			Runs:            entry.runs,
		}
	}
	return result
}

func buildSections(aggregates map[string]benchmarkAggregate) []reportSection {
	sectionOrder := []string{"Document Path", "Batch Ingest Path", "Secondary Index Path", "Maintenance", "Other"}
	sections := make(map[string][]benchmarkAggregate, len(sectionOrder))
	seen := make(map[string]struct{}, len(aggregates))

	for _, spec := range benchmarkSpecs {
		aggregate, ok := aggregates[spec.Name]
		if !ok {
			continue
		}
		sections[aggregate.Section] = append(sections[aggregate.Section], aggregate)
		seen[aggregate.Name] = struct{}{}
	}

	var other []benchmarkAggregate
	for name, aggregate := range aggregates {
		if _, ok := seen[name]; ok {
			continue
		}
		other = append(other, aggregate)
	}
	sort.Slice(other, func(i, j int) bool { return other[i].Name < other[j].Name })
	if len(other) > 0 {
		sections["Other"] = append(sections["Other"], other...)
	}

	var out []reportSection
	for _, title := range sectionOrder {
		benchmarks := sections[title]
		if len(benchmarks) == 0 {
			continue
		}
		out = append(out, reportSection{
			Title:      title,
			Benchmarks: benchmarks,
		})
	}
	return out
}

func renderMarkdown(rep *report) string {
	var sb strings.Builder
	sb.WriteString("# Collections Benchmark Report\n\n")
	sb.WriteString(fmt.Sprintf("- generated: `%s`\n", rep.GeneratedAt))
	sb.WriteString(fmt.Sprintf("- status: `%s`\n", rep.Status))
	if rep.UnavailableReason != "" {
		sb.WriteString(fmt.Sprintf("- unavailable reason: `%s`\n", rep.UnavailableReason))
	}
	if rep.ExecutionPath != "" {
		sb.WriteString(fmt.Sprintf("- execution path: `%s`\n", rep.ExecutionPath))
	}
	if rep.BenchmarkEngine != "" {
		sb.WriteString(fmt.Sprintf("- benchmark engine: `%s`\n", rep.BenchmarkEngine))
	}
	if rep.Worktree != "" {
		sb.WriteString(fmt.Sprintf("- worktree: `%s`\n", rep.Worktree))
	}
	if rep.Branch != "" {
		sb.WriteString(fmt.Sprintf("- branch: `%s`\n", rep.Branch))
	}
	if rep.Commit != "" {
		sb.WriteString(fmt.Sprintf("- commit: `%s`\n", rep.Commit))
	}
	if rep.BenchPattern != "" {
		sb.WriteString(fmt.Sprintf("- benchmark regex: `%s`\n", rep.BenchPattern))
	}
	if rep.Count > 0 {
		sb.WriteString(fmt.Sprintf("- benchmark count: `%d`\n", rep.Count))
	}
	if rep.CollectionBatchSize > 0 {
		sb.WriteString(fmt.Sprintf("- collection batch size: `%d`\n", rep.CollectionBatchSize))
	}
	if rep.RawJSONPath != "" {
		sb.WriteString(fmt.Sprintf("- raw benchmark json: `%s`\n", rep.RawJSONPath))
	}
	sb.WriteString("- ops/sec is derived from the mean `ns/op` across captured benchmark runs\n\n")

	if rep.Status == "unavailable" {
		sb.WriteString("## Availability\n\n")
		sb.WriteString("This branch does not currently contain a runnable collections benchmark harness.\n")
		sb.WriteString("Treat this artifact as an explicit placeholder, not as a benchmark result.\n")
		return sb.String()
	}

	for _, section := range rep.Sections {
		sb.WriteString(fmt.Sprintf("## %s\n\n", section.Title))
		sb.WriteString("| Benchmark | Description | Samples | Ops/sec | ns/op | B/op | allocs/op |\n")
		sb.WriteString("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n")
		for _, benchmark := range section.Benchmarks {
			sb.WriteString("| ")
			sb.WriteString(fmt.Sprintf("`%s`", benchmark.Name))
			sb.WriteString(" | ")
			sb.WriteString(escapeTableCell(benchmark.Description))
			sb.WriteString(" | ")
			sb.WriteString(strconv.Itoa(benchmark.Samples))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(benchmark.OpsPerSec))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(benchmark.MeanNsPerOp))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(benchmark.MeanBytesPerOp))
			sb.WriteString(" | ")
			sb.WriteString(formatFloat(benchmark.MeanAllocsPerOp))
			sb.WriteString(" |\n")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func escapeTableCell(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}

func formatFloat(value float64) string {
	return humanizeFloat(value)
}

func humanizeFloat(value float64) string {
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

func markdownToHTMLDoc(markdown string) ([]byte, error) {
	var body bytes.Buffer
	md := goldmark.New()
	if err := md.Convert([]byte(markdown), &body); err != nil {
		return nil, err
	}
	const pageTmpl = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Collections Benchmark Report</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 2rem auto; max-width: 1100px; padding: 0 1rem; line-height: 1.5; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid #d0d7de; padding: 0.45rem 0.65rem; vertical-align: top; }
    th { background: #f6f8fa; text-align: left; }
    code { background: #f6f8fa; padding: 0.1rem 0.25rem; border-radius: 4px; }
  </style>
</head>
<body>{{.Body}}</body>
</html>`
	tmpl, err := template.New("page").Parse(pageTmpl)
	if err != nil {
		return nil, err
	}
	var doc bytes.Buffer
	if err := tmpl.Execute(&doc, struct{ Body template.HTML }{Body: template.HTML(body.String())}); err != nil {
		return nil, err
	}
	return doc.Bytes(), nil
}
