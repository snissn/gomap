package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	schemaVersion          = "treedb_text_hybrid_scoreboard/v1"
	phase2SynthesisVersion = "treedb_text_v2_phase2_gap_synthesis/v1"
)

type config struct {
	outDir               string
	repoRoot             string
	contextPath          string
	branch               string
	commit               string
	baseRef              string
	baseSHA              string
	issue                string
	parentIssue          string
	allowCounterFailures bool
	goBenches            namedPaths
	externals            namedPaths
	unavailable          namedValues
	commands             namedValues
	caveats              multiValues
}

type namedPath struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type namedPaths []namedPath

func (n *namedPaths) String() string { return fmt.Sprintf("%v", []namedPath(*n)) }
func (n *namedPaths) Set(raw string) error {
	name, value, err := splitNameValue(raw)
	if err != nil {
		return err
	}
	*n = append(*n, namedPath{Name: name, Path: value})
	return nil
}

type namedValue struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type namedValues []namedValue

func (n *namedValues) String() string { return fmt.Sprintf("%v", []namedValue(*n)) }
func (n *namedValues) Set(raw string) error {
	name, value, err := splitNameValue(raw)
	if err != nil {
		return err
	}
	*n = append(*n, namedValue{Name: name, Value: value})
	return nil
}

type multiValues []string

func (m *multiValues) String() string { return strings.Join(*m, ",") }
func (m *multiValues) Set(raw string) error {
	*m = append(*m, raw)
	return nil
}

func splitNameValue(raw string) (string, string, error) {
	idx := strings.Index(raw, "=")
	if idx <= 0 || idx == len(raw)-1 {
		return "", "", fmt.Errorf("expected name=value, got %q", raw)
	}
	name := strings.TrimSpace(raw[:idx])
	value := strings.TrimSpace(raw[idx+1:])
	if name == "" || value == "" {
		return "", "", fmt.Errorf("expected non-empty name=value, got %q", raw)
	}
	return name, value, nil
}

type report struct {
	SchemaVersion      string              `json:"schema_version"`
	GeneratedAt        string              `json:"generated_at"`
	Issue              string              `json:"issue,omitempty"`
	ParentIssue        string              `json:"parent_issue,omitempty"`
	Context            reportContext       `json:"context"`
	Commands           []namedValue        `json:"commands,omitempty"`
	Inputs             reportInputs        `json:"inputs"`
	Rows               []scoreboardRow     `json:"rows"`
	CounterValidations []counterValidation `json:"counter_validations,omitempty"`
	Unavailable        []unavailableRow    `json:"unavailable,omitempty"`
	Phase2Synthesis    phase2Synthesis     `json:"phase2_synthesis"`
	Caveats            []string            `json:"caveats,omitempty"`
}

type reportContext struct {
	RepoRoot    string `json:"repo_root,omitempty"`
	Branch      string `json:"branch,omitempty"`
	Commit      string `json:"commit,omitempty"`
	BaseRef     string `json:"base_ref,omitempty"`
	BaseSHA     string `json:"base_sha,omitempty"`
	GoVersion   string `json:"go_version,omitempty"`
	OS          string `json:"os,omitempty"`
	Arch        string `json:"arch,omitempty"`
	Host        string `json:"host,omitempty"`
	ContextPath string `json:"context_path,omitempty"`
	ContextText string `json:"context_text,omitempty"`
}

type reportInputs struct {
	GoBenchmarks []namedPath `json:"go_benchmarks,omitempty"`
	ExternalJSON []namedPath `json:"external_json,omitempty"`
}

type scoreboardRow struct {
	SourceLabel        string             `json:"source_label"`
	SourcePath         string             `json:"source_path,omitempty"`
	System             string             `json:"system"`
	Engine             string             `json:"engine,omitempty"`
	Modality           string             `json:"modality"`
	Dataset            string             `json:"dataset,omitempty"`
	QuerySet           string             `json:"query_set,omitempty"`
	QueryShape         string             `json:"query_shape,omitempty"`
	TopK               int                `json:"top_k,omitempty"`
	Boundary           string             `json:"boundary,omitempty"`
	Benchmark          string             `json:"benchmark,omitempty"`
	Samples            int                `json:"samples,omitempty"`
	Iterations         int64              `json:"iterations,omitempty"`
	NsPerOp            *float64           `json:"ns_per_op,omitempty"`
	MinNsPerOp         *float64           `json:"min_ns_per_op,omitempty"`
	MaxNsPerOp         *float64           `json:"max_ns_per_op,omitempty"`
	OpsPerSec          *float64           `json:"ops_per_sec,omitempty"`
	BytesPerOp         *float64           `json:"bytes_per_op,omitempty"`
	AllocsPerOp        *float64           `json:"allocs_per_op,omitempty"`
	BuildSeconds       *float64           `json:"build_seconds,omitempty"`
	StorageBytes       *int64             `json:"storage_bytes,omitempty"`
	StorageBytesPerDoc *float64           `json:"storage_bytes_per_doc,omitempty"`
	Metrics            map[string]float64 `json:"metrics,omitempty"`
	Caveats            []string           `json:"caveats,omitempty"`
}

type counterValidation struct {
	SourceLabel string   `json:"source_label"`
	Benchmark   string   `json:"benchmark"`
	Modality    string   `json:"modality"`
	Boundary    string   `json:"boundary"`
	OK          bool     `json:"ok"`
	Checks      []string `json:"checks"`
	Failures    []string `json:"failures,omitempty"`
}

type unavailableRow struct {
	System string `json:"system"`
	Reason string `json:"reason"`
	Source string `json:"source,omitempty"`
}

type phase2Synthesis struct {
	SchemaVersion          string                    `json:"schema_version"`
	ClassificationBasis    string                    `json:"classification_basis"`
	ExternalBaselines      []phase2ExternalBaseline  `json:"external_baselines"`
	BenchmarkTargets       []phase2BenchmarkTarget   `json:"benchmark_targets"`
	GapClassifications     []phase2GapClassification `json:"gap_classifications"`
	PriorityOrder          []phase2Priority          `json:"priority_order"`
	AnalyzerSemanticsNotes []string                  `json:"analyzer_semantics_notes"`
}

type phase2ExternalBaseline struct {
	System   string `json:"system"`
	Status   string `json:"status"`
	Evidence string `json:"evidence"`
	Caveat   string `json:"caveat,omitempty"`
}

type phase2BenchmarkTarget struct {
	ShapeID          string   `json:"shape_id"`
	Shape            string   `json:"shape"`
	Description      string   `json:"description"`
	PrimaryIssue     string   `json:"primary_issue"`
	RequiredRows     []string `json:"required_rows"`
	RequiredCounters []string `json:"required_counters"`
}

type phase2GapClassification struct {
	ShapeID                string   `json:"shape_id"`
	Shape                  string   `json:"shape"`
	Classification         string   `json:"classification"`
	Priority               int      `json:"priority"`
	PrimaryIssue           string   `json:"primary_issue"`
	TreeDBEvidence         string   `json:"treedb_evidence"`
	ExternalEvidence       string   `json:"external_evidence"`
	Rationale              string   `json:"rationale"`
	RequiredFollowUpRows   []string `json:"required_follow_up_rows"`
	NonEquivalentSemantics []string `json:"non_equivalent_semantics,omitempty"`
}

type phase2Priority struct {
	Priority             int      `json:"priority"`
	ShapeID              string   `json:"shape_id"`
	PrimaryIssue         string   `json:"primary_issue"`
	Rationale            string   `json:"rationale"`
	RequiredFollowUpRows []string `json:"required_follow_up_rows"`
}

type goBenchSample struct {
	Name        string
	Iterations  int64
	NsPerOp     float64
	BytesPerOp  *float64
	AllocsPerOp *float64
	Metrics     map[string]float64
}

type goBenchAggregate struct {
	Name          string
	Samples       int
	Iterations    int64
	MeanNsPerOp   float64
	MinNsPerOp    float64
	MaxNsPerOp    float64
	BytesPerOp    *float64
	AllocsPerOp   *float64
	MeanMetrics   map[string]float64
	MetricSamples map[string]int
}

var (
	benchmarkNameRE   = regexp.MustCompile(`^(Benchmark\S+)-\d+$`)
	docsSourceLabelRE = regexp.MustCompile(`(?:^|[_-])docs[_-]([0-9]+)(?:$|[_-])`)
)

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "treedb_text_hybrid_scoreboard: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "treedb_text_hybrid_scoreboard: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("treedb_text_hybrid_scoreboard", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.outDir, "out-dir", "", "Output directory for scoreboard.json and scoreboard.md")
	fs.StringVar(&cfg.repoRoot, "repo-root", "", "Repository root; defaults to git rev-parse --show-toplevel when available")
	fs.StringVar(&cfg.contextPath, "context", "", "Optional context text file captured beside benchmark artifacts")
	fs.StringVar(&cfg.branch, "branch", "", "Git branch label; defaults to current branch when available")
	fs.StringVar(&cfg.commit, "commit", "", "Git commit label; defaults to current HEAD when available")
	fs.StringVar(&cfg.baseRef, "base-ref", "", "Baseline/base ref label")
	fs.StringVar(&cfg.baseSHA, "base-sha", "", "Baseline/base commit SHA")
	fs.StringVar(&cfg.issue, "issue", "2727", "Issue number or URL")
	fs.StringVar(&cfg.parentIssue, "parent-issue", "2726", "Parent issue number or URL")
	fs.BoolVar(&cfg.allowCounterFailures, "allow-counter-failures", false, "Write reports even when zero-doc counter validation fails")
	fs.Var(&cfg.goBenches, "go-bench", "Go benchmark artifact as name=path; may be repeated")
	fs.Var(&cfg.externals, "external", "External baseline JSON artifact as name=path; may be repeated")
	fs.Var(&cfg.unavailable, "unavailable", "Unavailable external baseline as system=reason; may be repeated")
	fs.Var(&cfg.commands, "command", "Reproduction command as name=command text; may be repeated")
	fs.Var(&cfg.caveats, "caveat", "Caveat text to include in the report; may be repeated")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.outDir == "" {
		return config{}, fmt.Errorf("-out-dir is required")
	}
	if len(cfg.goBenches) == 0 && len(cfg.externals) == 0 && len(cfg.unavailable) == 0 {
		return config{}, fmt.Errorf("at least one -go-bench, -external, or -unavailable input is required")
	}
	return cfg, nil
}

func run(cfg config) error {
	rep, reportErr := buildReport(cfg)
	var validationErr counterValidationFailureError
	if reportErr != nil && !errors.As(reportErr, &validationErr) {
		return reportErr
	}
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return fmt.Errorf("create out dir: %w", err)
	}
	jsonPath := filepath.Join(cfg.outDir, "scoreboard.json")
	mdPath := filepath.Join(cfg.outDir, "scoreboard.md")
	payload, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if err := os.WriteFile(jsonPath, payload, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", jsonPath, err)
	}
	if err := os.WriteFile(mdPath, []byte(renderMarkdown(rep)), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", mdPath, err)
	}
	fmt.Printf("wrote scoreboard json: %s\n", jsonPath)
	fmt.Printf("wrote scoreboard md:   %s\n", mdPath)
	if reportErr != nil && !cfg.allowCounterFailures {
		return reportErr
	}
	return nil
}

func buildReport(cfg config) (report, error) {
	ctx := buildContext(cfg)
	rep := report{
		SchemaVersion: schemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Issue:         cfg.issue,
		ParentIssue:   cfg.parentIssue,
		Context:       ctx,
		Commands:      []namedValue(cfg.commands),
		Inputs: reportInputs{
			GoBenchmarks: []namedPath(cfg.goBenches),
			ExternalJSON: []namedPath(cfg.externals),
		},
		Caveats: append(defaultCaveats(), cfg.caveats...),
	}

	for _, input := range cfg.goBenches {
		rows, err := rowsFromGoBench(input)
		if err != nil {
			return rep, err
		}
		rep.Rows = append(rep.Rows, rows...)
	}
	for _, input := range cfg.externals {
		rows, unavailable, err := rowsFromExternal(input)
		if err != nil {
			return rep, err
		}
		rep.Rows = append(rep.Rows, rows...)
		rep.Unavailable = append(rep.Unavailable, unavailable...)
	}
	for _, item := range cfg.unavailable {
		rep.Unavailable = append(rep.Unavailable, unavailableRow{System: item.Name, Reason: item.Value})
	}

	sortRows(rep.Rows)
	rep.Phase2Synthesis = buildPhase2Synthesis(rep.Rows, rep.Unavailable)
	rep.CounterValidations = validateCounterRows(rep.Rows)
	var failures []string
	for _, check := range rep.CounterValidations {
		if !check.OK {
			failures = append(failures, fmt.Sprintf("%s %s: %s", check.SourceLabel, check.Benchmark, strings.Join(check.Failures, "; ")))
		}
	}
	if len(failures) > 0 {
		return rep, counterValidationFailureError{Failures: failures}
	}
	return rep, nil
}

type counterValidationFailureError struct {
	Failures []string
}

func (e counterValidationFailureError) Error() string {
	return "zero-doc counter validation failed: " + strings.Join(e.Failures, " | ")
}

func buildContext(cfg config) reportContext {
	repoRoot := cfg.repoRoot
	if repoRoot == "" {
		repoRoot = strings.TrimSpace(runGit(repoRoot, "rev-parse", "--show-toplevel"))
	}
	branch := cfg.branch
	if branch == "" {
		branch = strings.TrimSpace(runGit(repoRoot, "branch", "--show-current"))
	}
	commit := cfg.commit
	if commit == "" {
		commit = strings.TrimSpace(runGit(repoRoot, "rev-parse", "HEAD"))
	}
	contextText := ""
	if cfg.contextPath != "" {
		if raw, err := os.ReadFile(cfg.contextPath); err == nil {
			contextText = strings.TrimSpace(string(raw))
		}
	}
	goVersion := goVersionFromContextText(contextText)
	if goVersion == "" {
		goVersion = runtimeGoVersion()
	}
	host, _ := os.Hostname()
	return reportContext{
		RepoRoot:    repoRoot,
		Branch:      branch,
		Commit:      commit,
		BaseRef:     cfg.baseRef,
		BaseSHA:     cfg.baseSHA,
		GoVersion:   goVersion,
		OS:          runtimeGOOS(),
		Arch:        runtimeGOARCH(),
		Host:        host,
		ContextPath: cfg.contextPath,
		ContextText: contextText,
	}
}

func runGit(repoRoot string, args ...string) string {
	cmd := exec.Command("git", args...)
	if repoRoot != "" {
		cmd.Dir = repoRoot
	}
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func goVersionFromContextText(contextText string) string {
	for _, line := range strings.Split(contextText, "\n") {
		line = strings.TrimSpace(line)
		value, ok := strings.CutPrefix(line, "go=")
		if ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func runtimeGoVersion() string {
	return fmt.Sprintf("go version %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH)
}

func runtimeGOOS() string   { return runtime.GOOS }
func runtimeGOARCH() string { return runtime.GOARCH }

func defaultCaveats() []string {
	return []string{
		"TreeDB rows are same-host local benchmark rows, not standalone industry-parity claims.",
		"No-document candidate rows must keep docs_fetched/search=0, full_doc_fallbacks/search=0, and fail_closed/search=0.",
		"External systems expose different counters; unavailable counters are left blank instead of inferred.",
	}
}

func rowsFromGoBench(input namedPath) ([]scoreboardRow, error) {
	raw, err := os.ReadFile(input.Path)
	if err != nil {
		return nil, fmt.Errorf("read go bench %s: %w", input.Path, err)
	}
	samples, err := parseGoBench(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse go bench %s: %w", input.Path, err)
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("parse go bench %s: no benchmark rows found", input.Path)
	}
	aggs := aggregateGoBenchSamples(samples)
	if len(aggs) == 0 {
		return nil, fmt.Errorf("parse go bench %s: no benchmark aggregates found", input.Path)
	}
	rows := make([]scoreboardRow, 0, len(aggs))
	for _, agg := range aggs {
		row := classifyGoBenchmark(input, agg)
		rows = append(rows, row)
	}
	return rows, nil
}

func parseGoBench(r io.Reader) ([]goBenchSample, error) {
	var samples []goBenchSample
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "{") {
			var event struct {
				Output string `json:"Output"`
			}
			if err := json.Unmarshal([]byte(line), &event); err == nil && event.Output != "" {
				line = strings.TrimSpace(event.Output)
			}
		}
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}
		sample, ok, err := parseGoBenchLine(line)
		if err != nil {
			return nil, err
		}
		if ok {
			samples = append(samples, sample)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return samples, nil
}

func parseGoBenchLine(line string) (goBenchSample, bool, error) {
	fields := strings.Fields(line)
	if len(fields) < 4 {
		return goBenchSample{}, false, nil
	}
	name := normalizeBenchmarkName(fields[0])
	iterations, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return goBenchSample{}, false, nil
	}
	sample := goBenchSample{Name: name, Iterations: iterations, Metrics: make(map[string]float64)}
	for i := 2; i+1 < len(fields); i += 2 {
		value, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return goBenchSample{}, false, fmt.Errorf("parse metric value %q in %q: %w", fields[i], line, err)
		}
		unit := fields[i+1]
		switch unit {
		case "ns/op":
			sample.NsPerOp = value
		case "B/op":
			sample.BytesPerOp = floatPtr(value)
		case "allocs/op":
			sample.AllocsPerOp = floatPtr(value)
		default:
			sample.Metrics[unit] = value
		}
	}
	if sample.NsPerOp <= 0 {
		return goBenchSample{}, false, nil
	}
	return sample, true, nil
}

func normalizeBenchmarkName(name string) string {
	if match := benchmarkNameRE.FindStringSubmatch(name); match != nil {
		return match[1]
	}
	return name
}

func aggregateGoBenchSamples(samples []goBenchSample) []goBenchAggregate {
	byName := make(map[string][]goBenchSample)
	for _, sample := range samples {
		byName[sample.Name] = append(byName[sample.Name], sample)
	}
	out := make([]goBenchAggregate, 0, len(byName))
	for name, group := range byName {
		agg := goBenchAggregate{Name: name, Samples: len(group), MinNsPerOp: math.Inf(1), MaxNsPerOp: 0, MeanMetrics: make(map[string]float64), MetricSamples: make(map[string]int)}
		var nsTotal float64
		var bytesTotal float64
		var bytesN int
		var allocsTotal float64
		var allocsN int
		for _, sample := range group {
			agg.Iterations += sample.Iterations
			nsTotal += sample.NsPerOp
			if sample.NsPerOp < agg.MinNsPerOp {
				agg.MinNsPerOp = sample.NsPerOp
			}
			if sample.NsPerOp > agg.MaxNsPerOp {
				agg.MaxNsPerOp = sample.NsPerOp
			}
			if sample.BytesPerOp != nil {
				bytesTotal += *sample.BytesPerOp
				bytesN++
			}
			if sample.AllocsPerOp != nil {
				allocsTotal += *sample.AllocsPerOp
				allocsN++
			}
			for k, v := range sample.Metrics {
				agg.MeanMetrics[k] += v
				agg.MetricSamples[k]++
			}
		}
		agg.MeanNsPerOp = nsTotal / float64(len(group))
		if bytesN > 0 {
			agg.BytesPerOp = floatPtr(bytesTotal / float64(bytesN))
		}
		if allocsN > 0 {
			agg.AllocsPerOp = floatPtr(allocsTotal / float64(allocsN))
		}
		for k, total := range agg.MeanMetrics {
			agg.MeanMetrics[k] = total / float64(agg.MetricSamples[k])
		}
		out = append(out, agg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func classifyGoBenchmark(input namedPath, agg goBenchAggregate) scoreboardRow {
	name := agg.Name
	metrics := cloneMetrics(agg.MeanMetrics)
	modality := "other"
	engine := "treedb"
	boundary := "Go benchmark timed operation"
	queryShape := ""
	querySet := "synthetic"

	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "indexed_insert_batch_flush_vector_rebuild"):
		modality = "build_storage"
		engine = "treedb_collection_text_vector"
		boundary = "InsertBatch + Flush + vector RebuildVectorIndex timed by benchmark; setup excluded per Go benchmark"
		queryShape = "index build / ingest"
	case strings.Contains(lower, "create_text_v2_index_backfill"):
		modality = "build_storage"
		engine = "treedb_text_v2"
		boundary = "CreateTextIndex v2 backfill timed by benchmark; primary insert setup excluded"
		queryShape = "text-v2 index build"
	case strings.Contains(lower, "mode_vector_only_no_docs") && !strings.Contains(lower, "filter_none_100pct"):
		modality = "vector_scalar"
		engine = "treedb_hybrid_executor"
		boundary = "No-document vector+scalar candidate generation and filtering"
		queryShape = "vector top-k candidates + indexed scalar filter"
	case strings.Contains(lower, "mode_text_only_no_docs") && !strings.Contains(lower, "filter_none_100pct"):
		modality = "text_scalar"
		engine = "treedb_hybrid_executor"
		boundary = "No-document text+scalar candidate generation and filtering"
		queryShape = "refund policy BM25F candidates + indexed scalar filter"
	case strings.Contains(lower, "search_vector_candidates") || strings.Contains(lower, "mode_vector_only_no_docs"):
		modality = "vector_only"
		engine = "treedb_column_graph"
		boundary = "No-document vector candidate generation"
		queryShape = "vector top-k candidates"
	case strings.Contains(lower, "search_hybrid_v2_no_docs_scalar_filter"):
		modality = "text_scalar"
		engine = "treedb_hybrid_executor"
		boundary = "No-document text+scalar candidate generation through the hybrid executor"
		queryShape = "refund policy BM25F candidates + indexed scalar filter"
	case strings.Contains(lower, "mode_hybrid_no_docs") && !strings.Contains(lower, "filter_none_100pct"):
		modality = "hybrid_scalar"
		engine = "treedb_hybrid_executor"
		boundary = "No-document text+vector/scalar candidate generation and fusion"
		queryShape = "refund policy + vector + indexed scalar filter"
	case strings.Contains(lower, "search_hybrid_no_docs_scalar_filter"):
		modality = "hybrid_scalar"
		engine = "treedb_hybrid_executor"
		boundary = "No-document hybrid+scalar candidate generation and fusion"
		queryShape = "refund policy + vector + indexed scalar filter"
	case strings.Contains(lower, "mode_hybrid_no_docs"):
		modality = "hybrid"
		engine = "treedb_hybrid_executor"
		boundary = "No-document text+vector candidate generation and fusion"
		queryShape = "refund policy + vector"
	case strings.Contains(lower, "search_text_v2_candidates"):
		modality = "text_only"
		engine = "treedb_text_v2"
		boundary = "No-document text-v2 candidate generation"
		queryShape = "refund policy BM25F candidates"
	case strings.Contains(lower, "mode_text_only_no_docs"):
		modality = "text_only"
		engine = "treedb_text_v2"
		boundary = "No-document text-only SearchHybrid executor row"
		queryShape = "refund policy BM25F candidates"
	case strings.Contains(lower, "benchmarktextv2") || strings.Contains(lower, "blockmax_common_topk") || strings.Contains(lower, "score_only"):
		modality = "text_only"
		engine = "treedb_text_v2"
		boundary = "No-document text-v2 score-only BM25F search"
		queryShape = textQueryShapeFromBenchmark(name)
	}
	if strings.Contains(lower, "fetch_topk") || strings.Contains(lower, "include") {
		boundary = "Final top-k document fetch/materialization"
	}

	row := scoreboardRow{
		SourceLabel: input.Name,
		SourcePath:  input.Path,
		System:      "TreeDB",
		Engine:      engine,
		Modality:    modality,
		Dataset:     datasetFromMetricsAndName(metrics, name, input.Name),
		QuerySet:    querySet,
		QueryShape:  queryShape,
		TopK:        topKFromMetricsAndName(metrics, name),
		Boundary:    boundary,
		Benchmark:   name,
		Samples:     agg.Samples,
		Iterations:  agg.Iterations,
		NsPerOp:     floatPtr(agg.MeanNsPerOp),
		MinNsPerOp:  floatPtr(agg.MinNsPerOp),
		MaxNsPerOp:  floatPtr(agg.MaxNsPerOp),
		OpsPerSec:   floatPtr(opsPerSecond(agg.MeanNsPerOp)),
		BytesPerOp:  agg.BytesPerOp,
		AllocsPerOp: agg.AllocsPerOp,
		Metrics:     metrics,
	}
	if v, ok := metrics["index_bytes/doc"]; ok {
		row.StorageBytesPerDoc = floatPtr(v)
	}
	return row
}

func textQueryShapeFromBenchmark(name string) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "blockmax_common_topk"):
		return "common term BM25F top-k with block-max pruning"
	case strings.Contains(lower, "exhaustive_common_topk"):
		return "common term BM25F exhaustive reference"
	case strings.Contains(lower, "multi_term_and"):
		return "multi-term AND BM25F"
	case strings.Contains(lower, "rare"):
		return "rare term BM25F"
	case strings.Contains(lower, "common"):
		return "common term BM25F"
	default:
		return "BM25F text search"
	}
}

func datasetFromMetricsAndName(metrics map[string]float64, name, sourceLabel string) string {
	var parts []string
	if docs, ok := metricAny(metrics, "docs_fixture", "docs/op"); ok && docs > 0 {
		parts = append(parts, fmt.Sprintf("docs=%s", formatNumber(docs)))
	} else if docs := docsFromBenchmarkName(name); docs > 0 {
		parts = append(parts, fmt.Sprintf("docs=%d", docs))
	} else if docs := docsFromSourceLabel(sourceLabel); docs > 0 {
		parts = append(parts, fmt.Sprintf("docs=%d", docs))
	}
	if dims, ok := metrics["vector_dims"]; ok && dims > 0 {
		parts = append(parts, fmt.Sprintf("dims=%s", formatNumber(dims)))
	}
	if budget, ok := metricAny(metrics, "candidate_budget/source", "candidate_budget/search"); ok && budget > 0 {
		parts = append(parts, fmt.Sprintf("candidate_budget=%s", formatNumber(budget)))
	} else if budget := candidateBudgetFromBenchmarkName(name); budget > 0 {
		parts = append(parts, fmt.Sprintf("candidate_budget=%d", budget))
	}
	if topk, ok := metrics["topk/search"]; ok && topk > 0 {
		parts = append(parts, fmt.Sprintf("topK=%s", formatNumber(topk)))
	} else if topk := topKFromBenchmarkName(name); topk > 0 {
		parts = append(parts, fmt.Sprintf("topK=%d", topk))
	}
	return strings.Join(parts, ", ")
}

func topKFromMetricsAndName(metrics map[string]float64, name string) int {
	if v := intMetric(metrics, "topk/search"); v > 0 {
		return v
	}
	return topKFromBenchmarkName(name)
}

func topKFromBenchmarkName(name string) int {
	return intAfterToken(name, "topK_")
}

func candidateBudgetFromBenchmarkName(name string) int {
	return intAfterToken(name, "candidates_")
}

func docsFromBenchmarkName(name string) int {
	return intAfterToken(name, "docs_")
}

func docsFromSourceLabel(label string) int {
	lower := strings.ToLower(label)
	if match := docsSourceLabelRE.FindStringSubmatch(lower); match != nil {
		if docs, err := strconv.Atoi(match[1]); err == nil {
			return docs
		}
	}
	if strings.Contains(lower, "100k") {
		return 100_000
	}
	if strings.Contains(lower, "10k") {
		return 10_000
	}
	if strings.Contains(lower, "1m") {
		return 1_000_000
	}
	return 0
}

func intAfterToken(s, token string) int {
	idx := strings.Index(s, token)
	if idx < 0 {
		return 0
	}
	start := idx + len(token)
	end := start
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end == start {
		return 0
	}
	value, _ := strconv.Atoi(s[start:end])
	return value
}

func rowsFromExternal(input namedPath) ([]scoreboardRow, []unavailableRow, error) {
	raw, err := os.ReadFile(input.Path)
	if err != nil {
		return nil, nil, fmt.Errorf("read external %s: %w", input.Path, err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, nil, fmt.Errorf("parse external %s: %w", input.Path, err)
	}
	if status, _ := obj["status"].(string); strings.EqualFold(status, "unavailable") {
		system := stringField(obj, "system", input.Name)
		reason := stringField(obj, "unavailable_reason", stringField(obj, "reason", "external baseline unavailable"))
		return nil, []unavailableRow{{System: system, Reason: reason, Source: input.Path}}, nil
	}
	if _, ok := obj["search_benchmarks"]; ok {
		return rowsFromVectorExternal(input, obj), nil, nil
	}
	row := rowFromGenericExternal(input, obj)
	return []scoreboardRow{row}, nil, nil
}

func rowFromGenericExternal(input namedPath, obj map[string]any) scoreboardRow {
	metrics := numberMap(obj["metrics"])
	search := mapValue(obj["search"])
	build := mapValue(obj["build"])
	storage := mapValue(obj["storage"])
	if len(storage) == 0 {
		storage = mapValue(obj["storage_after_build"])
	}
	ns := numberPtrAny(search, "avg_nanos", "ns_per_op")
	ops := numberPtrAny(search, "ops_per_second", "ops_per_sec")
	if ops == nil && ns != nil {
		ops = floatPtr(opsPerSecond(*ns))
	}
	bytesPerOp := numberPtrAny(search, "bytes_per_op")
	allocsPerOp := numberPtrAny(search, "allocs_per_op")
	buildSeconds := numberPtrAny(build, "seconds", "build_seconds")
	storageBytes := int64PtrAny(storage, "total_bytes", "storage_bytes")
	storagePerDoc := numberPtrAny(storage, "bytes_per_doc", "storage_bytes_per_doc")
	return scoreboardRow{
		SourceLabel:        input.Name,
		SourcePath:         input.Path,
		System:             stringField(obj, "system", input.Name),
		Engine:             stringField(obj, "engine", "external"),
		Modality:           stringField(obj, "modality", "text_only"),
		Dataset:            datasetFromExternal(obj),
		QuerySet:           stringField(obj, "query_set", "synthetic"),
		QueryShape:         stringField(obj, "query_shape", "external top-k query"),
		TopK:               externalTopK(obj),
		Boundary:           stringField(obj, "boundary", "external benchmark boundary"),
		Benchmark:          stringField(obj, "benchmark", input.Name),
		Samples:            1,
		NsPerOp:            ns,
		OpsPerSec:          ops,
		BytesPerOp:         bytesPerOp,
		AllocsPerOp:        allocsPerOp,
		BuildSeconds:       buildSeconds,
		StorageBytes:       storageBytes,
		StorageBytesPerDoc: storagePerDoc,
		Metrics:            metrics,
		Caveats:            stringSlice(obj["caveats"]),
	}
}

func rowsFromVectorExternal(input namedPath, obj map[string]any) []scoreboardRow {
	benchmarks, _ := obj["search_benchmarks"].([]any)
	rows := make([]scoreboardRow, 0, len(benchmarks))
	buildSeconds := phaseSeconds(obj)
	storage := preferredStorage(obj)
	storageBytes := int64PtrAny(storage, "total_bytes")
	storagePerDoc := numberPtrAny(storage, "bytes_per_doc")
	for _, item := range benchmarks {
		rowMap := mapValue(item)
		if len(rowMap) == 0 {
			continue
		}
		metrics := map[string]float64{}
		for _, key := range []string{"avg_documents_fetched", "avg_response_owned_result_allocs", "avg_search_route_hnsw_search_pack", "avg_search_route_quantized_only", "avg_search_route_quantized_rerank", "avg_search_route_column_graph_prepared", "avg_search_route_column_graph_fallback", "avg_graph_row_fallbacks", "avg_typed_column_fallbacks", "avg_vector_scratch_decodes", "avg_candidates", "avg_vector_bytes", "avg_norm_bytes"} {
			if v, ok := number(rowMap[key]); ok {
				metrics[key] = v
			}
		}
		if v, ok := metrics["avg_documents_fetched"]; ok {
			metrics["docs_fetched/search"] = v
		}
		ns := numberPtrAny(rowMap, "avg_nanos")
		ops := numberPtrAny(rowMap, "ops_per_second")
		if ops == nil && ns != nil {
			ops = floatPtr(opsPerSecond(*ns))
		}
		concurrency := int(numberAny(rowMap, "concurrency"))
		dataset := fmt.Sprintf("docs=%s, dims=%s, topK=%s, concurrency=%d", formatNumber(numberAny(obj, "docs")), formatNumber(numberAny(obj, "dimensions")), formatNumber(numberAny(obj, "top_k")), concurrency)
		rows = append(rows, scoreboardRow{
			SourceLabel:        input.Name,
			SourcePath:         input.Path,
			System:             vectorSystemName(obj),
			Engine:             stringField(obj, "engine", stringField(obj, "backend", input.Name)),
			Modality:           "vector_only",
			Dataset:            dataset,
			QuerySet:           "TreeDB-exported synthetic vector set",
			QueryShape:         "vector ANN top-k",
			TopK:               externalTopK(obj),
			Boundary:           "Vector search; external harness semantics documented by source artifact",
			Benchmark:          fmt.Sprintf("%s/search_c%d", stringField(obj, "backend", input.Name), concurrency),
			Samples:            1,
			NsPerOp:            ns,
			OpsPerSec:          ops,
			BuildSeconds:       buildSeconds,
			StorageBytes:       storageBytes,
			StorageBytesPerDoc: storagePerDoc,
			Metrics:            metrics,
		})
	}
	return rows
}

func phaseSeconds(obj map[string]any) *float64 {
	for _, key := range []string{"build", "rebuild"} {
		m := mapValue(obj[key])
		if v := numberPtrAny(m, "seconds"); v != nil {
			return v
		}
	}
	return nil
}

func preferredStorage(obj map[string]any) map[string]any {
	for _, key := range []string{"storage_after_index_vacuum", "storage_after_build", "storage_after_close", "storage_after_compact", "storage"} {
		m := mapValue(obj[key])
		if len(m) > 0 {
			return m
		}
	}
	return nil
}

func vectorSystemName(obj map[string]any) string {
	backend := stringField(obj, "backend", "external_vector")
	switch backend {
	case "sqlite_vectorlite":
		return "SQLite+Vectorlite"
	case "pgvector":
		return "PostgreSQL+pgvector"
	case "mongodb_vector_search":
		return "MongoDB Vector Search"
	case "treedb", "treedb_column_graph":
		return "TreeDB"
	default:
		if strings.HasPrefix(backend, "treedb_") {
			return "TreeDB"
		}
		return backend
	}
}

func externalTopK(obj map[string]any) int {
	if topK := int(numberAny(obj, "top_k", "topK")); topK > 0 {
		return topK
	}
	dataset := mapValue(obj["dataset"])
	return int(numberAny(dataset, "top_k", "topK"))
}

func datasetFromExternal(obj map[string]any) string {
	if raw, ok := obj["dataset"].(string); ok {
		return raw
	}
	m := mapValue(obj["dataset"])
	var parts []string
	for _, key := range []string{"docs", "documents", "dims", "dimensions", "queries", "top_k", "topK", "candidate_budget"} {
		if v, ok := number(m[key]); ok {
			label := key
			if key == "documents" {
				label = "docs"
			}
			if key == "dimensions" {
				label = "dims"
			}
			if key == "top_k" || key == "topK" {
				label = "topK"
			}
			parts = append(parts, fmt.Sprintf("%s=%s", label, formatNumber(v)))
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, ", ")
	}
	return ""
}

func validateCounterRows(rows []scoreboardRow) []counterValidation {
	var checks []counterValidation
	for _, row := range rows {
		if !requiresZeroDocValidation(row) {
			continue
		}
		check := counterValidation{SourceLabel: row.SourceLabel, Benchmark: row.Benchmark, Modality: row.Modality, Boundary: row.Boundary, OK: true}
		for _, metric := range []string{"docs_fetched/search", "full_doc_fallbacks/search", "fail_closed/search"} {
			checkMetricZero(row, metric, &check)
		}
		if row.System == "TreeDB" && (row.Modality == "text_only" || row.Modality == "text_scalar" || row.Modality == "hybrid" || row.Modality == "hybrid_scalar") {
			checkEitherMetricZero(row, []string{"text_state_lookups/search", "state_lookups/search"}, &check)
			checkEitherMetricZero(row, []string{"text_match_details/search", "match_details/search"}, &check)
		}
		checks = append(checks, check)
	}
	return checks
}

func buildPhase2Synthesis(rows []scoreboardRow, unavailable []unavailableRow) phase2Synthesis {
	targets := phase2BenchmarkTargets()
	classifications := make([]phase2GapClassification, 0, len(targets))
	for _, target := range targets {
		classifications = append(classifications, classifyPhase2Gap(target, rows, unavailable))
	}
	sort.Slice(classifications, func(i, j int) bool {
		if classifications[i].Priority == classifications[j].Priority {
			return classifications[i].ShapeID < classifications[j].ShapeID
		}
		return classifications[i].Priority < classifications[j].Priority
	})
	priorities := make([]phase2Priority, 0, len(classifications))
	for _, row := range classifications {
		priorities = append(priorities, phase2Priority{
			Priority:             row.Priority,
			ShapeID:              row.ShapeID,
			PrimaryIssue:         row.PrimaryIssue,
			Rationale:            row.Rationale,
			RequiredFollowUpRows: append([]string(nil), row.RequiredFollowUpRows...),
		})
	}
	return phase2Synthesis{
		SchemaVersion:          phase2SynthesisVersion,
		ClassificationBasis:    "phase-2 target taxonomy plus captured scoreboard rows; ahead/near require comparable external rows, while missing or non-equivalent external rows stay behind/far by evidence gap rather than parity claim",
		ExternalBaselines:      phase2ExternalBaselines(rows, unavailable),
		BenchmarkTargets:       targets,
		GapClassifications:     classifications,
		PriorityOrder:          priorities,
		AnalyzerSemanticsNotes: phase2AnalyzerSemanticsNotes(),
	}
}

func phase2ExternalBaselines(rows []scoreboardRow, unavailable []unavailableRow) []phase2ExternalBaseline {
	systems := []string{"SQLite FTS5", "Bleve", "Tantivy", "Lucene"}
	out := make([]phase2ExternalBaseline, 0, len(systems))
	for _, system := range systems {
		var rowLabels []string
		for _, row := range rows {
			if strings.EqualFold(row.System, system) {
				rowLabels = append(rowLabels, row.SourceLabel)
			}
		}
		if len(rowLabels) > 0 {
			sort.Strings(rowLabels)
			out = append(out, phase2ExternalBaseline{System: system, Status: "available", Evidence: fmt.Sprintf("captured scoreboard row(s): %s", strings.Join(rowLabels, ", ")), Caveat: externalBaselineCaveat(system)})
			continue
		}
		if reason, source, ok := unavailableReason(system, unavailable); ok {
			evidence := reason
			if source != "" {
				evidence = evidence + " (source: " + source + ")"
			}
			out = append(out, phase2ExternalBaseline{System: system, Status: "unavailable", Evidence: evidence, Caveat: externalBaselineCaveat(system)})
			continue
		}
		out = append(out, phase2ExternalBaseline{System: system, Status: "not_captured", Evidence: "no artifact or explicit unavailable marker in this scoreboard run", Caveat: externalBaselineCaveat(system)})
	}
	return out
}

func unavailableReason(system string, unavailable []unavailableRow) (reason, source string, ok bool) {
	for _, row := range unavailable {
		if strings.EqualFold(row.System, system) {
			return row.Reason, row.Source, true
		}
	}
	return "", "", false
}

func externalBaselineCaveat(system string) string {
	switch system {
	case "SQLite FTS5":
		return "embedded FTS baseline; rowid+bm25 retrieval is not Lucene/Tantivy/Bleve analyzer or query-parser parity"
	case "Bleve":
		return "requires a pinned Go harness with the same corpus, analyzer, query semantics, and result-order checks"
	case "Tantivy":
		return "requires a pinned Rust/Tantivy harness with the same corpus, analyzer, query semantics, and result-order checks"
	case "Lucene":
		return "requires a pinned Lucene/JMH or OpenSearch run with the same corpus, analyzer, query semantics, and result-order checks"
	default:
		return ""
	}
}

func phase2BenchmarkTargets() []phase2BenchmarkTarget {
	commonCounters := []string{"docs_fetched/search=0", "full_doc_fallbacks/search=0", "fail_closed/search=0", "text_state_lookups/search=0", "text_match_details/search=0"}
	blockCounters := append(append([]string(nil), commonCounters...), "posting_blocks_visited/search", "posting_blocks_skipped/search", "threshold_updates/search")
	return []phase2BenchmarkTarget{
		{
			ShapeID:          "single_term_common",
			Shape:            "single-term common",
			Description:      "High-document-frequency BM25F top-k term with exact block-max skipping.",
			PrimaryIssue:     "#2835",
			RequiredRows:     []string{"TreeDB 10k and 100k blockmax_common_topk", "SQLite FTS5/Lucene/Tantivy/Bleve common-term top-k rows"},
			RequiredCounters: blockCounters,
		},
		{
			ShapeID:          "single_term_rare",
			Shape:            "single-term rare",
			Description:      "Low-document-frequency BM25F top-k term where decode/setup overhead dominates.",
			PrimaryIssue:     "#2835",
			RequiredRows:     []string{"TreeDB rare term score-only 10k/100k/1M", "SQLite FTS5/Lucene/Tantivy/Bleve rare-term top-k rows"},
			RequiredCounters: commonCounters,
		},
		{
			ShapeID:          "multi_term_and",
			Shape:            "multi-term AND",
			Description:      "Exact BM25F conjunction over multiple query terms with deterministic ranking.",
			PrimaryIssue:     "#2837",
			RequiredRows:     []string{"TreeDB multi_term_and score-only 10k/100k/1M", "SQLite FTS5/Lucene/Tantivy/Bleve explicit AND rows"},
			RequiredCounters: append(append([]string(nil), commonCounters...), "posting_blocks_visited/search", "candidates_scored/search"),
		},
		{
			ShapeID:          "multi_term_or_wand",
			Shape:            "multi-term OR/WAND",
			Description:      "Exact BM25F disjunction with block-max/WAND pruning and deterministic ranking.",
			PrimaryIssue:     "#2836",
			RequiredRows:     []string{"TreeDB or_common/or_common_rare/or_high_frequency blockmax and exhaustive rows", "Lucene/Tantivy/Bleve OR/WAND rows", "SQLite FTS5 OR row if semantics are made explicit"},
			RequiredCounters: append(append([]string(nil), blockCounters...), "blockmax_fallbacks/search", "candidates_scored/search"),
		},
		{
			ShapeID:          "phrase",
			Shape:            "phrase/proximity",
			Description:      "Bounded position-lane phrase/proximity query over StorePositions text-v2 indexes.",
			PrimaryIssue:     "#2839",
			RequiredRows:     []string{"TreeDB phrase exact/slop 10k/100k", "Lucene/Tantivy/Bleve phrase/proximity rows with analyzer notes"},
			RequiredCounters: append(append([]string(nil), commonCounters...), "position_lookups/search", "phrase_candidates_checked/search", "phrase_candidates_matched/search"),
		},
		{
			ShapeID:          "hybrid_text_scalar",
			Shape:            "hybrid text+scalar",
			Description:      "BM25F candidate generation composed with indexed scalar filtering without full-document fetch.",
			PrimaryIssue:     "#2836",
			RequiredRows:     []string{"TreeDB text_scalar and hybrid_scalar no-doc rows at rare/broad selectivity", "Lucene/Tantivy/Bleve equivalent filter rows or documented non-equivalence"},
			RequiredCounters: append(append([]string(nil), blockCounters...), "scalar_prefilter_ids/search", "text_candidates/search"),
		},
		{
			ShapeID:          "index_build_ingest",
			Shape:            "index build/ingest",
			Description:      "Text-v2 CreateTextIndex backfill and maintained insert/update/delete throughput.",
			PrimaryIssue:     "#2835",
			RequiredRows:     []string{"TreeDB CreateTextIndex/backfill and InsertBatch text-v2 rows", "SQLite FTS5/Lucene/Tantivy/Bleve build rows with WAL/checkpoint boundary"},
			RequiredCounters: []string{"rows/s", "B/op", "allocs/op", "index_bytes/doc", "posting_blocks/doc", "write_amp_entries/doc"},
		},
		{
			ShapeID:          "index_size",
			Shape:            "index size",
			Description:      "Durable text index bytes/doc, separating primary payload, WAL, value-log, and text-v2 roots.",
			PrimaryIssue:     "#2835",
			RequiredRows:     []string{"TreeDB durable text-v2 bytes/doc after checkpoint and rewrite", "SQLite FTS5/Lucene/Tantivy/Bleve index bytes/doc after equivalent optimize/vacuum"},
			RequiredCounters: []string{"storage_bytes", "storage_bytes_per_doc", "index_bytes/doc", "value-log bytes", "WAL excluded"},
		},
		{
			ShapeID:          "reopen",
			Shape:            "reopen/recovery",
			Description:      "Close/open and post-reopen text/hybrid probe latency with snapshot-bound roots.",
			PrimaryIssue:     "#2837",
			RequiredRows:     []string{"TreeDB scale harness reopen probe at 10k/1M/10M", "External engine reopen/open-reader rows where embedded equivalents exist"},
			RequiredCounters: []string{"close_seconds", "open_seconds", "probe_seconds", "docs_fetched/search=0", "fail_closed/search=0"},
		},
		{
			ShapeID:          "maintenance_rewrite",
			Shape:            "maintenance/rewrite",
			Description:      "Bounded logical text-v2 rewrite plus normal TreeDB physical reclamation and post-rewrite search probes.",
			PrimaryIssue:     "#2840",
			RequiredRows:     []string{"TreeDB RewriteTextIndex/MaintainTextIndexes rows with stale-posting debt", "External optimize/merge rows if used as a comparator"},
			RequiredCounters: []string{"stale_postings_purged/op", "tombstones_purged/op", "posting_blocks_read/op", "posting_blocks_written/op", "rewrite seconds", "post-rewrite search ns/op"},
		},
	}
}

func classifyPhase2Gap(target phase2BenchmarkTarget, rows []scoreboardRow, unavailable []unavailableRow) phase2GapClassification {
	treeRows := matchingRowsForPhase2Target(rows, target.ShapeID, true)
	externalRows := matchingRowsForPhase2Target(rows, target.ShapeID, false)
	classification, rationale := defaultPhase2Classification(target.ShapeID)
	if measuredClass, measuredRationale, ok := measuredPhase2Classification(target.ShapeID, treeRows, externalRows); ok {
		classification, rationale = measuredClass, measuredRationale
	}
	return phase2GapClassification{
		ShapeID:                target.ShapeID,
		Shape:                  target.Shape,
		Classification:         classification,
		Priority:               phase2PriorityForTarget(target.ShapeID),
		PrimaryIssue:           target.PrimaryIssue,
		TreeDBEvidence:         phase2EvidenceSummary("TreeDB", target.ShapeID, treeRows),
		ExternalEvidence:       phase2ExternalEvidenceSummary(target.ShapeID, externalRows, unavailable),
		Rationale:              rationale,
		RequiredFollowUpRows:   append([]string(nil), target.RequiredRows...),
		NonEquivalentSemantics: phase2SemanticsForTarget(target.ShapeID),
	}
}

func measuredPhase2Classification(shapeID string, treeRows, externalRows []scoreboardRow) (string, string, bool) {
	pairs := phase2ComparablePairs(treeRows, externalRows)
	if len(pairs) == 0 {
		return "", "", false
	}
	bestRatio := math.Inf(1)
	var best phase2ComparablePair
	for _, pair := range pairs {
		var treeMetric, externalMetric float64
		var ok bool
		if shapeID == "index_size" {
			treeMetric, externalMetric, ok = comparableStoragePair(pair.TreeDB, pair.External)
		} else if shapeID == "index_build_ingest" {
			treeMetric, externalMetric, ok = comparableBuildPair(pair.TreeDB, pair.External)
		} else {
			treeMetric, externalMetric, ok = comparableLatencyPair(pair.TreeDB, pair.External)
		}
		if !ok || externalMetric <= 0 {
			continue
		}
		ratio := treeMetric / externalMetric
		if ratio < bestRatio {
			bestRatio = ratio
			best = pair
		}
	}
	if math.IsInf(bestRatio, 1) {
		return "", "", false
	}
	if shapeID == "index_size" {
		return classFromLowerIsBetterRatio(bestRatio), fmt.Sprintf("captured explicit comparable storage pair: TreeDB %s vs %s (ratio %.3gx)", phase2RowMetricSummary(best.TreeDB, "storage"), phase2RowMetricSummary(best.External, "storage"), bestRatio), true
	}
	if shapeID == "index_build_ingest" {
		return classFromLowerIsBetterRatio(bestRatio), fmt.Sprintf("captured explicit comparable build pair: TreeDB %s vs %s (ratio %.3gx)", phase2RowMetricSummary(best.TreeDB, "build"), phase2RowMetricSummary(best.External, "build"), bestRatio), true
	}
	return classFromLowerIsBetterRatio(bestRatio), fmt.Sprintf("captured explicit comparable latency pair: TreeDB %s vs %s (ratio %.3gx)", phase2RowMetricSummary(best.TreeDB, "latency"), phase2RowMetricSummary(best.External, "latency"), bestRatio), true
}

type phase2ComparablePair struct {
	TreeDB   scoreboardRow
	External scoreboardRow
}

func phase2ComparablePairs(treeRows, externalRows []scoreboardRow) []phase2ComparablePair {
	var pairs []phase2ComparablePair
	for _, treeRow := range treeRows {
		if !phase2ExplicitComparableRow(treeRow) {
			continue
		}
		for _, externalRow := range externalRows {
			if !phase2ExplicitComparableRow(externalRow) || !phase2NonSQLiteExternalRow(externalRow) {
				continue
			}
			if !phase2ComparableRowMetadata(treeRow, externalRow) {
				continue
			}
			pairs = append(pairs, phase2ComparablePair{TreeDB: treeRow, External: externalRow})
		}
	}
	return pairs
}

func phase2ExplicitComparableRow(row scoreboardRow) bool {
	if row.Metrics != nil && row.Metrics["phase2_comparable"] == 1 {
		return true
	}
	for _, caveat := range row.Caveats {
		if strings.Contains(strings.ToLower(caveat), "phase2_comparable") {
			return true
		}
	}
	return false
}

func phase2NonSQLiteExternalRow(row scoreboardRow) bool {
	return !strings.EqualFold(row.System, "SQLite FTS5") && !strings.EqualFold(row.Engine, "sqlite_fts5")
}

func phase2ComparableRowMetadata(a, b scoreboardRow) bool {
	if aDocs, okA := phase2DatasetDocs(a.Dataset); okA {
		if bDocs, okB := phase2DatasetDocs(b.Dataset); okB && aDocs != bDocs {
			return false
		}
	} else if bDocs, okB := phase2DatasetDocs(b.Dataset); okB && bDocs != "" {
		return false
	}
	if a.TopK != b.TopK {
		return false
	}
	if a.QuerySet != "" && b.QuerySet != "" && a.QuerySet != b.QuerySet {
		return false
	}
	return true
}

func phase2DatasetDocs(dataset string) (string, bool) {
	for _, field := range strings.Fields(dataset) {
		if strings.HasPrefix(field, "docs=") {
			value := strings.TrimPrefix(field, "docs=")
			return value, value != ""
		}
	}
	return "", false
}

func comparableLatencyPair(treeRow, externalRow scoreboardRow) (float64, float64, bool) {
	if treeRow.NsPerOp == nil || externalRow.NsPerOp == nil || *treeRow.NsPerOp <= 0 || *externalRow.NsPerOp <= 0 {
		return 0, 0, false
	}
	return *treeRow.NsPerOp, *externalRow.NsPerOp, true
}

func comparableStoragePair(treeRow, externalRow scoreboardRow) (float64, float64, bool) {
	if treeRow.StorageBytesPerDoc == nil || externalRow.StorageBytesPerDoc == nil || *treeRow.StorageBytesPerDoc <= 0 || *externalRow.StorageBytesPerDoc <= 0 {
		return 0, 0, false
	}
	return *treeRow.StorageBytesPerDoc, *externalRow.StorageBytesPerDoc, true
}

func comparableBuildPair(treeRow, externalRow scoreboardRow) (float64, float64, bool) {
	treeSeconds, okTree := rowBuildSecondsMetric(treeRow)
	externalSeconds, okExternal := rowBuildSecondsMetric(externalRow)
	if !okTree || !okExternal || treeSeconds <= 0 || externalSeconds <= 0 {
		return 0, 0, false
	}
	return treeSeconds, externalSeconds, true
}

func rowBuildSecondsMetric(row scoreboardRow) (float64, bool) {
	if row.BuildSeconds != nil && *row.BuildSeconds > 0 {
		return *row.BuildSeconds, true
	}
	if row.NsPerOp != nil && *row.NsPerOp > 0 {
		return *row.NsPerOp / 1e9, true
	}
	return 0, false
}

func phase2RowMetricSummary(row scoreboardRow, metricKind string) string {
	metric := ""
	switch metricKind {
	case "storage":
		if row.StorageBytesPerDoc != nil {
			metric = " storage=" + formatNumber(*row.StorageBytesPerDoc) + " B/doc"
		}
	case "build":
		if seconds, ok := rowBuildSecondsMetric(row); ok {
			metric = " build=" + formatSecondsPtr(&seconds)
		}
	default:
		if row.NsPerOp != nil {
			metric = " ns/op=" + formatNumber(*row.NsPerOp)
		}
	}
	return fmt.Sprintf("%s/%s%s", row.SourceLabel, row.Benchmark, metric)
}

func classFromLowerIsBetterRatio(ratio float64) string {
	switch {
	case ratio <= 0.8:
		return "ahead"
	case ratio <= 1.5:
		return "near_parity"
	case ratio <= 8:
		return "behind_but_tractable"
	default:
		return "far_behind"
	}
}

func defaultPhase2Classification(shapeID string) (string, string) {
	switch shapeID {
	case "index_size":
		return "far_behind", "phase-1 scoreboard has external storage rows but lacks a durable TreeDB text-v2 bytes/doc row, so footprint parity is the first evidence and optimization gap"
	case "single_term_common":
		return "behind_but_tractable", "TreeDB has exact block-max common-term evidence, but comparable Lucene/Tantivy/Bleve/SQLite rows and bytes/doc context are missing"
	case "multi_term_or_wand":
		return "behind_but_tractable", "exact OR/WAND block-max landed in phase 1, but scalar-aware pruning and external OR/WAND rows are still required before parity claims"
	case "hybrid_text_scalar":
		return "behind_but_tractable", "TreeDB has zero-document hybrid/scalar rows, but industry baselines are non-equivalent or unavailable and scalar-aware WAND counters need expansion"
	case "index_build_ingest":
		return "behind_but_tractable", "build and ingest rows exist but are not yet separated into text-only durable bytes/doc and external-equivalent build boundaries"
	case "phrase":
		return "behind_but_tractable", "bounded phrase/proximity exists, but Lucene/Tantivy/Bleve phrase semantics and analyzer behavior are broader and unmeasured"
	case "reopen":
		return "behind_but_tractable", "scale harness covers reopen, but phase-2 needs refreshed 1M/10M rows and external open-reader comparators where feasible"
	case "maintenance_rewrite":
		return "behind_but_tractable", "bounded rewrite/maintenance exists, but phase-2 needs debt, duration, and post-maintenance search rows at larger scale"
	case "multi_term_and":
		return "behind_but_tractable", "exact AND serving exists, but current scoreboard lacks external conjunction rows and scale refreshes"
	case "single_term_rare":
		return "behind_but_tractable", "rare-term serving should be tractable, but phase-2 needs explicit rows because setup/decode overhead can dominate"
	default:
		return "behind_but_tractable", "target requires phase-2 evidence before parity claims"
	}
}

func phase2PriorityForTarget(shapeID string) int {
	switch shapeID {
	case "index_size":
		return 1
	case "single_term_common":
		return 2
	case "multi_term_or_wand":
		return 3
	case "hybrid_text_scalar":
		return 4
	case "index_build_ingest":
		return 5
	case "phrase":
		return 6
	case "reopen":
		return 7
	case "maintenance_rewrite":
		return 8
	case "multi_term_and":
		return 9
	case "single_term_rare":
		return 10
	default:
		return 99
	}
}

func matchingRowsForPhase2Target(rows []scoreboardRow, shapeID string, treeDB bool) []scoreboardRow {
	var matches []scoreboardRow
	for _, row := range rows {
		isTreeDB := strings.EqualFold(row.System, "TreeDB")
		if treeDB != isTreeDB {
			continue
		}
		if phase2RowMatchesTarget(row, shapeID) {
			matches = append(matches, row)
		}
	}
	sortRows(matches)
	return matches
}

func phase2RowMatchesTarget(row scoreboardRow, shapeID string) bool {
	text := strings.ToLower(strings.Join([]string{row.Modality, row.Engine, row.QueryShape, row.Boundary, row.Benchmark}, " "))
	switch shapeID {
	case "single_term_common":
		return strings.Contains(text, "common term") || strings.Contains(text, "blockmax_common_topk") || strings.Contains(text, "exhaustive_common_topk")
	case "single_term_rare":
		return strings.Contains(text, "rare term") || strings.Contains(text, "rare_no_docs") || strings.Contains(text, "rare_score")
	case "multi_term_and":
		return strings.Contains(text, "multi-term and") || strings.Contains(text, "multi_term_and") || strings.Contains(text, "and_common")
	case "multi_term_or_wand":
		return strings.Contains(text, "multi-term or") || strings.Contains(text, "or/wand") || strings.Contains(text, "wand") || strings.Contains(text, "or_common") || strings.Contains(text, "or_high_frequency")
	case "phrase":
		return strings.Contains(text, "phrase") || strings.Contains(text, "proximity")
	case "hybrid_text_scalar":
		return row.Modality == "text_scalar" || row.Modality == "hybrid_scalar" || (strings.Contains(text, "scalar") && strings.Contains(text, "text"))
	case "index_build_ingest":
		return row.Modality == "build_storage" || strings.Contains(text, "index build") || strings.Contains(text, "ingest") || strings.Contains(text, "insertbatch") || strings.Contains(text, "backfill")
	case "index_size":
		// Why: many retrieval and vector rows carry storage_bytes_per_doc for
		// context. Treat index-size parity as measured only when the row's own
		// benchmark/query/boundary explicitly represents a text index footprint
		// boundary, not merely because a storage metric is attached.
		return strings.Contains(text, "index size") || strings.Contains(text, "bytes/doc") || strings.Contains(text, "index_bytes") || (strings.Contains(text, "storage") && strings.Contains(text, "text"))
	case "reopen":
		return strings.Contains(text, "reopen") || strings.Contains(text, "recovery")
	case "maintenance_rewrite":
		return strings.Contains(text, "maintenance") || strings.Contains(text, "rewrite") || strings.Contains(text, "compact")
	default:
		return false
	}
}

func phase2EvidenceSummary(system, shapeID string, rows []scoreboardRow) string {
	if len(rows) == 0 {
		if system == "TreeDB" {
			return phase2TreeDBFallbackEvidence(shapeID)
		}
		return "no comparable row captured"
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		var metricParts []string
		if row.NsPerOp != nil {
			metricParts = append(metricParts, "ns/op="+formatNumber(*row.NsPerOp))
		}
		if row.StorageBytesPerDoc != nil {
			metricParts = append(metricParts, "storage="+formatNumber(*row.StorageBytesPerDoc)+" B/doc")
		}
		if row.BuildSeconds != nil {
			metricParts = append(metricParts, "build="+formatSecondsPtr(row.BuildSeconds))
		}
		metric := ""
		if len(metricParts) > 0 {
			metric = " " + strings.Join(metricParts, ", ")
		}
		parts = append(parts, fmt.Sprintf("%s/%s%s", row.SourceLabel, row.Benchmark, metric))
	}
	return fmt.Sprintf("captured %d row(s): %s", len(rows), strings.Join(parts, "; "))
}

func phase2TreeDBFallbackEvidence(shapeID string) string {
	switch shapeID {
	case "single_term_common":
		return "phase-1 #2810/#2809 evidence includes TreeDB block-max common-term rows; rerun scoreboard for current numbers"
	case "single_term_rare":
		return "scale harness defines rare-term score-only rows; current scoreboard run did not capture them"
	case "multi_term_and":
		return "phase-1 text-v2 contract and scale harness define exact multi-term AND rows; current scoreboard run did not capture them"
	case "multi_term_or_wand":
		return "#2812 landed exact multi-term OR/WAND block-max rows; current scoreboard run did not capture them"
	case "phrase":
		return "#2815 landed bounded phrase/proximity rows; current scoreboard run did not capture them"
	case "hybrid_text_scalar":
		return "#2810 scoreboard includes zero-doc hybrid/scalar row support; rerun with scalar selectivity rows if absent"
	case "index_build_ingest":
		return "#2810/#2813 harnesses include build/backfill/ingest rows; current scoreboard run did not capture a text-only equivalent"
	case "index_size":
		return "TreeDB durable text-v2 bytes/doc is not captured in the current scoreboard run"
	case "reopen":
		return "#2813 scale harness includes reopen probes; current scoreboard run did not capture reopen rows"
	case "maintenance_rewrite":
		return "#2814 maintenance policy and #2813 scale harness include rewrite/maintenance rows; current scoreboard run did not capture them"
	default:
		return "no TreeDB row captured"
	}
}

func phase2ExternalEvidenceSummary(shapeID string, externalRows []scoreboardRow, unavailable []unavailableRow) string {
	if len(externalRows) > 0 {
		return phase2EvidenceSummary("external", shapeID, externalRows)
	}
	var statuses []string
	for _, system := range []string{"SQLite FTS5", "Bleve", "Tantivy", "Lucene"} {
		if reason, _, ok := unavailableReason(system, unavailable); ok {
			statuses = append(statuses, fmt.Sprintf("%s=unavailable (%s)", system, reason))
		} else {
			statuses = append(statuses, fmt.Sprintf("%s=not_captured", system))
		}
	}
	return "no comparable external row captured; " + strings.Join(statuses, "; ")
}

func phase2SemanticsForTarget(shapeID string) []string {
	switch shapeID {
	case "single_term_common", "single_term_rare", "multi_term_and", "multi_term_or_wand":
		return []string{"TreeDB simple analyzer/BM25F weighting is not automatically equivalent to SQLite unicode61/porter, Bleve analyzers, Tantivy tokenizers, or Lucene analyzers", "explicit AND/OR query semantics must be pinned before latency ratios are parity evidence"}
	case "phrase":
		return []string{"TreeDB phrase/proximity is structured and bounded over StorePositions indexes; it is not a Lucene-compatible query DSL", "slop, stopword position gaps, and unsupported stemming/synonym expansion must be recorded per row"}
	case "hybrid_text_scalar":
		return []string{"SQLite FTS5 is not a hybrid text+scalar engine by itself", "Lucene/Tantivy/Bleve filter semantics and whether filters are pre- or post-score must be documented"}
	case "index_build_ingest", "index_size", "reopen", "maintenance_rewrite":
		return []string{"storage/build boundaries must state WAL inclusion, checkpoint/optimize/vacuum state, and retained primary-payload treatment"}
	default:
		return nil
	}
}

func phase2AnalyzerSemanticsNotes() []string {
	return []string{
		"Do not compare TreeDB BM25F rows with external BM25/BM25F rows unless analyzer, field weights, top-k, query operator, and final-document fetch boundaries are pinned.",
		"SQLite FTS5 default rows are useful embedded baselines, but they are not Lucene/Tantivy/Bleve analyzer or query-parser parity.",
		"Phrase/proximity rows must record StorePositions, slop, stopword behavior, and unsupported stemming/synonym states; unsupported states must fail closed.",
		"Candidate-generation rows must keep full-document fetch counters at zero; final-fetch rows belong in separate tables.",
	}
}

func requiresZeroDocValidation(row scoreboardRow) bool {
	lowerBoundary := strings.ToLower(row.Boundary)
	lowerName := strings.ToLower(row.Benchmark)
	candidate := strings.Contains(lowerBoundary, "no-document") || strings.Contains(lowerBoundary, "no-doc") || strings.Contains(lowerName, "no_docs") || strings.Contains(lowerName, "candidates_no_docs") || strings.Contains(lowerName, "score_only") || strings.Contains(lowerName, "blockmax_common_topk")
	if !candidate {
		return false
	}
	if row.System == "TreeDB" {
		return true
	}
	_, hasDocs := row.Metrics["docs_fetched/search"]
	_, hasFallback := row.Metrics["full_doc_fallbacks/search"]
	_, hasFailClosed := row.Metrics["fail_closed/search"]
	return hasDocs && hasFallback && hasFailClosed
}

func checkMetricZero(row scoreboardRow, metric string, check *counterValidation) {
	value, ok := row.Metrics[metric]
	if !ok {
		check.Failures = append(check.Failures, fmt.Sprintf("missing %s", metric))
		check.OK = false
		return
	}
	check.Checks = append(check.Checks, fmt.Sprintf("%s=%s", metric, formatNumber(value)))
	if value != 0 {
		check.Failures = append(check.Failures, fmt.Sprintf("%s=%s want 0", metric, formatNumber(value)))
		check.OK = false
	}
}

func checkEitherMetricZero(row scoreboardRow, names []string, check *counterValidation) {
	for _, name := range names {
		if _, ok := row.Metrics[name]; ok {
			checkMetricZero(row, name, check)
			return
		}
	}
	check.Failures = append(check.Failures, fmt.Sprintf("missing one of %s", strings.Join(names, ", ")))
	check.OK = false
}

func renderMarkdown(rep report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# TreeDB text-v2/hybrid industry comparison scoreboard\n\n")
	fmt.Fprintf(&b, "Generated: `%s`  \n", rep.GeneratedAt)
	if rep.Issue != "" {
		fmt.Fprintf(&b, "Issue: `%s`  \n", rep.Issue)
	}
	if rep.ParentIssue != "" {
		fmt.Fprintf(&b, "Parent: `%s`  \n", rep.ParentIssue)
	}
	fmt.Fprintf(&b, "Branch: `%s`  \nCommit: `%s`  \nBase: `%s` `%s`  \nGo: `%s`  \nHost: `%s`\n\n", rep.Context.Branch, rep.Context.Commit, rep.Context.BaseRef, rep.Context.BaseSHA, rep.Context.GoVersion, rep.Context.Host)

	fmt.Fprintf(&b, "## Retrieval scoreboard\n\n")
	fmt.Fprintf(&b, "| System | Modality | Dataset | Query / boundary | ns/op | ops/sec | B/op | allocs/op | Build | Storage | Counters | Source |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
	for _, row := range rep.Rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | `%s` |\n",
			escape(row.System), escape(row.Modality), escape(row.Dataset), escape(compactQueryBoundary(row)), formatFloatPtr(row.NsPerOp), formatFloatPtr(row.OpsPerSec), formatFloatPtr(row.BytesPerOp), formatFloatPtr(row.AllocsPerOp), formatSecondsPtr(row.BuildSeconds), formatStorage(row), escape(counterSummary(row)), escape(row.SourceLabel))
	}
	if len(rep.Rows) == 0 {
		fmt.Fprintf(&b, "| _none_ | | | | | | | | | | | |\n")
	}

	renderPhase2SynthesisMarkdown(&b, rep.Phase2Synthesis)

	fmt.Fprintf(&b, "\n## Zero-doc counter validation\n\n")
	fmt.Fprintf(&b, "| Source | Benchmark | OK | Checks / failures |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- |\n")
	for _, check := range rep.CounterValidations {
		details := strings.Join(check.Checks, "; ")
		if !check.OK {
			details = details + " FAIL: " + strings.Join(check.Failures, "; ")
		}
		fmt.Fprintf(&b, "| `%s` | `%s` | %t | %s |\n", escape(check.SourceLabel), escape(check.Benchmark), check.OK, escape(details))
	}
	if len(rep.CounterValidations) == 0 {
		fmt.Fprintf(&b, "| _none_ | | true | no zero-doc rows detected |\n")
	}

	if len(rep.Commands) > 0 {
		fmt.Fprintf(&b, "\n## Reproduction commands\n\n")
		for _, command := range rep.Commands {
			fmt.Fprintf(&b, "### %s\n\n```sh\n%s\n```\n\n", command.Name, command.Value)
		}
	}

	if len(rep.Unavailable) > 0 {
		fmt.Fprintf(&b, "\n## External baselines not captured in this run\n\n")
		fmt.Fprintf(&b, "| System | Reason | Source |\n| --- | --- | --- |\n")
		for _, row := range rep.Unavailable {
			fmt.Fprintf(&b, "| %s | %s | `%s` |\n", escape(row.System), escape(row.Reason), escape(row.Source))
		}
	}

	fmt.Fprintf(&b, "\n## Caveats\n\n")
	for _, caveat := range rep.Caveats {
		fmt.Fprintf(&b, "- %s\n", caveat)
	}
	if rep.Context.ContextText != "" {
		fmt.Fprintf(&b, "\n## Captured host/context\n\n```text\n%s\n```\n", rep.Context.ContextText)
	}
	return b.String()
}

func renderPhase2SynthesisMarkdown(b *strings.Builder, syn phase2Synthesis) {
	if syn.SchemaVersion == "" {
		return
	}
	fmt.Fprintf(b, "\n## Phase-2 gap classification\n\n")
	fmt.Fprintf(b, "Schema: `%s`  \n", syn.SchemaVersion)
	fmt.Fprintf(b, "Basis: %s\n\n", syn.ClassificationBasis)
	fmt.Fprintf(b, "Classification vocabulary: `ahead` and `near_parity` require comparable external evidence; `behind_but_tractable` means the feature/evidence exists but phase-2 work is still needed; `far_behind` marks a blocking parity/evidence gap and must not be read as an unmeasured performance ratio.\n\n")

	fmt.Fprintf(b, "### External baseline coverage\n\n")
	fmt.Fprintf(b, "| System | Status | Evidence | Caveat |\n")
	fmt.Fprintf(b, "| --- | --- | --- | --- |\n")
	for _, row := range syn.ExternalBaselines {
		fmt.Fprintf(b, "| %s | `%s` | %s | %s |\n", escape(row.System), escape(row.Status), escape(row.Evidence), escape(row.Caveat))
	}

	fmt.Fprintf(b, "\n### Benchmark target taxonomy\n\n")
	fmt.Fprintf(b, "| Target | Shape | Primary issue | Required rows | Required counters |\n")
	fmt.Fprintf(b, "| --- | --- | --- | --- | --- |\n")
	for _, row := range syn.BenchmarkTargets {
		fmt.Fprintf(b, "| `%s` | %s | %s | %s | %s |\n", escape(row.ShapeID), escape(row.Shape), escape(row.PrimaryIssue), escape(strings.Join(row.RequiredRows, "; ")), escape(strings.Join(row.RequiredCounters, "; ")))
	}

	fmt.Fprintf(b, "\n### Gap classification and priority order\n\n")
	fmt.Fprintf(b, "| Priority | Target | Classification | TreeDB evidence | External evidence | Required follow-up rows | Rationale |\n")
	fmt.Fprintf(b, "| ---: | --- | --- | --- | --- | --- | --- |\n")
	for _, row := range syn.GapClassifications {
		fmt.Fprintf(b, "| %d | `%s` | `%s` | %s | %s | %s | %s |\n", row.Priority, escape(row.ShapeID), escape(row.Classification), escape(row.TreeDBEvidence), escape(row.ExternalEvidence), escape(strings.Join(row.RequiredFollowUpRows, "; ")), escape(row.Rationale))
	}

	if len(syn.AnalyzerSemanticsNotes) > 0 {
		fmt.Fprintf(b, "\n### Non-equivalent analyzer/query semantics\n\n")
		for _, note := range syn.AnalyzerSemanticsNotes {
			fmt.Fprintf(b, "- %s\n", note)
		}
	}
}

func compactQueryBoundary(row scoreboardRow) string {
	parts := []string{}
	if row.QueryShape != "" {
		parts = append(parts, row.QueryShape)
	}
	if row.Boundary != "" {
		parts = append(parts, row.Boundary)
	}
	if row.Benchmark != "" {
		parts = append(parts, row.Benchmark)
	}
	return strings.Join(parts, " — ")
}

func counterSummary(row scoreboardRow) string {
	keys := []string{"docs_fetched/search", "full_doc_fallbacks/search", "fail_closed/search", "text_state_lookups/search", "state_lookups/search", "text_postings/search", "postings_scanned/search", "posting_blocks_visited/search", "posting_blocks_skipped/search", "vector_candidates/search", "vector_examined/search", "scalar_prefilter_ids/search"}
	var parts []string
	for _, key := range keys {
		if v, ok := row.Metrics[key]; ok {
			label := strings.TrimSuffix(key, "/search")
			parts = append(parts, fmt.Sprintf("%s=%s", label, formatNumber(v)))
		}
	}
	return strings.Join(parts, ", ")
}

func formatStorage(row scoreboardRow) string {
	if row.StorageBytes == nil && row.StorageBytesPerDoc == nil {
		return ""
	}
	var parts []string
	if row.StorageBytes != nil {
		parts = append(parts, bytesHuman(*row.StorageBytes))
	}
	if row.StorageBytesPerDoc != nil {
		parts = append(parts, fmt.Sprintf("%s B/doc", formatNumber(*row.StorageBytesPerDoc)))
	}
	return strings.Join(parts, ", ")
}

func sortRows(rows []scoreboardRow) {
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		for _, cmp := range []int{strings.Compare(a.Modality, b.Modality), strings.Compare(a.System, b.System), strings.Compare(a.Dataset, b.Dataset), strings.Compare(a.Benchmark, b.Benchmark)} {
			if cmp < 0 {
				return true
			}
			if cmp > 0 {
				return false
			}
		}
		return false
	})
}

func cloneMetrics(in map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func metricAny(metrics map[string]float64, names ...string) (float64, bool) {
	for _, name := range names {
		if v, ok := metrics[name]; ok {
			return v, true
		}
	}
	return 0, false
}

func intMetric(metrics map[string]float64, name string) int {
	if v, ok := metrics[name]; ok {
		return int(v)
	}
	return 0
}

func opsPerSecond(ns float64) float64 {
	if ns <= 0 {
		return 0
	}
	return 1e9 / ns
}

func floatPtr(v float64) *float64 { return &v }
func int64Ptr(v int64) *int64     { return &v }

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func numberAny(m map[string]any, names ...string) float64 {
	if v := numberPtrAny(m, names...); v != nil {
		return *v
	}
	return 0
}

func numberPtrAny(m map[string]any, names ...string) *float64 {
	for _, name := range names {
		if v, ok := number(m[name]); ok {
			return floatPtr(v)
		}
	}
	return nil
}

func int64PtrAny(m map[string]any, names ...string) *int64 {
	for _, name := range names {
		if v, ok := number(m[name]); ok {
			return int64Ptr(int64(v))
		}
	}
	return nil
}

func numberMap(v any) map[string]float64 {
	m := mapValue(v)
	out := make(map[string]float64, len(m))
	for k, raw := range m {
		if f, ok := number(raw); ok {
			out[k] = f
		}
	}
	return out
}

func mapValue(v any) map[string]any {
	if v == nil {
		return nil
	}
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return nil
}

func stringField(m map[string]any, name, fallback string) string {
	if v, ok := m[name].(string); ok && v != "" {
		return v
	}
	return fallback
}

func stringSlice(v any) []string {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func formatNumber(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return ""
	}
	if math.Abs(v-math.Round(v)) < 0.000001 {
		return strconv.FormatInt(int64(math.Round(v)), 10)
	}
	if math.Abs(v) >= 1000 {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	return strconv.FormatFloat(v, 'f', 3, 64)
}

func formatFloatPtr(v *float64) string {
	if v == nil {
		return ""
	}
	return formatNumber(*v)
}

func formatSecondsPtr(v *float64) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%ss", formatNumber(*v))
}

func bytesHuman(v int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	f := float64(v)
	unit := 0
	for f >= 1024 && unit < len(units)-1 {
		f /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%dB", v)
	}
	return fmt.Sprintf("%.2f%s", f, units[unit])
}

func escape(s string) string {
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
