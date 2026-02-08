package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
)

type config struct {
	profilesDir string
	binPath     string
	runMarkdown string
	outMarkdown string
	outJSON     string
	outHTML     string
	writeHTML   bool
	nodeCount   int
}

type report struct {
	GeneratedAt string `json:"generated_at"`
	ProfilesDir string `json:"profiles_dir"`
	Binary      string `json:"binary"`
	OpsSource   string `json:"ops_source,omitempty"`

	OpsRows      []opsRow         `json:"ops_rows,omitempty"`
	CPUProfiles  []pprofSummary   `json:"cpu_profiles,omitempty"`
	Comparisons  []scanComparison `json:"comparisons,omitempty"`
	BlockProfile *pprofSummary    `json:"block_profile,omitempty"`
	MutexProfile *pprofSummary    `json:"mutex_profile,omitempty"`

	Insights []string `json:"insights,omitempty"`
	Warnings []string `json:"warnings,omitempty"`

	InvestigationTargets []investigationTarget `json:"investigation_targets,omitempty"`
}

type opsRow struct {
	Label     string  `json:"label"`
	FullScan  float64 `json:"full_scan_ops_sec"`
	Prefix    float64 `json:"prefix_scan_ops_sec"`
	PrefixDiv float64 `json:"prefix_over_full"`
}

type pprofSummary struct {
	Kind       string       `json:"kind"` // cpu|block|mutex
	Test       string       `json:"test,omitempty"`
	DBTag      string       `json:"db_tag,omitempty"`
	Path       string       `json:"path"`
	Total      string       `json:"total,omitempty"`
	TopEntries []pprofEntry `json:"top_entries,omitempty"`
}

type pprofEntry struct {
	Flat     string  `json:"flat"`
	FlatPct  float64 `json:"flat_pct"`
	Cum      string  `json:"cum"`
	CumPct   float64 `json:"cum_pct"`
	Function string  `json:"function"`
}

type scanComparison struct {
	DBTag         string       `json:"db_tag"`
	SharedTop     []sharedHot  `json:"shared_top,omitempty"`
	PrefixOnlyTop []pprofEntry `json:"prefix_only_top,omitempty"`
	FullOnlyTop   []pprofEntry `json:"full_only_top,omitempty"`
}

type investigationTarget struct {
	DBTag     string  `json:"db_tag,omitempty"`
	Test      string  `json:"test,omitempty"`
	Category  string  `json:"category"`
	Function  string  `json:"function"`
	FlatPct   float64 `json:"flat_pct,omitempty"`
	Why       string  `json:"why,omitempty"`
	File      string  `json:"file,omitempty"`
	Line      int     `json:"line,omitempty"`
	LineHint  string  `json:"line_hint,omitempty"`
	Reference string  `json:"reference,omitempty"`
}

type sharedHot struct {
	Function      string  `json:"function"`
	FullFlatPct   float64 `json:"full_flat_pct"`
	PrefixFlatPct float64 `json:"prefix_flat_pct"`
}

type profileFiles struct {
	fullScanCPU   map[string]string
	prefixScanCPU map[string]string
	blockPath     string
	mutexPath     string
	tracePath     string
}

type benchprofResultsFile struct {
	Runs []benchprofResultsRun `json:"runs"`
}

type benchprofResultsRun struct {
	Keys    int                           `json:"keys"`
	Results map[string]map[string]float64 `json:"results"`
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchprof: %v\n", err)
		os.Exit(2)
	}

	rep, err := buildReport(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchprof: %v\n", err)
		os.Exit(1)
	}

	md := renderMarkdown(rep)
	if err := os.MkdirAll(filepath.Dir(cfg.outMarkdown), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "benchprof: create output dir: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(cfg.outMarkdown, []byte(md), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "benchprof: write markdown: %v\n", err)
		os.Exit(1)
	}

	js, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchprof: json marshal: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(cfg.outJSON, js, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "benchprof: write json: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("wrote markdown: %s\n", cfg.outMarkdown)
	fmt.Printf("wrote json:     %s\n", cfg.outJSON)

	if cfg.writeHTML {
		htmlDoc, err := markdownToHTMLDoc(md)
		if err != nil {
			fmt.Fprintf(os.Stderr, "benchprof: render html: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(cfg.outHTML, htmlDoc, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "benchprof: write html: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("wrote html:     %s\n", cfg.outHTML)
	}
}

func parseFlags() (config, error) {
	var cfg config

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.StringVar(&cfg.profilesDir, "profiles-dir", "", "Directory containing profile artifacts (required)")
	fs.StringVar(&cfg.binPath, "bin", "", "Path to benchmark binary used to generate profiles (optional; defaults to profile-only mode)")
	fs.StringVar(&cfg.runMarkdown, "run-md", "", "Path to benchmark markdown/stdout capture (optional; usually auto-discovered)")
	fs.StringVar(&cfg.outMarkdown, "out-md", "", "Output markdown report path (default <profiles-dir>/insights.md)")
	fs.StringVar(&cfg.outJSON, "out-json", "", "Output JSON report path (default <profiles-dir>/insights.json)")
	fs.StringVar(&cfg.outHTML, "out-html", "", "Output HTML report path (implies -html)")
	fs.BoolVar(&cfg.writeHTML, "html", false, "Also write an HTML report generated from markdown")
	fs.IntVar(&cfg.nodeCount, "nodecount", 25, "Top node count to request from go tool pprof")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return config{}, err
	}
	if extras := fs.Args(); len(extras) > 0 {
		return config{}, fmt.Errorf("unexpected positional args: %q", extras)
	}
	if strings.TrimSpace(cfg.profilesDir) == "" {
		return config{}, errors.New("missing required -profiles-dir")
	}
	if cfg.nodeCount <= 0 {
		return config{}, fmt.Errorf("invalid -nodecount=%d (must be > 0)", cfg.nodeCount)
	}

	cfg.profilesDir = strings.TrimSpace(cfg.profilesDir)
	cfg.binPath = strings.TrimSpace(cfg.binPath)
	cfg.runMarkdown = strings.TrimSpace(cfg.runMarkdown)
	cfg.outMarkdown = strings.TrimSpace(cfg.outMarkdown)
	cfg.outJSON = strings.TrimSpace(cfg.outJSON)
	cfg.outHTML = strings.TrimSpace(cfg.outHTML)
	if cfg.binPath == "" {
		cfg.binPath = discoverDefaultBinaryPath(cfg.profilesDir)
	}

	if cfg.outMarkdown == "" {
		cfg.outMarkdown = filepath.Join(cfg.profilesDir, "insights.md")
	}
	if cfg.outJSON == "" {
		cfg.outJSON = filepath.Join(cfg.profilesDir, "insights.json")
	}
	if cfg.outHTML != "" {
		cfg.writeHTML = true
	}
	if cfg.writeHTML && cfg.outHTML == "" {
		cfg.outHTML = deriveHTMLOutPath(cfg.outMarkdown)
	}
	return cfg, nil
}

func deriveHTMLOutPath(markdownPath string) string {
	if strings.HasSuffix(strings.ToLower(markdownPath), ".md") {
		return markdownPath[:len(markdownPath)-3] + ".html"
	}
	return markdownPath + ".html"
}

func discoverDefaultBinaryPath(profilesDir string) string {
	candidates := []string{
		filepath.Join(profilesDir, "unified-bench"),
		"./bin/unified-bench",
	}
	for _, p := range candidates {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	return ""
}

func buildReport(cfg config) (report, error) {
	rep := report{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		ProfilesDir: cfg.profilesDir,
		Binary:      cfg.binPath,
	}

	files, err := discoverProfileFiles(cfg.profilesDir)
	if err != nil {
		return report{}, err
	}
	if len(files.fullScanCPU) == 0 {
		rep.Warnings = append(rep.Warnings, "no cpu_full_scan_*.pprof profiles found")
	}
	if len(files.prefixScanCPU) == 0 {
		rep.Warnings = append(rep.Warnings, "no cpu_prefix_scan_*.pprof profiles found")
	}

	if rows, source, err := loadOpsRows(cfg); err != nil {
		rep.Warnings = append(rep.Warnings, err.Error())
	} else {
		rep.OpsRows = rows
		rep.OpsSource = source
	}
	if len(rep.OpsRows) == 0 {
		rep.Warnings = append(rep.Warnings, "no scan ops found; unified-bench -profile-dir now writes benchprof_results.json automatically")
	}

	rep.CPUProfiles, rep.Warnings = appendCPUProfiles(rep.CPUProfiles, rep.Warnings, cfg, files)
	for _, prof := range rep.CPUProfiles {
		if d := parseProfileDuration(prof.Total); d == 0 {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("no CPU samples for %s/%s: consider larger -keys or longer scan workload", prof.Test, prof.DBTag))
		} else if d < 20*time.Millisecond {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("low CPU sample duration for %s/%s (%s): consider larger -keys or longer scan workload", prof.Test, prof.DBTag, prof.Total))
		}
	}
	rep.BlockProfile, rep.Warnings = analyzeOptionalProfile("block", files.blockPath, cfg, rep.Warnings)
	rep.MutexProfile, rep.Warnings = analyzeOptionalProfile("mutex", files.mutexPath, cfg, rep.Warnings)
	if files.tracePath != "" {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("trace detected at %q (trace analysis not yet implemented)", files.tracePath))
	}

	rep.Comparisons = buildComparisons(rep.CPUProfiles)
	inferredInsights := []string(nil)
	rep.InvestigationTargets, inferredInsights = buildInvestigations(rep)
	rep.Insights = buildInsights(rep, inferredInsights)
	return rep, nil
}

func appendCPUProfiles(dst []pprofSummary, warnings []string, cfg config, files profileFiles) ([]pprofSummary, []string) {
	keys := make(map[string]struct{}, len(files.fullScanCPU)+len(files.prefixScanCPU))
	for k := range files.fullScanCPU {
		keys[k] = struct{}{}
	}
	for k := range files.prefixScanCPU {
		keys[k] = struct{}{}
	}
	dbTags := make([]string, 0, len(keys))
	for k := range keys {
		dbTags = append(dbTags, k)
	}
	sort.Strings(dbTags)

	for _, dbTag := range dbTags {
		if p := files.fullScanCPU[dbTag]; p != "" {
			s, err := analyzePprofPath("cpu", "full_scan", dbTag, p, cfg)
			if err != nil {
				warnings = append(warnings, err.Error())
			} else {
				dst = append(dst, s)
			}
		}
		if p := files.prefixScanCPU[dbTag]; p != "" {
			s, err := analyzePprofPath("cpu", "prefix_scan", dbTag, p, cfg)
			if err != nil {
				warnings = append(warnings, err.Error())
			} else {
				dst = append(dst, s)
			}
		}
	}
	return dst, warnings
}

func analyzeOptionalProfile(kind, path string, cfg config, warnings []string) (*pprofSummary, []string) {
	if path == "" {
		return nil, warnings
	}
	s, err := analyzePprofPath(kind, "", "", path, cfg)
	if err != nil {
		warnings = append(warnings, err.Error())
		return nil, warnings
	}
	return &s, warnings
}

func discoverProfileFiles(dir string) (profileFiles, error) {
	out := profileFiles{
		fullScanCPU:   map[string]string{},
		prefixScanCPU: map[string]string{},
	}

	if st, err := os.Stat(dir); err != nil {
		return out, fmt.Errorf("profiles dir: %w", err)
	} else if !st.IsDir() {
		return out, fmt.Errorf("profiles dir is not a directory: %q", dir)
	}

	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()

		switch {
		case strings.HasPrefix(name, "cpu_full_scan_") && strings.HasSuffix(name, ".pprof"):
			dbTag := strings.TrimSuffix(strings.TrimPrefix(name, "cpu_full_scan_"), ".pprof")
			out.fullScanCPU[dbTag] = path
		case strings.HasPrefix(name, "cpu_prefix_scan_") && strings.HasSuffix(name, ".pprof"):
			dbTag := strings.TrimSuffix(strings.TrimPrefix(name, "cpu_prefix_scan_"), ".pprof")
			out.prefixScanCPU[dbTag] = path
		case name == "block.pprof" && out.blockPath == "":
			out.blockPath = path
		case name == "mutex.pprof" && out.mutexPath == "":
			out.mutexPath = path
		case name == "trace.out" && out.tracePath == "":
			out.tracePath = path
		}
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("scan profiles dir %q: %w", dir, err)
	}
	return out, nil
}

func analyzePprofPath(kind, testName, dbTag, path string, cfg config) (pprofSummary, error) {
	pprofText, err := runPprofTop(cfg.binPath, path, cfg.nodeCount)
	if err != nil {
		return pprofSummary{}, fmt.Errorf("pprof (%s %s %s) %q: %w", kind, testName, dbTag, path, err)
	}
	parsed := parsePprofTopOutput(pprofText)
	return pprofSummary{
		Kind:       kind,
		Test:       testName,
		DBTag:      dbTag,
		Path:       path,
		Total:      parsed.total,
		TopEntries: parsed.entries,
	}, nil
}

func runPprofTop(binPath, profilePath string, nodeCount int) (string, error) {
	args := []string{"tool", "pprof", "-top", "-nodecount", strconv.Itoa(nodeCount)}
	if strings.TrimSpace(binPath) != "" {
		args = append(args, binPath, profilePath)
	} else {
		args = append(args, profilePath)
	}
	cmd := exec.Command("go", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

type parsedTop struct {
	total   string
	entries []pprofEntry
}

var (
	reTopTotal = regexp.MustCompile(`(?m)^Showing nodes accounting for .* of ([^ ]+) total$`)
)

func parsePprofTopOutput(text string) parsedTop {
	out := parsedTop{}
	if m := reTopTotal.FindStringSubmatch(text); len(m) == 2 {
		out.total = strings.TrimSpace(m[1])
	}

	lines := strings.Split(text, "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		if !isDurationLike(fields[0]) || !isPercent(fields[1]) || !isPercent(fields[2]) || !isDurationLike(fields[3]) || !isPercent(fields[4]) {
			continue
		}
		flatPct, err1 := parsePercent(fields[1])
		cumPct, err2 := parsePercent(fields[4])
		if err1 != nil || err2 != nil {
			continue
		}
		if flatPct <= 0 && cumPct < 1.0 {
			continue
		}
		out.entries = append(out.entries, pprofEntry{
			Flat:     fields[0],
			FlatPct:  flatPct,
			Cum:      fields[3],
			CumPct:   cumPct,
			Function: strings.Join(fields[5:], " "),
		})
	}
	return out
}

func isDurationLike(s string) bool {
	if s == "0" {
		return true
	}
	if s == "" {
		return false
	}
	// Examples: 1.23s, 53ms, 200us, 12µs
	last := s[len(s)-1]
	if (last >= '0' && last <= '9') || last == '.' {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch >= '0' && ch <= '9') || ch == '.' {
			continue
		}
		return i > 0
	}
	return false
}

func isPercent(s string) bool {
	return strings.HasSuffix(s, "%")
}

func parsePercent(s string) (float64, error) {
	s = strings.TrimSuffix(strings.TrimSpace(s), "%")
	return strconv.ParseFloat(s, 64)
}

func parseProfileDuration(total string) time.Duration {
	total = strings.TrimSpace(total)
	if total == "" || total == "0" {
		return 0
	}
	// pprof totals use Go duration units (s, ms, us, µs, ns).
	d, err := time.ParseDuration(total)
	if err != nil {
		return 0
	}
	return d
}

var (
	reScanOpsDiag = regexp.MustCompile(`(?m)^(Full Scan|Prefix Scan)\s*/\s*(.+?)\s*=\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*$`)
)

func parseScanOpsMarkdown(text string) []opsRow {
	ops := map[string]map[string]float64{
		"full_scan":   {},
		"prefix_scan": {},
	}
	for _, m := range reScanOpsDiag.FindAllStringSubmatch(text, -1) {
		if len(m) != 4 {
			continue
		}
		testKey := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(m[1]), " ", "_"))
		label := strings.TrimSpace(m[2])
		val, err := parseNumber(m[3])
		if err != nil {
			continue
		}
		if _, ok := ops[testKey]; !ok {
			continue
		}
		ops[testKey][label] = val
	}

	labelSet := map[string]struct{}{}
	for label := range ops["full_scan"] {
		labelSet[label] = struct{}{}
	}
	for label := range ops["prefix_scan"] {
		labelSet[label] = struct{}{}
	}

	labels := make([]string, 0, len(labelSet))
	for label := range labelSet {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	rows := make([]opsRow, 0, len(labels))
	for _, label := range labels {
		full := ops["full_scan"][label]
		prefix := ops["prefix_scan"][label]
		ratio := 0.0
		if full > 0 {
			ratio = prefix / full
		}
		rows = append(rows, opsRow{
			Label:     label,
			FullScan:  full,
			Prefix:    prefix,
			PrefixDiv: ratio,
		})
	}
	return rows
}

func loadOpsRows(cfg config) ([]opsRow, string, error) {
	jsonPath := filepath.Join(cfg.profilesDir, "benchprof_results.json")
	if st, err := os.Stat(jsonPath); err == nil && !st.IsDir() {
		rows, err := parseScanOpsResultsJSON(jsonPath)
		if err != nil {
			return nil, "", fmt.Errorf("parse %q: %w", jsonPath, err)
		}
		if len(rows) > 0 {
			return rows, jsonPath, nil
		}
	}

	candidates := make([]string, 0, 3)
	if strings.TrimSpace(cfg.runMarkdown) != "" {
		candidates = append(candidates, cfg.runMarkdown)
	}
	candidates = append(candidates,
		filepath.Join(cfg.profilesDir, "benchprof_results.md"),
		filepath.Join(cfg.profilesDir, "run.md"),
	)
	for _, path := range candidates {
		if strings.TrimSpace(path) == "" {
			continue
		}
		st, err := os.Stat(path)
		if err != nil || st.IsDir() {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, "", fmt.Errorf("read %q: %w", path, err)
		}
		rows := parseScanOpsMarkdown(string(data))
		if len(rows) > 0 {
			return rows, path, nil
		}
	}
	return nil, "", nil
}

func parseScanOpsResultsJSON(path string) ([]opsRow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var parsed benchprofResultsFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Runs) == 0 {
		return nil, nil
	}
	includeKeys := len(parsed.Runs) > 1
	rows := make([]opsRow, 0, len(parsed.Runs))
	for _, run := range parsed.Runs {
		full := run.Results["full_scan"]
		prefix := run.Results["prefix_scan"]
		labelSet := map[string]struct{}{}
		for label := range full {
			labelSet[label] = struct{}{}
		}
		for label := range prefix {
			labelSet[label] = struct{}{}
		}
		labels := make([]string, 0, len(labelSet))
		for label := range labelSet {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		for _, label := range labels {
			rowLabel := label
			if includeKeys {
				rowLabel = fmt.Sprintf("keys=%d / %s", run.Keys, label)
			}
			fullV := full[label]
			prefixV := prefix[label]
			ratio := 0.0
			if fullV > 0 {
				ratio = prefixV / fullV
			}
			rows = append(rows, opsRow{
				Label:     rowLabel,
				FullScan:  fullV,
				Prefix:    prefixV,
				PrefixDiv: ratio,
			})
		}
	}
	return rows, nil
}

func parseNumber(s string) (float64, error) {
	s = strings.ReplaceAll(strings.TrimSpace(s), ",", "")
	return strconv.ParseFloat(s, 64)
}

func buildComparisons(profiles []pprofSummary) []scanComparison {
	fullByDB := map[string]pprofSummary{}
	prefixByDB := map[string]pprofSummary{}
	for _, p := range profiles {
		if p.Kind != "cpu" {
			continue
		}
		switch p.Test {
		case "full_scan":
			fullByDB[p.DBTag] = p
		case "prefix_scan":
			prefixByDB[p.DBTag] = p
		}
	}

	dbSet := map[string]struct{}{}
	for k := range fullByDB {
		if _, ok := prefixByDB[k]; ok {
			dbSet[k] = struct{}{}
		}
	}
	dbTags := make([]string, 0, len(dbSet))
	for k := range dbSet {
		dbTags = append(dbTags, k)
	}
	sort.Strings(dbTags)

	out := make([]scanComparison, 0, len(dbTags))
	for _, dbTag := range dbTags {
		full := fullByDB[dbTag]
		prefix := prefixByDB[dbTag]
		if len(full.TopEntries) == 0 || len(prefix.TopEntries) == 0 {
			continue
		}
		fullDur := parseProfileDuration(full.Total)
		prefixDur := parseProfileDuration(prefix.Total)
		if (fullDur > 0 && fullDur < 20*time.Millisecond) || (prefixDur > 0 && prefixDur < 20*time.Millisecond) {
			continue
		}

		fullByFn := make(map[string]pprofEntry, len(full.TopEntries))
		prefixByFn := make(map[string]pprofEntry, len(prefix.TopEntries))
		for _, e := range full.TopEntries {
			fullByFn[e.Function] = e
		}
		for _, e := range prefix.TopEntries {
			prefixByFn[e.Function] = e
		}

		shared := make([]sharedHot, 0)
		for fn, fe := range fullByFn {
			pe, ok := prefixByFn[fn]
			if !ok {
				continue
			}
			shared = append(shared, sharedHot{
				Function:      fn,
				FullFlatPct:   fe.FlatPct,
				PrefixFlatPct: pe.FlatPct,
			})
		}
		sort.Slice(shared, func(i, j int) bool {
			a := shared[i].FullFlatPct + shared[i].PrefixFlatPct
			b := shared[j].FullFlatPct + shared[j].PrefixFlatPct
			if a == b {
				return shared[i].Function < shared[j].Function
			}
			return a > b
		})
		if len(shared) > 5 {
			shared = shared[:5]
		}

		prefixOnly := make([]pprofEntry, 0)
		for _, e := range prefix.TopEntries {
			if _, ok := fullByFn[e.Function]; ok {
				continue
			}
			prefixOnly = append(prefixOnly, e)
		}
		if len(prefixOnly) > 5 {
			prefixOnly = prefixOnly[:5]
		}

		fullOnly := make([]pprofEntry, 0)
		for _, e := range full.TopEntries {
			if _, ok := prefixByFn[e.Function]; ok {
				continue
			}
			fullOnly = append(fullOnly, e)
		}
		if len(fullOnly) > 5 {
			fullOnly = fullOnly[:5]
		}

		out = append(out, scanComparison{
			DBTag:         dbTag,
			SharedTop:     shared,
			PrefixOnlyTop: prefixOnly,
			FullOnlyTop:   fullOnly,
		})
	}
	return out
}

type sourceHint struct {
	MatchAny  []string
	File      string
	LineMatch string
	Why       string
}

var iteratorSeekPatterns = []string{
	"(*db).iterator",
	"(*tree).iterator",
	"(*iterator).seek",
	"(*hashsorted).newiterator",
	"(*hashiterator).seek",
	"(*hashiterator).refresh",
	"(*hashiterator).ensureloaded",
	"iteratorheap.less",
	"iteratorheap.pop",
	"iteratorheap.up",
	"iteratorheap.down",
	"(*mergingiterator).advance",
	"sort.search",
	"cmpbody",
}

var valueDecodePatterns = []string{
	"(*valuelogiterator).loadvalue",
	"(*db).readvaluelog",
	"readviammapappend",
	"decodepayloadappend",
}

var investigationSourceHints = []sourceHint{
	{
		MatchAny:  []string{"(*db).iterator"},
		File:      "TreeDB/caching/db.go",
		LineMatch: "func (db *DB) Iterator(start, end []byte) (merging.Iterator, error)",
		Why:       "DB-level iterator setup and snapshot rotation.",
	},
	{
		MatchAny:  []string{"(*tree).iterator"},
		File:      "TreeDB/tree/iterator.go",
		LineMatch: "func (t *Tree) Iterator(start, end []byte) iterator.UnsafeIterator",
		Why:       "Tree iterator initialization and initial seek.",
	},
	{
		MatchAny:  []string{"(*iterator).seek"},
		File:      "TreeDB/tree/iterator.go",
		LineMatch: "func (it *Iterator) seek(key []byte)",
		Why:       "Core btree seek path for range start.",
	},
	{
		MatchAny:  []string{"(*hashsorted).newiterator"},
		File:      "TreeDB/internal/memtable/hash_sorted.go",
		LineMatch: "func (m *HashSorted) NewIterator(start, end []byte) iterator.UnsafeIterator",
		Why:       "Memtable iterator creation and search bootstrap.",
	},
	{
		MatchAny:  []string{"(*hashiterator).seek"},
		File:      "TreeDB/internal/memtable/hash_sorted.go",
		LineMatch: "func (it *hashIterator) Seek(key []byte)",
		Why:       "Prefix seek inside sorted memtable key list.",
	},
	{
		MatchAny:  []string{"(*hashiterator).refresh"},
		File:      "TreeDB/internal/memtable/hash_sorted.go",
		LineMatch: "func (it *hashIterator) refresh()",
		Why:       "Iterator validity and bound checks on each move.",
	},
	{
		MatchAny:  []string{"(*hashiterator).ensureloaded"},
		File:      "TreeDB/internal/memtable/hash_sorted.go",
		LineMatch: "func (it *hashIterator) ensureLoaded()",
		Why:       "Loads entries from memtable map for current key.",
	},
	{
		MatchAny:  []string{"sort.search"},
		File:      "TreeDB/internal/memtable/hash_sorted.go",
		LineMatch: "sort.SearchStrings(",
		Why:       "Binary search callsite for iterator seeks.",
	},
	{
		MatchAny:  []string{"cmpbody"},
		File:      "TreeDB/internal/merging/merging.go",
		LineMatch: "cmp := bytes.Compare(h[i].key, h[j].key)",
		Why:       "Key-compare callsite used in merge heap ordering.",
	},
	{
		MatchAny:  []string{"iteratorheap.less"},
		File:      "TreeDB/internal/merging/merging.go",
		LineMatch: "func (h iteratorHeap) Less(i, j int) bool",
		Why:       "Heap comparator in merging iterator.",
	},
	{
		MatchAny:  []string{"iteratorheap.pop"},
		File:      "TreeDB/internal/merging/merging.go",
		LineMatch: "func (h *iteratorHeap) pop() heapItem",
		Why:       "Heap pop in merging iterator advance path.",
	},
	{
		MatchAny:  []string{"iteratorheap.up"},
		File:      "TreeDB/internal/merging/merging.go",
		LineMatch: "func (h *iteratorHeap) up(j int)",
		Why:       "Heap up-heap operation after push.",
	},
	{
		MatchAny:  []string{"iteratorheap.down"},
		File:      "TreeDB/internal/merging/merging.go",
		LineMatch: "func (h *iteratorHeap) down(i0, n int) bool",
		Why:       "Heap down-heap operation after pop/heapify.",
	},
	{
		MatchAny:  []string{"(*mergingiterator).advance"},
		File:      "TreeDB/internal/merging/merging.go",
		LineMatch: "func (mi *MergingIterator) advance()",
		Why:       "Main merge loop and shadow-key skipping.",
	},
	{
		MatchAny:  []string{"(*valuelogiterator).loadvalue"},
		File:      "TreeDB/caching/value_log_iterator.go",
		LineMatch: "func (it *valueLogIterator) loadValue()",
		Why:       "Value pointer materialization path.",
	},
	{
		MatchAny:  []string{"(*db).readvaluelog"},
		File:      "TreeDB/caching/db.go",
		LineMatch: "func (db *DB) readValueLog(ptr page.ValuePtr) ([]byte, error)",
		Why:       "DB wrapper around value-log reads.",
	},
	{
		MatchAny:  []string{"readviammapappend"},
		File:      "TreeDB/internal/valuelog/reader_mmap.go",
		LineMatch: "func (f *File) readViaMmapAppend(ptr page.ValuePtr, verifyCRC bool, dst []byte) ([]byte, error, bool)",
		Why:       "Value-log mmap read/validate path.",
	},
}

func buildInvestigations(rep report) ([]investigationTarget, []string) {
	if len(rep.CPUProfiles) == 0 {
		return nil, nil
	}

	prefixByDB := map[string]pprofSummary{}
	for _, prof := range rep.CPUProfiles {
		if prof.Kind == "cpu" && prof.Test == "prefix_scan" {
			prefixByDB[prof.DBTag] = prof
		}
	}
	if len(prefixByDB) == 0 {
		return nil, nil
	}

	prefixSlower := hasAnyPrefixSlowerRow(rep.OpsRows)
	dbTags := make([]string, 0, len(prefixByDB))
	for dbTag := range prefixByDB {
		dbTags = append(dbTags, dbTag)
	}
	sort.Strings(dbTags)

	lineCache := make(map[string]int, len(investigationSourceHints))
	targets := make([]investigationTarget, 0, 12)
	inferred := make([]string, 0, len(dbTags))
	seen := map[string]struct{}{}

	for _, dbTag := range dbTags {
		prof := prefixByDB[dbTag]
		if len(prof.TopEntries) == 0 {
			continue
		}

		iteratorPct := sumFlatPct(prof.TopEntries, iteratorSeekPatterns)
		valueDecodePct := sumFlatPct(prof.TopEntries, valueDecodePatterns)
		iterEntries := matchingEntries(prof.TopEntries, iteratorSeekPatterns)
		if len(iterEntries) == 0 {
			continue
		}

		// Signal: prefix workload and CPU point to iterator/seek path, not value decode.
		if prefixSlower && (iteratorPct >= valueDecodePct+2.0) && len(iterEntries) >= 2 {
			inferred = append(inferred,
				fmt.Sprintf("%s: prefix_scan CPU is weighted toward iterator/seek work (%.2f%% flat) vs value decoding (%.2f%% flat). That points to iterator setup/seek overhead, not value decoding.",
					dbTag, iteratorPct, valueDecodePct))
		}

		limit := len(iterEntries)
		if limit > 8 {
			limit = 8
		}
		for i := 0; i < limit; i++ {
			entry := iterEntries[i]
			target := investigationTarget{
				DBTag:    dbTag,
				Test:     "prefix_scan",
				Category: "iterator_setup_seek",
				Function: entry.Function,
				FlatPct:  entry.FlatPct,
				Why:      "Prefix-scan hotspot in iterator/seek path.",
			}
			if file, line, hint, ok := resolveSourceHint(entry.Function, lineCache); ok {
				target.File = file
				target.Line = line
				target.LineHint = hint
				target.Reference = sourceRef(file, line)
				if target.Why == "" {
					target.Why = hint
				}
			}
			keyRef := target.Reference
			if keyRef == "" {
				keyRef = target.Function
			}
			key := target.DBTag + "|" + target.Test + "|" + target.Category + "|" + keyRef
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			targets = append(targets, target)
		}
	}

	return targets, inferred
}

func hasAnyPrefixSlowerRow(rows []opsRow) bool {
	for _, row := range rows {
		if row.FullScan > 0 && row.Prefix > 0 && row.PrefixDiv > 0 && row.PrefixDiv < 0.95 {
			return true
		}
	}
	return false
}

func sumFlatPct(entries []pprofEntry, patterns []string) float64 {
	var total float64
	for _, e := range entries {
		if matchesAnyPattern(e.Function, patterns) {
			total += e.FlatPct
		}
	}
	return total
}

func matchingEntries(entries []pprofEntry, patterns []string) []pprofEntry {
	out := make([]pprofEntry, 0, len(entries))
	for _, e := range entries {
		if e.FlatPct <= 0 {
			continue
		}
		if matchesAnyPattern(e.Function, patterns) {
			out = append(out, e)
		}
	}
	return out
}

func matchesAnyPattern(function string, patterns []string) bool {
	fn := strings.ToLower(function)
	for _, p := range patterns {
		if strings.Contains(fn, p) {
			return true
		}
	}
	return false
}

func resolveSourceHint(function string, lineCache map[string]int) (string, int, string, bool) {
	for _, hint := range investigationSourceHints {
		if !matchesAnyPattern(function, hint.MatchAny) {
			continue
		}
		line := findLineInFileCached(hint.File, hint.LineMatch, lineCache)
		return hint.File, line, hint.Why, true
	}
	return "", 0, "", false
}

func findLineInFileCached(path, lineMatch string, cache map[string]int) int {
	cacheKey := path + "|" + lineMatch
	if line, ok := cache[cacheKey]; ok {
		return line
	}
	line := findLineInFile(path, lineMatch)
	cache[cacheKey] = line
	return line
}

func findLineInFile(path, lineMatch string) int {
	readPath := resolveReadablePath(path)
	data, err := os.ReadFile(readPath)
	if err != nil {
		return 0
	}
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		if strings.Contains(line, lineMatch) {
			return i + 1
		}
	}
	return 0
}

func resolveReadablePath(path string) string {
	if strings.TrimSpace(path) == "" {
		return path
	}
	if st, err := os.Stat(path); err == nil && !st.IsDir() {
		return path
	}
	cwd, err := os.Getwd()
	if err != nil {
		return path
	}
	dir := cwd
	for i := 0; i < 8; i++ {
		candidate := filepath.Join(dir, path)
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return path
}

func sourceRef(file string, line int) string {
	if strings.TrimSpace(file) == "" {
		return ""
	}
	if line > 0 {
		return fmt.Sprintf("%s:%d", file, line)
	}
	return file
}

func buildInsights(rep report, inferredInsights []string) []string {
	insights := make([]string, 0, 12)

	for _, row := range rep.OpsRows {
		if row.FullScan <= 0 || row.Prefix <= 0 {
			continue
		}
		if row.PrefixDiv >= 1.10 {
			insights = append(insights, fmt.Sprintf("%s: prefix_scan is %.2fx faster than full_scan (%.0f vs %.0f ops/s).", row.Label, row.PrefixDiv, row.Prefix, row.FullScan))
		} else if row.PrefixDiv <= 0.90 {
			insights = append(insights, fmt.Sprintf("%s: prefix_scan is %.2fx slower than full_scan (%.0f vs %.0f ops/s).", row.Label, row.PrefixDiv, row.Prefix, row.FullScan))
		} else {
			insights = append(insights, fmt.Sprintf("%s: prefix_scan and full_scan are close (ratio %.2fx).", row.Label, row.PrefixDiv))
		}
	}

	for _, cmp := range rep.Comparisons {
		if len(cmp.SharedTop) > 0 {
			top := cmp.SharedTop[0]
			insights = append(insights, fmt.Sprintf("%s: shared hotspot %q (full %.2f%% flat, prefix %.2f%% flat).", cmp.DBTag, top.Function, top.FullFlatPct, top.PrefixFlatPct))
		}
		if len(cmp.PrefixOnlyTop) > 0 {
			top := cmp.PrefixOnlyTop[0]
			insights = append(insights, fmt.Sprintf("%s: prefix_scan-specific hotspot %q (%.2f%% flat).", cmp.DBTag, top.Function, top.FlatPct))
		}
		if len(cmp.FullOnlyTop) > 0 {
			top := cmp.FullOnlyTop[0]
			insights = append(insights, fmt.Sprintf("%s: full_scan-specific hotspot %q (%.2f%% flat).", cmp.DBTag, top.Function, top.FlatPct))
		}
	}

	if rep.BlockProfile != nil && len(rep.BlockProfile.TopEntries) > 0 {
		top := rep.BlockProfile.TopEntries[0]
		insights = append(insights, fmt.Sprintf("Block contention hotspot: %q (%.2f%% flat).", top.Function, top.FlatPct))
	}
	if rep.MutexProfile != nil && len(rep.MutexProfile.TopEntries) > 0 {
		top := rep.MutexProfile.TopEntries[0]
		insights = append(insights, fmt.Sprintf("Mutex contention hotspot: %q (%.2f%% flat).", top.Function, top.FlatPct))
	}

	for _, in := range inferredInsights {
		insights = append(insights, in)
	}

	if len(insights) == 0 {
		insights = append(insights, "No strong insights found. Ensure cpu_full_scan_*.pprof and cpu_prefix_scan_*.pprof files are present.")
	}
	return insights
}

func renderMarkdown(rep report) string {
	var sb strings.Builder
	sb.WriteString("# Bench Profile Insights\n\n")
	sb.WriteString(fmt.Sprintf("- generated: `%s`\n", rep.GeneratedAt))
	sb.WriteString(fmt.Sprintf("- profiles dir: `%s`\n", rep.ProfilesDir))
	if strings.TrimSpace(rep.Binary) != "" {
		sb.WriteString(fmt.Sprintf("- binary: `%s`\n", rep.Binary))
	} else {
		sb.WriteString("- binary: `(profile-only mode)`\n")
	}
	if strings.TrimSpace(rep.OpsSource) != "" {
		sb.WriteString(fmt.Sprintf("- ops source: `%s`\n", rep.OpsSource))
	}
	sb.WriteString("\n")

	if len(rep.OpsRows) > 0 {
		sb.WriteString("## Scan Ops/Sec\n\n")
		sb.WriteString("| Label | Full Scan | Prefix Scan | Prefix / Full |\n")
		sb.WriteString("|---|---:|---:|---:|\n")
		for _, row := range rep.OpsRows {
			ratio := "-"
			if row.FullScan > 0 {
				ratio = fmt.Sprintf("%.2fx", row.PrefixDiv)
			}
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
				row.Label,
				formatOps(row.FullScan),
				formatOps(row.Prefix),
				ratio,
			))
		}
		sb.WriteString("\n")
	}

	if len(rep.CPUProfiles) > 0 {
		sb.WriteString("## CPU Hotspots\n\n")
		for _, prof := range rep.CPUProfiles {
			title := prof.Path
			if prof.Test != "" {
				title = fmt.Sprintf("%s (%s)", prof.Test, prof.DBTag)
			}
			sb.WriteString(fmt.Sprintf("### %s\n\n", title))
			sb.WriteString(fmt.Sprintf("- source: `%s`\n", prof.Path))
			if prof.Total != "" {
				sb.WriteString(fmt.Sprintf("- total samples: `%s`\n", prof.Total))
			}
			sb.WriteString("\n")
			if len(prof.TopEntries) == 0 {
				sb.WriteString("_No entries parsed._\n\n")
				continue
			}
			sb.WriteString("| flat | flat% | cum | cum% | function |\n")
			sb.WriteString("|---:|---:|---:|---:|---|\n")
			for _, e := range prof.TopEntries {
				sb.WriteString(fmt.Sprintf("| %s | %.2f%% | %s | %.2f%% | `%s` |\n", e.Flat, e.FlatPct, e.Cum, e.CumPct, e.Function))
			}
			sb.WriteString("\n")
		}
	}

	if len(rep.Comparisons) > 0 {
		sb.WriteString("## Full vs Prefix Comparison\n\n")
		for _, cmp := range rep.Comparisons {
			sb.WriteString(fmt.Sprintf("### %s\n\n", cmp.DBTag))

			sb.WriteString("- shared hotspots:\n")
			if len(cmp.SharedTop) == 0 {
				sb.WriteString("  - (none)\n")
			} else {
				for _, e := range cmp.SharedTop {
					sb.WriteString(fmt.Sprintf("  - `%s` (full %.2f%%, prefix %.2f%%)\n", e.Function, e.FullFlatPct, e.PrefixFlatPct))
				}
			}
			sb.WriteString("- prefix-only hotspots:\n")
			if len(cmp.PrefixOnlyTop) == 0 {
				sb.WriteString("  - (none)\n")
			} else {
				for _, e := range cmp.PrefixOnlyTop {
					sb.WriteString(fmt.Sprintf("  - `%s` (%.2f%% flat)\n", e.Function, e.FlatPct))
				}
			}
			sb.WriteString("- full-only hotspots:\n")
			if len(cmp.FullOnlyTop) == 0 {
				sb.WriteString("  - (none)\n")
			} else {
				for _, e := range cmp.FullOnlyTop {
					sb.WriteString(fmt.Sprintf("  - `%s` (%.2f%% flat)\n", e.Function, e.FlatPct))
				}
			}
			sb.WriteString("\n")
		}
	}

	if rep.BlockProfile != nil || rep.MutexProfile != nil {
		sb.WriteString("## Contention Profiles\n\n")
		if rep.BlockProfile != nil {
			writeOneProfileMarkdown(&sb, "block", *rep.BlockProfile)
		}
		if rep.MutexProfile != nil {
			writeOneProfileMarkdown(&sb, "mutex", *rep.MutexProfile)
		}
	}

	if len(rep.Insights) > 0 {
		sb.WriteString("## Key Insights\n\n")
		for _, in := range rep.Insights {
			sb.WriteString("- ")
			sb.WriteString(in)
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}

	if len(rep.InvestigationTargets) > 0 {
		sb.WriteString("## Investigation Targets\n\n")
		sb.WriteString("| db tag | test | category | function | flat% | code | why |\n")
		sb.WriteString("|---|---|---|---|---:|---|---|\n")
		for _, t := range rep.InvestigationTargets {
			codeRef := "-"
			if t.Reference != "" {
				codeRef = fmt.Sprintf("`%s`", t.Reference)
			}
			why := t.Why
			if strings.TrimSpace(t.LineHint) != "" {
				if why != "" {
					why += " "
				}
				why += t.LineHint
			}
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | `%s` | %.2f%% | %s | %s |\n",
				t.DBTag,
				t.Test,
				t.Category,
				t.Function,
				t.FlatPct,
				codeRef,
				escapePipe(why),
			))
		}
		sb.WriteString("\n")
	}

	if len(rep.Warnings) > 0 {
		sb.WriteString("## Warnings\n\n")
		for _, w := range rep.Warnings {
			sb.WriteString("- ")
			sb.WriteString(w)
			sb.WriteString("\n")
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

func writeOneProfileMarkdown(sb *strings.Builder, title string, prof pprofSummary) {
	sb.WriteString(fmt.Sprintf("### %s\n\n", title))
	sb.WriteString(fmt.Sprintf("- source: `%s`\n", prof.Path))
	if prof.Total != "" {
		sb.WriteString(fmt.Sprintf("- total samples: `%s`\n", prof.Total))
	}
	sb.WriteString("\n")
	if len(prof.TopEntries) == 0 {
		sb.WriteString("_No entries parsed._\n\n")
		return
	}
	sb.WriteString("| flat | flat% | cum | cum% | function |\n")
	sb.WriteString("|---:|---:|---:|---:|---|\n")
	for _, e := range prof.TopEntries {
		sb.WriteString(fmt.Sprintf("| %s | %.2f%% | %s | %.2f%% | `%s` |\n", e.Flat, e.FlatPct, e.Cum, e.CumPct, e.Function))
	}
	sb.WriteString("\n")
}

func escapePipe(s string) string {
	if s == "" {
		return ""
	}
	return strings.ReplaceAll(s, "|", "\\|")
}

func formatOps(v float64) string {
	if v <= 0 {
		return "-"
	}
	return formatCommasInt(int64(math.Round(v)))
}

func formatCommasInt(n int64) string {
	neg := n < 0
	if neg {
		n = -n
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		if neg {
			return "-" + s
		}
		return s
	}
	var out []byte
	prefix := len(s) % 3
	if prefix > 0 {
		out = append(out, s[:prefix]...)
		if len(s) > prefix {
			out = append(out, ',')
		}
	}
	for i := prefix; i < len(s); i += 3 {
		out = append(out, s[i:i+3]...)
		if i+3 < len(s) {
			out = append(out, ',')
		}
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}

func markdownToHTMLDoc(markdown string) ([]byte, error) {
	md := goldmark.New(
		goldmark.WithExtensions(extension.GFM),
	)
	var body bytes.Buffer
	if err := md.Convert([]byte(markdown), &body); err != nil {
		return nil, err
	}

	const pageTmpl = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Bench Profile Insights</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; margin: 2rem auto; max-width: 1100px; line-height: 1.5; padding: 0 1rem; color: #111; }
    code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    pre { overflow-x: auto; background: #f6f8fa; padding: 0.8rem; border-radius: 6px; }
    table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
    th, td { border: 1px solid #d0d7de; padding: 0.4rem 0.6rem; text-align: left; vertical-align: top; }
    th { background: #f6f8fa; }
  </style>
</head>
<body>
{{.Body}}
</body>
</html>`
	tmpl, err := template.New("page").Parse(pageTmpl)
	if err != nil {
		return nil, err
	}

	var doc bytes.Buffer
	err = tmpl.Execute(&doc, struct {
		Body template.HTML
	}{
		Body: template.HTML(body.String()),
	})
	if err != nil {
		return nil, err
	}
	return doc.Bytes(), nil
}
