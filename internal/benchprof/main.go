package benchprof

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
	nodeCount   int
}

type report struct {
	GeneratedAt string `json:"generated_at"`
	ProfilesDir string `json:"profiles_dir"`
	Binary      string `json:"binary"`
	OpsSource   string `json:"ops_source,omitempty"`

	OpsRows             []opsRow                      `json:"ops_rows,omitempty"`
	CPUProfiles         []pprofSummary                `json:"cpu_profiles,omitempty"`
	AllocSpace          []pprofSummary                `json:"alloc_space_profiles,omitempty"`
	AllocObjects        []pprofSummary                `json:"alloc_object_profiles,omitempty"`
	BlockProfiles       []pprofSummary                `json:"block_profiles,omitempty"`
	MutexProfiles       []pprofSummary                `json:"mutex_profiles,omitempty"`
	Comparisons         []scanComparison              `json:"comparisons,omitempty"`
	BlockProfile        *pprofSummary                 `json:"block_profile,omitempty"`
	MutexProfile        *pprofSummary                 `json:"mutex_profile,omitempty"`
	TreeDBStats         []treeDBStatsRun              `json:"treedb_stats,omitempty"`
	CollectionWorkloads []benchprofCollectionWorkload `json:"collection_workloads,omitempty"`

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
	Kind       string       `json:"kind"` // cpu|checkpoint_cpu|alloc_space|alloc_objects|block|mutex
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

type treeDBStatsRun struct {
	Keys   int               `json:"keys,omitempty"`
	DBName string            `json:"db_name"`
	Stats  map[string]string `json:"stats"`
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

type cpuProfileFile struct {
	Kind  string
	Test  string
	DBTag string
	Path  string
}

type profileFiles struct {
	cpuProfiles   []cpuProfileFile
	allocs        []cpuProfileFile
	blockProfiles []cpuProfileFile
	mutexProfiles []cpuProfileFile
	blockPath     string
	mutexPath     string
	tracePath     string
}

type benchprofResultsFile struct {
	Runs []benchprofResultsRun `json:"runs"`
}

type benchprofResultsRun struct {
	Keys                int                           `json:"keys"`
	Results             map[string]map[string]float64 `json:"results"`
	TreeDBStats         map[string]map[string]string  `json:"treedb_stats,omitempty"`
	CollectionWorkloads []benchprofCollectionWorkload `json:"collection_workloads,omitempty"`
}

type benchprofCollectionWorkload struct {
	Suite                 string                              `json:"suite"`
	Mode                  string                              `json:"mode"`
	Workload              string                              `json:"workload"`
	Rows                  int                                 `json:"rows"`
	SemanticEquivalent    bool                                `json:"semantic_equivalent"`
	SemanticNote          string                              `json:"semantic_note,omitempty"`
	CorrectnessValidated  bool                                `json:"correctness_validated"`
	RowsPerSecond         float64                             `json:"rows_per_second,omitempty"`
	QueriesPerSecond      float64                             `json:"queries_per_second,omitempty"`
	MatchesPerSecond      float64                             `json:"matches_per_second,omitempty"`
	OpsPerSecond          float64                             `json:"ops_per_second,omitempty"`
	NsPerOp               float64                             `json:"ns_per_op,omitempty"`
	BytesPerOp            float64                             `json:"bytes_per_op,omitempty"`
	AllocsPerOp           float64                             `json:"allocs_per_op,omitempty"`
	DBTotalBytes          uint64                              `json:"db_total_bytes,omitempty"`
	TypedRowAssetBytes    int64                               `json:"typed_row_asset_bytes,omitempty"`
	TypedColumnAssetBytes int64                               `json:"typed_column_asset_bytes,omitempty"`
	Counters              benchprofCollectionWorkloadCounters `json:"counters,omitempty"`
}

type benchprofCollectionWorkloadCounters struct {
	MappedBytes                  uint64 `json:"mapped_bytes,omitempty"`
	HeapCopyBytes                uint64 `json:"heap_copy_bytes,omitempty"`
	DecodedBytes                 uint64 `json:"decoded_bytes,omitempty"`
	DocumentMaterializations     int64  `json:"document_materializations,omitempty"`
	DocumentReconstructions      int64  `json:"document_reconstructions,omitempty"`
	RowMaterializations          int64  `json:"row_materializations,omitempty"`
	RowLocatorDecodes            int64  `json:"row_locator_decodes,omitempty"`
	PhysicalRowAssetReads        int64  `json:"physical_row_asset_reads,omitempty"`
	PhysicalRowIDLookups         int64  `json:"physical_row_id_lookups,omitempty"`
	TypedColumnPartsConsidered   int64  `json:"typed_column_parts_considered,omitempty"`
	TypedColumnPartsPruned       int64  `json:"typed_column_parts_pruned,omitempty"`
	TypedColumnPartsDecoded      int64  `json:"typed_column_parts_decoded,omitempty"`
	TypedColumnBlocksConsidered  int64  `json:"typed_column_blocks_considered,omitempty"`
	TypedColumnBlocksPruned      int64  `json:"typed_column_blocks_pruned,omitempty"`
	TypedColumnBlocksDecoded     int64  `json:"typed_column_blocks_decoded,omitempty"`
	DirectTypedColumnAssetReads  int64  `json:"direct_typed_column_asset_reads,omitempty"`
	AssetOpenMapChecksumReads    int64  `json:"asset_open_map_checksum_reads,omitempty"`
	SegmentFileCacheHits         uint64 `json:"segment_file_cache_hits,omitempty"`
	SegmentFileCacheMisses       uint64 `json:"segment_file_cache_misses,omitempty"`
	VectorCandidates             uint64 `json:"vector_candidates,omitempty"`
	VectorEdges                  uint64 `json:"vector_edges,omitempty"`
	VectorDirectViews            uint64 `json:"vector_direct_views,omitempty"`
	VectorScratchDecodes         uint64 `json:"vector_scratch_decodes,omitempty"`
	VectorDocumentsFetched       uint64 `json:"vector_documents_fetched,omitempty"`
	VectorTypedColumnMappedBytes uint64 `json:"vector_typed_column_mapped_bytes,omitempty"`
	VectorTypedColumnHeapBytes   uint64 `json:"vector_typed_column_heap_copy_bytes,omitempty"`
	VectorTypedColumnDecoded     uint64 `json:"vector_typed_column_decoded_bytes,omitempty"`
}

func RunFromProfilesDir(profilesDir string) error {
	return RunFromArgs([]string{"-profiles-dir", profilesDir})
}

func RunFromArgs(args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	return runFromConfig(cfg)
}

func runFromConfig(cfg config) error {
	rep, err := buildReport(cfg)
	if err != nil {
		return err
	}

	md := renderMarkdown(rep)
	if err := os.MkdirAll(filepath.Dir(cfg.outMarkdown), 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	if err := os.WriteFile(cfg.outMarkdown, []byte(md), 0o644); err != nil {
		return fmt.Errorf("write markdown: %w", err)
	}

	js, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("json marshal: %w", err)
	}
	if err := os.WriteFile(cfg.outJSON, js, 0o644); err != nil {
		return fmt.Errorf("write json: %w", err)
	}

	fmt.Printf("wrote markdown: %s\n", cfg.outMarkdown)
	fmt.Printf("wrote json:     %s\n", cfg.outJSON)

	htmlDoc, err := markdownToHTMLDoc(md)
	if err != nil {
		return fmt.Errorf("render html: %w", err)
	}
	if err := os.WriteFile(cfg.outHTML, htmlDoc, 0o644); err != nil {
		return fmt.Errorf("write html: %w", err)
	}
	fmt.Printf("wrote html:     %s\n", cfg.outHTML)
	return nil
}

func parseFlags(args []string) (config, error) {
	var cfg config

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.StringVar(&cfg.profilesDir, "profiles-dir", "", "Directory containing profile artifacts (required)")
	fs.StringVar(&cfg.binPath, "bin", "", "Path to benchmark binary used to generate profiles (optional; defaults to profile-only mode)")
	fs.StringVar(&cfg.runMarkdown, "run-md", "", "Path to benchmark markdown/stdout capture (optional; usually auto-discovered)")
	fs.StringVar(&cfg.outMarkdown, "out-md", "", "Output markdown report path (default <profiles-dir>/insights.md)")
	fs.StringVar(&cfg.outJSON, "out-json", "", "Output JSON report path (default <profiles-dir>/insights.json)")
	fs.StringVar(&cfg.outHTML, "out-html", "", "Output HTML report path (default <profiles-dir>/insights.html)")
	fs.IntVar(&cfg.nodeCount, "nodecount", 25, "Top node count to request from go tool pprof")
	fs.SetOutput(os.Stderr)
	if err := fs.Parse(args); err != nil {
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
	if cfg.outHTML == "" {
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

	knownTests := loadKnownTests(cfg.profilesDir)
	files, err := discoverProfileFiles(cfg.profilesDir, knownTests)
	if err != nil {
		return report{}, err
	}
	if len(files.cpuProfiles) == 0 {
		rep.Warnings = append(rep.Warnings, "no cpu_*.pprof profiles found")
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
	if stats, err := loadTreeDBStatsMetadata(cfg.profilesDir); err != nil {
		rep.Warnings = append(rep.Warnings, err.Error())
	} else {
		rep.TreeDBStats = stats
	}
	if workloads, err := loadCollectionWorkloadMetadata(cfg.profilesDir); err != nil {
		rep.Warnings = append(rep.Warnings, err.Error())
	} else {
		rep.CollectionWorkloads = workloads
	}

	rep.CPUProfiles, rep.Warnings = appendCPUProfiles(rep.CPUProfiles, rep.Warnings, cfg, files)
	rep.AllocSpace, rep.AllocObjects, rep.Warnings = appendAllocsProfiles(rep.AllocSpace, rep.AllocObjects, rep.Warnings, cfg, files)
	rep.BlockProfiles, rep.Warnings = appendContentionProfiles("block", rep.BlockProfiles, rep.Warnings, cfg, files.blockProfiles)
	rep.MutexProfiles, rep.Warnings = appendContentionProfiles("mutex", rep.MutexProfiles, rep.Warnings, cfg, files.mutexProfiles)
	for _, prof := range rep.CPUProfiles {
		label := profileName(prof)
		if d := parseProfileDuration(prof.Total); d == 0 {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("no CPU samples for %s: consider larger -keys or longer workload", label))
		} else if d < 20*time.Millisecond {
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("low CPU sample duration for %s (%s): consider larger -keys or longer workload", label, prof.Total))
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
	profiles := append([]cpuProfileFile(nil), files.cpuProfiles...)
	sort.Slice(profiles, func(i, j int) bool {
		a := profiles[i]
		b := profiles[j]
		if a.Test != b.Test {
			return a.Test < b.Test
		}
		if a.DBTag != b.DBTag {
			return a.DBTag < b.DBTag
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Path < b.Path
	})

	for _, prof := range profiles {
		s, err := analyzePprofPath("cpu", prof.Test, prof.DBTag, prof.Path, "", cfg)
		if err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		dst = append(dst, s)
	}
	return dst, warnings
}

func appendAllocsProfiles(spaceDst, objectsDst []pprofSummary, warnings []string, cfg config, files profileFiles) ([]pprofSummary, []pprofSummary, []string) {
	profiles := append([]cpuProfileFile(nil), files.allocs...)
	sort.Slice(profiles, func(i, j int) bool {
		a := profiles[i]
		b := profiles[j]
		if a.Test != b.Test {
			return a.Test < b.Test
		}
		if a.DBTag != b.DBTag {
			return a.DBTag < b.DBTag
		}
		return a.Path < b.Path
	})

	for _, prof := range profiles {
		space, err := analyzePprofPath("alloc_space", prof.Test, prof.DBTag, prof.Path, "alloc_space", cfg)
		if err != nil {
			warnings = append(warnings, err.Error())
		} else {
			spaceDst = append(spaceDst, space)
		}
		objects, err := analyzePprofPath("alloc_objects", prof.Test, prof.DBTag, prof.Path, "alloc_objects", cfg)
		if err != nil {
			warnings = append(warnings, err.Error())
		} else {
			objectsDst = append(objectsDst, objects)
		}
	}
	return spaceDst, objectsDst, warnings
}

func appendContentionProfiles(kind string, dst []pprofSummary, warnings []string, cfg config, profiles []cpuProfileFile) ([]pprofSummary, []string) {
	items := append([]cpuProfileFile(nil), profiles...)
	sort.Slice(items, func(i, j int) bool {
		a := items[i]
		b := items[j]
		if a.Test != b.Test {
			return a.Test < b.Test
		}
		if a.DBTag != b.DBTag {
			return a.DBTag < b.DBTag
		}
		return a.Path < b.Path
	})

	for _, prof := range items {
		s, err := analyzePprofPath(kind, prof.Test, prof.DBTag, prof.Path, "", cfg)
		if err != nil {
			warnings = append(warnings, err.Error())
			continue
		}
		dst = append(dst, s)
	}
	return dst, warnings
}

func analyzeOptionalProfile(kind, path string, cfg config, warnings []string) (*pprofSummary, []string) {
	if path == "" {
		return nil, warnings
	}
	s, err := analyzePprofPath(kind, "", "", path, "", cfg)
	if err != nil {
		warnings = append(warnings, err.Error())
		return nil, warnings
	}
	return &s, warnings
}

func discoverProfileFiles(dir string, knownTests map[string]struct{}) (profileFiles, error) {
	out := profileFiles{}

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
		case name == "block.pprof" && out.blockPath == "":
			out.blockPath = path
		case name == "mutex.pprof" && out.mutexPath == "":
			out.mutexPath = path
		case name == "trace.out" && out.tracePath == "":
			out.tracePath = path
		default:
			if cpu, ok := parseCPUProfileFilename(name, knownTests); ok {
				cpu.Path = path
				out.cpuProfiles = append(out.cpuProfiles, cpu)
			} else if alloc, ok := parseAllocsProfileFilename(name, knownTests); ok {
				alloc.Path = path
				out.allocs = append(out.allocs, alloc)
			} else if block, ok := parseContentionProfileFilename(name, "block", knownTests); ok {
				block.Path = path
				out.blockProfiles = append(out.blockProfiles, block)
			} else if mutex, ok := parseContentionProfileFilename(name, "mutex", knownTests); ok {
				mutex.Path = path
				out.mutexProfiles = append(out.mutexProfiles, mutex)
			}
		}
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("scan profiles dir %q: %w", dir, err)
	}
	return out, nil
}

func parseCPUProfileFilename(name string, knownTests map[string]struct{}) (cpuProfileFile, bool) {
	if !strings.HasSuffix(name, ".pprof") {
		return cpuProfileFile{}, false
	}

	stem := strings.TrimSuffix(name, ".pprof")
	switch {
	case strings.HasPrefix(stem, "cpu_"):
		tail := strings.TrimPrefix(stem, "cpu_")
		testName, dbTag := splitProfileTail(tail, knownTests)
		if testName == "" {
			return cpuProfileFile{}, false
		}
		return cpuProfileFile{
			Kind:  "cpu",
			Test:  testName,
			DBTag: dbTag,
		}, true
	case strings.HasPrefix(stem, "checkpoint_cpu_checkpoint_"):
		tail := strings.TrimPrefix(stem, "checkpoint_cpu_checkpoint_")
		testName, dbTag := splitProfileTail(tail, knownTests)
		if testName == "" {
			return cpuProfileFile{}, false
		}
		return cpuProfileFile{
			Kind:  "checkpoint_cpu",
			Test:  "checkpoint/" + testName,
			DBTag: dbTag,
		}, true
	default:
		return cpuProfileFile{}, false
	}
}

func parseAllocsProfileFilename(name string, knownTests map[string]struct{}) (cpuProfileFile, bool) {
	if !strings.HasSuffix(name, ".pprof") {
		return cpuProfileFile{}, false
	}
	stem := strings.TrimSuffix(name, ".pprof")
	if !strings.HasPrefix(stem, "allocs_") {
		return cpuProfileFile{}, false
	}
	tail := strings.TrimPrefix(stem, "allocs_")
	testName, dbTag := splitProfileTail(tail, knownTests)
	if testName == "" {
		return cpuProfileFile{}, false
	}
	return cpuProfileFile{
		Kind:  "allocs",
		Test:  testName,
		DBTag: dbTag,
	}, true
}

func parseContentionProfileFilename(name, kind string, knownTests map[string]struct{}) (cpuProfileFile, bool) {
	if !strings.HasSuffix(name, ".pprof") {
		return cpuProfileFile{}, false
	}
	stem := strings.TrimSuffix(name, ".pprof")
	prefix := strings.TrimSpace(kind) + "_"
	if !strings.HasPrefix(stem, prefix) {
		return cpuProfileFile{}, false
	}
	tail := strings.TrimPrefix(stem, prefix)
	testName, dbTag := splitProfileTail(tail, knownTests)
	if testName == "" {
		return cpuProfileFile{}, false
	}
	return cpuProfileFile{
		Kind:  kind,
		Test:  testName,
		DBTag: dbTag,
	}, true
}

func splitProfileTail(tail string, knownTests map[string]struct{}) (testName, dbTag string) {
	tail = strings.TrimSpace(tail)
	if tail == "" {
		return "", ""
	}

	longestTest := ""
	for test := range knownTests {
		if test == "" {
			continue
		}
		prefix := test + "_"
		if strings.HasPrefix(tail, prefix) && len(test) > len(longestTest) && len(tail) > len(prefix) {
			longestTest = test
		}
	}
	if longestTest != "" {
		return longestTest, tail[len(longestTest)+1:]
	}

	// Fallback when no known test list is available: split on the first underscore.
	if idx := strings.IndexByte(tail, '_'); idx > 0 && idx < len(tail)-1 {
		return tail[:idx], tail[idx+1:]
	}
	return tail, ""
}

func loadKnownTests(profilesDir string) map[string]struct{} {
	tests := make(map[string]struct{})
	path := filepath.Join(strings.TrimSpace(profilesDir), "benchprof_results.json")
	st, err := os.Stat(path)
	if err != nil || st.IsDir() {
		return tests
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return tests
	}
	var parsed benchprofResultsFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		return tests
	}
	for _, run := range parsed.Runs {
		for testName := range run.Results {
			if strings.TrimSpace(testName) == "" {
				continue
			}
			tests[testName] = struct{}{}
		}
	}
	return tests
}

func loadCollectionWorkloadMetadata(profilesDir string) ([]benchprofCollectionWorkload, error) {
	path := filepath.Join(strings.TrimSpace(profilesDir), "benchprof_results.json")
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if st.IsDir() {
		return nil, fmt.Errorf("stat %q: expected file, got directory", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	var parsed benchprofResultsFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}
	var out []benchprofCollectionWorkload
	for _, run := range parsed.Runs {
		out = append(out, run.CollectionWorkloads...)
	}
	if len(out) == 0 {
		return nil, nil
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Suite != out[j].Suite {
			return out[i].Suite < out[j].Suite
		}
		if out[i].Mode != out[j].Mode {
			return out[i].Mode < out[j].Mode
		}
		return out[i].Workload < out[j].Workload
	})
	return out, nil
}

func loadTreeDBStatsMetadata(profilesDir string) ([]treeDBStatsRun, error) {
	path := filepath.Join(strings.TrimSpace(profilesDir), "benchprof_results.json")
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if st.IsDir() {
		return nil, fmt.Errorf("stat %q: expected file, got directory", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	var parsed benchprofResultsFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}
	out := make([]treeDBStatsRun, 0)
	for _, run := range parsed.Runs {
		dbNames := make([]string, 0, len(run.TreeDBStats))
		for dbName, stats := range run.TreeDBStats {
			if len(stats) == 0 {
				continue
			}
			dbNames = append(dbNames, dbName)
		}
		sort.Strings(dbNames)
		for _, dbName := range dbNames {
			stats := run.TreeDBStats[dbName]
			copyStats := make(map[string]string, len(stats))
			for key, value := range stats {
				copyStats[key] = value
			}
			out = append(out, treeDBStatsRun{
				Keys:   run.Keys,
				DBName: dbName,
				Stats:  copyStats,
			})
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func analyzePprofPath(kind, testName, dbTag, path, sampleIndex string, cfg config) (pprofSummary, error) {
	pprofText, err := runPprofTop(cfg.binPath, path, cfg.nodeCount, sampleIndex)
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

func runPprofTop(binPath, profilePath string, nodeCount int, sampleIndex string) (string, error) {
	args := []string{"tool", "pprof", "-top", "-nodecount", strconv.Itoa(nodeCount)}
	if strings.TrimSpace(sampleIndex) != "" {
		args = append(args, "-sample_index", sampleIndex)
	}
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
		if !isPercent(fields[1]) || !isPercent(fields[2]) || !isPercent(fields[4]) {
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

type themeDef struct {
	Key               string
	Label             string
	Keywords          []string
	InvestigationHint string
}

const (
	themeIterator = "iterator_seek_compare"
	themeDecode   = "decode_read_io"
	themeWrite    = "write_delete_flush"
	themeMapHash  = "map_hash_lookup"
	themeAlloc    = "alloc_copy_gc"
	themeLocking  = "locking_scheduling"
	themeRuntime  = "runtime_syscall"
	themeOther    = "other"
)

var themeDefinitions = []themeDef{
	{
		Key:               themeIterator,
		Label:             "iterator/seek/compare overhead",
		Keywords:          []string{"iterator", "seek", "search", "range", "scan", "cmp", "compare", "iteratorheap", "next"},
		InvestigationHint: "Focus on iterator construction, seek boundaries, and key-comparison loops.",
	},
	{
		Key:               themeDecode,
		Label:             "value decode/read I/O",
		Keywords:          []string{"decode", "encode", "read", "mmap", "vlog", "valuelog", "crc", "checksum", "compress"},
		InvestigationHint: "Focus on value materialization, read amplification, and decode/verification work.",
	},
	{
		Key:               themeWrite,
		Label:             "write/delete/flush path",
		Keywords:          []string{"write", "delete", "batch", "flush", "checkpoint", "commit", "compact", "append", "put", "set", "update"},
		InvestigationHint: "Focus on mutation hot loops, flush cadence, and checkpoint/compaction boundaries.",
	},
	{
		Key:               themeMapHash,
		Label:             "map/hash lookup",
		Keywords:          []string{"mapaccess", "mapassign", "mapiter", "hash", "lookup"},
		InvestigationHint: "Focus on key-shape and hash-table lookup/update costs.",
	},
	{
		Key:               themeAlloc,
		Label:             "allocation/copy/gc",
		Keywords:          []string{"alloc", "malloc", "newobject", "makeslice", "growslice", "memmove", "memclr", "copystack", "scanobject"},
		InvestigationHint: "Focus on allocation pressure, buffer growth, and copy-heavy code paths.",
	},
	{
		Key:               themeLocking,
		Label:             "locking/scheduling contention",
		Keywords:          []string{"mutex", "rwmutex", "lock", "unlock", "sema", "selectgo", "chan", "park", "cond"},
		InvestigationHint: "Focus on lock scope, contention points, and channel/select coordination.",
	},
	{
		Key:               themeRuntime,
		Label:             "runtime/syscall overhead",
		Keywords:          []string{"runtime.", "syscall.", "internal/poll", "madvise", "futex"},
		InvestigationHint: "Focus on page-fault behavior, syscall frequency, and runtime scheduling effects.",
	},
}

type sourceLocation struct {
	File string
	Line int
}

type sourceResolver struct {
	modulePath string
	byFunc     map[string][]sourceLocation
	byMethod   map[string][]sourceLocation
}

var (
	reFuncDecl     = regexp.MustCompile(`^\s*func\s*(\(([^)]*)\)\s*)?([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
	reMethodSymbol = regexp.MustCompile(`\(\*?([A-Za-z_][A-Za-z0-9_]*)\)\.([A-Za-z_][A-Za-z0-9_]*)`)
	reInlineSuffix = regexp.MustCompile(`\s+\(inline\)$`)
	reFuncN        = regexp.MustCompile(`\.func[0-9]+$`)
	reLastIdent    = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)$`)
)

func buildInvestigations(rep report) ([]investigationTarget, []string) {
	profiles := make([]pprofSummary, 0, len(rep.CPUProfiles)+len(rep.AllocSpace)+len(rep.AllocObjects)+len(rep.BlockProfiles)+len(rep.MutexProfiles))
	profiles = append(profiles, rep.CPUProfiles...)
	profiles = append(profiles, rep.AllocSpace...)
	profiles = append(profiles, rep.AllocObjects...)
	profiles = append(profiles, rep.BlockProfiles...)
	profiles = append(profiles, rep.MutexProfiles...)
	if len(profiles) == 0 {
		return nil, nil
	}

	sort.Slice(profiles, func(i, j int) bool {
		a := profiles[i]
		b := profiles[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Test != b.Test {
			return a.Test < b.Test
		}
		if a.DBTag != b.DBTag {
			return a.DBTag < b.DBTag
		}
		return a.Path < b.Path
	})

	prefixSlower := hasAnyPrefixSlowerRow(rep.OpsRows)
	resolver := newSourceResolver()

	targets := make([]investigationTarget, 0, len(profiles)*5)
	inferred := make([]string, 0, len(profiles)*2)
	seen := map[string]struct{}{}

	for _, prof := range profiles {
		if len(prof.TopEntries) == 0 {
			continue
		}
		themes := themeBreakdown(prof.TopEntries)
		dominantTheme, dominantPct := dominantTheme(themes)
		if dominantTheme != themeOther && dominantPct >= 12.0 {
			inferred = append(inferred, fmt.Sprintf("%s: dominant theme is %s (%.2f%% flat across top nodes).",
				profileName(prof), themeLabel(dominantTheme), dominantPct))
		}

		top := prof.TopEntries[0]
		switch prof.Kind {
		case "alloc_space":
			inferred = append(inferred, fmt.Sprintf("%s: top allocator by bytes is %q (%s flat, %.2f%%).",
				profileName(prof), top.Function, top.Flat, top.FlatPct))
		case "alloc_objects":
			inferred = append(inferred, fmt.Sprintf("%s: top allocator by count is %q (%s flat, %.2f%%).",
				profileName(prof), top.Function, top.Flat, top.FlatPct))
		}
		if top.FlatPct >= 35 {
			inferred = append(inferred, fmt.Sprintf("%s: top hotspot %q is %.2f%% flat; this section is highly concentrated in one function.",
				profileName(prof), top.Function, top.FlatPct))
		}

		iteratorPct := themes[themeIterator]
		decodePct := themes[themeDecode]
		if prof.Kind == "cpu" && prefixSlower && strings.HasPrefix(strings.ToLower(prof.Test), "prefix_scan") && iteratorPct >= decodePct+2.0 {
			inferred = append(inferred,
				fmt.Sprintf("%s: prefix_scan CPU is weighted toward iterator/seek work (%.2f%% flat) vs value decoding (%.2f%% flat). That points to iterator setup/seek overhead, not value decoding.",
					prof.DBTag, iteratorPct, decodePct))
		}

		limit := len(prof.TopEntries)
		if limit > 8 {
			limit = 8
		}
		for i := 0; i < limit; i++ {
			entry := prof.TopEntries[i]
			if entry.FlatPct <= 0 {
				continue
			}
			theme := classifyFunctionTheme(entry.Function)
			target := investigationTarget{
				DBTag:    prof.DBTag,
				Test:     prof.Test,
				Category: theme,
				Function: entry.Function,
				FlatPct:  entry.FlatPct,
				Why:      themeHint(theme),
			}

			if resolver != nil {
				if loc, ok := resolver.Resolve(entry.Function); ok {
					target.File = loc.File
					target.Line = loc.Line
					target.Reference = sourceRef(loc.File, loc.Line)
				}
			}

			keyRef := target.Reference
			if keyRef == "" {
				keyRef = target.Function
			}
			key := prof.Kind + "|" + target.DBTag + "|" + target.Test + "|" + target.Category + "|" + keyRef
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			targets = append(targets, target)
		}
	}

	return targets, inferred
}

func themeBreakdown(entries []pprofEntry) map[string]float64 {
	out := make(map[string]float64, len(themeDefinitions)+1)
	for _, e := range entries {
		if e.FlatPct <= 0 {
			continue
		}
		out[classifyFunctionTheme(e.Function)] += e.FlatPct
	}
	return out
}

func dominantTheme(themes map[string]float64) (string, float64) {
	bestKey := themeOther
	best := 0.0
	for key, val := range themes {
		if val > best {
			bestKey = key
			best = val
		}
	}
	if best <= 0 {
		return themeOther, 0
	}
	return bestKey, best
}

func classifyFunctionTheme(function string) string {
	fn := strings.ToLower(strings.TrimSpace(function))
	if fn == "" {
		return themeOther
	}

	bestKey := themeOther
	bestScore := 0
	for _, def := range themeDefinitions {
		score := 0
		for _, kw := range def.Keywords {
			if strings.Contains(fn, kw) {
				score++
			}
		}
		if score > bestScore {
			bestScore = score
			bestKey = def.Key
		}
	}
	if bestScore == 0 {
		return themeOther
	}
	return bestKey
}

func themeLabel(key string) string {
	for _, def := range themeDefinitions {
		if def.Key == key {
			return def.Label
		}
	}
	return "mixed/other"
}

func themeHint(key string) string {
	for _, def := range themeDefinitions {
		if def.Key == key {
			return def.InvestigationHint
		}
	}
	return "Inspect this symbol and its callers in the section-specific hot path."
}

func hasAnyPrefixSlowerRow(rows []opsRow) bool {
	for _, row := range rows {
		if row.FullScan > 0 && row.Prefix > 0 && row.PrefixDiv > 0 && row.PrefixDiv < 0.95 {
			return true
		}
	}
	return false
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

func profileName(prof pprofSummary) string {
	prefix := ""
	if strings.TrimSpace(prof.Kind) != "" && prof.Kind != "cpu" {
		prefix = prof.Kind + "/"
	}
	if strings.TrimSpace(prof.DBTag) == "" {
		return prefix + prof.Test
	}
	if strings.TrimSpace(prof.Test) == "" {
		return prefix + prof.DBTag
	}
	return prefix + prof.Test + "/" + prof.DBTag
}

func newSourceResolver() *sourceResolver {
	cwd, err := os.Getwd()
	if err != nil {
		return nil
	}
	root, modulePath := findModuleRoot(cwd)
	if root == "" {
		return nil
	}
	res := &sourceResolver{
		modulePath: modulePath,
		byFunc:     make(map[string][]sourceLocation, 1024),
		byMethod:   make(map[string][]sourceLocation, 1024),
	}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if d.IsDir() {
			name := d.Name()
			switch name {
			case ".git", "vendor", "bin", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			rel = path
		}
		rel = filepath.ToSlash(rel)
		lines := strings.Split(string(data), "\n")
		for i, line := range lines {
			m := reFuncDecl.FindStringSubmatch(line)
			if len(m) != 4 {
				continue
			}
			name := strings.ToLower(strings.TrimSpace(m[3]))
			if name == "" {
				continue
			}
			loc := sourceLocation{File: rel, Line: i + 1}
			res.byFunc[name] = append(res.byFunc[name], loc)
			recvType := parseReceiverType(m[2])
			if recvType != "" {
				key := strings.ToLower(recvType + "." + name)
				res.byMethod[key] = append(res.byMethod[key], loc)
			}
		}
		return nil
	})
	return res
}

func parseReceiverType(recv string) string {
	recv = strings.TrimSpace(recv)
	if recv == "" {
		return ""
	}
	fields := strings.Fields(recv)
	if len(fields) == 0 {
		return ""
	}
	tok := fields[len(fields)-1]
	tok = strings.TrimPrefix(tok, "*")
	if idx := strings.Index(tok, "["); idx >= 0 {
		tok = tok[:idx]
	}
	if idx := strings.LastIndex(tok, "."); idx >= 0 {
		tok = tok[idx+1:]
	}
	if !reLastIdent.MatchString(tok) {
		return ""
	}
	return tok
}

func (r *sourceResolver) Resolve(symbol string) (sourceLocation, bool) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return sourceLocation{}, false
	}

	pkgHint := packageHintFromSymbol(symbol, r.modulePath)
	candidates := make([]sourceLocation, 0, 4)
	if recv, method := receiverMethodFromSymbol(symbol); recv != "" && method != "" {
		key := strings.ToLower(recv + "." + method)
		candidates = append(candidates, r.byMethod[key]...)
	}
	if len(candidates) == 0 {
		if name := functionNameFromSymbol(symbol); name != "" {
			candidates = append(candidates, r.byFunc[strings.ToLower(name)]...)
		}
	}
	if len(candidates) == 0 {
		return sourceLocation{}, false
	}
	return bestSourceLocation(candidates, pkgHint), true
}

func receiverMethodFromSymbol(symbol string) (string, string) {
	m := reMethodSymbol.FindStringSubmatch(symbol)
	if len(m) != 3 {
		return "", ""
	}
	return m[1], m[2]
}

func functionNameFromSymbol(symbol string) string {
	s := strings.TrimSpace(symbol)
	s = reInlineSuffix.ReplaceAllString(s, "")
	s = reFuncN.ReplaceAllString(s, "")
	if idx := strings.LastIndex(s, "."); idx >= 0 && idx < len(s)-1 {
		s = s[idx+1:]
	}
	m := reLastIdent.FindStringSubmatch(s)
	if len(m) == 2 {
		return m[1]
	}
	return ""
}

func packageHintFromSymbol(symbol, modulePath string) string {
	s := strings.TrimSpace(symbol)
	s = reInlineSuffix.ReplaceAllString(s, "")
	cut := s
	if idx := strings.Index(cut, ".("); idx >= 0 {
		cut = cut[:idx]
	} else if idx := strings.LastIndex(cut, "."); idx >= 0 {
		cut = cut[:idx]
	}
	if modulePath != "" {
		prefix := modulePath + "/"
		if strings.HasPrefix(cut, prefix) {
			cut = strings.TrimPrefix(cut, prefix)
		}
	}
	// For symbols like ".../merging.iteratorHeap", keep only package path.
	if idx := strings.Index(cut, "."); idx >= 0 {
		cut = cut[:idx]
	}
	return strings.ToLower(strings.Trim(cut, "/"))
}

func bestSourceLocation(candidates []sourceLocation, pkgHint string) sourceLocation {
	if len(candidates) == 1 {
		return candidates[0]
	}
	best := candidates[0]
	bestScore := scoreSourceLocation(best, pkgHint)
	for _, c := range candidates[1:] {
		score := scoreSourceLocation(c, pkgHint)
		if score > bestScore {
			best = c
			bestScore = score
			continue
		}
		if score == bestScore && len(c.File) < len(best.File) {
			best = c
		}
	}
	return best
}

func scoreSourceLocation(loc sourceLocation, pkgHint string) int {
	score := 0
	file := strings.ToLower(filepath.ToSlash(loc.File))
	if pkgHint != "" && strings.Contains(file, pkgHint) {
		score += 20
	}
	if strings.Contains(file, "/treedb/") || strings.HasPrefix(file, "treedb/") {
		score += 1
	}
	return score
}

func findModuleRoot(start string) (string, string) {
	dir := start
	for {
		goMod := filepath.Join(dir, "go.mod")
		if st, err := os.Stat(goMod); err == nil && !st.IsDir() {
			modulePath := readModulePath(goMod)
			return dir, modulePath
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", ""
}

func readModulePath(goModPath string) string {
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return ""
	}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module "))
		}
	}
	return ""
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

	perTestContention := make([]pprofSummary, 0, len(rep.BlockProfiles)+len(rep.MutexProfiles))
	perTestContention = append(perTestContention, rep.BlockProfiles...)
	perTestContention = append(perTestContention, rep.MutexProfiles...)
	sort.Slice(perTestContention, func(i, j int) bool {
		a := perTestContention[i]
		b := perTestContention[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Test != b.Test {
			return a.Test < b.Test
		}
		if a.DBTag != b.DBTag {
			return a.DBTag < b.DBTag
		}
		return a.Path < b.Path
	})
	for _, prof := range perTestContention {
		if len(prof.TopEntries) == 0 {
			continue
		}
		top := prof.TopEntries[0]
		insights = append(insights, fmt.Sprintf("%s: contention hotspot %q (%.2f%% flat).", profileName(prof), top.Function, top.FlatPct))
	}

	if rep.BlockProfile != nil && len(rep.BlockProfile.TopEntries) > 0 {
		top := rep.BlockProfile.TopEntries[0]
		insights = append(insights, fmt.Sprintf("Global block contention hotspot: %q (%.2f%% flat).", top.Function, top.FlatPct))
	}
	if rep.MutexProfile != nil && len(rep.MutexProfile.TopEntries) > 0 {
		top := rep.MutexProfile.TopEntries[0]
		insights = append(insights, fmt.Sprintf("Global mutex contention hotspot: %q (%.2f%% flat).", top.Function, top.FlatPct))
	}

	for _, in := range inferredInsights {
		insights = append(insights, in)
	}

	if len(insights) == 0 {
		insights = append(insights, "No strong insights found. Ensure cpu_*.pprof files are present and contain enough samples.")
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

	if len(rep.AllocSpace) > 0 || len(rep.AllocObjects) > 0 {
		sb.WriteString("## Allocation Hotspots\n\n")
		allocProfiles := make([]pprofSummary, 0, len(rep.AllocSpace)+len(rep.AllocObjects))
		allocProfiles = append(allocProfiles, rep.AllocSpace...)
		allocProfiles = append(allocProfiles, rep.AllocObjects...)
		sort.Slice(allocProfiles, func(i, j int) bool {
			a := allocProfiles[i]
			b := allocProfiles[j]
			if a.Test != b.Test {
				return a.Test < b.Test
			}
			if a.DBTag != b.DBTag {
				return a.DBTag < b.DBTag
			}
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			return a.Path < b.Path
		})
		for _, prof := range allocProfiles {
			title := fmt.Sprintf("%s (%s/%s)", prof.Test, prof.DBTag, prof.Kind)
			sb.WriteString(fmt.Sprintf("### %s\n\n", title))
			sb.WriteString(fmt.Sprintf("- source: `%s`\n", prof.Path))
			if prof.Total != "" {
				sb.WriteString(fmt.Sprintf("- total: `%s`\n", prof.Total))
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

	if len(rep.BlockProfiles) > 0 || len(rep.MutexProfiles) > 0 {
		sb.WriteString("## Contention Profiles (Per Test)\n\n")
		contentionProfiles := make([]pprofSummary, 0, len(rep.BlockProfiles)+len(rep.MutexProfiles))
		contentionProfiles = append(contentionProfiles, rep.BlockProfiles...)
		contentionProfiles = append(contentionProfiles, rep.MutexProfiles...)
		sort.Slice(contentionProfiles, func(i, j int) bool {
			a := contentionProfiles[i]
			b := contentionProfiles[j]
			if a.Test != b.Test {
				return a.Test < b.Test
			}
			if a.DBTag != b.DBTag {
				return a.DBTag < b.DBTag
			}
			if a.Kind != b.Kind {
				return a.Kind < b.Kind
			}
			return a.Path < b.Path
		})
		for _, prof := range contentionProfiles {
			title := fmt.Sprintf("%s (%s/%s)", prof.Test, prof.DBTag, prof.Kind)
			writeOneProfileMarkdown(&sb, title, prof)
		}
	}

	if rep.BlockProfile != nil || rep.MutexProfile != nil {
		sb.WriteString("## Contention Profiles (Global)\n\n")
		if rep.BlockProfile != nil {
			writeOneProfileMarkdown(&sb, "block", *rep.BlockProfile)
		}
		if rep.MutexProfile != nil {
			writeOneProfileMarkdown(&sb, "mutex", *rep.MutexProfile)
		}
	}

	if len(rep.TreeDBStats) > 0 {
		sb.WriteString("## TreeDB Stats Metadata\n\n")
		for _, run := range rep.TreeDBStats {
			title := run.DBName
			if run.Keys > 0 {
				title = fmt.Sprintf("keys=%d / %s", run.Keys, run.DBName)
			}
			sb.WriteString(fmt.Sprintf("### %s\n\n", title))
			sb.WriteString("| stat | value |\n")
			sb.WriteString("|---|---:|\n")
			keys := make([]string, 0, len(run.Stats))
			for key := range run.Stats {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				sb.WriteString(fmt.Sprintf("| `%s` | `%s` |\n", key, run.Stats[key]))
			}
			sb.WriteString("\n")
		}
	}

	if len(rep.CollectionWorkloads) > 0 {
		sb.WriteString("## Collection Workload Metadata\n\n")
		sb.WriteString("| suite | mode | workload | rows/s | queries/s | matches/s | ns/op | B/op | allocs/op | row asset bytes | column asset bytes | correctness | semantic | note |\n")
		sb.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|\n")
		for _, w := range rep.CollectionWorkloads {
			correctness := "no"
			if w.CorrectnessValidated {
				correctness = "yes"
			}
			semantic := "no"
			if w.SemanticEquivalent {
				semantic = "yes"
			}
			sb.WriteString(fmt.Sprintf("| `%s` | `%s` | `%s` | %s | %s | %s | %s | %s | %s | %d | %d | %s | %s | %s |\n",
				w.Suite,
				w.Mode,
				w.Workload,
				formatOptionalMetric(w.RowsPerSecond, 3),
				formatOptionalMetric(w.QueriesPerSecond, 3),
				formatOptionalMetric(w.MatchesPerSecond, 3),
				formatOptionalMetric(w.NsPerOp, 1),
				formatOptionalMetric(w.BytesPerOp, 1),
				formatOptionalMetric(w.AllocsPerOp, 3),
				w.TypedRowAssetBytes,
				w.TypedColumnAssetBytes,
				correctness,
				semantic,
				escapePipe(w.SemanticNote),
			))
		}
		sb.WriteString("\n")
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

func formatOptionalMetric(v float64, precision int) string {
	if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return "-"
	}
	return strconv.FormatFloat(v, 'f', precision, 64)
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
