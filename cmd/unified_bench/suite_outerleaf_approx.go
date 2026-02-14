package main

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	treedbadapter "github.com/snissn/gomap/kvstore/adapters/treedb"
)

var (
	outerLeafApproxQueries             = flag.Int("outerleaf-approx-queries", 5000, "outerleaf_approx suite: random lookup samples for p95 proxy")
	outerLeafApproxMaxKeys             = flag.Int("outerleaf-approx-max-keys", 200000, "outerleaf_approx suite: max keys to collect from dataset for approximation")
	outerLeafApproxValueSizes          = flag.String("outerleaf-approx-value-sizes", "128,1024", "outerleaf_approx suite: comma-separated value sizes for matrix runs")
	outerLeafApproxBlockCacheMB        = flag.Int("outerleaf-approx-block-cache-mb", 32, "outerleaf_approx suite: simulated outer block-cache size in MiB (0 disables cache)")
	outerLeafApproxFenceFPR            = flag.Float64("outerleaf-approx-fence-fpr", 0.01, "outerleaf_approx suite: simulated fence-index false-positive rate [0,1]")
	outerLeafApproxWALBytesPerRecord   = flag.Int("outerleaf-approx-wal-bytes-per-record", 24, "outerleaf_approx suite: modeled WAL metadata bytes per appended record")
	outerLeafApproxVlogRecordOverhead  = flag.Int("outerleaf-approx-vlog-record-overhead-bytes", outerLeafApproxRecordOverhead, "outerleaf_approx suite: modeled value-log per-record overhead bytes")
	outerLeafApproxGateBytesReduction  = flag.Float64("outerleaf-approx-gate-bytes-reduction", 0.20, "outerleaf_approx suite: minimum estimated total bytes reduction gate (fraction)")
	outerLeafApproxGateLookupSlowdown  = flag.Float64("outerleaf-approx-gate-lookup-slowdown", 0.15, "outerleaf_approx suite: maximum lookup p95 slowdown gate (fraction)")
	outerLeafApproxGateWriteRegression = flag.Float64("outerleaf-approx-gate-write-regression", 0.10, "outerleaf_approx suite: maximum write-throughput regression gate (fraction)")
	outerLeafApproxGateWAIncrease      = flag.Float64("outerleaf-approx-gate-wa-increase", 0.10, "outerleaf_approx suite: maximum write-amplification increase gate (fraction)")
	outerLeafApproxReportJSON          = flag.String("outerleaf-approx-report-json", "", "outerleaf_approx suite: optional path to write JSON report")
)

const (
	outerLeafApproxDefaultBlockTarget = 16 << 10
	outerLeafApproxRecordOverhead     = 4 + 1 + 1 + 2 + 8 + 4 // valuelog header bytes
)

var outerLeafApproxWorkloads = []outerLeafApproxWorkloadSpec{
	{
		Name:       "random_write_parallel",
		TestsArg:   "random_write_parallel",
		MetricTest: "random_write_parallel",
	},
	{
		Name:       "random_read",
		TestsArg:   "random_read",
		MetricTest: "random_read",
	},
	{
		Name:       "prefix_scan",
		TestsArg:   "prefix_scan",
		MetricTest: "prefix_scan",
	},
	{
		Name:                "churn_settle",
		TestsArg:            "random_write_parallel,random_delete,full_scan",
		MetricTest:          "random_delete",
		SecondaryMetricTest: "full_scan",
	},
}

type outerLeafApproxWorkloadSpec struct {
	Name                string
	TestsArg            string
	MetricTest          string
	SecondaryMetricTest string
}

type outerLeafApproxSuiteConfig struct {
	Queries             int
	MaxKeys             int
	ValueSizes          []int
	BlockTargetBytes    int
	BlockCacheBytes     int64
	FenceFPR            float64
	WALBytesPerRecord   int
	VlogRecordOverhead  int
	GateBytesReduction  float64
	GateLookupSlowdown  float64
	GateWriteRegression float64
	GateWAIncrease      float64
	ReportJSONPath      string
	Codecs              []treedb.ValueLogBlockCodec
}

type outerLeafApproxScenarioRun struct {
	WorkloadName string
	TestsArg     string
	MetricTest   string
	ValueSize    int
	Dir          string
	Wall         time.Duration
	WorkloadOps  float64
	SecondaryOps float64

	MainIndexBytes uint64
	MainWALBytes   uint64
	MainValueBytes uint64

	Keys   [][]byte
	Values [][]byte
}

type outerLeafApproxBlock struct {
	firstKey   []byte
	start      int
	end        int
	rawLen     int
	stored     []byte
	compressed bool
}

type outerLeafApproxValuePayload struct {
	rawLen     int
	stored     []byte
	compressed bool
}

type outerLeafApproxBaselinePayloadBuild struct {
	payloads      []outerLeafApproxValuePayload
	payloadBytes  uint64
	writeProxyOps float64
}

type outerLeafApproxLookupStats struct {
	P95              time.Duration
	HitP95           time.Duration
	MissP95          time.Duration
	CacheHits        int
	CacheMisses      int
	FenceFalsePos    int
	FallbackSearches int
}

type outerLeafApproxLookupPattern struct {
	CacheHits        int
	CacheMisses      int
	FenceFalsePos    int
	FallbackSearches int
}

type outerLeafApproxWriteAmpMetrics struct {
	LogicalBytes       uint64
	BaselineVlogBytes  uint64
	BaselineWALBytes   uint64
	BaselineWATotal    uint64
	BaselineWA         float64
	OuterVlogBytes     uint64
	OuterWALBytes      uint64
	OuterWATotal       uint64
	OuterWA            float64
	WAIncreaseFraction float64
	BaselineWALActual  uint64
}

type outerLeafApproxGates struct {
	Bytes  bool `json:"bytes"`
	Lookup bool `json:"lookup"`
	Write  bool `json:"write"`
	WA     bool `json:"wa"`
	All    bool `json:"all"`
}

type outerLeafApproxCaseResult struct {
	Workload  string `json:"workload"`
	ValueSize int    `json:"value_size"`
	Codec     string `json:"codec"`

	TestsArg     string  `json:"tests_arg"`
	MetricTest   string  `json:"metric_test"`
	WorkloadOps  float64 `json:"workload_ops_sec"`
	SecondaryOps float64 `json:"secondary_ops_sec,omitempty"`
	WallMillis   int64   `json:"wall_ms"`

	KeyCount int `json:"key_count"`

	BaselineIndexBytes uint64 `json:"baseline_index_bytes"`
	ApproxIndexBytes   uint64 `json:"approx_index_bytes"`

	BaselineVlogPayloadBytes uint64 `json:"baseline_vlog_payload_bytes"`
	BaselineVlogMetaBytes    uint64 `json:"baseline_vlog_meta_bytes"`
	BaselineVlogBytes        uint64 `json:"baseline_vlog_bytes"`
	ApproxVlogPayloadBytes   uint64 `json:"approx_vlog_payload_bytes"`
	ApproxVlogMetaBytes      uint64 `json:"approx_vlog_meta_bytes"`
	ApproxVlogBytes          uint64 `json:"approx_vlog_bytes"`

	BaselineTotalBytes uint64 `json:"baseline_total_bytes"`
	ApproxTotalBytes   uint64 `json:"approx_total_bytes"`

	LeafPagesBaseline int `json:"leaf_pages_baseline"`
	LeafPagesFence    int `json:"leaf_pages_fence"`
	BlockCount        int `json:"block_count"`

	BytesReductionFraction float64 `json:"bytes_reduction_fraction"`

	LookupQueries          int     `json:"lookup_queries"`
	LookupP95BaselineNanos int64   `json:"lookup_p95_baseline_ns"`
	LookupP95OuterNanos    int64   `json:"lookup_p95_outer_ns"`
	LookupP95HitNanos      int64   `json:"lookup_p95_hit_ns"`
	LookupP95MissNanos     int64   `json:"lookup_p95_miss_ns"`
	LookupSlowdownFraction float64 `json:"lookup_slowdown_fraction"`
	CacheHits              int     `json:"cache_hits"`
	CacheMisses            int     `json:"cache_misses"`
	FenceFalsePositives    int     `json:"fence_false_positives"`
	FallbackSearches       int     `json:"fallback_searches"`

	WriteProxyOpsBaseline   float64 `json:"write_proxy_ops_baseline"`
	WriteProxyOpsOuter      float64 `json:"write_proxy_ops_outer"`
	WriteRegressionFraction float64 `json:"write_regression_fraction"`

	WALogicalBytes         uint64  `json:"wa_logical_bytes"`
	WABaselineVlogBytes    uint64  `json:"wa_baseline_vlog_bytes"`
	WABaselineWALBytes     uint64  `json:"wa_baseline_wal_bytes"`
	WABaselineTotalBytes   uint64  `json:"wa_baseline_total_bytes"`
	WABaselineRatio        float64 `json:"wa_baseline_ratio"`
	WAOuterVlogBytes       uint64  `json:"wa_outer_vlog_bytes"`
	WAOuterWALBytes        uint64  `json:"wa_outer_wal_bytes"`
	WAOuterTotalBytes      uint64  `json:"wa_outer_total_bytes"`
	WAOuterRatio           float64 `json:"wa_outer_ratio"`
	WAIncreaseFraction     float64 `json:"wa_increase_fraction"`
	BaselineWALActualBytes uint64  `json:"baseline_wal_actual_bytes"`

	Gates outerLeafApproxGates `json:"gates"`
}

type outerLeafApproxWorkloadSummary struct {
	Workload string `json:"workload"`
	Cases    int    `json:"cases"`
	Pass     string `json:"pass"`
}

type outerLeafApproxReportAssumptions struct {
	ForcedPointers          bool     `json:"forced_pointers"`
	ValueSizes              []int    `json:"value_sizes"`
	Codecs                  []string `json:"codecs"`
	BlockTargetBytes        int      `json:"block_target_bytes"`
	BlockCacheBytes         int64    `json:"block_cache_bytes"`
	FenceFPR                float64  `json:"fence_fpr"`
	WALBytesPerRecord       int      `json:"wal_bytes_per_record"`
	VlogRecordOverheadBytes int      `json:"vlog_record_overhead_bytes"`
	MaxKeys                 int      `json:"max_keys"`
	Queries                 int      `json:"queries"`
}

type outerLeafApproxGateThresholds struct {
	BytesReduction  float64 `json:"bytes_reduction"`
	LookupSlowdown  float64 `json:"lookup_slowdown"`
	WriteRegression float64 `json:"write_regression"`
	WAIncrease      float64 `json:"wa_increase"`
}

type outerLeafApproxReport struct {
	GeneratedAt   string                           `json:"generated_at"`
	Overall       string                           `json:"overall"`
	Assumptions   outerLeafApproxReportAssumptions `json:"assumptions"`
	Gates         outerLeafApproxGateThresholds    `json:"gates"`
	WorkloadGates []outerLeafApproxWorkloadSummary `json:"workload_gates"`
	Cases         []outerLeafApproxCaseResult      `json:"cases"`
}

func runOuterLeafApproxSuite(baseCfg BenchConfig) (string, error) {
	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		return "", err
	}

	cfg, err := buildOuterLeafApproxSuiteConfig(opts)
	if err != nil {
		return "", err
	}

	// The approximation assumes pointer-backed values in the current format.
	forcedPointers := false
	restoreForcePointers := *treedbForceValuePointers
	if !*treedbForceValuePointers {
		*treedbForceValuePointers = true
		forcedPointers = true
	}
	defer func() { *treedbForceValuePointers = restoreForcePointers }()

	builderOpts := node.BuilderOptions{
		LeafPrefixCompression: opts.LeafPrefixCompression,
		LeafColumnar:          opts.IndexColumnarLeaves,
		PackedValuePtr:        opts.IndexPackedValuePtr,
	}

	cases := make([]outerLeafApproxCaseResult, 0, len(cfg.ValueSizes)*len(outerLeafApproxWorkloads)*len(cfg.Codecs))

	for _, valueSize := range cfg.ValueSizes {
		for _, workload := range outerLeafApproxWorkloads {
			scenario, cleanup, err := runOuterLeafApproxScenario(baseCfg, workload, valueSize, cfg.MaxKeys)
			if err != nil {
				return "", err
			}

			for _, codec := range cfg.Codecs {
				result, err := evaluateOuterLeafApproxCase(scenario, codec, cfg, builderOpts)
				if err != nil {
					if !baseCfg.KeepDir {
						cleanup()
					}
					return "", err
				}
				cases = append(cases, result)
			}

			if !baseCfg.KeepDir {
				cleanup()
			}
		}
	}

	workloadSummary, overall := summarizeOuterLeafApproxCases(cases)
	report := buildOuterLeafApproxReport(cfg, cases, workloadSummary, overall, forcedPointers)
	sanitizeOuterLeafApproxReport(&report)
	if err := maybeWriteOuterLeafApproxReportJSON(cfg.ReportJSONPath, report); err != nil {
		return "", err
	}

	md := renderOuterLeafApproxMarkdown(report)
	if cfg.ReportJSONPath != "" {
		md += fmt.Sprintf("\n- json_report: `%s`\n", cfg.ReportJSONPath)
	}
	md += "- note: this suite is an approximation prototype; it does not mutate the live TreeDB on-disk format.\n"
	return md, nil
}

func buildOuterLeafApproxSuiteConfig(opts treedb.Options) (outerLeafApproxSuiteConfig, error) {
	valueSizes, err := parseOuterLeafApproxValueSizes(*outerLeafApproxValueSizes)
	if err != nil {
		return outerLeafApproxSuiteConfig{}, err
	}
	cfg := outerLeafApproxSuiteConfig{
		Queries:             *outerLeafApproxQueries,
		MaxKeys:             *outerLeafApproxMaxKeys,
		ValueSizes:          valueSizes,
		BlockCacheBytes:     int64(*outerLeafApproxBlockCacheMB) << 20,
		FenceFPR:            *outerLeafApproxFenceFPR,
		WALBytesPerRecord:   *outerLeafApproxWALBytesPerRecord,
		VlogRecordOverhead:  *outerLeafApproxVlogRecordOverhead,
		GateBytesReduction:  *outerLeafApproxGateBytesReduction,
		GateLookupSlowdown:  *outerLeafApproxGateLookupSlowdown,
		GateWriteRegression: *outerLeafApproxGateWriteRegression,
		GateWAIncrease:      *outerLeafApproxGateWAIncrease,
		ReportJSONPath:      strings.TrimSpace(*outerLeafApproxReportJSON),
		Codecs:              []treedb.ValueLogBlockCodec{treedb.ValueLogBlockSnappy, treedb.ValueLogBlockLZ4},
	}

	target := opts.ValueLog.OuterLeafBlockTargetBytes
	if target <= 0 {
		target = outerLeafApproxDefaultBlockTarget
	}
	cfg.BlockTargetBytes = target

	if err := validateOuterLeafApproxSuiteConfig(cfg); err != nil {
		return outerLeafApproxSuiteConfig{}, err
	}
	return cfg, nil
}

func validateOuterLeafApproxSuiteConfig(cfg outerLeafApproxSuiteConfig) error {
	if cfg.Queries <= 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-queries must be > 0 (got %d)", cfg.Queries)
	}
	if cfg.MaxKeys == 0 || cfg.MaxKeys < -1 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-max-keys must be -1 or > 0 (got %d)", cfg.MaxKeys)
	}
	if len(cfg.ValueSizes) == 0 {
		return fmt.Errorf("outerleaf_approx: no value sizes configured")
	}
	for _, sz := range cfg.ValueSizes {
		if sz <= 0 {
			return fmt.Errorf("outerleaf_approx: invalid value size %d", sz)
		}
	}
	if cfg.BlockTargetBytes <= 0 {
		return fmt.Errorf("outerleaf_approx: block target bytes must be > 0 (got %d)", cfg.BlockTargetBytes)
	}
	if cfg.BlockCacheBytes < 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-block-cache-mb must be >= 0")
	}
	if cfg.FenceFPR < 0 || cfg.FenceFPR > 1 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-fence-fpr out of range [0,1]: %.6f", cfg.FenceFPR)
	}
	if cfg.WALBytesPerRecord < 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-wal-bytes-per-record must be >= 0 (got %d)", cfg.WALBytesPerRecord)
	}
	if cfg.VlogRecordOverhead < 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-vlog-record-overhead-bytes must be >= 0 (got %d)", cfg.VlogRecordOverhead)
	}
	if cfg.GateBytesReduction < 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-gate-bytes-reduction must be >= 0")
	}
	if cfg.GateLookupSlowdown < 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-gate-lookup-slowdown must be >= 0")
	}
	if cfg.GateWriteRegression < 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-gate-write-regression must be >= 0")
	}
	if cfg.GateWAIncrease < 0 {
		return fmt.Errorf("outerleaf_approx: -outerleaf-approx-gate-wa-increase must be >= 0")
	}
	return nil
}

func parseOuterLeafApproxValueSizes(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := make(map[int]struct{}, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("outerleaf_approx: parse -outerleaf-approx-value-sizes %q: %w", raw, err)
		}
		if v <= 0 {
			return nil, fmt.Errorf("outerleaf_approx: value size must be > 0 (got %d)", v)
		}
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("outerleaf_approx: no valid value sizes in %q", raw)
	}
	return out, nil
}

func runOuterLeafApproxScenario(baseCfg BenchConfig, workload outerLeafApproxWorkloadSpec, valueSize, maxKeys int) (outerLeafApproxScenarioRun, func(), error) {
	cfg := baseCfg
	cfg.Progress = false
	cfg.KeepDir = true
	cfg.DBsArg = "treedb"
	cfg.DBsExcludeArg = ""
	cfg.TestsArg = workload.TestsArg
	cfg.ValueSize = valueSize
	if cfg.WriteWorkers <= 0 {
		cfg.WriteWorkers = 1
	}
	if maxKeys > 0 && cfg.Keys > maxKeys {
		cfg.Keys = maxKeys
	}

	start := time.Now()
	run, err := runBenchmark(cfg)
	if err != nil {
		return outerLeafApproxScenarioRun{}, nil, err
	}
	cleaned := false
	cleanup := func() {
		if cleaned {
			return
		}
		_ = suiteCleanupDirs(run.Instances)
		cleaned = true
	}

	inst, err := findSuiteInstance(run.Instances, "treedb")
	if err != nil {
		cleanup()
		return outerLeafApproxScenarioRun{}, nil, err
	}
	dbName := inst.Wrapper.Name()
	ops, err := lookupRunResultOps(run.Results, workload.MetricTest, dbName)
	if err != nil {
		cleanup()
		return outerLeafApproxScenarioRun{}, nil, err
	}
	secondaryOps := 0.0
	if workload.SecondaryMetricTest != "" {
		secondaryOps, _ = lookupRunResultOps(run.Results, workload.SecondaryMetricTest, dbName)
	}

	out := outerLeafApproxScenarioRun{
		WorkloadName: workload.Name,
		TestsArg:     workload.TestsArg,
		MetricTest:   workload.MetricTest,
		ValueSize:    valueSize,
		Dir:          inst.Dir,
		Wall:         time.Since(start),
		WorkloadOps:  ops,
		SecondaryOps: secondaryOps,
	}
	if usage, ok := run.TreeDBDiskUsage[dbName]; ok {
		out.MainIndexBytes = usage.MainIndexBytes
		out.MainWALBytes = usage.MainWAL.TotalBytes
		out.MainValueBytes = usage.MainWAL.ValueBytes
	}

	tdb, err := openTreeDBAdapterFromDir(out.Dir)
	if err != nil {
		cleanup()
		return outerLeafApproxScenarioRun{}, nil, err
	}
	defer func() { _ = tdb.Close() }()

	out.Keys, out.Values, err = collectTreeDBKeyValues(tdb, maxKeys)
	if err != nil {
		cleanup()
		return outerLeafApproxScenarioRun{}, nil, err
	}
	if len(out.Keys) == 0 {
		cleanup()
		return outerLeafApproxScenarioRun{}, nil, fmt.Errorf("outerleaf_approx: no keys collected from %s", out.Dir)
	}
	return out, cleanup, nil
}

func lookupRunResultOps(results map[string]map[string]float64, testName, dbName string) (float64, error) {
	perTest, ok := results[testName]
	if !ok {
		return 0, fmt.Errorf("outerleaf_approx: missing result row %q", testName)
	}
	ops, ok := perTest[dbName]
	if !ok {
		return 0, fmt.Errorf("outerleaf_approx: missing result for %s/%s", testName, dbName)
	}
	return ops, nil
}

func evaluateOuterLeafApproxCase(s outerLeafApproxScenarioRun, codec treedb.ValueLogBlockCodec, cfg outerLeafApproxSuiteConfig, builderOpts node.BuilderOptions) (outerLeafApproxCaseResult, error) {
	blocks, outerPayloadBytes, err := buildOuterLeafApproxBlocks(s.Keys, s.Values, cfg.BlockTargetBytes, codec)
	if err != nil {
		return outerLeafApproxCaseResult{}, err
	}
	baselineBuild, err := buildBaselineValuePayloads(s.Values, codec)
	if err != nil {
		return outerLeafApproxCaseResult{}, err
	}

	outerWriteStart := time.Now()
	if _, _, err := buildOuterLeafApproxBlocks(s.Keys, s.Values, cfg.BlockTargetBytes, codec); err != nil {
		return outerLeafApproxCaseResult{}, err
	}
	outerWriteOps := 0.0
	if dt := time.Since(outerWriteStart); dt > 0 {
		outerWriteOps = float64(len(s.Keys)) / dt.Seconds()
	}

	queries := generateOuterLeafApproxQueries(s.Keys, cfg.Queries, int64(s.ValueSize)<<32|int64(len(s.Keys)))
	baselineP95, err := measureBaselineLookupP95(s.Keys, baselineBuild.payloads, queries, codec)
	if err != nil {
		return outerLeafApproxCaseResult{}, err
	}
	lookupStats, err := measureOuterLookupP95(s.Keys, blocks, queries, codec, cfg.BlockCacheBytes, cfg.FenceFPR, int64(s.ValueSize)^0x6f757465726c6561)
	if err != nil {
		return outerLeafApproxCaseResult{}, err
	}

	baselineLeafPages, err := estimateLeafPagesForKeys(s.Keys, builderOpts)
	if err != nil {
		return outerLeafApproxCaseResult{}, err
	}
	fenceKeys := make([][]byte, len(blocks))
	for i := range blocks {
		fenceKeys[i] = blocks[i].firstKey
	}
	fenceLeafPages, err := estimateLeafPagesForKeys(fenceKeys, builderOpts)
	if err != nil {
		return outerLeafApproxCaseResult{}, err
	}

	baselineLeafBytesEst := uint64(baselineLeafPages) * page.PageSize
	fenceLeafBytesEst := uint64(fenceLeafPages) * page.PageSize
	approxIndexBytes := s.MainIndexBytes
	if baselineLeafBytesEst > fenceLeafBytesEst {
		delta := baselineLeafBytesEst - fenceLeafBytesEst
		if delta > approxIndexBytes {
			delta = approxIndexBytes
		}
		approxIndexBytes -= delta
	}

	baselineVlogMeta := uint64(len(s.Values)) * uint64(cfg.VlogRecordOverhead)
	outerVlogMeta := uint64(len(blocks)) * uint64(cfg.VlogRecordOverhead)
	baselineVlogBytes := baselineBuild.payloadBytes + baselineVlogMeta
	outerVlogBytes := outerPayloadBytes + outerVlogMeta

	baselineTotal := s.MainIndexBytes + baselineVlogBytes
	approxTotal := approxIndexBytes + outerVlogBytes

	bytesReduction := ratioDiffFraction(baselineTotal, approxTotal)
	lookupSlowdown := ratioIncreaseFraction(baselineP95, lookupStats.P95)
	writeRegression := ratioDecreaseFraction(baselineBuild.writeProxyOps, outerWriteOps)

	wa := modelOuterLeafWriteAmp(s.Keys, s.Values, baselineVlogBytes, len(s.Values), outerVlogBytes, len(blocks), cfg.WALBytesPerRecord, s.MainWALBytes)

	passBytes := bytesReduction >= cfg.GateBytesReduction
	passLookup := lookupSlowdown <= cfg.GateLookupSlowdown
	passWrite := writeRegression <= cfg.GateWriteRegression
	passWA := wa.WAIncreaseFraction <= cfg.GateWAIncrease
	passAll := passBytes && passLookup && passWrite && passWA

	return outerLeafApproxCaseResult{
		Workload:  s.WorkloadName,
		ValueSize: s.ValueSize,
		Codec:     formatTreeDBVlogBlockCodec(codec),

		TestsArg:           s.TestsArg,
		MetricTest:         s.MetricTest,
		WorkloadOps:        s.WorkloadOps,
		SecondaryOps:       s.SecondaryOps,
		WallMillis:         s.Wall.Milliseconds(),
		KeyCount:           len(s.Keys),
		BaselineIndexBytes: s.MainIndexBytes,
		ApproxIndexBytes:   approxIndexBytes,

		BaselineVlogPayloadBytes: baselineBuild.payloadBytes,
		BaselineVlogMetaBytes:    baselineVlogMeta,
		BaselineVlogBytes:        baselineVlogBytes,
		ApproxVlogPayloadBytes:   outerPayloadBytes,
		ApproxVlogMetaBytes:      outerVlogMeta,
		ApproxVlogBytes:          outerVlogBytes,

		BaselineTotalBytes: baselineTotal,
		ApproxTotalBytes:   approxTotal,
		LeafPagesBaseline:  baselineLeafPages,
		LeafPagesFence:     fenceLeafPages,
		BlockCount:         len(blocks),

		BytesReductionFraction: bytesReduction,

		LookupQueries:          len(queries),
		LookupP95BaselineNanos: baselineP95.Nanoseconds(),
		LookupP95OuterNanos:    lookupStats.P95.Nanoseconds(),
		LookupP95HitNanos:      lookupStats.HitP95.Nanoseconds(),
		LookupP95MissNanos:     lookupStats.MissP95.Nanoseconds(),
		LookupSlowdownFraction: lookupSlowdown,
		CacheHits:              lookupStats.CacheHits,
		CacheMisses:            lookupStats.CacheMisses,
		FenceFalsePositives:    lookupStats.FenceFalsePos,
		FallbackSearches:       lookupStats.FallbackSearches,

		WriteProxyOpsBaseline:   baselineBuild.writeProxyOps,
		WriteProxyOpsOuter:      outerWriteOps,
		WriteRegressionFraction: writeRegression,

		WALogicalBytes:         wa.LogicalBytes,
		WABaselineVlogBytes:    wa.BaselineVlogBytes,
		WABaselineWALBytes:     wa.BaselineWALBytes,
		WABaselineTotalBytes:   wa.BaselineWATotal,
		WABaselineRatio:        wa.BaselineWA,
		WAOuterVlogBytes:       wa.OuterVlogBytes,
		WAOuterWALBytes:        wa.OuterWALBytes,
		WAOuterTotalBytes:      wa.OuterWATotal,
		WAOuterRatio:           wa.OuterWA,
		WAIncreaseFraction:     wa.WAIncreaseFraction,
		BaselineWALActualBytes: wa.BaselineWALActual,

		Gates: outerLeafApproxGates{
			Bytes:  passBytes,
			Lookup: passLookup,
			Write:  passWrite,
			WA:     passWA,
			All:    passAll,
		},
	}, nil
}

func summarizeOuterLeafApproxCases(cases []outerLeafApproxCaseResult) ([]outerLeafApproxWorkloadSummary, bool) {
	byWorkload := make(map[string]*outerLeafApproxWorkloadSummary, len(outerLeafApproxWorkloads))
	for _, w := range outerLeafApproxWorkloads {
		byWorkload[w.Name] = &outerLeafApproxWorkloadSummary{Workload: w.Name, Pass: "PASS"}
	}
	overall := true
	for _, c := range cases {
		s, ok := byWorkload[c.Workload]
		if !ok {
			s = &outerLeafApproxWorkloadSummary{Workload: c.Workload, Pass: "PASS"}
			byWorkload[c.Workload] = s
		}
		s.Cases++
		if !c.Gates.All {
			s.Pass = "FAIL"
			overall = false
		}
	}
	out := make([]outerLeafApproxWorkloadSummary, 0, len(byWorkload))
	for _, w := range outerLeafApproxWorkloads {
		if s, ok := byWorkload[w.Name]; ok {
			out = append(out, *s)
			delete(byWorkload, w.Name)
		}
	}
	if len(byWorkload) > 0 {
		extra := make([]string, 0, len(byWorkload))
		for k := range byWorkload {
			extra = append(extra, k)
		}
		sort.Strings(extra)
		for _, k := range extra {
			out = append(out, *byWorkload[k])
		}
	}
	if len(cases) == 0 {
		overall = false
	}
	return out, overall
}

func buildOuterLeafApproxReport(cfg outerLeafApproxSuiteConfig, cases []outerLeafApproxCaseResult, workloadSummary []outerLeafApproxWorkloadSummary, overall bool, forcedPointers bool) outerLeafApproxReport {
	codecs := make([]string, 0, len(cfg.Codecs))
	for _, c := range cfg.Codecs {
		codecs = append(codecs, formatTreeDBVlogBlockCodec(c))
	}
	return outerLeafApproxReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Overall:     passFail(overall),
		Assumptions: outerLeafApproxReportAssumptions{
			ForcedPointers:          forcedPointers,
			ValueSizes:              append([]int(nil), cfg.ValueSizes...),
			Codecs:                  codecs,
			BlockTargetBytes:        cfg.BlockTargetBytes,
			BlockCacheBytes:         cfg.BlockCacheBytes,
			FenceFPR:                cfg.FenceFPR,
			WALBytesPerRecord:       cfg.WALBytesPerRecord,
			VlogRecordOverheadBytes: cfg.VlogRecordOverhead,
			MaxKeys:                 cfg.MaxKeys,
			Queries:                 cfg.Queries,
		},
		Gates: outerLeafApproxGateThresholds{
			BytesReduction:  cfg.GateBytesReduction,
			LookupSlowdown:  cfg.GateLookupSlowdown,
			WriteRegression: cfg.GateWriteRegression,
			WAIncrease:      cfg.GateWAIncrease,
		},
		WorkloadGates: workloadSummary,
		Cases:         cases,
	}
}

func renderOuterLeafApproxMarkdown(report outerLeafApproxReport) string {
	var sb strings.Builder
	sb.WriteString("# unified_bench suite: outerleaf_approx\n\n")
	sb.WriteString("## Assumptions\n")
	sb.WriteString(fmt.Sprintf("- forced_value_pointers: %t\n", report.Assumptions.ForcedPointers))
	sb.WriteString(fmt.Sprintf("- value_sizes: %v\n", report.Assumptions.ValueSizes))
	sb.WriteString(fmt.Sprintf("- codecs: %s\n", strings.Join(report.Assumptions.Codecs, ",")))
	sb.WriteString(fmt.Sprintf("- outer_block_target_bytes: %d\n", report.Assumptions.BlockTargetBytes))
	sb.WriteString(fmt.Sprintf("- outer_block_cache_bytes: %d\n", report.Assumptions.BlockCacheBytes))
	sb.WriteString(fmt.Sprintf("- fence_fpr: %.6f\n", report.Assumptions.FenceFPR))
	sb.WriteString(fmt.Sprintf("- wal_bytes_per_record: %d\n", report.Assumptions.WALBytesPerRecord))
	sb.WriteString(fmt.Sprintf("- vlog_record_overhead_bytes: %d\n", report.Assumptions.VlogRecordOverheadBytes))
	sb.WriteString(fmt.Sprintf("- max_keys: %d\n", report.Assumptions.MaxKeys))
	sb.WriteString(fmt.Sprintf("- lookup_queries: %d\n", report.Assumptions.Queries))
	sb.WriteString("\n")

	sb.WriteString("## Gate Thresholds\n")
	sb.WriteString(fmt.Sprintf("- bytes_reduction >= %.3f\n", report.Gates.BytesReduction))
	sb.WriteString(fmt.Sprintf("- lookup_slowdown <= %.3f\n", report.Gates.LookupSlowdown))
	sb.WriteString(fmt.Sprintf("- write_regression <= %.3f\n", report.Gates.WriteRegression))
	sb.WriteString(fmt.Sprintf("- wa_increase <= %.3f\n", report.Gates.WAIncrease))
	sb.WriteString("\n")

	sb.WriteString("## Workload Gate Summary\n")
	for _, w := range report.WorkloadGates {
		sb.WriteString(fmt.Sprintf("- %s: %s (cases=%d)\n", w.Workload, w.Pass, w.Cases))
	}
	sb.WriteString("\n")

	sb.WriteString("## Matrix Results\n")
	for _, c := range report.Cases {
		sb.WriteString(fmt.Sprintf("### %s / %dB / %s\n", c.Workload, c.ValueSize, c.Codec))
		sb.WriteString(fmt.Sprintf("- workload_ops_sec: %s (%s)\n", formatFloat(c.WorkloadOps), c.MetricTest))
		if c.SecondaryOps > 0 {
			sb.WriteString(fmt.Sprintf("- secondary_ops_sec: %s\n", formatFloat(c.SecondaryOps)))
		}
		sb.WriteString(fmt.Sprintf("- wall: %dms\n", c.WallMillis))
		sb.WriteString(fmt.Sprintf("- key_count: %s\n", formatInt(c.KeyCount)))
		sb.WriteString(fmt.Sprintf("- bytes_reduction_fraction: %.6f (%s)\n", c.BytesReductionFraction, passFail(c.Gates.Bytes)))
		sb.WriteString(fmt.Sprintf("- lookup_p95_baseline: %s\n", time.Duration(c.LookupP95BaselineNanos)))
		sb.WriteString(fmt.Sprintf("- lookup_p95_outer: %s\n", time.Duration(c.LookupP95OuterNanos)))
		sb.WriteString(fmt.Sprintf("- lookup_p95_hit: %s\n", time.Duration(c.LookupP95HitNanos)))
		sb.WriteString(fmt.Sprintf("- lookup_p95_miss: %s\n", time.Duration(c.LookupP95MissNanos)))
		sb.WriteString(fmt.Sprintf("- lookup_slowdown_fraction: %.6f (%s)\n", c.LookupSlowdownFraction, passFail(c.Gates.Lookup)))
		sb.WriteString(fmt.Sprintf("- cache_hits: %s\n", formatInt(c.CacheHits)))
		sb.WriteString(fmt.Sprintf("- cache_misses: %s\n", formatInt(c.CacheMisses)))
		sb.WriteString(fmt.Sprintf("- fence_false_positives: %s\n", formatInt(c.FenceFalsePositives)))
		sb.WriteString(fmt.Sprintf("- fallback_searches: %s\n", formatInt(c.FallbackSearches)))
		sb.WriteString(fmt.Sprintf("- write_proxy_regression_fraction: %.6f (%s)\n", c.WriteRegressionFraction, passFail(c.Gates.Write)))
		sb.WriteString(fmt.Sprintf("- wa_baseline_ratio: %.6f\n", c.WABaselineRatio))
		sb.WriteString(fmt.Sprintf("- wa_outer_ratio: %.6f\n", c.WAOuterRatio))
		sb.WriteString(fmt.Sprintf("- wa_increase_fraction: %.6f (%s)\n", c.WAIncreaseFraction, passFail(c.Gates.WA)))
		sb.WriteString(fmt.Sprintf("- case_overall: %s\n", passFail(c.Gates.All)))
		sb.WriteString("\n")
	}

	sb.WriteString("## Gate Result\n")
	sb.WriteString(fmt.Sprintf("- overall: %s\n", report.Overall))
	return sb.String()
}

func maybeWriteOuterLeafApproxReportJSON(path string, report outerLeafApproxReport) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("outerleaf_approx: mkdir json report dir: %w", err)
		}
	}
	buf, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("outerleaf_approx: marshal json report: %w", err)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return fmt.Errorf("outerleaf_approx: write json report: %w", err)
	}
	return nil
}

func collectTreeDBKeyValues(db *treedbadapter.DB, maxKeys int) ([][]byte, [][]byte, error) {
	it, err := db.Iterator(nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = it.Close() }()

	capHint := 1024
	if maxKeys > 0 {
		capHint = max(1024, maxKeys)
	}
	keys := make([][]byte, 0, capHint)
	values := make([][]byte, 0, capHint)
	for it.Valid() {
		if maxKeys > 0 && len(keys) >= maxKeys {
			break
		}
		k := it.Key()
		v := it.Value()
		keys = append(keys, append([]byte(nil), k...))
		values = append(values, append([]byte(nil), v...))
		it.Next()
	}
	if err := it.Error(); err != nil {
		return nil, nil, err
	}
	return keys, values, nil
}

func appendUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

func encodeOuterLeafKV(raw []byte, key, value []byte) []byte {
	raw = appendUvarint(raw, uint64(len(key)))
	raw = appendUvarint(raw, uint64(len(value)))
	raw = append(raw, key...)
	raw = append(raw, value...)
	return raw
}

func buildOuterLeafApproxBlocks(keys, values [][]byte, targetBytes int, codec treedb.ValueLogBlockCodec) ([]outerLeafApproxBlock, uint64, error) {
	if len(keys) != len(values) {
		return nil, 0, fmt.Errorf("outerleaf_approx: key/value length mismatch: %d/%d", len(keys), len(values))
	}
	if len(keys) == 0 {
		return nil, 0, nil
	}
	if targetBytes <= 0 {
		targetBytes = outerLeafApproxDefaultBlockTarget
	}

	blocks := make([]outerLeafApproxBlock, 0, len(keys)/64+1)
	raw := make([]byte, 0, targetBytes*2)
	start := 0
	var payloadTotal uint64

	finalize := func(end int) error {
		if end <= start {
			return nil
		}
		rawLen := len(raw)
		stored := raw
		compressed := false
		if len(raw) > 0 {
			enc, err := outerLeafApproxCompress(codec, raw)
			if err != nil {
				return err
			}
			if len(enc) < len(raw) {
				stored = enc
				compressed = true
			}
		}
		copied := append([]byte(nil), stored...)
		blocks = append(blocks, outerLeafApproxBlock{
			firstKey:   keys[start],
			start:      start,
			end:        end,
			rawLen:     rawLen,
			stored:     copied,
			compressed: compressed,
		})
		payloadTotal += uint64(len(copied))
		return nil
	}

	for i := range keys {
		entryEstimate := binary.MaxVarintLen64 + binary.MaxVarintLen64 + len(keys[i]) + len(values[i])
		if len(raw) > 0 && len(raw)+entryEstimate > targetBytes {
			if err := finalize(i); err != nil {
				return nil, 0, err
			}
			start = i
			raw = raw[:0]
		}
		raw = encodeOuterLeafKV(raw, keys[i], values[i])
	}
	if err := finalize(len(keys)); err != nil {
		return nil, 0, err
	}
	return blocks, payloadTotal, nil
}

func buildBaselineValuePayloads(values [][]byte, codec treedb.ValueLogBlockCodec) (outerLeafApproxBaselinePayloadBuild, error) {
	payloads := make([]outerLeafApproxValuePayload, len(values))
	var payloadTotal uint64
	start := time.Now()
	for i := range values {
		raw := values[i]
		stored := raw
		compressed := false
		enc, err := outerLeafApproxCompress(codec, raw)
		if err != nil {
			return outerLeafApproxBaselinePayloadBuild{}, err
		}
		if len(enc) < len(raw) {
			stored = enc
			compressed = true
		}
		payloads[i] = outerLeafApproxValuePayload{
			rawLen:     len(raw),
			stored:     append([]byte(nil), stored...),
			compressed: compressed,
		}
		payloadTotal += uint64(len(stored))
	}
	elapsed := time.Since(start)
	ops := 0.0
	if elapsed > 0 {
		ops = float64(len(values)) / elapsed.Seconds()
	}
	return outerLeafApproxBaselinePayloadBuild{
		payloads:      payloads,
		payloadBytes:  payloadTotal,
		writeProxyOps: ops,
	}, nil
}

func generateOuterLeafApproxQueries(keys [][]byte, n int, seed int64) [][]byte {
	if len(keys) == 0 || n <= 0 {
		return nil
	}
	if n > len(keys)*8 {
		n = len(keys) * 8
	}
	out := make([][]byte, n)
	rng := rand.New(rand.NewSource(seed ^ 0x6f757465726c6561))
	for i := 0; i < n; i++ {
		out[i] = keys[rng.Intn(len(keys))]
	}
	return out
}

func measureBaselineLookupP95(keys [][]byte, payloads []outerLeafApproxValuePayload, queries [][]byte, codec treedb.ValueLogBlockCodec) (time.Duration, error) {
	if len(queries) == 0 {
		return 0, nil
	}
	samples := make([]time.Duration, 0, len(queries))
	scratch := make([]byte, 0, 1024)
	for _, q := range queries {
		start := time.Now()
		idx := sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i], q) >= 0
		})
		if idx < len(keys) && bytes.Equal(keys[idx], q) {
			p := payloads[idx]
			if p.compressed {
				var err error
				scratch, err = outerLeafApproxDecompress(codec, p.stored, p.rawLen, scratch)
				if err != nil {
					return 0, err
				}
			} else {
				if cap(scratch) < len(p.stored) {
					scratch = make([]byte, len(p.stored))
				}
				copy(scratch[:len(p.stored)], p.stored)
			}
		}
		samples = append(samples, time.Since(start))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return percentileDuration(samples, 0.95), nil
}

func measureOuterLookupP95(keys [][]byte, blocks []outerLeafApproxBlock, queries [][]byte, codec treedb.ValueLogBlockCodec, cacheBytes int64, fenceFPR float64, seed int64) (outerLeafApproxLookupStats, error) {
	if len(queries) == 0 {
		return outerLeafApproxLookupStats{}, nil
	}
	if len(blocks) == 0 {
		return outerLeafApproxLookupStats{}, fmt.Errorf("outerleaf_approx: missing blocks")
	}
	cache := newOuterLeafBlockCache(cacheBytes)
	rng := rand.New(rand.NewSource(seed ^ 0x6f757465726c6561))
	samples := make([]time.Duration, 0, len(queries))
	hitSamples := make([]time.Duration, 0, len(queries))
	missSamples := make([]time.Duration, 0, len(queries))
	scratch := make([]byte, 0, outerLeafApproxDefaultBlockTarget*2)

	stats := outerLeafApproxLookupStats{}
	for _, q := range queries {
		start := time.Now()
		expected := locateOuterLeafBlock(blocks, q)
		candidate, falsePos := chooseOuterLeafCandidate(rng, expected, len(blocks), fenceFPR)
		if falsePos {
			stats.FenceFalsePos++
		}

		firstHit, err := accessOuterLeafBlock(cache, blocks, candidate, codec, &scratch)
		if err != nil {
			return outerLeafApproxLookupStats{}, err
		}
		if firstHit {
			stats.CacheHits++
		} else {
			stats.CacheMisses++
		}

		found := lookupKeyInBlockRange(keys, blocks[candidate], q)
		if !found {
			stats.FallbackSearches++
			if candidate != expected {
				if _, err := accessOuterLeafBlock(cache, blocks, expected, codec, &scratch); err != nil {
					return outerLeafApproxLookupStats{}, err
				}
				_ = lookupKeyInBlockRange(keys, blocks[expected], q)
			} else {
				_ = sort.Search(len(keys), func(i int) bool {
					return bytes.Compare(keys[i], q) >= 0
				})
			}
		}

		elapsed := time.Since(start)
		samples = append(samples, elapsed)
		if firstHit {
			hitSamples = append(hitSamples, elapsed)
		} else {
			missSamples = append(missSamples, elapsed)
		}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	sort.Slice(hitSamples, func(i, j int) bool { return hitSamples[i] < hitSamples[j] })
	sort.Slice(missSamples, func(i, j int) bool { return missSamples[i] < missSamples[j] })
	stats.P95 = percentileDuration(samples, 0.95)
	stats.HitP95 = percentileDuration(hitSamples, 0.95)
	stats.MissP95 = percentileDuration(missSamples, 0.95)
	return stats, nil
}

func simulateOuterLeafLookupPattern(keys [][]byte, blocks []outerLeafApproxBlock, queries [][]byte, cacheBytes int64, fenceFPR float64, seed int64) outerLeafApproxLookupPattern {
	if len(blocks) == 0 || len(queries) == 0 {
		return outerLeafApproxLookupPattern{}
	}
	cache := newOuterLeafBlockCache(cacheBytes)
	rng := rand.New(rand.NewSource(seed ^ 0x6f757465726c6561))
	stats := outerLeafApproxLookupPattern{}
	for _, q := range queries {
		expected := locateOuterLeafBlock(blocks, q)
		candidate, falsePos := chooseOuterLeafCandidate(rng, expected, len(blocks), fenceFPR)
		if falsePos {
			stats.FenceFalsePos++
		}
		sz := int64(blocks[candidate].rawLen)
		if sz <= 0 {
			sz = int64(len(blocks[candidate].stored))
		}
		if cache.access(candidate, sz) {
			stats.CacheHits++
		} else {
			stats.CacheMisses++
		}
		if !lookupKeyInBlockRange(keys, blocks[candidate], q) {
			stats.FallbackSearches++
			if candidate != expected {
				esz := int64(blocks[expected].rawLen)
				if esz <= 0 {
					esz = int64(len(blocks[expected].stored))
				}
				_ = cache.access(expected, esz)
			}
		}
	}
	return stats
}

func locateOuterLeafBlock(blocks []outerLeafApproxBlock, key []byte) int {
	idx := sort.Search(len(blocks), func(i int) bool {
		return bytes.Compare(blocks[i].firstKey, key) > 0
	}) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(blocks) {
		idx = len(blocks) - 1
	}
	return idx
}

func chooseOuterLeafCandidate(rng *rand.Rand, expected, blockCount int, fpr float64) (int, bool) {
	if blockCount <= 1 || expected < 0 || expected >= blockCount || fpr <= 0 {
		return expected, false
	}
	if rng.Float64() >= fpr {
		return expected, false
	}
	if expected == 0 {
		return 1, true
	}
	if expected == blockCount-1 {
		return blockCount - 2, true
	}
	if rng.Intn(2) == 0 {
		return expected - 1, true
	}
	return expected + 1, true
}

func lookupKeyInBlockRange(keys [][]byte, block outerLeafApproxBlock, target []byte) bool {
	sub := keys[block.start:block.end]
	pos := sort.Search(len(sub), func(i int) bool {
		return bytes.Compare(sub[i], target) >= 0
	})
	return pos < len(sub) && bytes.Equal(sub[pos], target)
}

func accessOuterLeafBlock(cache *outerLeafBlockCache, blocks []outerLeafApproxBlock, idx int, codec treedb.ValueLogBlockCodec, scratch *[]byte) (bool, error) {
	b := blocks[idx]
	size := int64(b.rawLen)
	if size <= 0 {
		size = int64(len(b.stored))
	}
	if cache.access(idx, size) {
		return true, nil
	}
	if b.compressed {
		var err error
		*scratch, err = outerLeafApproxDecompress(codec, b.stored, b.rawLen, *scratch)
		if err != nil {
			return false, err
		}
	} else {
		if cap(*scratch) < len(b.stored) {
			*scratch = make([]byte, len(b.stored))
		}
		copy((*scratch)[:len(b.stored)], b.stored)
	}
	return false, nil
}

type outerLeafBlockCache struct {
	capacityBytes int64
	usedBytes     int64
	order         *list.List
	entries       map[int]*list.Element
}

type outerLeafBlockCacheEntry struct {
	blockID int
	size    int64
}

func newOuterLeafBlockCache(capacityBytes int64) *outerLeafBlockCache {
	if capacityBytes < 0 {
		capacityBytes = 0
	}
	return &outerLeafBlockCache{
		capacityBytes: capacityBytes,
		order:         list.New(),
		entries:       make(map[int]*list.Element),
	}
}

func (c *outerLeafBlockCache) access(blockID int, size int64) bool {
	if c.capacityBytes <= 0 {
		return false
	}
	if elem, ok := c.entries[blockID]; ok {
		c.order.MoveToFront(elem)
		return true
	}
	if size > c.capacityBytes {
		c.reset()
		return false
	}
	entry := outerLeafBlockCacheEntry{blockID: blockID, size: size}
	elem := c.order.PushFront(entry)
	c.entries[blockID] = elem
	c.usedBytes += size
	for c.usedBytes > c.capacityBytes {
		back := c.order.Back()
		if back == nil {
			break
		}
		old := back.Value.(outerLeafBlockCacheEntry)
		delete(c.entries, old.blockID)
		c.usedBytes -= old.size
		c.order.Remove(back)
	}
	return false
}

func (c *outerLeafBlockCache) reset() {
	c.order.Init()
	c.usedBytes = 0
	for k := range c.entries {
		delete(c.entries, k)
	}
}

func modelOuterLeafWriteAmp(keys, values [][]byte, baselineVlogBytes uint64, baselineRecords int, outerVlogBytes uint64, outerRecords int, walBytesPerRecord int, baselineWALActual uint64) outerLeafApproxWriteAmpMetrics {
	logical := logicalKVBytes(keys, values)
	if logical == 0 {
		logical = 1
	}
	baselineWAL := baselineVlogBytes + uint64(max(0, walBytesPerRecord))*uint64(max(0, baselineRecords))
	outerWAL := outerVlogBytes + uint64(max(0, walBytesPerRecord))*uint64(max(0, outerRecords))

	baselineTotal := baselineVlogBytes + baselineWAL
	outerTotal := outerVlogBytes + outerWAL
	baselineWA := float64(baselineTotal) / float64(logical)
	outerWA := float64(outerTotal) / float64(logical)
	waIncrease := 0.0
	if baselineWA > 0 {
		waIncrease = (outerWA - baselineWA) / baselineWA
	}
	return outerLeafApproxWriteAmpMetrics{
		LogicalBytes:       logical,
		BaselineVlogBytes:  baselineVlogBytes,
		BaselineWALBytes:   baselineWAL,
		BaselineWATotal:    baselineTotal,
		BaselineWA:         baselineWA,
		OuterVlogBytes:     outerVlogBytes,
		OuterWALBytes:      outerWAL,
		OuterWATotal:       outerTotal,
		OuterWA:            outerWA,
		WAIncreaseFraction: waIncrease,
		BaselineWALActual:  baselineWALActual,
	}
}

func logicalKVBytes(keys, values [][]byte) uint64 {
	n := min(len(keys), len(values))
	var out uint64
	for i := 0; i < n; i++ {
		out += uint64(len(keys[i]) + len(values[i]))
	}
	return out
}

func outerLeafApproxCompress(codec treedb.ValueLogBlockCodec, raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	switch codec {
	case treedb.ValueLogBlockLZ4:
		bound := lz4.CompressBlockBound(len(raw))
		dst := make([]byte, bound)
		n, err := lz4.CompressBlock(raw, dst, nil)
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			return raw, nil
		}
		return dst[:n], nil
	default:
		return snappy.Encode(nil, raw), nil
	}
}

func outerLeafApproxDecompress(codec treedb.ValueLogBlockCodec, payload []byte, rawLen int, dst []byte) ([]byte, error) {
	if rawLen <= 0 {
		return dst[:0], nil
	}
	switch codec {
	case treedb.ValueLogBlockLZ4:
		if cap(dst) < rawLen {
			dst = make([]byte, rawLen)
		} else {
			dst = dst[:rawLen]
		}
		n, err := lz4.UncompressBlock(payload, dst)
		if err != nil {
			return nil, err
		}
		if n != rawLen {
			return nil, fmt.Errorf("outerleaf_approx: lz4 decode size mismatch got=%d want=%d", n, rawLen)
		}
		return dst[:n], nil
	default:
		out, err := snappy.Decode(dst[:0], payload)
		if err != nil {
			return nil, err
		}
		if len(out) != rawLen {
			return nil, fmt.Errorf("outerleaf_approx: snappy decode size mismatch got=%d want=%d", len(out), rawLen)
		}
		return out, nil
	}
}

func estimateLeafPagesForKeys(keys [][]byte, opts node.BuilderOptions) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	newBuilder := func(pageID uint64) *node.Builder {
		data := make([]byte, page.PageSize)
		b := node.NewBuilderWithOptions(data, page.PageTypeLeaf, opts)
		b.SetPageID(pageID)
		return b
	}
	b := newBuilder(1)
	ptr := page.ValuePtr{Offset: 4, Length: 32, FileID: page.ValueLogFileID(1)}
	pages := 0
	for _, key := range keys {
		err := b.AddLeafEntry(key, nil, node.FlagPointer, ptr)
		if err == nil {
			continue
		}
		if err != node.ErrNodeFull {
			return 0, err
		}
		b.FinishNoNode()
		pages++
		b = newBuilder(uint64(pages + 1))
		if err := b.AddLeafEntry(key, nil, node.FlagPointer, ptr); err != nil {
			return 0, err
		}
	}
	b.FinishNoNode()
	pages++
	return pages, nil
}

func ratioDiffFraction(base, next uint64) float64 {
	if base == 0 {
		return 0
	}
	if next >= base {
		return -float64(next-base) / float64(base)
	}
	return float64(base-next) / float64(base)
}

func ratioIncreaseFraction(base, next time.Duration) float64 {
	if base <= 0 {
		return 0
	}
	return float64(next-base) / float64(base)
}

func ratioDecreaseFraction(base, next float64) float64 {
	if base <= 0 {
		return 0
	}
	return (base - next) / base
}

func finiteOrZero(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

func sanitizeOuterLeafApproxReport(report *outerLeafApproxReport) {
	if report == nil {
		return
	}
	report.Assumptions.FenceFPR = finiteOrZero(report.Assumptions.FenceFPR)
	report.Gates.BytesReduction = finiteOrZero(report.Gates.BytesReduction)
	report.Gates.LookupSlowdown = finiteOrZero(report.Gates.LookupSlowdown)
	report.Gates.WriteRegression = finiteOrZero(report.Gates.WriteRegression)
	report.Gates.WAIncrease = finiteOrZero(report.Gates.WAIncrease)
	for i := range report.Cases {
		c := &report.Cases[i]
		c.WorkloadOps = finiteOrZero(c.WorkloadOps)
		c.SecondaryOps = finiteOrZero(c.SecondaryOps)
		c.BytesReductionFraction = finiteOrZero(c.BytesReductionFraction)
		c.LookupSlowdownFraction = finiteOrZero(c.LookupSlowdownFraction)
		c.WriteProxyOpsBaseline = finiteOrZero(c.WriteProxyOpsBaseline)
		c.WriteProxyOpsOuter = finiteOrZero(c.WriteProxyOpsOuter)
		c.WriteRegressionFraction = finiteOrZero(c.WriteRegressionFraction)
		c.WABaselineRatio = finiteOrZero(c.WABaselineRatio)
		c.WAOuterRatio = finiteOrZero(c.WAOuterRatio)
		c.WAIncreaseFraction = finiteOrZero(c.WAIncreaseFraction)
	}
}
