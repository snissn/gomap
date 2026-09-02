package main

import (
	"flag"
	"runtime"
	"strconv"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
)

func TestTreeDBIndexOuterLeavesInVlogFlag_DefaultIsTrue(t *testing.T) {
	f := flag.Lookup("treedb-index-outer-leaves-in-vlog")
	if f == nil {
		t.Fatalf("missing treedb-index-outer-leaves-in-vlog flag")
	}
	if got := f.DefValue; got != "true" {
		t.Fatalf("flag default=%q want %q", got, "true")
	}
}

func TestBuildTreeDBOptions_IndexOuterLeavesInVlogEnable(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeavesInVlog = true

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions index outer leaves in vlog: %v", err)
	}
	if got := opts.IndexOuterLeavesInValueLog; !got {
		t.Fatalf("IndexOuterLeavesInValueLog=%t want true", got)
	}
	if got := rep.formatText(""); !strings.Contains(got, "index_outer_leaves_in_vlog=true") {
		t.Fatalf("resolved options missing index_outer_leaves_in_vlog=true: %q", got)
	}
}

func TestBuildTreeDBOptions_LeafPageReadCacheEntries(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbLeafPageReadCacheEntries = 32768

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions leaf page read cache entries: %v", err)
	}
	if got := opts.LeafPageReadCacheEntries; got != 32768 {
		t.Fatalf("LeafPageReadCacheEntries=%d want 32768", got)
	}
	if got := rep.formatText(""); !strings.Contains(got, "outer_leaf_read_cache_entries=32768") {
		t.Fatalf("resolved options missing outer_leaf_read_cache_entries=32768: %q", got)
	}
}

func TestBuildTreeDBOptions_LeafPageReadCacheEntriesDefaultReportsEffective(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	t.Setenv(treedbdb.LeafPageReadCacheEntriesEnvKey, "")

	resetTreeDBIndexFlagsForTest()

	_, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions default leaf page read cache entries: %v", err)
	}
	if got := rep.formatText(""); !strings.Contains(got, "outer_leaf_read_cache_entries=default/env (effective=8192)") {
		t.Fatalf("resolved options missing effective default cache entries: %q", got)
	}
}

func TestBuildTreeDBOptions_LeafPageReadCacheWriteAdmission(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbLeafPageReadCacheWriteAdmission = "adaptive"

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions leaf page write admission: %v", err)
	}
	if got := opts.LeafPageReadCacheWriteAdmission; got != treedb.LeafPageReadCacheWriteAdmissionAdaptive {
		t.Fatalf("LeafPageReadCacheWriteAdmission=%v want adaptive", got)
	}
	text := rep.formatText("")
	if !strings.Contains(text, "outer_leaf_read_cache_write_admission=adaptive") {
		t.Fatalf("resolved options missing adaptive write admission: %q", text)
	}
}

func TestBuildTreeDBOptions_LeafPageReadCacheWriteAdmissionRejectsInvalid(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbLeafPageReadCacheWriteAdmission = "bad-policy"

	_, _, err := buildTreeDBOptions("")
	if err == nil {
		t.Fatal("buildTreeDBOptions unexpectedly accepted invalid write admission policy")
	}
	if !strings.Contains(err.Error(), "leaf page read cache write admission") {
		t.Fatalf("buildTreeDBOptions error=%q, want write admission context", err)
	}
}

func TestBuildTreeDBOptions_FlushAdmissionDefaultAutoReportsHardwareAwareAdaptive(t *testing.T) {
	if physical := treedbdb.DetectPhysicalCores(); physical == 1 {
		t.Skip("default auto correctly declines one-physical-core hosts; formula coverage lives in TreeDB/db hardware tests")
	}
	oldGOMAXPROCS := runtime.GOMAXPROCS(12)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions default auto: %v", err)
	}
	if opts.FlushAdmissionPolicy != treedb.FlushAdmissionPolicyAuto || opts.FlushApplyConcurrency < 2 || opts.FlushApplyConcurrency > 8 || opts.FlushApplyConcurrency > runtime.GOMAXPROCS(0) || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("default auto candidate not selected: policy=%s concurrency=%d span=%t backlog=%t", opts.FlushAdmissionPolicy, opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	if physical := treedbdb.DetectPhysicalCores(); physical > 0 && opts.FlushApplyConcurrency > physical {
		t.Fatalf("default auto concurrency=%d exceeds detected physical cores=%d", opts.FlushApplyConcurrency, physical)
	}
	if opts.FlushApplyMinEntries != 1 || opts.FlushApplyMinSpans != 1 || opts.FlushApplyMinBytes != 1 {
		t.Fatalf("default auto min gates not selected: entries=%d spans=%d bytes=%d", opts.FlushApplyMinEntries, opts.FlushApplyMinSpans, opts.FlushApplyMinBytes)
	}
	if opts.LeafPageReadCacheWriteAdmission != treedb.LeafPageReadCacheWriteAdmissionAdaptive {
		t.Fatalf("default auto cache admission=%s want adaptive", opts.LeafPageReadCacheWriteAdmission)
	}
	text := rep.formatText("")
	for _, want := range []string{
		"flush_admission_policy=auto",
		"flush_admission_admitted=true",
		"flush_admission_reason=auto_admitted_hardware_aware",
		"flush_admission_configured_concurrency=0",
		"flush_admission_effective_concurrency=" + strconv.Itoa(opts.FlushApplyConcurrency),
		"flush_admission_concurrency_cap_reason=" + treedbdb.FlushAdmissionDecisionForOptions(opts).FlushApplyConcurrencyCapReason,
		"flush_admission_concurrency_defaulted=true",
		"runtime_gomaxprocs=12",
		"flush_admission_flush_apply_span_native=true",
		"flush_admission_flush_backlog_coalescing=true",
		"flush_apply_concurrency=" + strconv.Itoa(opts.FlushApplyConcurrency),
		"flush_apply_min_entries_configured=1",
		"flush_apply_min_spans_configured=1",
		"flush_apply_min_bytes_configured=1",
		"flush_apply_span_native=true",
		"flush_backlog_coalescing=true",
		"journal_lanes_effective_default=1",
		"journal_lanes_hot=1",
		"journal_lanes_warm=0",
		"journal_lanes_cold=0",
		"outer_leaf_read_cache_write_admission=adaptive",
		"  - flush_admission_policy=auto admitted: auto_admitted_hardware_aware",
	} {
		if !treeDBReportHasLine(text, want) {
			t.Fatalf("resolved options missing line %q: %q", want, text)
		}
	}
}

func TestBuildTreeDBOptions_FlushAdmissionExplicitPreservesOptInFlags(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbFlushAdmissionPolicy = "explicit"
	*treedbFlushApplyConcurrency = 4
	*treedbFlushApplySpanNative = true
	*treedbFlushBacklogCoalescing = true

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions flush admission explicit: %v", err)
	}
	if opts.FlushAdmissionPolicy != treedb.FlushAdmissionPolicyExplicit || opts.FlushApplyConcurrency != 4 || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("explicit opt-in not preserved: policy=%s concurrency=%d span=%t backlog=%t", opts.FlushAdmissionPolicy, opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	text := rep.formatText("")
	decision := treedbdb.FlushAdmissionDecisionForOptions(opts)
	for _, want := range []string{
		"flush_admission_policy=explicit",
		"flush_admission_admitted=true",
		"flush_admission_reason=explicit_opt_in",
		"flush_admission_configured_concurrency=4",
		"flush_admission_effective_concurrency=" + strconv.Itoa(decision.FlushApplyConcurrency),
		"flush_admission_concurrency_cap_reason=" + decision.FlushApplyConcurrencyCapReason,
		"flush_admission_concurrency_defaulted=false",
		"runtime_gomaxprocs=" + strconv.Itoa(runtime.GOMAXPROCS(0)),
		"flush_admission_flush_apply_span_native=true",
		"flush_admission_flush_backlog_coalescing=true",
		"flush_apply_concurrency=4",
		"flush_apply_span_native=true",
		"flush_backlog_coalescing=true",
	} {
		if !treeDBReportHasLine(text, want) {
			t.Fatalf("resolved options missing line %q: %q", want, text)
		}
	}
}

func TestBuildTreeDBOptions_FlushAdmissionOffForcesOff(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbFlushAdmissionPolicy = "off"
	*treedbFlushApplyConcurrency = 4
	*treedbFlushApplySpanNative = true
	*treedbFlushBacklogCoalescing = true

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions flush admission off: %v", err)
	}
	if opts.FlushAdmissionPolicy != treedb.FlushAdmissionPolicyOff || opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("off policy not forced off: policy=%s concurrency=%d span=%t backlog=%t", opts.FlushAdmissionPolicy, opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	text := rep.formatText("")
	for _, want := range []string{
		"flush_admission_policy=off",
		"flush_admission_admitted=false",
		"flush_admission_reason=policy_off",
		"flush_admission_configured_concurrency=4",
		"flush_admission_effective_concurrency=0",
		"flush_admission_concurrency_cap_reason=disabled",
		"flush_admission_concurrency_defaulted=false",
		"flush_admission_flush_apply_span_native=false",
		"flush_admission_flush_backlog_coalescing=false",
		"flush_apply_concurrency=0",
		"flush_apply_span_native=false",
		"flush_backlog_coalescing=false",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("resolved options missing %q: %q", want, text)
		}
	}
}

func TestBuildTreeDBOptions_FlushAdmissionAutoDeclinesLowConcurrency(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbFlushAdmissionPolicy = "auto"
	*treedbFlushApplyConcurrency = 1
	*treedbFlushApplySpanNative = true
	*treedbFlushBacklogCoalescing = true

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions flush admission auto: %v", err)
	}
	if opts.FlushAdmissionPolicy != treedb.FlushAdmissionPolicyAuto || opts.FlushApplyConcurrency != 0 || opts.FlushApplySpanNative || opts.FlushBacklogCoalescing {
		t.Fatalf("auto decline not forced off: policy=%s concurrency=%d span=%t backlog=%t", opts.FlushAdmissionPolicy, opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	text := rep.formatText("")
	for _, want := range []string{
		"flush_admission_policy=auto",
		"flush_admission_admitted=false",
		"flush_admission_reason=low_concurrency",
		"flush_admission_configured_concurrency=1",
		"flush_admission_effective_concurrency=0",
		"flush_admission_concurrency_cap_reason=disabled",
		"flush_admission_concurrency_defaulted=false",
		"  - flush_admission_policy=auto declined: low_concurrency",
	} {
		if !treeDBReportHasLine(text, want) {
			t.Fatalf("resolved options missing line %q: %q", want, text)
		}
	}
}

func TestBuildTreeDBOptions_FlushAdmissionAutoReportsConfiguredC16CappedByGOMAXPROCS(t *testing.T) {
	oldGOMAXPROCS := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(oldGOMAXPROCS)
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbFlushAdmissionPolicy = "auto"
	*treedbFlushApplyConcurrency = 16

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions flush admission auto c16: %v", err)
	}
	if opts.FlushAdmissionPolicy != treedb.FlushAdmissionPolicyAuto || opts.FlushApplyConcurrency != 8 || !opts.FlushApplySpanNative || !opts.FlushBacklogCoalescing {
		t.Fatalf("auto c16 not capped/enabled: policy=%s concurrency=%d span=%t backlog=%t", opts.FlushAdmissionPolicy, opts.FlushApplyConcurrency, opts.FlushApplySpanNative, opts.FlushBacklogCoalescing)
	}
	text := rep.formatText("")
	for _, want := range []string{
		"flush_admission_policy=auto",
		"flush_admission_admitted=true",
		"flush_admission_reason=auto_admitted_hardware_aware",
		"flush_admission_configured_concurrency=16",
		"flush_admission_effective_concurrency=8",
		"flush_admission_concurrency_cap_reason=configured_gomaxprocs_cap",
		"flush_admission_concurrency_defaulted=false",
		"runtime_gomaxprocs=8",
		"flush_apply_concurrency=8",
	} {
		if !treeDBReportHasLine(text, want) {
			t.Fatalf("resolved options missing line %q: %q", want, text)
		}
	}
}

func treeDBReportHasLine(text, want string) bool {
	for _, line := range strings.Split(text, "\n") {
		if line == want {
			return true
		}
	}
	return false
}

func TestBuildTreeDBOptions_FlushAdmissionRejectsInvalidPolicy(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbFlushAdmissionPolicy = "bad-policy"

	_, _, err := buildTreeDBOptions("")
	if err == nil {
		t.Fatal("buildTreeDBOptions unexpectedly accepted invalid flush admission policy")
	}
	if !strings.Contains(err.Error(), "flush admission policy") {
		t.Fatalf("buildTreeDBOptions error=%q, want flush admission context", err)
	}
}

func TestBuildTreeDBOptions_LeafPageReadCacheEntriesDefaultRejectsOutOfRangeEnv(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	t.Setenv(treedbdb.LeafPageReadCacheEntriesEnvKey, strconv.Itoa(1<<30))

	resetTreeDBIndexFlagsForTest()

	_, _, err := buildTreeDBOptions("")
	if err == nil {
		t.Fatal("buildTreeDBOptions unexpectedly accepted out-of-range leaf page read cache env")
	}
	if !strings.Contains(err.Error(), "leaf page read cache entries") {
		t.Fatalf("buildTreeDBOptions error=%q, want leaf page read cache context", err)
	}
}

func TestFormatTreeDBLeafPageReadCacheEntriesDefaultReportsOutOfRangeEnv(t *testing.T) {
	t.Setenv(treedbdb.LeafPageReadCacheEntriesEnvKey, strconv.Itoa(1<<30))

	got := formatTreeDBLeafPageReadCacheEntries(0)
	if strings.Contains(got, "effective=0") {
		t.Fatalf("formatTreeDBLeafPageReadCacheEntries()=%q, must not mask out-of-range env as effective=0", got)
	}
	if !strings.Contains(got, "default/env (invalid:") {
		t.Fatalf("formatTreeDBLeafPageReadCacheEntries()=%q, want invalid default/env state for out-of-range env", got)
	}
}

func TestParseTreeDBVlogGenerationPolicy(t *testing.T) {
	got, err := parseTreeDBVlogGenerationPolicy("default")
	if err != nil {
		t.Fatalf("parseTreeDBVlogGenerationPolicy: %v", err)
	}
	if got != treedb.ValueLogGenerationDefault {
		t.Fatalf("policy=%d want %d", got, treedb.ValueLogGenerationDefault)
	}

	got, err = parseTreeDBVlogGenerationPolicy("hot_warm_cold")
	if err != nil {
		t.Fatalf("parseTreeDBVlogGenerationPolicy: %v", err)
	}
	if got != treedb.ValueLogGenerationHotWarmCold {
		t.Fatalf("policy=%d want %d", got, treedb.ValueLogGenerationHotWarmCold)
	}
	if _, err := parseTreeDBVlogGenerationPolicy("invalid"); err == nil {
		t.Fatalf("expected invalid generation policy error")
	}
}

func TestBuildTreeDBOptions_VlogGenerationConfig(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbMaintenanceMode = "bench"
	*treedbVlogGenerationPolicy = "hot_warm_cold"
	explicitFlags["treedb-vlog-generation-policy"] = true
	*treedbVlogGenerationHotSegmentBytes = 32 << 20
	*treedbVlogGenerationWarmSegmentBytes = 64 << 20
	*treedbVlogGenerationColdSegmentBytes = 128 << 20
	*treedbVlogRewriteBudgetBytesPerSec = 8 << 20
	*treedbVlogRewriteBudgetRecordsPerSec = 4096

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ValueLog.Generational.Policy != treedb.ValueLogGenerationHotWarmCold {
		t.Fatalf("generation policy=%d", opts.ValueLog.Generational.Policy)
	}
	if opts.ValueLog.Generational.HotSegmentTargetBytes != 32<<20 {
		t.Fatalf("hot segment target=%d", opts.ValueLog.Generational.HotSegmentTargetBytes)
	}
	if opts.ValueLog.Generational.WarmSegmentTargetBytes != 64<<20 {
		t.Fatalf("warm segment target=%d", opts.ValueLog.Generational.WarmSegmentTargetBytes)
	}
	if opts.ValueLog.Generational.ColdSegmentTargetBytes != 128<<20 {
		t.Fatalf("cold segment target=%d", opts.ValueLog.Generational.ColdSegmentTargetBytes)
	}
	if opts.ValueLog.Generational.RewriteBudgetBytesPerSec != 8<<20 {
		t.Fatalf("rewrite budget bps=%d", opts.ValueLog.Generational.RewriteBudgetBytesPerSec)
	}
	if opts.ValueLog.Generational.RewriteBudgetRecordsPerSec != 4096 {
		t.Fatalf("rewrite budget rps=%d", opts.ValueLog.Generational.RewriteBudgetRecordsPerSec)
	}
	txt := rep.formatText("")
	if !strings.Contains(txt, "vlog.generation_policy=hot_warm_cold") {
		t.Fatalf("report missing generation policy: %q", txt)
	}
}

func TestBuildTreeDBOptions_MaintenanceModeNormalDefaultsGenerationPolicy(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbMaintenanceMode = "normal"
	// Keep -treedb-vlog-generation-policy at its default ("default") but not explicit.
	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ValueLog.Generational.Policy != treedb.ValueLogGenerationHotWarmCold {
		t.Fatalf("generation policy=%d want %d", opts.ValueLog.Generational.Policy, treedb.ValueLogGenerationHotWarmCold)
	}
	formatted := rep.formatText("")
	if !strings.Contains(formatted, "maintenance_mode=normal") {
		t.Fatalf("report missing maintenance_mode: %q", formatted)
	}
	if !strings.Contains(formatted, "vlog.generation_policy=hot_warm_cold") {
		t.Fatalf("report missing generation policy: %q", formatted)
	}
}

func TestBuildTreeDBOptions_MaintenanceModeBenchDisablesBackgroundLoops(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbMaintenanceMode = "bench"
	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.BackgroundCheckpointInterval >= 0 {
		t.Fatalf("BackgroundCheckpointInterval=%v want disabled (<0)", opts.BackgroundCheckpointInterval)
	}
	if opts.BackgroundCheckpointIdleDuration >= 0 {
		t.Fatalf("BackgroundCheckpointIdleDuration=%v want disabled (<0)", opts.BackgroundCheckpointIdleDuration)
	}
	if opts.MaxWALBytes >= 0 {
		t.Fatalf("MaxWALBytes=%d want disabled (<0)", opts.MaxWALBytes)
	}
	if opts.BackgroundIndexVacuumInterval >= 0 {
		t.Fatalf("BackgroundIndexVacuumInterval=%v want disabled (<0)", opts.BackgroundIndexVacuumInterval)
	}
}

type savedTreeDBFlagState struct {
	indexOptimizations      bool
	indexOuterLeavesInVlog  bool
	preferAppendAlloc       bool
	forcePointers           bool
	leafPrefix              bool
	columnarLeaves          bool
	packedValuePtr          bool
	internalBaseDelta       bool
	leafPageReadCache       int
	leafPageWriteAdmission  string
	chunkSize               int64
	vlogCompression         string
	vlogBlockCodec          string
	vlogAutoPolicy          string
	vlogDictClassMode       string
	vlogCompressionAutotune string
	vlogDictHoldBytes       int
	vlogDictProbeBytes      int
	vlogGenerationPolicy    string
	vlogGenHotBytes         int64
	vlogGenWarmBytes        int64
	vlogGenColdBytes        int64
	vlogRewriteBudgetBPS    int64
	vlogRewriteBudgetRPS    int
	vlogRewriteMinAgeMS     int
	vacuumAfterVlogRewrite  bool
	commandWALStatsScan     bool
	disableWAL              bool
	relaxedSync             bool
	disableChecksum         bool
	allowUnsafe             bool
	maintenanceMode         string
	flushThreshold          int64
	flushAdmissionPolicy    string
	flushApplyConcurrency   int
	flushApplySpanNative    bool
	flushBacklogCoalescing  bool
	explicitFlags           map[string]bool
}

func saveTreeDBFlagState() savedTreeDBFlagState {
	copyMap := make(map[string]bool, len(explicitFlags))
	for k, v := range explicitFlags {
		copyMap[k] = v
	}
	return savedTreeDBFlagState{
		indexOptimizations:      *treedbIndexOptimizations,
		indexOuterLeavesInVlog:  *treedbIndexOuterLeavesInVlog,
		preferAppendAlloc:       *treedbPreferAppendAlloc,
		forcePointers:           *treedbForceValuePointers,
		leafPrefix:              *treedbLeafPrefixCompression,
		columnarLeaves:          *treedbIndexColumnarLeaves,
		packedValuePtr:          *treedbIndexPackedValuePtr,
		internalBaseDelta:       *treedbIndexInternalBaseDelta,
		leafPageReadCache:       *treedbLeafPageReadCacheEntries,
		leafPageWriteAdmission:  *treedbLeafPageReadCacheWriteAdmission,
		chunkSize:               *treedbChunkSize,
		vlogCompression:         *treedbVlogCompression,
		vlogBlockCodec:          *treedbVlogBlockCodec,
		vlogAutoPolicy:          *treedbVlogAutoPolicy,
		vlogDictClassMode:       *treedbVlogDictClassMode,
		vlogCompressionAutotune: *treedbVlogCompressionAutotune,
		vlogDictHoldBytes:       *treedbVlogDictIncompressibleHoldBytes,
		vlogDictProbeBytes:      *treedbVlogDictProbeIntervalBytes,
		vlogGenerationPolicy:    *treedbVlogGenerationPolicy,
		vlogGenHotBytes:         *treedbVlogGenerationHotSegmentBytes,
		vlogGenWarmBytes:        *treedbVlogGenerationWarmSegmentBytes,
		vlogGenColdBytes:        *treedbVlogGenerationColdSegmentBytes,
		vlogRewriteBudgetBPS:    *treedbVlogRewriteBudgetBytesPerSec,
		vlogRewriteBudgetRPS:    *treedbVlogRewriteBudgetRecordsPerSec,
		vlogRewriteMinAgeMS:     *treedbVlogRewriteMinSegmentAgeMS,
		vacuumAfterVlogRewrite:  *treedbVacuumAfterVlogRewriteRun,
		commandWALStatsScan:     *treedbCommandWALStatsScan,
		disableWAL:              *treedbDisableWAL,
		relaxedSync:             *treedbRelaxedSync,
		disableChecksum:         *treedbDisableReadChecksum,
		allowUnsafe:             *treedbAllowUnsafe,
		maintenanceMode:         *treedbMaintenanceMode,
		flushThreshold:          *treedbFlushThreshold,
		flushAdmissionPolicy:    *treedbFlushAdmissionPolicy,
		flushApplyConcurrency:   *treedbFlushApplyConcurrency,
		flushApplySpanNative:    *treedbFlushApplySpanNative,
		flushBacklogCoalescing:  *treedbFlushBacklogCoalescing,
		explicitFlags:           copyMap,
	}
}

func restoreTreeDBFlagState(s savedTreeDBFlagState) {
	*treedbIndexOptimizations = s.indexOptimizations
	*treedbIndexOuterLeavesInVlog = s.indexOuterLeavesInVlog
	*treedbPreferAppendAlloc = s.preferAppendAlloc
	*treedbForceValuePointers = s.forcePointers
	*treedbLeafPrefixCompression = s.leafPrefix
	*treedbIndexColumnarLeaves = s.columnarLeaves
	*treedbIndexPackedValuePtr = s.packedValuePtr
	*treedbIndexInternalBaseDelta = s.internalBaseDelta
	*treedbLeafPageReadCacheEntries = s.leafPageReadCache
	*treedbLeafPageReadCacheWriteAdmission = s.leafPageWriteAdmission
	*treedbChunkSize = s.chunkSize
	*treedbVlogCompression = s.vlogCompression
	*treedbVlogBlockCodec = s.vlogBlockCodec
	*treedbVlogAutoPolicy = s.vlogAutoPolicy
	*treedbVlogDictClassMode = s.vlogDictClassMode
	*treedbVlogCompressionAutotune = s.vlogCompressionAutotune
	*treedbVlogDictIncompressibleHoldBytes = s.vlogDictHoldBytes
	*treedbVlogDictProbeIntervalBytes = s.vlogDictProbeBytes
	*treedbVlogGenerationPolicy = s.vlogGenerationPolicy
	*treedbVlogGenerationHotSegmentBytes = s.vlogGenHotBytes
	*treedbVlogGenerationWarmSegmentBytes = s.vlogGenWarmBytes
	*treedbVlogGenerationColdSegmentBytes = s.vlogGenColdBytes
	*treedbVlogRewriteBudgetBytesPerSec = s.vlogRewriteBudgetBPS
	*treedbVlogRewriteBudgetRecordsPerSec = s.vlogRewriteBudgetRPS
	*treedbVlogRewriteMinSegmentAgeMS = s.vlogRewriteMinAgeMS
	*treedbVacuumAfterVlogRewriteRun = s.vacuumAfterVlogRewrite
	*treedbCommandWALStatsScan = s.commandWALStatsScan
	*treedbDisableWAL = s.disableWAL
	*treedbRelaxedSync = s.relaxedSync
	*treedbDisableReadChecksum = s.disableChecksum
	*treedbAllowUnsafe = s.allowUnsafe
	*treedbMaintenanceMode = s.maintenanceMode
	*treedbFlushThreshold = s.flushThreshold
	*treedbFlushAdmissionPolicy = s.flushAdmissionPolicy
	*treedbFlushApplyConcurrency = s.flushApplyConcurrency
	*treedbFlushApplySpanNative = s.flushApplySpanNative
	*treedbFlushBacklogCoalescing = s.flushBacklogCoalescing
	explicitFlags = s.explicitFlags
}

func resetTreeDBIndexFlagsForTest() {
	*treedbIndexOptimizations = false
	*treedbIndexOuterLeavesInVlog = true
	*treedbLeafPageReadCacheEntries = 0
	*treedbPreferAppendAlloc = false
	*treedbForceValuePointers = false
	*treedbLeafPrefixCompression = false
	*treedbIndexColumnarLeaves = false
	*treedbIndexPackedValuePtr = false
	*treedbIndexInternalBaseDelta = false
	*treedbLeafPageReadCacheWriteAdmission = "immediate"
	*treedbChunkSize = defaultTreeDBChunkSizeBytes
	*treedbVlogCompression = "default"
	*treedbVlogBlockCodec = "snappy"
	*treedbVlogAutoPolicy = "balanced"
	*treedbVlogDictClassMode = "single"
	*treedbVlogCompressionAutotune = "default"
	*treedbVlogDictIncompressibleHoldBytes = 0
	*treedbVlogDictProbeIntervalBytes = 0
	*treedbVlogGenerationPolicy = "default"
	*treedbVlogGenerationHotSegmentBytes = 0
	*treedbVlogGenerationWarmSegmentBytes = 0
	*treedbVlogGenerationColdSegmentBytes = 0
	*treedbVlogRewriteBudgetBytesPerSec = 0
	*treedbVlogRewriteBudgetRecordsPerSec = 0
	*treedbVlogRewriteMinSegmentAgeMS = 0
	*treedbVacuumAfterVlogRewriteRun = true
	*treedbCommandWALStatsScan = false
	*treedbDisableWAL = false
	*treedbRelaxedSync = false
	*treedbDisableReadChecksum = false
	*treedbAllowUnsafe = false
	*treedbMaintenanceMode = "bench"
	*treedbFlushThreshold = 64 * 1024 * 1024
	*treedbFlushAdmissionPolicy = "auto"
	*treedbFlushApplyConcurrency = 0
	*treedbFlushApplySpanNative = false
	*treedbFlushBacklogCoalescing = false
	explicitFlags = map[string]bool{}
}

func TestApplyProfile_FastAndWALOnFastEnableIndexOptimizations(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile fast: %v", err)
	}
	if !*treedbIndexOptimizations {
		t.Fatalf("expected fast profile to set treedb-index-optimizations")
	}
	if got := *treedbVlogCompression; got != "default" {
		t.Fatalf("expected fast profile to keep treedb-vlog-compression on the default path, got %q", got)
	}
	if got := *treedbVlogBlockCodec; got != "snappy" {
		t.Fatalf("expected fast profile to set treedb-vlog-block-codec=snappy, got %q", got)
	}
	if got := *treedbVlogAutoPolicy; got != "balanced" {
		t.Fatalf("expected fast profile to set treedb-vlog-auto-policy=balanced, got %q", got)
	}
	if got := *treedbVlogCompressionAutotune; got != "medium" {
		t.Fatalf("expected fast profile to set treedb-vlog-compression-autotune=medium, got %q", got)
	}
	if got := *treedbVlogDictIncompressibleHoldBytes; got != 64<<20 {
		t.Fatalf("expected fast profile to set treedb-vlog-dict-incompressible-hold-bytes=64MiB, got %d", got)
	}
	if got := *treedbVlogDictProbeIntervalBytes; got != 32<<20 {
		t.Fatalf("expected fast profile to set treedb-vlog-dict-probe-interval-bytes=32MiB, got %d", got)
	}
	if *treedbPreferAppendAlloc {
		t.Fatalf("expected fast profile to leave treedb-prefer-append-alloc=false")
	}
	if !*treedbDisableWAL {
		t.Fatalf("expected fast profile to disable WAL")
	}
	if !*treedbDisableReadChecksum {
		t.Fatalf("expected fast profile to disable read checksum")
	}

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("wal_on_fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile wal_on_fast: %v", err)
	}
	if !*treedbIndexOptimizations {
		t.Fatalf("expected wal_on_fast profile to set treedb-index-optimizations")
	}
	if got := *treedbVlogCompression; got != "default" {
		t.Fatalf("expected wal_on_fast profile to keep treedb-vlog-compression on the default path, got %q", got)
	}
	if got := *treedbVlogBlockCodec; got != "snappy" {
		t.Fatalf("expected wal_on_fast profile to set treedb-vlog-block-codec=snappy, got %q", got)
	}
	if got := *treedbVlogAutoPolicy; got != "balanced" {
		t.Fatalf("expected wal_on_fast profile to set treedb-vlog-auto-policy=balanced, got %q", got)
	}
	if got := *treedbVlogCompressionAutotune; got != "medium" {
		t.Fatalf("expected wal_on_fast profile to set treedb-vlog-compression-autotune=medium, got %q", got)
	}
	if got := *treedbVlogDictIncompressibleHoldBytes; got != 64<<20 {
		t.Fatalf("expected wal_on_fast profile to set treedb-vlog-dict-incompressible-hold-bytes=64MiB, got %d", got)
	}
	if got := *treedbVlogDictProbeIntervalBytes; got != 32<<20 {
		t.Fatalf("expected wal_on_fast profile to set treedb-vlog-dict-probe-interval-bytes=32MiB, got %d", got)
	}
	if *treedbPreferAppendAlloc {
		t.Fatalf("expected wal_on_fast profile to leave treedb-prefer-append-alloc=false")
	}
	if *treedbDisableWAL {
		t.Fatalf("expected wal_on_fast profile to keep WAL enabled")
	}
	if !*treedbRelaxedSync {
		t.Fatalf("expected wal_on_fast profile to enable relaxed sync")
	}
	if *treedbDisableReadChecksum {
		t.Fatalf("expected wal_on_fast profile to preserve verified read integrity")
	}
}

func TestBuildTreeDBOptions_ResolvedDurabilityProfileMatrix(t *testing.T) {
	tests := []struct {
		name            string
		disableWAL      bool
		relaxedSync     bool
		disableChecksum bool
		wantProfile     treedb.Profile
		wantBenchmark   bool
	}{
		{name: "durable", wantProfile: treedb.ProfileCommandWALDurable},
		{name: "relaxed", relaxedSync: true, wantProfile: treedb.ProfileCommandWALRelaxed},
		{name: "no_wal", disableWAL: true, wantProfile: treedb.ProfileNoWALFast},
		{name: "bench_unsafe", disableWAL: true, relaxedSync: true, disableChecksum: true, wantProfile: treedb.ProfileBenchUnsafe, wantBenchmark: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			saved := saveTreeDBFlagState()
			defer restoreTreeDBFlagState(saved)
			resetTreeDBIndexFlagsForTest()
			*treedbAllowUnsafe = tc.disableWAL || tc.relaxedSync || tc.disableChecksum
			*treedbDisableWAL = tc.disableWAL
			*treedbRelaxedSync = tc.relaxedSync
			*treedbDisableReadChecksum = tc.disableChecksum

			opts, rep, err := buildTreeDBOptions("")
			if err != nil {
				t.Fatalf("buildTreeDBOptions: %v", err)
			}
			if got := treedb.Profile(opts.ResolvedProfile); got != tc.wantProfile {
				t.Fatalf("ResolvedProfile=%q want %q", got, tc.wantProfile)
			}
			if opts.UnsafeBenchmarkProfile != tc.wantBenchmark {
				t.Fatalf("UnsafeBenchmarkProfile=%t want %t", opts.UnsafeBenchmarkProfile, tc.wantBenchmark)
			}
			if got := rep.formatText(""); !strings.Contains(got, "profile_resolved="+string(tc.wantProfile)) {
				t.Fatalf("resolved report missing profile: %q", got)
			}
		})
	}
}

func TestBuildTreeDBOptions_RejectsChecksumSkippingWALHybrid(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)
	resetTreeDBIndexFlagsForTest()
	*treedbAllowUnsafe = true
	*treedbRelaxedSync = true
	*treedbDisableReadChecksum = true

	_, _, err := buildTreeDBOptions("")
	if err == nil || !strings.Contains(err.Error(), "bench_unsafe") {
		t.Fatalf("buildTreeDBOptions error=%v want bench_unsafe admission error", err)
	}
}

func TestApplyProfile_FastKeepsImplicitCompressionPathForAutotuneOff(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile fast: %v", err)
	}
	*treedbVlogCompressionAutotune = "off"

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.ValueLog.Compression != treedb.ValueLogCompressionAuto {
		t.Fatalf("ValueLog.Compression = %v, want auto", opts.ValueLog.Compression)
	}
	if opts.ValueLog.CompressionAutotune.Mode != treedb.AutotuneOff {
		t.Fatalf("CompressionAutotune.Mode = %v, want off", opts.ValueLog.CompressionAutotune.Mode)
	}
	if opts.ValueLog.DictTrain.TrainBytes != -1 {
		t.Fatalf("DictTrain.TrainBytes = %d, want -1 when autotune=off", opts.ValueLog.DictTrain.TrainBytes)
	}
}

func TestApplyProfile_FastKeepsDefaultFlushThreshold(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile fast: %v", err)
	}
	if got, want := *treedbFlushThreshold, int64(64*1024*1024); got != want {
		t.Fatalf("fast profile flush threshold = %d want %d", got, want)
	}

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("wal_on_fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile wal_on_fast: %v", err)
	}
	if got, want := *treedbFlushThreshold, int64(64*1024*1024); got != want {
		t.Fatalf("wal_on_fast profile flush threshold = %d want %d", got, want)
	}
}

func TestApplyProfile_DurableAndBalancedDoNotEnableIndexOptimizations(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("durable", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile durable: %v", err)
	}
	if *treedbIndexOptimizations {
		t.Fatalf("did not expect durable profile to set treedb-index-optimizations")
	}

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("balanced", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile balanced: %v", err)
	}
	if *treedbIndexOptimizations {
		t.Fatalf("did not expect balanced profile to set treedb-index-optimizations")
	}
}

func TestBuildTreeDBOptions_IndexOptimizationsPerFlagOverride(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOptimizations = true
	*treedbIndexColumnarLeaves = false
	explicitFlags = map[string]bool{
		"treedb-index-columnar-leaves": true,
	}

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.IndexColumnarLeaves {
		t.Fatalf("expected explicit per-flag override to keep IndexColumnarLeaves=false")
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected index optimizations to not enable force pointers")
	}
	if !opts.LeafPrefixCompression || !opts.IndexPackedValuePtr {
		t.Fatalf("expected composite optimization settings to apply to non-overridden fields")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta to be disabled when outer leaves are stored in the value log")
	}
}

func TestBuildTreeDBOptions_ExplicitCompositeFalseWinsUnlessPerFlagExplicit(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOptimizations = false
	*treedbForceValuePointers = true
	*treedbIndexPackedValuePtr = true
	explicitFlags = map[string]bool{
		"treedb-index-optimizations":   true,
		"treedb-index-packed-valueptr": true,
	}

	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if !opts.ValueLog.ForcePointers {
		t.Fatalf("expected force pointers to remain enabled when explicitly set")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected explicit per-flag override to keep IndexPackedValuePtr=true")
	}
	if opts.LeafPrefixCompression || opts.IndexColumnarLeaves || opts.IndexInternalBaseDelta {
		t.Fatalf("expected explicit composite=false to disable remaining optimization fields")
	}
}

func TestBuildTreeDBOptions_ExplicitInternalBaseDeltaDisabledWithOuterLeaves(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexInternalBaseDelta = true
	explicitFlags = map[string]bool{
		"treedb-index-internal-base-delta": true,
	}

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected outer leaves in value log to remain enabled")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected explicit internal base-delta to resolve false with outer leaves in value log")
	}
	text := rep.formatText("")
	if !strings.Contains(text, "index_internal_base_delta=false") {
		t.Fatalf("resolved report missing disabled internal base-delta line:\n%s", text)
	}
	if !strings.Contains(text, "index_internal_base_delta disabled: leaf-log child pages use explicit LogRecordRef entries") {
		t.Fatalf("resolved report missing leaf-log compatibility note:\n%s", text)
	}
}

func TestBuildTreeDBOptions_ExplicitInternalBaseDeltaEnabledWithPagerLeaves(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOptimizations = true
	*treedbIndexOuterLeavesInVlog = false
	*treedbIndexInternalBaseDelta = true
	explicitFlags = map[string]bool{
		"treedb-index-optimizations":        true,
		"treedb-index-outer-leaves-in-vlog": true,
		"treedb-index-internal-base-delta":  true,
	}

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected explicit pager-leaf mode")
	}
	if !opts.IndexInternalBaseDelta {
		t.Fatalf("expected internal base-delta to resolve true when pager leaves are used")
	}
	text := rep.formatText("")
	if !strings.Contains(text, "index_optimizations=true") || !strings.Contains(text, "index_internal_base_delta=true") {
		t.Fatalf("resolved report missing enabled index optimization lines:\n%s", text)
	}
}
