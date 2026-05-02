package main

import (
	"flag"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
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
	disableWAL              bool
	relaxedSync             bool
	disableChecksum         bool
	allowUnsafe             bool
	maintenanceMode         string
	flushThreshold          int64
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
		disableWAL:              *treedbDisableWAL,
		relaxedSync:             *treedbRelaxedSync,
		disableChecksum:         *treedbDisableReadChecksum,
		allowUnsafe:             *treedbAllowUnsafe,
		maintenanceMode:         *treedbMaintenanceMode,
		flushThreshold:          *treedbFlushThreshold,
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
	*treedbDisableWAL = s.disableWAL
	*treedbRelaxedSync = s.relaxedSync
	*treedbDisableReadChecksum = s.disableChecksum
	*treedbAllowUnsafe = s.allowUnsafe
	*treedbMaintenanceMode = s.maintenanceMode
	*treedbFlushThreshold = s.flushThreshold
	explicitFlags = s.explicitFlags
}

func resetTreeDBIndexFlagsForTest() {
	*treedbIndexOptimizations = false
	*treedbIndexOuterLeavesInVlog = true
	*treedbPreferAppendAlloc = false
	*treedbForceValuePointers = false
	*treedbLeafPrefixCompression = false
	*treedbIndexColumnarLeaves = false
	*treedbIndexPackedValuePtr = false
	*treedbIndexInternalBaseDelta = false
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
	*treedbDisableWAL = false
	*treedbRelaxedSync = false
	*treedbDisableReadChecksum = false
	*treedbAllowUnsafe = false
	*treedbMaintenanceMode = "bench"
	*treedbFlushThreshold = 64 * 1024 * 1024
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
	if !*treedbDisableReadChecksum {
		t.Fatalf("expected wal_on_fast profile to disable read checksum")
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
