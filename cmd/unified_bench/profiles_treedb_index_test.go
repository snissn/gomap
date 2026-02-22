package main

import (
	"flag"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

func TestTreeDBIndexOuterLeafModeFlag_DefaultIsV2FencePtr(t *testing.T) {
	f := flag.Lookup("treedb-index-outer-leaf-mode")
	if f == nil {
		t.Fatalf("missing treedb-index-outer-leaf-mode flag")
	}
	if got := f.DefValue; got != "v2_fenceptr" {
		t.Fatalf("flag default=%q want %q", got, "v2_fenceptr")
	}
}

func TestBuildTreeDBOptions_DefaultOuterLeafModeUsesV2FencePtr(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v2_fenceptr"

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions default outer leaf mode: %v", err)
	}
	if got := opts.IndexOuterLeafMode; got != treedb.IndexOuterLeafModeV2FencePtr {
		t.Fatalf("IndexOuterLeafMode=%q want %q", got, treedb.IndexOuterLeafModeV2FencePtr)
	}
	if got := opts.ValueLog.WALFenceMode; got != "simple_inline" {
		t.Fatalf("expected WAL-enabled v2_fenceptr default fence mode simple_inline, got %q", got)
	}
	if got := rep.formatText(""); !strings.Contains(got, "index_outer_leaf_mode=v2_fenceptr") {
		t.Fatalf("resolved options missing default v2_fenceptr outer-leaf mode: %q", got)
	}
}

func TestBuildTreeDBOptions_V1LeafLogModeAccepted(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v1_leaflog"

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions v1_leaflog: %v", err)
	}
	if got := opts.IndexOuterLeafMode; got != treedb.IndexOuterLeafModeV1LeafLog {
		t.Fatalf("IndexOuterLeafMode=%q want %q", got, treedb.IndexOuterLeafModeV1LeafLog)
	}
	if got := rep.formatText(""); !strings.Contains(got, "index_outer_leaf_mode=v1_leaflog") {
		t.Fatalf("resolved options missing v1_leaflog outer-leaf mode: %q", got)
	}
}

func TestBuildTreeDBOptions_V1LeafLogLegacyModeAccepted(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v1_leaflog_legacy"

	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions v1_leaflog_legacy: %v", err)
	}
	if got := opts.IndexOuterLeafMode; got != treedb.IndexOuterLeafModeV1LeafLogLegacy {
		t.Fatalf("IndexOuterLeafMode=%q want %q", got, treedb.IndexOuterLeafModeV1LeafLogLegacy)
	}
	if got := rep.formatText(""); !strings.Contains(got, "index_outer_leaf_mode=v1_leaflog_legacy") {
		t.Fatalf("resolved options missing v1_leaflog_legacy outer-leaf mode: %q", got)
	}
}

func TestParseTreeDBOuterLeafMode_V1LeafLogLegacyAccepted(t *testing.T) {
	got, err := parseTreeDBOuterLeafMode("v1_leaflog_legacy")
	if err != nil {
		t.Fatalf("parseTreeDBOuterLeafMode: %v", err)
	}
	if got != treedb.IndexOuterLeafModeV1LeafLogLegacy {
		t.Fatalf("parseTreeDBOuterLeafMode=%q want %q", got, treedb.IndexOuterLeafModeV1LeafLogLegacy)
	}
}

func TestParseTreeDBOuterLeafMode_InvalidErrorMentionsLegacyAlias(t *testing.T) {
	_, err := parseTreeDBOuterLeafMode("unknown_mode")
	if err == nil {
		t.Fatalf("expected parse error")
	}
	if !strings.Contains(err.Error(), "v1_leaflog_legacy") {
		t.Fatalf("expected error to mention v1_leaflog_legacy, got %v", err)
	}
}

type savedTreeDBFlagState struct {
	indexOptimizations bool
	indexOuterLeafMode string
	forcePointers      bool
	leafPrefix         bool
	columnarLeaves     bool
	packedValuePtr     bool
	internalBaseDelta  bool
	chunkSize          int64
	vlogAutoPolicy     string
	walFenceMode       string
	outerLeafBlobBytes int
	outerLeafCache     int
	disableWAL         bool
	relaxedSync        bool
	disableChecksum    bool
	allowUnsafe        bool
	flushThreshold     int64
	explicitFlags      map[string]bool
}

func saveTreeDBFlagState() savedTreeDBFlagState {
	copyMap := make(map[string]bool, len(explicitFlags))
	for k, v := range explicitFlags {
		copyMap[k] = v
	}
	return savedTreeDBFlagState{
		indexOptimizations: *treedbIndexOptimizations,
		indexOuterLeafMode: *treedbIndexOuterLeafMode,
		forcePointers:      *treedbForceValuePointers,
		leafPrefix:         *treedbLeafPrefixCompression,
		columnarLeaves:     *treedbIndexColumnarLeaves,
		packedValuePtr:     *treedbIndexPackedValuePtr,
		internalBaseDelta:  *treedbIndexInternalBaseDelta,
		chunkSize:          *treedbChunkSize,
		vlogAutoPolicy:     *treedbVlogAutoPolicy,
		walFenceMode:       *treedbWALFenceMode,
		outerLeafBlobBytes: *treedbOuterLeafBlobThresholdBytes,
		outerLeafCache:     *treedbOuterLeafBlockCacheEntries,
		disableWAL:         *treedbDisableWAL,
		relaxedSync:        *treedbRelaxedSync,
		disableChecksum:    *treedbDisableReadChecksum,
		allowUnsafe:        *treedbAllowUnsafe,
		flushThreshold:     *treedbFlushThreshold,
		explicitFlags:      copyMap,
	}
}

func restoreTreeDBFlagState(s savedTreeDBFlagState) {
	*treedbIndexOptimizations = s.indexOptimizations
	*treedbIndexOuterLeafMode = s.indexOuterLeafMode
	*treedbForceValuePointers = s.forcePointers
	*treedbLeafPrefixCompression = s.leafPrefix
	*treedbIndexColumnarLeaves = s.columnarLeaves
	*treedbIndexPackedValuePtr = s.packedValuePtr
	*treedbIndexInternalBaseDelta = s.internalBaseDelta
	*treedbChunkSize = s.chunkSize
	*treedbVlogAutoPolicy = s.vlogAutoPolicy
	*treedbWALFenceMode = s.walFenceMode
	*treedbOuterLeafBlobThresholdBytes = s.outerLeafBlobBytes
	*treedbOuterLeafBlockCacheEntries = s.outerLeafCache
	*treedbDisableWAL = s.disableWAL
	*treedbRelaxedSync = s.relaxedSync
	*treedbDisableReadChecksum = s.disableChecksum
	*treedbAllowUnsafe = s.allowUnsafe
	*treedbFlushThreshold = s.flushThreshold
	explicitFlags = s.explicitFlags
}

func resetTreeDBIndexFlagsForTest() {
	*treedbIndexOptimizations = false
	*treedbIndexOuterLeafMode = "v1"
	*treedbForceValuePointers = false
	*treedbLeafPrefixCompression = false
	*treedbIndexColumnarLeaves = false
	*treedbIndexPackedValuePtr = false
	*treedbIndexInternalBaseDelta = false
	*treedbChunkSize = defaultTreeDBChunkSizeBytes
	*treedbVlogAutoPolicy = "balanced"
	*treedbWALFenceMode = "rid_join"
	*treedbOuterLeafBlobThresholdBytes = 0
	*treedbOuterLeafBlockCacheEntries = 0
	*treedbDisableWAL = false
	*treedbRelaxedSync = false
	*treedbDisableReadChecksum = false
	*treedbAllowUnsafe = false
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
	if got := *treedbVlogAutoPolicy; got != "throughput" {
		t.Fatalf("expected fast profile to set treedb-vlog-auto-policy=throughput, got %q", got)
	}
	if got := *treedbOuterLeafBlockCacheEntries; got != 8192 {
		t.Fatalf("expected fast profile to set treedb-outer-leaf-block-cache-entries=8192, got %d", got)
	}
	if !*treedbDisableWAL {
		t.Fatalf("expected fast profile to disable WAL")
	}

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("wal_on_fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile wal_on_fast: %v", err)
	}
	if !*treedbIndexOptimizations {
		t.Fatalf("expected wal_on_fast profile to set treedb-index-optimizations")
	}
	if got := *treedbVlogAutoPolicy; got != "throughput" {
		t.Fatalf("expected wal_on_fast profile to set treedb-vlog-auto-policy=throughput, got %q", got)
	}
	if got := *treedbOuterLeafBlockCacheEntries; got != 8192 {
		t.Fatalf("expected wal_on_fast profile to set treedb-outer-leaf-block-cache-entries=8192, got %d", got)
	}
	if *treedbDisableWAL {
		t.Fatalf("expected wal_on_fast profile to keep WAL enabled")
	}
	if !*treedbRelaxedSync {
		t.Fatalf("expected wal_on_fast profile to enable relaxed sync")
	}
}

func TestApplyProfile_FastAndWALOnFast_V2FencePtrOuterLeafCacheDefault(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v2_fenceptr"
	if err := applyProfile("fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile fast: %v", err)
	}
	if got := *treedbOuterLeafBlockCacheEntries; got != 16384 {
		t.Fatalf("expected fast profile v2_fenceptr default cache entries=16384, got %d", got)
	}

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v2_fenceptr"
	if err := applyProfile("wal_on_fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile wal_on_fast: %v", err)
	}
	if got := *treedbOuterLeafBlockCacheEntries; got != 16384 {
		t.Fatalf("expected wal_on_fast profile v2_fenceptr default cache entries=16384, got %d", got)
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

func TestApplyProfile_FastRespectsExplicitOuterLeafCacheOverride(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbOuterLeafBlockCacheEntries = 256
	explicitFlags = map[string]bool{
		"treedb-outer-leaf-block-cache-entries": true,
	}
	if err := applyProfile("fast", explicitFlags); err != nil {
		t.Fatalf("applyProfile fast: %v", err)
	}
	if got := *treedbOuterLeafBlockCacheEntries; got != 256 {
		t.Fatalf("expected explicit outer leaf cache override to remain 256, got %d", got)
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
	if !opts.ValueLog.ForcePointers || !opts.LeafPrefixCompression || !opts.IndexPackedValuePtr || !opts.IndexInternalBaseDelta {
		t.Fatalf("expected composite optimization settings to apply to non-overridden fields")
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
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected explicit composite=false to disable non-explicit force pointers")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected explicit per-flag override to keep IndexPackedValuePtr=true")
	}
	if opts.LeafPrefixCompression || opts.IndexColumnarLeaves || opts.IndexInternalBaseDelta {
		t.Fatalf("expected explicit composite=false to disable remaining optimization fields")
	}
}

func TestBuildTreeDBOptions_WALFenceMode_DefaultAndOverride(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions default: %v", err)
	}
	if got := opts.ValueLog.WALFenceMode; got != "rid_join" {
		t.Fatalf("expected default WAL fence mode rid_join, got %q", got)
	}
	if got := rep.formatText(""); !strings.Contains(got, "vlog.wal_fence_mode=rid_join") {
		t.Fatalf("resolved options missing default WAL fence mode: %q", got)
	}

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v2_fenceptr"
	*treedbWALFenceMode = "simple_inline"
	opts, rep, err = buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions simple_inline: %v", err)
	}
	if got := opts.ValueLog.WALFenceMode; got != "simple_inline" {
		t.Fatalf("expected WAL fence mode simple_inline, got %q", got)
	}
	if got := rep.formatText(""); !strings.Contains(got, "vlog.wal_fence_mode=simple_inline") {
		t.Fatalf("resolved options missing simple_inline mode: %q", got)
	}
}

func TestBuildTreeDBOptions_WALFenceMode_V2FencePtrWALOn_AutoSimpleInline(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v2_fenceptr"
	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions v2_fenceptr WAL-on: %v", err)
	}
	if got := opts.ValueLog.WALFenceMode; got != "simple_inline" {
		t.Fatalf("expected WAL-enabled v2_fenceptr to auto-select simple_inline, got %q", got)
	}
	formatted := rep.formatText("")
	if !strings.Contains(formatted, "vlog.wal_fence_mode=simple_inline") {
		t.Fatalf("resolved options missing auto-selected simple_inline mode: %q", formatted)
	}
	if !strings.Contains(formatted, "v2_fenceptr with WAL enabled defaults vlog.wal_fence_mode=simple_inline") {
		t.Fatalf("resolved options missing WAL-on v2_fenceptr auto-select note: %q", formatted)
	}
}

func TestBuildTreeDBOptions_WALOnFastV2FencePtr_UsesWALOnAndSimpleInline(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	if err := applyProfile("wal_on_fast", map[string]bool{}); err != nil {
		t.Fatalf("applyProfile wal_on_fast: %v", err)
	}
	*treedbIndexOuterLeafMode = "v2_fenceptr"
	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions wal_on_fast v2_fenceptr: %v", err)
	}
	if got := opts.Durability; got != treedb.DurabilityWALOnRelaxed {
		t.Fatalf("expected wal_on_fast durability to keep WAL enabled (WALOnRelaxed), got %v", got)
	}
	if got := opts.ValueLog.WALFenceMode; got != "simple_inline" {
		t.Fatalf("expected wal_on_fast v2_fenceptr to resolve WAL fence mode simple_inline, got %q", got)
	}
	formatted := rep.formatText("")
	if !strings.Contains(formatted, "durability=wal_on_relaxed") {
		t.Fatalf("resolved options missing wal_on_relaxed durability: %q", formatted)
	}
	if !strings.Contains(formatted, "vlog.wal_fence_mode=simple_inline") {
		t.Fatalf("resolved options missing simple_inline WAL fence mode: %q", formatted)
	}
}

func TestBuildTreeDBOptions_WALFenceMode_V2FencePtrWALOn_ExplicitRIDJoinAllowed(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v2_fenceptr"
	*treedbWALFenceMode = "rid_join"
	explicitFlags = map[string]bool{
		"treedb-wal-fence-mode": true,
	}
	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("expected explicit rid_join to be accepted, got %v", err)
	}
	if got := opts.ValueLog.WALFenceMode; got != "rid_join" {
		t.Fatalf("expected explicit rid_join to be preserved, got %q", got)
	}
	formatted := rep.formatText("")
	if !strings.Contains(formatted, "vlog.wal_fence_mode=rid_join") {
		t.Fatalf("resolved options missing rid_join WAL fence mode: %q", formatted)
	}
}

func TestBuildTreeDBOptions_WALFenceMode_V2FencePtrWALOff_AllowsRIDJoin(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v2_fenceptr"
	*treedbWALFenceMode = "rid_join"
	*treedbAllowUnsafe = true
	*treedbDisableWAL = true
	explicitFlags = map[string]bool{
		"treedb-wal-fence-mode": true,
		"treedb-disable-wal":    true,
		"treedb-allow-unsafe":   true,
	}
	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions WAL-off v2_fenceptr rid_join: %v", err)
	}
	if got := opts.ValueLog.WALFenceMode; got != "rid_join" {
		t.Fatalf("expected WAL-off v2_fenceptr to preserve explicit rid_join, got %q", got)
	}
}

func TestBuildTreeDBOptions_WALFenceMode_InvalidRejected(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbWALFenceMode = "bad_mode"
	if _, _, err := buildTreeDBOptions(""); err == nil {
		t.Fatalf("expected invalid wal fence mode to fail")
	}
}

func TestBuildTreeDBOptions_WALFenceMode_V1LeafLog_RejectsSimpleInline(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v1_leaflog"
	*treedbWALFenceMode = "simple_inline"
	explicitFlags = map[string]bool{
		"treedb-wal-fence-mode": true,
	}
	if _, _, err := buildTreeDBOptions(""); err == nil {
		t.Fatalf("expected simple_inline with v1_leaflog to fail")
	}
}

func TestBuildTreeDBOptions_WALFenceMode_V1LeafLogLegacy_RejectsSimpleInline(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbIndexOuterLeafMode = "v1_leaflog_legacy"
	*treedbWALFenceMode = "simple_inline"
	explicitFlags = map[string]bool{
		"treedb-wal-fence-mode": true,
	}
	if _, _, err := buildTreeDBOptions(""); err == nil {
		t.Fatalf("expected simple_inline with v1_leaflog_legacy to fail")
	}
}

func TestBuildTreeDBOptions_OuterLeafBlobThresholdFlag(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	*treedbOuterLeafBlobThresholdBytes = 32768
	explicitFlags = map[string]bool{
		"treedb-vlog-outer-leaf-blob-threshold-bytes": true,
	}
	opts, rep, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions: %v", err)
	}
	if got, want := opts.ValueLog.OuterLeafBlobThresholdBytes, 32768; got != want {
		t.Fatalf("OuterLeafBlobThresholdBytes=%d want=%d", got, want)
	}
	if got := rep.formatText(""); !strings.Contains(got, "vlog.outer_leaf_blob_threshold_bytes=32768B") {
		t.Fatalf("resolved options missing blob threshold: %q", got)
	}
}
