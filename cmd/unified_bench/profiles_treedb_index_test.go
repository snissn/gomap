package main

import (
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

type savedTreeDBFlagState struct {
	indexOptimizations bool
	forcePointers      bool
	leafPrefix         bool
	columnarLeaves     bool
	packedValuePtr     bool
	internalBaseDelta  bool
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
		forcePointers:      *treedbForceValuePointers,
		leafPrefix:         *treedbLeafPrefixCompression,
		columnarLeaves:     *treedbIndexColumnarLeaves,
		packedValuePtr:     *treedbIndexPackedValuePtr,
		internalBaseDelta:  *treedbIndexInternalBaseDelta,
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
	*treedbForceValuePointers = s.forcePointers
	*treedbLeafPrefixCompression = s.leafPrefix
	*treedbIndexColumnarLeaves = s.columnarLeaves
	*treedbIndexPackedValuePtr = s.packedValuePtr
	*treedbIndexInternalBaseDelta = s.internalBaseDelta
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
	*treedbForceValuePointers = false
	*treedbLeafPrefixCompression = false
	*treedbIndexColumnarLeaves = false
	*treedbIndexPackedValuePtr = false
	*treedbIndexInternalBaseDelta = false
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
