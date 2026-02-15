package main

import (
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
	walFenceEncoding   string
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
		walFenceEncoding:   *treedbWALFenceGroupEncoding,
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
	*treedbWALFenceGroupEncoding = s.walFenceEncoding
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
	*treedbWALFenceGroupEncoding = treedb.ValueLogWALFenceGroupEncodingSimple
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

func TestBuildTreeDBOptions_WALFenceGroupEncoding_DefaultAndOverride(t *testing.T) {
	saved := saveTreeDBFlagState()
	defer restoreTreeDBFlagState(saved)

	resetTreeDBIndexFlagsForTest()
	opts, _, err := buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions default: %v", err)
	}
	if got := opts.ValueLog.WALFenceGroupEncoding; got != treedb.ValueLogWALFenceGroupEncodingSimple {
		t.Fatalf("default WAL fence group encoding=%q want %q", got, treedb.ValueLogWALFenceGroupEncodingSimple)
	}

	*treedbWALFenceGroupEncoding = treedb.ValueLogWALFenceGroupEncodingPrefix
	opts, _, err = buildTreeDBOptions("")
	if err != nil {
		t.Fatalf("buildTreeDBOptions prefix: %v", err)
	}
	if got := opts.ValueLog.WALFenceGroupEncoding; got != treedb.ValueLogWALFenceGroupEncodingPrefix {
		t.Fatalf("override WAL fence group encoding=%q want %q", got, treedb.ValueLogWALFenceGroupEncodingPrefix)
	}
}
