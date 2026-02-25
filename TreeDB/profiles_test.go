package treedb

import (
	"testing"
	"time"
)

func TestOptionsFor_ProfileSetsDir(t *testing.T) {
	opts := OptionsFor(ProfileDurable, "/tmp/treedb-profiles-test")
	if opts.Dir != "/tmp/treedb-profiles-test" {
		t.Fatalf("Dir mismatch: got %q", opts.Dir)
	}
}

func TestApplyProfile_FastSetsPolicyBools(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileFast)

	if opts.Durability != DurabilityWALOffRelaxed {
		t.Fatalf("expected DurabilityWALOffRelaxed for fast profile, got %v", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity != IntegritySkipChecksums {
		t.Fatalf("expected IntegritySkipChecksums for fast profile, got %v", opts.ValueLog.ReadIntegrity)
	}
	if !opts.PreferAppendAlloc {
		t.Fatalf("expected PreferAppendAlloc=true for fast profile")
	}
	if !opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=true for fast profile")
	}
	if !opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=true for fast profile")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=true for fast profile")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=true for fast profile")
	}
	if !opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=true for fast profile")
	}
}

func TestApplyProfile_WALOnFastKeepsWALOn(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileWALOnFast)

	if opts.Durability != DurabilityWALOnRelaxed {
		t.Fatalf("expected DurabilityWALOnRelaxed for wal_on_fast profile, got %v", opts.Durability)
	}
	if opts.ValueLog.ReadIntegrity != IntegritySkipChecksums {
		t.Fatalf("expected IntegritySkipChecksums for wal_on_fast profile, got %v", opts.ValueLog.ReadIntegrity)
	}
	if !opts.PreferAppendAlloc {
		t.Fatalf("expected PreferAppendAlloc=true for wal_on_fast profile")
	}
	if !opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=true for wal_on_fast profile")
	}
	if !opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=true for wal_on_fast profile")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=true for wal_on_fast profile")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=true for wal_on_fast profile")
	}
	if !opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=true for wal_on_fast profile")
	}
}

func TestApplyProfile_FastAndWALOnFast_V2FencePtrOuterLeafCacheDefault(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := Options{IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr}
			ApplyProfile(&opts, profile)
			if got := opts.ValueLog.OuterLeafBlockCacheEntries; got != 16384 {
				t.Fatalf("expected v2_fenceptr profile default OuterLeafBlockCacheEntries=16384, got %d", got)
			}
		})
	}
}

func TestApplyProfile_V2FencePtrPreservesExplicitOuterLeafCacheEntries(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := Options{
				IndexOuterLeafMode: IndexOuterLeafModeV2FencePtr,
				ValueLog: ValueLogOptions{
					OuterLeafBlockCacheEntries: 4096,
				},
			}
			ApplyProfile(&opts, profile)
			if got := opts.ValueLog.OuterLeafBlockCacheEntries; got != 4096 {
				t.Fatalf("expected explicit OuterLeafBlockCacheEntries to be preserved, got %d", got)
			}
		})
	}
}

func TestApplyProfile_NonV2FencePtrLeavesOuterLeafCacheEntriesUnset(t *testing.T) {
	modes := []struct {
		name string
		mode string
	}{
		{name: "v1", mode: IndexOuterLeafModeV1},
		{name: "v2_blockptr", mode: IndexOuterLeafModeV2BlockPtr},
	}

	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		for _, tc := range modes {
			t.Run(string(profile)+"_"+tc.name, func(t *testing.T) {
				opts := Options{IndexOuterLeafMode: tc.mode}
				ApplyProfile(&opts, profile)
				if got := opts.ValueLog.OuterLeafBlockCacheEntries; got != 0 {
					t.Fatalf("expected non-v2_fenceptr mode %q to keep OuterLeafBlockCacheEntries=0, got %d", tc.mode, got)
				}
			})
		}
	}
}

func TestApplyProfile_EmptyOuterLeafModeUsesV2FencePtrCacheDefault(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		t.Run(string(profile), func(t *testing.T) {
			var opts Options
			ApplyProfile(&opts, profile)
			if got := opts.ValueLog.OuterLeafBlockCacheEntries; got != 16384 {
				t.Fatalf("expected empty mode to use v2_fenceptr default OuterLeafBlockCacheEntries=16384, got %d", got)
			}
		})
	}
}

func TestApplyProfile_BenchDisablesBackgroundDefaults(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileBench)

	if opts.BackgroundCheckpointInterval >= 0 {
		t.Fatalf("expected BackgroundCheckpointInterval < 0 for bench profile, got %v", opts.BackgroundCheckpointInterval)
	}
	if opts.BackgroundCheckpointIdleDuration >= 0 {
		t.Fatalf("expected BackgroundCheckpointIdleDuration < 0 for bench profile, got %v", opts.BackgroundCheckpointIdleDuration)
	}
	if opts.MaxWALBytes >= 0 {
		t.Fatalf("expected MaxWALBytes < 0 for bench profile, got %d", opts.MaxWALBytes)
	}
	if opts.BackgroundValueLogGCInterval >= 0 {
		t.Fatalf("expected BackgroundValueLogGCInterval < 0 for bench profile, got %v", opts.BackgroundValueLogGCInterval)
	}
	if opts.BackgroundValueLogRewriteInterval >= 0 {
		t.Fatalf("expected BackgroundValueLogRewriteInterval < 0 for bench profile, got %v", opts.BackgroundValueLogRewriteInterval)
	}
	if !opts.DisableBackgroundPrune {
		t.Fatalf("expected DisableBackgroundPrune=true for bench profile")
	}
	if !opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=true for bench profile")
	}
	if !opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=true for bench profile")
	}
	if !opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=true for bench profile")
	}
	if !opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=true for bench profile")
	}
	if !opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=true for bench profile")
	}
}

func TestApplyProfile_DurableKeepsIndexOptimizationsDisabled(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileDurable)
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for durable profile")
	}
	if opts.LeafPrefixCompression {
		t.Fatalf("expected LeafPrefixCompression=false for durable profile")
	}
	if opts.IndexColumnarLeaves {
		t.Fatalf("expected IndexColumnarLeaves=false for durable profile")
	}
	if opts.IndexPackedValuePtr {
		t.Fatalf("expected IndexPackedValuePtr=false for durable profile")
	}
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for durable profile")
	}
}

func TestApplyProfile_DoesNotOverrideNonZeroNumericFields(t *testing.T) {
	opts := Options{
		BackgroundCheckpointInterval:     7 * time.Second,
		BackgroundCheckpointIdleDuration: 3 * time.Second,
		MaxWALBytes:                      123,
	}
	ApplyProfile(&opts, ProfileBench)

	if opts.BackgroundCheckpointInterval != 7*time.Second {
		t.Fatalf("BackgroundCheckpointInterval overridden: got %v", opts.BackgroundCheckpointInterval)
	}
	if opts.BackgroundCheckpointIdleDuration != 3*time.Second {
		t.Fatalf("BackgroundCheckpointIdleDuration overridden: got %v", opts.BackgroundCheckpointIdleDuration)
	}
	if opts.MaxWALBytes != 123 {
		t.Fatalf("MaxWALBytes overridden: got %d", opts.MaxWALBytes)
	}
}

func TestApplyProfile_PreservesNegativeDictHoldProbeValues(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := Options{
				ValueLog: ValueLogOptions{
					DictIncompressibleHoldBytes: -1,
					DictProbeIntervalBytes:      -1,
				},
			}
			ApplyProfile(&opts, profile)

			if opts.ValueLog.DictIncompressibleHoldBytes != -1 {
				t.Fatalf("DictIncompressibleHoldBytes overridden: got %d", opts.ValueLog.DictIncompressibleHoldBytes)
			}
			if opts.ValueLog.DictProbeIntervalBytes != -1 {
				t.Fatalf("DictProbeIntervalBytes overridden: got %d", opts.ValueLog.DictProbeIntervalBytes)
			}
		})
	}
}
