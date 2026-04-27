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
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected IndexOuterLeavesInValueLog=true for fast profile")
	}
	if !opts.PreferAppendAlloc {
		t.Fatalf("expected PreferAppendAlloc=true for fast profile")
	}
	if opts.ValueLog.Compression != ValueLogCompressionAuto {
		t.Fatalf("expected ValueLog.Compression=ValueLogCompressionAuto for fast profile, got %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.BlockCodec != ValueLogBlockSnappy {
		t.Fatalf("expected ValueLog.BlockCodec=ValueLogBlockSnappy for fast profile, got %v", opts.ValueLog.BlockCodec)
	}
	if opts.ValueLog.AutoPolicy != ValueLogAutoBalanced {
		t.Fatalf("expected ValueLog.AutoPolicy=ValueLogAutoBalanced for fast profile, got %v", opts.ValueLog.AutoPolicy)
	}
	if opts.ValueLog.CompressionAutotune.Mode != AutotuneMedium {
		t.Fatalf("expected ValueLog.CompressionAutotune.Mode=AutotuneMedium for fast profile, got %v", opts.ValueLog.CompressionAutotune.Mode)
	}
	if opts.ValueLog.DictIncompressibleHoldBytes != 64<<20 {
		t.Fatalf("expected DictIncompressibleHoldBytes=64MiB for fast profile, got %d", opts.ValueLog.DictIncompressibleHoldBytes)
	}
	if opts.ValueLog.DictProbeIntervalBytes != 32<<20 {
		t.Fatalf("expected DictProbeIntervalBytes=32MiB for fast profile, got %d", opts.ValueLog.DictProbeIntervalBytes)
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for fast profile")
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
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for fast profile (incompatible with outer leaves in value log)")
	}
	if opts.PagerSyncConcurrency != 4 {
		t.Fatalf("expected PagerSyncConcurrency=4 for fast profile, got %d", opts.PagerSyncConcurrency)
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
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected IndexOuterLeavesInValueLog=true for wal_on_fast profile")
	}
	if !opts.PreferAppendAlloc {
		t.Fatalf("expected PreferAppendAlloc=true for wal_on_fast profile")
	}
	if opts.ValueLog.Compression != ValueLogCompressionAuto {
		t.Fatalf("expected ValueLog.Compression=ValueLogCompressionAuto for wal_on_fast profile, got %v", opts.ValueLog.Compression)
	}
	if opts.ValueLog.BlockCodec != ValueLogBlockSnappy {
		t.Fatalf("expected ValueLog.BlockCodec=ValueLogBlockSnappy for wal_on_fast profile, got %v", opts.ValueLog.BlockCodec)
	}
	if opts.ValueLog.AutoPolicy != ValueLogAutoBalanced {
		t.Fatalf("expected ValueLog.AutoPolicy=ValueLogAutoBalanced for wal_on_fast profile, got %v", opts.ValueLog.AutoPolicy)
	}
	if opts.ValueLog.CompressionAutotune.Mode != AutotuneMedium {
		t.Fatalf("expected ValueLog.CompressionAutotune.Mode=AutotuneMedium for wal_on_fast profile, got %v", opts.ValueLog.CompressionAutotune.Mode)
	}
	if opts.ValueLog.DictIncompressibleHoldBytes != 64<<20 {
		t.Fatalf("expected DictIncompressibleHoldBytes=64MiB for wal_on_fast profile, got %d", opts.ValueLog.DictIncompressibleHoldBytes)
	}
	if opts.ValueLog.DictProbeIntervalBytes != 32<<20 {
		t.Fatalf("expected DictProbeIntervalBytes=32MiB for wal_on_fast profile, got %d", opts.ValueLog.DictProbeIntervalBytes)
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for wal_on_fast profile")
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
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for wal_on_fast profile (incompatible with outer leaves in value log)")
	}
	if opts.PagerSyncConcurrency != 4 {
		t.Fatalf("expected PagerSyncConcurrency=4 for wal_on_fast profile, got %d", opts.PagerSyncConcurrency)
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
	if !opts.DisableBackgroundPrune {
		t.Fatalf("expected DisableBackgroundPrune=true for bench profile")
	}
	if opts.ValueLog.ForcePointers {
		t.Fatalf("expected ValueLog.ForcePointers=false for bench profile")
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
	if opts.IndexInternalBaseDelta {
		t.Fatalf("expected IndexInternalBaseDelta=false for bench profile (incompatible with outer leaves in value log)")
	}
}

func TestApplyProfile_DurableKeepsIndexOptimizationsDisabled(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileDurable)
	if !opts.IndexOuterLeavesInValueLog {
		t.Fatalf("expected IndexOuterLeavesInValueLog=true for durable profile")
	}
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
		PagerSyncConcurrency:             2,
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
	if opts.PagerSyncConcurrency != 2 {
		t.Fatalf("PagerSyncConcurrency overridden: got %d", opts.PagerSyncConcurrency)
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

func TestApplyProfile_PreservesExplicitVLogCompressionOverrides(t *testing.T) {
	for _, profile := range []Profile{ProfileFast, ProfileWALOnFast} {
		t.Run(string(profile), func(t *testing.T) {
			opts := Options{
				ValueLog: ValueLogOptions{
					Compression: ValueLogCompressionBlock,
					BlockCodec:  ValueLogBlockLZ4,
					AutoPolicy:  ValueLogAutoSize,
					CompressionAutotune: AutotuneOptions{
						Mode: AutotuneAggressive,
					},
				},
			}
			ApplyProfile(&opts, profile)

			if opts.ValueLog.Compression != ValueLogCompressionBlock {
				t.Fatalf("ValueLog.Compression overridden: got %v", opts.ValueLog.Compression)
			}
			if opts.ValueLog.BlockCodec != ValueLogBlockLZ4 {
				t.Fatalf("ValueLog.BlockCodec overridden: got %v", opts.ValueLog.BlockCodec)
			}
			if opts.ValueLog.AutoPolicy != ValueLogAutoSize {
				t.Fatalf("ValueLog.AutoPolicy overridden: got %v", opts.ValueLog.AutoPolicy)
			}
			if opts.ValueLog.CompressionAutotune.Mode != AutotuneAggressive {
				t.Fatalf("ValueLog.CompressionAutotune overridden: got %v", opts.ValueLog.CompressionAutotune.Mode)
			}
		})
	}
}
