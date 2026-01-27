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
