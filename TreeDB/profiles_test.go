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

	if !opts.DisableWAL {
		t.Fatalf("expected DisableWAL=true for fast profile")
	}
	if !opts.RelaxedSync {
		t.Fatalf("expected RelaxedSync=true for fast profile")
	}
	if !opts.DisableReadChecksum {
		t.Fatalf("expected DisableReadChecksum=true for fast profile")
	}
	if !opts.PreferAppendAlloc {
		t.Fatalf("expected PreferAppendAlloc=true for fast profile")
	}
}

func TestApplyProfile_BenchDisablesBackgroundDefaults(t *testing.T) {
	var opts Options
	ApplyProfile(&opts, ProfileBench)

	if opts.BackgroundIndexVacuumInterval >= 0 {
		t.Fatalf("expected BackgroundIndexVacuumInterval < 0 for bench profile, got %v", opts.BackgroundIndexVacuumInterval)
	}
	if opts.BackgroundCheckpointInterval >= 0 {
		t.Fatalf("expected BackgroundCheckpointInterval < 0 for bench profile, got %v", opts.BackgroundCheckpointInterval)
	}
	if opts.BackgroundCheckpointIdleDuration >= 0 {
		t.Fatalf("expected BackgroundCheckpointIdleDuration < 0 for bench profile, got %v", opts.BackgroundCheckpointIdleDuration)
	}
	if opts.MaxWALBytes >= 0 {
		t.Fatalf("expected MaxWALBytes < 0 for bench profile, got %d", opts.MaxWALBytes)
	}
	if opts.BackgroundCompactionInterval >= 0 {
		t.Fatalf("expected BackgroundCompactionInterval < 0 for bench profile, got %v", opts.BackgroundCompactionInterval)
	}
	if !opts.DisableBackgroundPrune {
		t.Fatalf("expected DisableBackgroundPrune=true for bench profile")
	}
}

func TestApplyProfile_DoesNotOverrideNonZeroNumericFields(t *testing.T) {
	opts := Options{
		BackgroundIndexVacuumInterval:         5 * time.Second,
		BackgroundCheckpointInterval:          7 * time.Second,
		BackgroundCheckpointIdleDuration:      3 * time.Second,
		MaxWALBytes:                           123,
		BackgroundCompactionInterval:          9 * time.Second,
		BackgroundIndexVacuumSpanRatioPPM:     999,
		BackgroundCompactionMicroBatch:        777,
		BackgroundCompactionDeadRatio:         0.33,
		BackgroundCompactionRotateBeforeWrite: true,
	}
	ApplyProfile(&opts, ProfileBench)

	if opts.BackgroundIndexVacuumInterval != 5*time.Second {
		t.Fatalf("BackgroundIndexVacuumInterval overridden: got %v", opts.BackgroundIndexVacuumInterval)
	}
	if opts.BackgroundCheckpointInterval != 7*time.Second {
		t.Fatalf("BackgroundCheckpointInterval overridden: got %v", opts.BackgroundCheckpointInterval)
	}
	if opts.BackgroundCheckpointIdleDuration != 3*time.Second {
		t.Fatalf("BackgroundCheckpointIdleDuration overridden: got %v", opts.BackgroundCheckpointIdleDuration)
	}
	if opts.MaxWALBytes != 123 {
		t.Fatalf("MaxWALBytes overridden: got %d", opts.MaxWALBytes)
	}
	if opts.BackgroundCompactionInterval != 9*time.Second {
		t.Fatalf("BackgroundCompactionInterval overridden: got %v", opts.BackgroundCompactionInterval)
	}
	// Sanity: unrelated numeric fields should remain unchanged too.
	if opts.BackgroundIndexVacuumSpanRatioPPM != 999 {
		t.Fatalf("BackgroundIndexVacuumSpanRatioPPM changed: got %d", opts.BackgroundIndexVacuumSpanRatioPPM)
	}
	if opts.BackgroundCompactionMicroBatch != 777 {
		t.Fatalf("BackgroundCompactionMicroBatch changed: got %d", opts.BackgroundCompactionMicroBatch)
	}
	if opts.BackgroundCompactionDeadRatio != 0.33 {
		t.Fatalf("BackgroundCompactionDeadRatio changed: got %v", opts.BackgroundCompactionDeadRatio)
	}
	if !opts.BackgroundCompactionRotateBeforeWrite {
		t.Fatalf("BackgroundCompactionRotateBeforeWrite changed: got false")
	}
}
