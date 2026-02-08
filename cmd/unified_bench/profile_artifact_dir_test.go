package main

import (
	"os"
	"path/filepath"
	"testing"
)

type profilingFlagSnapshot struct {
	cpu        string
	allocs     string
	checkpoint string
	block      string
	mutex      string
	trace      string
}

func snapshotProfilingFlags() profilingFlagSnapshot {
	return profilingFlagSnapshot{
		cpu:        *cpuProfile,
		allocs:     *allocsProfile,
		checkpoint: *checkpointCPUProfile,
		block:      *blockProfile,
		mutex:      *mutexProfile,
		trace:      *traceProfile,
	}
}

func restoreProfilingFlags(s profilingFlagSnapshot) {
	*cpuProfile = s.cpu
	*allocsProfile = s.allocs
	*checkpointCPUProfile = s.checkpoint
	*blockProfile = s.block
	*mutexProfile = s.mutex
	*traceProfile = s.trace
}

func TestApplyProfileArtifactDir_SetsAllProfilingOutputs(t *testing.T) {
	snap := snapshotProfilingFlags()
	defer restoreProfilingFlags(snap)

	*cpuProfile = ""
	*allocsProfile = ""
	*checkpointCPUProfile = ""
	*blockProfile = ""
	*mutexProfile = ""
	*traceProfile = ""

	dir := filepath.Join(t.TempDir(), "profiles", "run1")
	if err := applyProfileArtifactDir(dir, map[string]bool{}); err != nil {
		t.Fatalf("applyProfileArtifactDir: %v", err)
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("expected profile dir to exist: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("expected %q to be a directory", dir)
	}

	if got, want := *cpuProfile, filepath.Join(dir, "cpu"); got != want {
		t.Fatalf("cpuprofile = %q, want %q", got, want)
	}
	if got, want := *allocsProfile, filepath.Join(dir, "allocs"); got != want {
		t.Fatalf("allocsprofile = %q, want %q", got, want)
	}
	if got, want := *checkpointCPUProfile, filepath.Join(dir, "checkpoint_cpu"); got != want {
		t.Fatalf("checkpoint-cpuprofile = %q, want %q", got, want)
	}
	if got, want := *blockProfile, filepath.Join(dir, "block.pprof"); got != want {
		t.Fatalf("blockprofile = %q, want %q", got, want)
	}
	if got, want := *mutexProfile, filepath.Join(dir, "mutex.pprof"); got != want {
		t.Fatalf("mutexprofile = %q, want %q", got, want)
	}
	if got, want := *traceProfile, filepath.Join(dir, "trace.out"); got != want {
		t.Fatalf("trace = %q, want %q", got, want)
	}
}

func TestApplyProfileArtifactDir_RespectsExplicitFlags(t *testing.T) {
	snap := snapshotProfilingFlags()
	defer restoreProfilingFlags(snap)

	*cpuProfile = "manual/cpu"
	*allocsProfile = "manual/allocs"
	*checkpointCPUProfile = "manual/checkpoint"
	*blockProfile = "manual/block.pprof"
	*mutexProfile = ""
	*traceProfile = ""

	dir := filepath.Join(t.TempDir(), "profiles")
	isSet := map[string]bool{
		"cpuprofile":            true,
		"allocsprofile":         true,
		"checkpoint-cpuprofile": true,
		"blockprofile":          true,
	}
	if err := applyProfileArtifactDir(dir, isSet); err != nil {
		t.Fatalf("applyProfileArtifactDir: %v", err)
	}

	if got, want := *cpuProfile, "manual/cpu"; got != want {
		t.Fatalf("cpuprofile = %q, want %q", got, want)
	}
	if got, want := *allocsProfile, "manual/allocs"; got != want {
		t.Fatalf("allocsprofile = %q, want %q", got, want)
	}
	if got, want := *checkpointCPUProfile, "manual/checkpoint"; got != want {
		t.Fatalf("checkpoint-cpuprofile = %q, want %q", got, want)
	}
	if got, want := *blockProfile, "manual/block.pprof"; got != want {
		t.Fatalf("blockprofile = %q, want %q", got, want)
	}
	if got, want := *mutexProfile, filepath.Join(dir, "mutex.pprof"); got != want {
		t.Fatalf("mutexprofile = %q, want %q", got, want)
	}
	if got, want := *traceProfile, filepath.Join(dir, "trace.out"); got != want {
		t.Fatalf("trace = %q, want %q", got, want)
	}
}
