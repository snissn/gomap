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

func TestCollectionStorageProfileArtifactPaths(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	cfg := BenchConfig{
		CPUProfile:           filepath.Join(dir, "cpu"),
		AllocsProfile:        filepath.Join(dir, "allocs"),
		CheckpointCPUProfile: filepath.Join(dir, "checkpoint_cpu"),
		BlockProfile:         filepath.Join(dir, "block.pprof"),
		MutexProfile:         filepath.Join(dir, "mutex.pprof"),
		TraceProfile:         filepath.Join(dir, "trace.out"),
	}
	paths := collectionStorageArtifactPathsForProfileDir(dir, cfg, true)
	want := map[string]string{
		"collection_json":         filepath.Join(dir, "collection_storage_results.json"),
		"collection_markdown":     filepath.Join(dir, "collection_storage_results.md"),
		"collection_html":         filepath.Join(dir, "collection_storage_results.html"),
		"benchprof_json":          filepath.Join(dir, "benchprof_results.json"),
		"benchprof_markdown":      filepath.Join(dir, "benchprof_results.md"),
		"cpu":                     filepath.Join(dir, "cpu_collection_storage_treedb_collection_storage.pprof"),
		"allocs":                  filepath.Join(dir, "allocs_collection_storage_treedb_collection_storage.pprof"),
		"checkpoint_cpu":          filepath.Join(dir, "checkpoint_cpu_checkpoint_collection_storage_treedb_collection_storage.pprof"),
		"block":                   filepath.Join(dir, "block.pprof"),
		"mutex":                   filepath.Join(dir, "mutex.pprof"),
		"trace":                   filepath.Join(dir, "trace.out"),
		"benchprof_insights_md":   filepath.Join(dir, "insights.md"),
		"benchprof_insights_json": filepath.Join(dir, "insights.json"),
		"benchprof_insights_html": filepath.Join(dir, "insights.html"),
	}
	got := map[string]string{
		"collection_json":         paths.CollectionJSON,
		"collection_markdown":     paths.CollectionMarkdown,
		"collection_html":         paths.CollectionHTML,
		"benchprof_json":          paths.BenchprofJSON,
		"benchprof_markdown":      paths.BenchprofMarkdown,
		"cpu":                     paths.CPUProfile,
		"allocs":                  paths.AllocsProfile,
		"checkpoint_cpu":          paths.CheckpointCPUProfile,
		"block":                   paths.BlockProfile,
		"mutex":                   paths.MutexProfile,
		"trace":                   paths.TraceProfile,
		"benchprof_insights_md":   paths.InsightsMarkdown,
		"benchprof_insights_json": paths.InsightsJSON,
		"benchprof_insights_html": paths.InsightsHTML,
	}
	for key, wantPath := range want {
		if got[key] != wantPath {
			t.Fatalf("%s path=%q want %q", key, got[key], wantPath)
		}
	}
}

func TestContentionProfilePath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "run")
	got := contentionProfilePath(filepath.Join(dir, "block.pprof"), "block", "random_read_batch", "TreeDB (vlog=off)")
	want := filepath.Join(dir, "block_random_read_batch_treedb__vlog_off_.pprof")
	if got != want {
		t.Fatalf("contentionProfilePath = %q, want %q", got, want)
	}
}
