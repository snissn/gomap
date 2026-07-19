package db

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCompactStorageM0FixtureCatalogHasStableRequiredFields(t *testing.T) {
	want := map[string]bool{
		"one-generation-per-pass": true, "full-default": true, "full-low-debt": true,
		"full-high-debt": true, "exhaustive-control": true, "foreground-writes": true,
	}
	seen := make(map[string]bool)
	for _, fixture := range compactStorageM0Fixtures {
		if fixture.Name == "" || fixture.Seed == 0 || fixture.KeyDistribution == "" ||
			fixture.ValueDistribution == "" || fixture.ValueLogPointerThreshold == 0 ||
			fixture.WALMode == "" || fixture.ExpectedMaintenanceWork == "" {
			t.Fatalf("fixture has incomplete required metadata: %+v", fixture)
		}
		if seen[fixture.Name] {
			t.Fatalf("duplicate fixture %q", fixture.Name)
		}
		seen[fixture.Name] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("fixture count=%d want=%d", len(seen), len(want))
	}
	for name := range want {
		if !seen[name] {
			t.Fatalf("missing fixture %q", name)
		}
	}
}

func TestNewCompactStorageMeasurementSeparatesTimedApplyAndWorkCounters(t *testing.T) {
	stats := CompactStorageStats{
		Audit: CompactStorageAuditStats{SharedScans: 2},
		LeafGenerationPacks: []LeafGenerationPackRunOnceStats{{
			Ran:       true,
			Selection: LeafGenerationPackSelection{GenerationIDs: []uint64{1}, BytesToCopy: 90},
			Pack: LeafGenerationPackStats{
				GenerationsMatched: 1, CreatedFileIDs: []uint32{7}, LeafPagesCopied: 3,
				BytesCopied: 80, LeafFramesWritten: 2, PublishHoldNanos: 11,
				ApplyStages: LeafGenerationPackApplyStageStats{TreeRewriteTimeNanos: 9},
			},
		}},
		LeafGenerationGC: LeafGenerationGCStats{GenerationsDeleted: 1, BytesDeleted: 100},
		Phases: []CompactStoragePhaseStats{
			{Name: "leaf-generation-pack-1", WallTimeNanos: 17},
			{Name: "index-vacuum", Skipped: true, SkipReason: "unsupported", WallTimeNanos: 5},
		},
	}
	m := newCompactStorageMeasurement(compactStorageM0Fixtures[0], "artifact", 123, stats, nil)
	if m.TotalWallTimeNanos != 123 || m.ApplyWallTimeNanos != 22 {
		t.Fatalf("timing boundary=%+v", m)
	}
	if len(m.Phases) != 2 || m.Phases[1].SkipReason != "unsupported" {
		t.Fatalf("phases=%+v", m.Phases)
	}
	if m.LeafPack.Runs != 1 || m.LeafPack.GenerationsPublished != 1 ||
		m.LeafPack.BytesPlanned != 90 || m.LeafPack.ReclaimedBytes != 100 {
		t.Fatalf("leaf pack=%+v", m.LeafPack)
	}
	if !m.Vacuum.Attempted || m.Vacuum.Ran || m.Vacuum.SkipReason != "unsupported" ||
		m.Vacuum.Availability != compactStorageMeasurementObserved {
		t.Fatalf("vacuum=%+v", m.Vacuum)
	}
}

func TestCompactStorageMeasurementJSONSchemaIsDeterministic(t *testing.T) {
	stats := CompactStorageStats{Phases: []CompactStoragePhaseStats{{Name: "leaf-generation-pack-1", WallTimeNanos: 7}}}
	first, err := json.Marshal(newCompactStorageMeasurement(compactStorageM0Fixtures[0], "artifact", 9, stats, nil))
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(newCompactStorageMeasurement(compactStorageM0Fixtures[0], "artifact", 9, stats, nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("non-deterministic JSON:\n%s\n%s", first, second)
	}
	for _, key := range []string{
		"schema_version", "fixture", "total_wall_time_nanos", "apply_wall_time_nanos",
		"leaf_pack", "value_log", "stable_calls", "checkpoints", "vacuum", "foreground_writes",
	} {
		if !containsJSONKey(first, key) {
			t.Fatalf("missing schema key %q: %s", key, first)
		}
	}
}

func containsJSONKey(raw []byte, key string) bool {
	return json.Valid(raw) && bytes.Contains(raw, []byte("\""+key+"\""))
}

func TestCompactStorageM0ArtifactNameParser(t *testing.T) {
	name := compactStorageM0ArtifactName("full-default", 2)
	if name != filepath.Join("compact-storage-m0", "full-default", "sample-3.json") {
		t.Fatalf("artifact name=%q", name)
	}
	fixture, sample, err := parseCompactStorageM0ArtifactName(name)
	if err != nil || fixture != "full-default" || sample != 3 {
		t.Fatalf("parsed=(%q,%d,%v)", fixture, sample, err)
	}
	for _, invalid := range []string{"full-default/sample-1.json", "compact-storage-m0/full-default/seed=1", "compact-storage-m0/full-default/sample-0.json"} {
		if _, _, err := parseCompactStorageM0ArtifactName(invalid); err == nil {
			t.Fatalf("accepted invalid artifact name %q", invalid)
		}
	}
}

func TestCompactStorageM0AllocsProfilePathsAreStable(t *testing.T) {
	root := t.TempDir()
	before := compactStorageM0AllocsProfilePath(root, "one-generation-per-pass", "before")
	after := compactStorageM0AllocsProfilePath(root, "one-generation-per-pass", "after")
	if before != filepath.Join(root, "allocs_one-generation-per-pass_before.pprof") {
		t.Fatalf("before path=%q", before)
	}
	if after != filepath.Join(root, "allocs_one-generation-per-pass_after.pprof") {
		t.Fatalf("after path=%q", after)
	}
}

func TestWriteCompactStorageM0ArtifactPersistsCanonicalJSON(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TREEDB_COMPACT_STORAGE_M0_ARTIFACT_DIR", root)
	measurement := newCompactStorageMeasurement(
		compactStorageM0Fixtures[0], compactStorageM0ArtifactName("one-generation-per-pass", 0),
		7, CompactStorageStats{}, nil,
	)
	if err := writeCompactStorageM0Artifact(measurement); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, measurement.ArtifactName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var decoded compactStorageMeasurement
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != 1 || decoded.ArtifactName != measurement.ArtifactName {
		t.Fatalf("artifact=%+v", decoded)
	}
}

func TestCompactStorageM0StableRecorderClassifiesPhaseResourceAndCall(t *testing.T) {
	r := newCompactStorageM0StableRecorder(nil)
	r.beginPhase("checkpoint")
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.BeforeIndexDataSync, Resource: durabilitycut.ResourceIndex}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Microsecond)
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.AfterIndexDataSync, Resource: durabilitycut.ResourceIndex}); err != nil {
		t.Fatal(err)
	}
	r.endPhase("checkpoint")
	calls := r.measurements()
	if r.totalCalls() != 1 || len(calls) != 1 || calls[0].Phase != "checkpoint" ||
		calls[0].Resource != "index" || calls[0].CallType != "index-stable" ||
		calls[0].WallTimeNanos <= 0 || calls[0].UnmatchedStarts != 0 || calls[0].UnmatchedFinishes != 0 {
		t.Fatalf("calls=%+v total=%d", calls, r.totalCalls())
	}
	checkpoints := r.checkpointMeasurements([]CompactStoragePhaseStats{{Name: "checkpoint", WallTimeNanos: 17}})
	if len(checkpoints) != 1 || checkpoints[0].Availability != compactStorageMeasurementUnavailable ||
		checkpoints[0].StableCalls != 1 || checkpoints[0].WallTimeNanos != 17 {
		t.Fatalf("checkpoints=%+v", checkpoints)
	}
}

func TestCompactStorageM0LatencyPercentiles(t *testing.T) {
	got := compactStorageMeasurementLatencyFor([]time.Duration{5, 1, 4, 2, 3})
	if got.Count != 5 || got.P50 != 3 || got.P95 != 5 || got.P99 != 5 || got.Max != 5 {
		t.Fatalf("latency=%+v", got)
	}
}
