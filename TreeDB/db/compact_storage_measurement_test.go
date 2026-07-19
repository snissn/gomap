package db

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
)

func TestCompactStorageM0FixtureCatalogHasStableRequiredFields(t *testing.T) {
	want := map[string]bool{"one-generation-per-pass": true, "full-default": true, "full-low-debt": true, "full-high-debt": true, "exhaustive-control": true, "foreground-writes": true}
	seen := make(map[string]bool)
	for _, fixture := range compactStorageM0Fixtures {
		if fixture.Name == "" || fixture.Seed == 0 || fixture.KeyDistribution == "" || fixture.ValueDistribution == "" || fixture.ValueLogPointerThreshold == 0 || fixture.WALMode == "" || fixture.ExpectedMaintenanceWork == "" {
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

func TestNewCompactStorageMeasurementPreservesPhaseBoundaryAndUnavailableCounters(t *testing.T) {
	// totalWallTimeNanos comes from a timer started only after fixture setup.
	stats := CompactStorageStats{Audit: CompactStorageAuditStats{SharedScans: 2}, LeafGenerationGC: LeafGenerationGCStats{GenerationsEligible: 3, GenerationsDeleted: 1, BytesEligible: 300, BytesDeleted: 100}, Phases: []CompactStoragePhaseStats{{Name: "leaf-generation-pack-1", WallTimeNanos: 17}, {Name: "index-vacuum", Skipped: true, SkipReason: "fenced", WallTimeNanos: 5}}}
	m := newCompactStorageMeasurement(compactStorageM0Fixtures[0], 123, stats)
	if m.TotalWallTimeNanos != 123 || m.ApplyWallTimeNanos != 22 {
		t.Fatalf("timing boundary=%+v", m)
	}
	if len(m.Phases) != 2 || m.Phases[1].SkipReason != "fenced" {
		t.Fatalf("phases=%+v", m.Phases)
	}
	if !m.Vacuum.Attempted || m.Vacuum.SkipReason != "fenced" || m.Vacuum.Availability != compactStorageMeasurementObserved || m.Vacuum.ReclaimedBytes != 100 {
		t.Fatalf("vacuum=%+v", m.Vacuum)
	}
	if m.Checkpoint.Availability != compactStorageMeasurementUnavailable || m.Checkpoint.CoverageReason != "not-in-compact-storage-stats" || m.Vacuum.StableCallCounter != compactStorageMeasurementUnavailable {
		t.Fatalf("availability checkpoint=%+v vacuum=%+v", m.Checkpoint, m.Vacuum)
	}
}

func TestCompactStorageMeasurementJSONSchemaIsDeterministic(t *testing.T) {
	stats := CompactStorageStats{Phases: []CompactStoragePhaseStats{{Name: "leaf-generation-pack-1", WallTimeNanos: 7}}}
	first, err := json.Marshal(newCompactStorageMeasurement(compactStorageM0Fixtures[0], 9, stats))
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(newCompactStorageMeasurement(compactStorageM0Fixtures[0], 9, stats))
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatalf("non-deterministic JSON:\n%s\n%s", first, second)
	}
	for _, key := range []string{"fixture", "total_wall_time_nanos", "apply_wall_time_nanos", "checkpoint", "stable_call_counter"} {
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
	if name != "compact-storage-m0/full-default/seed=3733003" {
		t.Fatalf("artifact name=%q", name)
	}
	parts := bytes.Split([]byte(name), []byte("/"))
	if len(parts) != 3 || string(parts[0]) != "compact-storage-m0" || string(parts[1]) != "full-default" || !bytes.HasPrefix(parts[2], []byte("seed=")) {
		t.Fatalf("artifact schema=%q", name)
	}
}

func TestCompactStorageM0StableRecorderClassifiesPhaseResourceAndCall(t *testing.T) {
	r := newCompactStorageM0StableRecorder()
	r.beginPhase("checkpoint")
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.BeforeIndexDataSync, Resource: durabilitycut.ResourceIndex}); err != nil {
		t.Fatal(err)
	}
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.AfterIndexDataSync, Resource: durabilitycut.ResourceIndex}); err != nil {
		t.Fatal(err)
	}
	if r.totalCalls() != 1 {
		t.Fatalf("stable calls=%d want 1", r.totalCalls())
	}
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.BeforeMetaSync, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	if err := r.observe(durabilitycut.Event{Point: durabilitycut.AfterMetaSync, Resource: durabilitycut.ResourceMeta}); err != nil {
		t.Fatal(err)
	}
	if len(r.calls) != 2 {
		t.Fatalf("classified calls=%d want 2", len(r.calls))
	}
}
