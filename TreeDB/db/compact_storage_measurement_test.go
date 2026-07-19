package db

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCompactStorageM0FixtureCatalogHasStableRequiredFields(t *testing.T) {
	want := map[string]bool{"one-generation-per-pass": true, "full-default": true, "full-low-debt": true, "full-high-debt": true, "exhaustive-control": true, "foreground-writes": true}
	seen := make(map[string]bool)
	for _, fixture := range CompactStorageM0Fixtures {
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
	m := NewCompactStorageMeasurement(CompactStorageM0Fixtures[0], 123, stats)
	if m.TotalWallTimeNanos != 123 || m.ApplyWallTimeNanos != 17 {
		t.Fatalf("timing boundary=%+v", m)
	}
	if len(m.Phases) != 2 || m.Phases[1].SkipReason != "fenced" {
		t.Fatalf("phases=%+v", m.Phases)
	}
	if !m.Vacuum.Attempted || m.Vacuum.SkipReason != "fenced" || m.Vacuum.Availability != CompactStorageMeasurementUnavailable || m.Vacuum.ReclaimedBytes != 0 {
		t.Fatalf("vacuum=%+v", m.Vacuum)
	}
	if m.Checkpoint.Availability != CompactStorageMeasurementUnavailable || m.Checkpoint.CoverageReason != "not-in-compact-storage-stats" || m.Vacuum.StableCallCounter != CompactStorageMeasurementUnavailable {
		t.Fatalf("availability checkpoint=%+v vacuum=%+v", m.Checkpoint, m.Vacuum)
	}
}

func TestCompactStorageMeasurementJSONSchemaIsDeterministic(t *testing.T) {
	stats := CompactStorageStats{Phases: []CompactStoragePhaseStats{{Name: "leaf-generation-pack-1", WallTimeNanos: 7}}}
	first, err := json.Marshal(NewCompactStorageMeasurement(CompactStorageM0Fixtures[0], 9, stats))
	if err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(NewCompactStorageMeasurement(CompactStorageM0Fixtures[0], 9, stats))
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
