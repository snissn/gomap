package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateAcceptsCompleteTextOnlySmoke(t *testing.T) {
	m := validManifest()
	r := validReport(t, m)
	if err := validate(m, r, manifestHash(t, m)); err != nil {
		t.Fatalf("validate complete report: %v", err)
	}
}

func TestValidateRejectsContractFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*manifest, *report)
		want string
	}{
		{"vector contamination", func(m *manifest, _ *report) { m.Vectors = true }, "vectors=false"},
		{"dirty provenance", func(m *manifest, _ *report) { m.Dirty = true }, "dirty=false"},
		{"false reopen", func(_ *manifest, r *report) { r.Rows[0].ReopenOK = false }, "checkpoint/reopen"},
		{"document fetch", func(_ *manifest, r *report) { r.Rows[0].Probe.DocumentsFetched = 1 }, "score-only probe"},
		{"count drift", func(_ *manifest, r *report) { r.Rows[0].LiveDocuments-- }, "exact document count"},
		{"missing storage", func(_ *manifest, r *report) { r.Rows[0].Storage.TotalBytes = 0 }, "storage accounting"},
		{"missing stage", func(_ *manifest, r *report) { delete(r.Rows[0].Stages, "value_log") }, "value_log stage"},
		{"bad manifest hash", func(_ *manifest, r *report) { r.ManifestSHA256 = "wrong" }, "manifest_sha256"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			r := validReport(t, m)
			tc.edit(&m, &r)
			if err := validate(m, r, manifestHash(t, m)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate err=%v want %q", err, tc.want)
			}
		})
	}
}

func TestValidateRequiresThreeRetainedRepetitionsAndSummaries(t *testing.T) {
	m := validManifest()
	r := validReport(t, m)
	row := r.Rows[0]
	row.Scale = 100_000
	row.Median = summary{WallSeconds: 1, IndexedRowsPerSec: 1}
	row.P95 = summary{WallSeconds: 1, IndexedRowsPerSec: 1}
	r.Rows = append(r.Rows, row)
	if err := validate(m, r, manifestHash(t, m)); err == nil || !strings.Contains(err.Error(), "repetitions 1,2,3") {
		t.Fatalf("validate err=%v", err)
	}
}

func validManifest() manifest {
	return manifest{SchemaVersion: contractVersion, FixtureSHA256: "fixture", Analyzer: "simple", FieldWeights: "title=3,body=1", IDsSHA256: "ids", Command: "go run", Commit: "abcdef", Host: "test", CacheState: "cold", Durability: "wal_on", TimedBoundary: "insert through checkpoint", Vectors: false, Dirty: false}
}

func manifestHash(t *testing.T, m manifest) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func validReport(t *testing.T, m manifest) report {
	t.Helper()
	return report{SchemaVersion: contractVersion, ManifestSHA256: manifestHash(t, m), Rows: []row{
		validRow("indexed_insert"), validRow("post_load_backfill"), validRow("source_chunk"), validRow("maintenance"),
	}}
}

func validRow(mode string) row {
	return row{Mode: mode, Scale: 10_000, Repetition: 1, ExpectedDocuments: 10_000, LiveDocuments: 10_000, Postings: 1, Terms: 1, Blocks: 1, Generations: 1, SourceDocsPerSec: 1, ChunksPerSec: 1, IndexedRowsPerSec: 1, WallSeconds: 1, CPUSeconds: 1, PeakRSSBytes: 1, Stages: map[string]float64{"analyzer": 0, "posting_builder": 0, "root_mutation": 0, "value_log": 0, "checkpoint": 0, "reopen": 0}, Storage: storage{TotalBytes: 1}, Probe: scoreOnlyProbe{Results: 1}, CheckpointOK: true, ReopenOK: true}
}
