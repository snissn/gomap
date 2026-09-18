package main

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
)

// Exercise the actual M8 CLI, producer, measured transcript and JSON validator.
// Fresh retained-asset reconstruction is tested separately: this producer owns
// and removes its temporary fixture assets, and does not claim qualification.
func TestM8RouterEffortActualProducerKeepsMeasuredBoundary(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	dataset := writeFixtureForTest(t, 96, 3, 8)
	fixture, err := loadFixture(dataset)
	if err != nil {
		t.Fatal(err)
	}
	args := []string{
		"-dataset", dataset, "-out", t.TempDir(), "-mode", m8ProductionMultiGroupModeV1,
		"-partitions", "4", "-raft-groups", "2", "-probes", "1,2", "-overlap", "0",
		"-top-k", "10", "-ef-search", "32", "-concurrency", "1,2", "-warmup", "0",
		"-seed", strconv.FormatInt(fixture.Seed, 10), "-format", "json",
		"-m8-quality-diagnostics", "-m8-router-policy-diagnostics", "-m8-router-policy-width", "2",
		"-m8-router-effort-mode", "hierarchical_all_scores", "-m8-router-effort-width", "2",
		"-m8-router-effort-beam", "4", "-m8-router-effort-score-budget", "64",
	}
	var out bytes.Buffer
	if err := runWithHermeticProvenance(t, args, &out); err != nil {
		t.Fatal(err)
	}
	var report m8ProductionReportV1
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 4 || report.Config.RouterEffort == nil {
		t.Fatal("lost producer coordinates")
	}
	transcript, err := m8ReadProductionMeasurementTranscriptV1(report)
	if err != nil {
		t.Fatal(err)
	}
	if transcript.SchemaVersion != 6 || len(transcript.AttributionSHA256) != len(report.Rows) {
		t.Fatal("missing bounded diagnostic transcript bindings")
	}
	if len(transcript.Outcomes) != 4 {
		t.Fatal("lost measured rows")
	}
	for i, row := range report.Rows {
		if row.Samples != fixture.Queries || row.Attribution.RouterEffort == nil || len(row.Attribution.RouterEffort.Queries) != fixture.Queries {
			t.Fatal("lost effort population")
		}
		if err := m8RouterEffortEvidenceSelectionV1(report.Config, row); err != nil {
			t.Fatal(err)
		}
		if row.Status == "pass" || row.Status == "fail" {
			if len(transcript.Outcomes[i].TopKIDs) != fixture.Queries {
				t.Fatal("missing measured coordinator outputs")
			}
		}
	}
	// A changed diagnostic cannot be smuggled into its measured transcript.
	report.Rows[0].Attribution.RouterEffort.Queries[0].Comparison.Work.VectorBytes++
	if _, err := m8ReadProductionMeasurementTranscriptV1(report); err == nil {
		t.Fatal("changed diagnostic accepted against measured transcript")
	}
}
