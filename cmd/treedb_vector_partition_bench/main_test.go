package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixturePath(t *testing.T) string {
	t.Helper()
	return filepath.Join("..", "..", "testdata", "vector_partition_10k")
}

func TestFixtureTruthDeterministicAndChecksumStable(t *testing.T) {
	m, err := loadFixture(fixturePath(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateFixture(m); err != nil {
		t.Fatal(err)
	}
	if got := fixtureChecksum(m.Vectors, m.Queries, m.Dimensions, m.Seed); got != m.Checksum {
		t.Fatalf("checksum=%s want %s", got, m.Checksum)
	}
	a, aq := deterministicFixture(m)
	b, bq := deterministicFixture(m)
	if len(a) != len(b) || len(aq) != len(bq) || a[0][0] != b[0][0] || aq[0][0] != bq[0][0] {
		t.Fatal("fixture is not deterministic")
	}
}

func TestTruthOracleTieOrderingAndAllPartitionParity(t *testing.T) {
	m, _ := loadFixture(fixturePath(t))
	vectors, queries := deterministicFixture(m)
	_ = vectors
	_ = queries
	got := exactTopK([][]float64{{1, 0}, {1, 0}, {0, 1}}, []float64{1, 0}, 2)
	if got[0].ID != "doc-000000" || got[1].ID != "doc-000001" {
		t.Fatalf("ties not ordered by stable ID: %#v", got[:2])
	}
	cfg := config{partitions: 4, topK: 10, recallTarget: .9, seed: 1}
	r, err := simulate(cfg, m, vectors, queries, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Stages[1].RecallAtK != 1 {
		t.Fatalf("all partition oracle recall=%f", r.Stages[1].RecallAtK)
	}
	if r.Stages[0].RecallAtK != r.Stages[1].RecallAtK {
		t.Fatal("oracle parity differs")
	}
}

func TestEveryLossStageIndependentlyLabeled(t *testing.T) {
	m, _ := loadFixture(fixturePath(t))
	vectors, queries := deterministicFixture(m)
	r, err := simulate(config{partitions: 4, topK: 10, recallTarget: .9, seed: 1}, m, vectors, queries, 1, .2)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"exact_global_top_k", "partition_oracle", "exact_representative_routing", "approximate_representative_routing", "exact_partition_local", "treedb_partition_local_hnsw", "end_to_end_distributed_simulation"}
	if len(r.Stages) != len(want) {
		t.Fatalf("stages=%#v", r.Stages)
	}
	for i, name := range want {
		if r.Stages[i].Name != name {
			t.Fatalf("stage[%d]=%q want %q", i, r.Stages[i].Name, name)
		}
	}
	if !r.Stages[1].Lossy || !r.Stages[6].Lossy {
		t.Fatal("loss stages not independently labeled")
	}
	if r.ResultKind != "simulation_only" || r.ProductionEvidence {
		t.Fatalf("simulation label=%+v", r)
	}
}

func TestSingleEnabledStageIsTheOnlyReportedStage(t *testing.T) {
	m, _ := loadFixture(fixturePath(t))
	v, q := deterministicFixture(m)
	r, err := simulate(config{partitions: 4, topK: 10, recallTarget: .9, seed: 1, stages: stageSet("partition_oracle")}, m, v, q, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Stages) != 1 || r.Stages[0].Name != "partition_oracle" {
		t.Fatalf("stages=%+v", r.Stages)
	}
}

func TestMalformedCapAndFiniteInputsRejectBeforeSimulation(t *testing.T) {
	for _, raw := range [][]string{{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "0", "-probes", "1"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "5"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-overlap", "NaN"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-recall-target", "NaN"}, {"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-stages", "unknown"}} {
		if _, err := parseConfig(raw); err == nil {
			t.Fatalf("accepted malformed config %#v", raw)
		}
	}
	bad := fixtureManifest{SchemaVersion: 1, Fixture: "bad", Generator: "treedb_vector_partition_fixture_v1", Vectors: maxVectors + 1, Queries: 1, Dimensions: 1, Metric: "cosine", Checksum: strings.Repeat("0", 64)}
	if err := validateFixture(bad); err == nil {
		t.Fatal("accepted capped fixture")
	}
	bad.Vectors = 1
	bad.Dimensions = 1
	bad.Checksum = "bad"
	if err := validateFixture(bad); err == nil {
		t.Fatal("accepted malformed checksum")
	}
}

func TestRunRejectsTopKAboveFixtureBeforeOracleAllocation(t *testing.T) {
	err := run([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-top-k", "10001"}, io.Discard)
	if err == nil {
		t.Fatal("accepted top-k above fixture")
	}
}

func TestConfigurableFixtureCapsRejectBeforeAllocation(t *testing.T) {
	cfg, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-max-vectors", "9999"})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := loadFixture(fixturePath(t))
	if err := validateFixtureWithCaps(m, cfg.maxVectors, cfg.maxBytes); err == nil {
		t.Fatal("accepted fixture beyond configured count cap")
	}
	if _, err := parseConfig([]string{"-dataset", fixturePath(t), "-out", t.TempDir(), "-partitions", "4", "-probes", "1", "-max-fixture-bytes", "7"}); err == nil {
		t.Fatal("accepted unsafe byte cap")
	}
}

func TestManifestRejectsTrailingJSON(t *testing.T) {
	d := t.TempDir()
	raw, err := os.ReadFile(filepath.Join(fixturePath(t), "fixture_manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "fixture_manifest.json"), append(raw, []byte("{}")...), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadFixture(d); err == nil {
		t.Fatal("accepted trailing JSON")
	}
}

func TestCanonicalRunWritesJSONAndMarkdown(t *testing.T) {
	out := t.TempDir()
	var stdout bytes.Buffer
	if err := run([]string{"-dataset", fixturePath(t), "-partitions", "4", "-probes", "1,2,4", "-overlap", "0,0.20", "-top-k", "10", "-recall-target", "0.90", "-seed", "1", "-format", "json", "-out", out}, &stdout); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 12 {
		t.Fatalf("artifacts=%d want 12", len(entries))
	}
	raw, err := os.ReadFile(filepath.Join(out, artifactBasename(runResult{Probes: 4, Overlap: .2, TopK: 10})+".json"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := decodeResult(raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.Command[0] != "treedb_vector_partition_bench" || result.Metrics.MeasurementStatus != "simulation_not_measured" || result.Metrics.SelectedPartitions != 4 {
		t.Fatalf("artifact metadata=%+v", result)
	}
	md, err := os.ReadFile(filepath.Join(out, artifactBasename(runResult{Probes: 4, Overlap: .2, TopK: 10})+".md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(md), "Simulation only; not production Raft evidence.") {
		t.Fatal("missing simulation disclaimer")
	}
}

func TestResultDecoderRejectsUnknownVersionAndTrailingValue(t *testing.T) {
	r := runResult{SchemaVersion: 99, ResultKind: "simulation_only", Stages: []stageResult{{Method: "x"}}, Metrics: metricsV1{MeasurementStatus: "simulation_not_measured"}}
	b, _ := json.Marshal(r)
	if _, err := decodeResult(b); err == nil {
		t.Fatal("accepted unknown schema")
	}
	r.SchemaVersion = 1
	b, _ = json.Marshal(r)
	if _, err := decodeResult(append(b, []byte("{}")...)); err == nil {
		t.Fatal("accepted trailing result")
	}
}

func TestValidateResultRejectsNonFiniteMetrics(t *testing.T) {
	r := runResult{SchemaVersion: 1, ResultKind: "simulation_only", Stages: []stageResult{{RecallAtK: math.NaN()}}}
	if err := validateResult(r); err == nil {
		t.Fatal("accepted nonfinite metric")
	}
}
