package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMinimaBoundedFixtures(t *testing.T) {
	for _, total := range []int{50000, 250000} {
		m, err := buildMinimaBoundedManifest(total)
		if err != nil {
			t.Fatal(err)
		}
		if err := validateMinimaManifest(&m); err != nil {
			t.Fatal(err)
		}
		rows := 0
		for _, s := range m.Corpora {
			rows += s.CorpusRows
		}
		if rows != total {
			t.Fatalf("rows=%d want total=%d", rows, total)
		}
		if minimaScenarioMap(&m)["over_limit_4097"].EligibleRows != 4097 {
			t.Fatal("lost boundary")
		}
		// The reduced corpus cannot preserve 4097 eligible rows at <1%.
		if minimaScenarioMap(&m)["sparse_over_limit"].Selectivity < 0.01 {
			t.Fatal("bounded fixture mislabeled as full sparse coverage")
		}
		path := filepath.Join(t.TempDir(), "manifest.json")
		if err := writeMinimaManifestRows(path, total); err != nil {
			t.Fatal(err)
		}
		if _, err := exec.LookPath("python3"); err == nil {
			cmd := exec.Command("python3", "-c", "import sys; sys.path.insert(0, '../../../benchmarks/vector_db_compare'); import minima_qdrant_runner as m; from pathlib import Path; m.load_manifest(Path(sys.argv[1]))", path)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("Python manifest validation: %v %s", err, out)
			}
		}
		artifact := minimaArtifact{Schema: minimaBoundedArtifactSchema, State: "partial", Manifest: m, Recommendation: "not_evaluated", NativePathProof: &minimaNativePathProof{Schema: minimaNativeProofSchema, Strategy: "native_runtime", Availability: "unavailable", Reason: "M1-M4 required"}}
		if err := validateMinimaArtifact(&artifact); err != nil {
			t.Fatal(err)
		}
		artifact.Passing = true
		if validateMinimaArtifact(&artifact) == nil {
			t.Fatal("bounded diagnostic qualified")
		}
		artifact.Passing = false
		artifact.NativePathProof.Availability = "measured"
		if validateMinimaArtifact(&artifact) == nil {
			t.Fatal("doctored route availability accepted")
		}
		m.Fixture = "bounded-unknown"
		if validateMinimaManifest(&m) == nil {
			t.Fatal("accepted unknown fixture")
		}
	}
}

func TestMinimaLookupBoundaryUsesAggregateEligibleCardinality(t *testing.T) {
	m, err := buildMinimaBoundedManifest(50000)
	if err != nil {
		t.Fatal(err)
	}
	for _, count := range []int{4096, 4097} {
		m.Corpora[2].EligibleRows = count
		_, _, got := minimaGlobalOracle(m.Corpora, m.Corpora[2])
		if got != count {
			t.Fatalf("aggregate boundary count=%d want=%d", got, count)
		}
	}
}

func TestMinimaCompletedBoundedRejectsFalseStrategyAndMissingPeak(t *testing.T) {
	m, _ := buildMinimaBoundedManifest(50000)
	a := minimaArtifact{Manifest: m, NativePathProof: &minimaNativePathProof{Strategy: "column_graph"}, Backends: []minimaBackendEvidence{{Configuration: map[string]string{"vector_strategy": "native_runtime"}}}}
	if err := validateMinimaCompletedBounded(&a, minimaRawBackendEvidence{}); err == nil || !strings.Contains(err.Error(), "strategy") {
		t.Fatalf("false strategy: %v", err)
	}
	a.NativePathProof.Strategy = "native_runtime"
	if err := validateMinimaCompletedBounded(&a, minimaRawBackendEvidence{}); err == nil || !strings.Contains(err.Error(), "peak RSS") {
		t.Fatalf("missing peak availability: %v", err)
	}
}

func TestMinimaPeakRSSRejectsSummedAndUnavailableAsZero(t *testing.T) {
	var resource minimaRawResourceMeasurement
	raw := `{"peak_rss_bytes":200,"peak_rss_availability":"measured","peak_rss_scope":"max_process_lifetime_highwater_through_segment_endpoints","segments":[{"end":{"peak_rss":{"availability":"measured","bytes":100,"pid":1,"process_identity":"1:10","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}}},{"end":{"peak_rss":{"availability":"measured","bytes":200,"pid":2,"process_identity":"2:20","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}}}]}`
	if err := json.Unmarshal([]byte(raw), &resource); err != nil {
		t.Fatal(err)
	}
	if err := validateMinimaPeakRSS(resource); err != nil {
		t.Fatal(err)
	}
	*resource.PeakRSSBytes = 300
	if validateMinimaPeakRSS(resource) == nil {
		t.Fatal("sum accepted as peak")
	}
	resource.PeakRSSAvailability = "unavailable"
	*resource.PeakRSSBytes = 0
	resource.Segments[0].End.PeakRSS = nil
	if validateMinimaPeakRSS(resource) == nil {
		t.Fatal("unavailable accepted as zero")
	}
	resource.PeakRSSBytes = nil
	if err := validateMinimaPeakRSS(resource); err != nil {
		t.Fatal(err)
	}
}

func TestMinimaNativeProofRejectsUnavailableAndZeroOnly(t *testing.T) {
	proof := minimaNativePathProof{Schema: minimaNativeProofSchema, Strategy: "column_graph", Availability: "unavailable", Reason: "M1-M4 not implemented"}
	if validateMinimaNativePathProof(&proof) == nil {
		t.Fatal("unavailable counters passed")
	}
	proof.Availability = "measured"
	proof.Counters = map[string]*uint64{}
	for _, key := range minimaNativeCounterNames {
		n := uint64(0)
		proof.Counters[key] = &n
	}
	if validateMinimaNativePathProof(&proof) == nil {
		t.Fatal("zero-only proof passed")
	}
	for _, key := range []string{"ann_base_searches", "base_candidates", "overlay_searches", "overlay_candidates"} {
		n := uint64(1)
		proof.Counters[key] = &n
	}
	if err := validateMinimaNativePathProof(&proof); err != nil {
		t.Fatal(err)
	}
	proof.Counters["indexed_json_reads"] = nil
	if validateMinimaNativePathProof(&proof) == nil {
		t.Fatal("missing counter passed as zero")
	}
}
