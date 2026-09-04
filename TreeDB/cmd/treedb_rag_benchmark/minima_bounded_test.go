package main

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
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

func TestMinimaDiagnosticArtifactRejectsMeasuredNativeProof(t *testing.T) {
	m, _ := buildMinimaBoundedManifest(50000)
	a := minimaArtifact{Schema: minimaBoundedArtifactSchema, Manifest: m, State: "partial", Recommendation: "not_evaluated", NativePathProof: &minimaNativePathProof{Schema: minimaNativeProofSchema, Strategy: "column_graph", Availability: "measured"}}
	if validateMinimaArtifact(&a) == nil {
		t.Fatal("current diagnostic validator accepted future measured proof")
	}
}

func TestMinimaPeakRSSLifetimesAcceptActualRunnerSnapshots(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux process lifetime highwater")
	}
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command("python3", "-c", `import sys,os,json
sys.path.insert(0, '../../../benchmarks/vector_db_compare')
import minima_qdrant_runner as m
from pathlib import Path
w=object.__new__(m.QdrantMinimaRunner)
w.server_pid=os.getpid(); w.storage_path=Path(sys.argv[1]); w.resource_server_name='test'
w.completed_resource_segments=[]
w.resource_baseline=m.server_resource_usage(w.server_pid,w.storage_path,w.resource_server_name)
print(json.dumps({'resource_measurement':w.resource_evidence()}))`, t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("runner resource capture: %v %s", err, out)
	}
	var raw minimaRawBackendEvidence
	if err := json.Unmarshal(out, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.ResourceMeasurement.PeakRSSAvailability != "measured" {
		t.Fatal("real Linux peak unavailable")
	}
	a := minimaArtifact{Schema: minimaArtifactSchema, Manifest: buildMinimaManifest(), State: "partial", Recommendation: "not_evaluated", RawEvidence: map[string]minimaRawBackendEvidence{"treedb": raw}}
	if err := validateMinimaArtifact(&a); err != nil {
		t.Fatal(err)
	}
	*raw.ResourceMeasurement.Segments[0].End.PeakRSS.PID += 100
	if validateMinimaArtifact(&a) == nil {
		t.Fatal("artifact accepted unrelated sampled process")
	}
}

func TestMinimaPeakRSSBindsOrderedRestartLifetimes(t *testing.T) {
	const raw = `{"resource_measurement":{"peak_rss_bytes":200,"peak_rss_availability":"measured","peak_rss_scope":"max_process_lifetime_highwater_through_segment_endpoints","segments":[{"baseline":{"pid":1,"linux_process_identity":"1:10"},"end":{"pid":1,"linux_process_identity":"1:10","peak_rss":{"availability":"measured","bytes":100,"pid":1,"process_identity":"1:10","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}}},{"baseline":{"pid":2,"linux_process_identity":"2:20"},"end":{"pid":2,"linux_process_identity":"2:20","peak_rss":{"availability":"measured","bytes":200,"pid":2,"process_identity":"2:20","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}}}]},"restart_boundary":{"old_pid":1,"new_pid":2,"old_linux_process_identity":"1:10","new_linux_process_identity":"2:20","verified":true,"pid_changed":true}}`
	for _, mutation := range []string{"valid", "unrelated", "reordered", "wrong_start_ticks", "missing_binding", "unverified"} {
		t.Run(mutation, func(t *testing.T) {
			var r minimaRawBackendEvidence
			if err := json.Unmarshal([]byte(raw), &r); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "unrelated":
				*r.ResourceMeasurement.Segments[0].End.PeakRSS.PID = 99
			case "reordered":
				r.ResourceMeasurement.Segments[0], r.ResourceMeasurement.Segments[1] = r.ResourceMeasurement.Segments[1], r.ResourceMeasurement.Segments[0]
			case "wrong_start_ticks":
				r.ResourceMeasurement.Segments[1].End.PeakRSS.ProcessIdentity = "2:999"
			case "missing_binding":
				r.RestartBoundary = minimaRawRestartBoundary{}
			case "unverified":
				r.RestartBoundary.Verified = false
			}
			err := validateMinimaPeakRSSLifetimes(r)
			if (err == nil) != (mutation == "valid") {
				t.Fatalf("mutation=%s err=%v", mutation, err)
			}
		})
	}
}
