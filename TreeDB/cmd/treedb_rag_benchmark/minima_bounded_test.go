package main

import (
	"encoding/json"
	"os"
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

func TestMinimaCompletedBoundedValidatesLifecycle(t *testing.T) {
	m, _ := buildMinimaBoundedManifest(50000)
	base := validMinimaArtifactForManifest(m)
	base.Schema, base.State, base.Passing, base.Recommendation = minimaBoundedArtifactSchema, "partial", false, "not_evaluated"
	base.NativePathProof = &minimaNativePathProof{Schema: minimaNativeProofSchema, Strategy: "native_runtime", Availability: "unavailable", Reason: "M1-M4 required"}
	base.Backends = base.Backends[:1]
	base.Backends[0].Configuration["vector_strategy"] = "native_runtime"
	base.Scenarios = base.Scenarios[:len(m.Corpora)]
	delete(base.RawEvidence, "qdrant")
	raw := base.RawEvidence["treedb"]
	raw.ResourceMeasurement.PeakRSSAvailability = "unavailable"
	raw.ResourceMeasurement.PeakRSSScope = "max_process_lifetime_highwater_through_segment_endpoints"
	base.RawEvidence["treedb"] = raw
	if err := validateMinimaArtifact(&base); err != nil {
		t.Fatalf("valid completed bounded diagnostic: %v", err)
	}
	incomplete := cloneMinimaArtifact(t, base)
	incomplete.Backends[0].Operations.ManifestOrdered = false
	incomplete.Failures = []string{"final scroll failed; diagnostic execution incomplete"}
	incompleteRaw := incomplete.RawEvidence["treedb"]
	incompleteRaw.FinalScrollState = minimaRawFinalState{}
	incomplete.RawEvidence["treedb"] = incompleteRaw
	if err := validateMinimaArtifact(&incomplete); err != nil {
		t.Fatalf("explicitly incomplete diagnostic: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*minimaArtifact)
	}{
		{"renamed_backend", func(a *minimaArtifact) { a.Backends[0].Name = "qdrant" }},
		{"extra_backend", func(a *minimaArtifact) { a.Backends = append(a.Backends, a.Backends[0]) }},
		{"removed_backend", func(a *minimaArtifact) { a.Backends = nil }},
		{"false_completion_flag", func(a *minimaArtifact) { a.Backends[0].Operations.ManifestOrdered = false }},
		{"false_completion_with_successful_scroll", func(a *minimaArtifact) {
			a.Backends[0].Operations.ManifestOrdered = false
			a.Failures = []string{"incomplete"}
		}},
		{"operation_flags", func(a *minimaArtifact) { a.Backends[0].Operations.ExplicitUpdateVisible = false }},
		{"timed_count", func(a *minimaArtifact) { a.Backends[0].Operations.TimedQueriesExecuted = 0 }},
		{"timed_trace", func(a *minimaArtifact) { a.Backends[0].Operations.TimedExecutionTrace.Queries = nil }},
		{"reindex_trace", func(a *minimaArtifact) { a.Backends[0].Operations.ReindexExecutionTrace.Operations = nil }},
		{"reopen", func(a *minimaArtifact) { a.Backends[0].Reopen = minimaReopenEvidence{} }},
		{"backend_manifest", func(a *minimaArtifact) { a.Backends[0].Manifest.OperationSHA256 = "wrong" }},
		{"raw_overlap", func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.TimedOverlap = minimaRawTimedOverlap{}
			a.RawEvidence["treedb"] = r
		}},
		{"final_scroll", func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.FinalScrollState = minimaRawFinalState{}
			a.RawEvidence["treedb"] = r
		}},
		{"batch_contract", func(a *minimaArtifact) {
			r := a.RawEvidence["treedb"]
			r.UpsertBatchCorrelationContract = nil
			a.RawEvidence["treedb"] = r
		}},
		{"resource_summary", func(a *minimaArtifact) { a.Scenarios[0].Resource.RSSBytes++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := cloneMinimaArtifact(t, base)
			tc.mutate(&a)
			if err := validateMinimaArtifact(&a); err == nil {
				t.Fatal("accepted incomplete completed bounded lifecycle")
			}
		})
	}
}

func TestMinimaPreservedBoundedLifecycle(t *testing.T) {
	paths := os.Getenv("MINIMA_CHARACTERIZE_ARTIFACTS")
	if paths == "" {
		t.Skip("optional preserved artifact lifecycle characterization")
	}
	for _, path := range filepath.SplitList(paths) {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var a minimaArtifact
			if err := json.Unmarshal(data, &a); err != nil {
				t.Fatal(err)
			}
			if a.Schema != minimaBoundedArtifactSchema || len(a.Backends) != 1 {
				t.Fatal("not single-backend bounded evidence")
			}
			if err := validateMinimaManifest(&a.Manifest); err != nil {
				t.Fatal(err)
			}
			if err := validateMinimaBackendLifecycle(a.Backends[0], &a.Manifest); err != nil {
				t.Fatal(err)
			}
			if err := validateMinimaRawEvidence(&a, map[string]minimaBackendEvidence{"treedb": a.Backends[0]}); err != nil {
				t.Fatal(err)
			}
			// Inspect original evidence without migrating it. A pre-binding peak
			// format must still fail the current full artifact validator.
			if err := validateMinimaArtifact(&a); err != nil {
				if !strings.Contains(err.Error(), "peak RSS") {
					t.Fatal(err)
				}
				t.Logf("lifecycle valid; current peak binding rejects original artifact: %v", err)
			}
		})
	}
}

func TestMinimaPeakRSSValidatesBothEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*minimaRawBackendEvidence)
		valid  bool
	}{
		{"valid", func(*minimaRawBackendEvidence) {}, true},
		{"decreasing_peak", func(r *minimaRawBackendEvidence) { *r.ResourceMeasurement.Segments[0].Baseline.PeakRSS.Bytes = 300 }, false},
		{"wrong_baseline_pid", func(r *minimaRawBackendEvidence) { *r.ResourceMeasurement.Segments[0].Baseline.PeakRSS.PID = 2 }, false},
		{"wrong_baseline_lifetime", func(r *minimaRawBackendEvidence) {
			r.ResourceMeasurement.Segments[0].Baseline.PeakRSS.ProcessIdentity = "1:999"
		}, false},
		{"wrong_baseline_source", func(r *minimaRawBackendEvidence) {
			r.ResourceMeasurement.Segments[0].Baseline.PeakRSS.Source = "endpoint_rss"
		}, false},
		{"missing_baseline_bytes", func(r *minimaRawBackendEvidence) { r.ResourceMeasurement.Segments[0].Baseline.PeakRSS.Bytes = nil }, false},
		{"unavailable_baseline_with_bytes", func(r *minimaRawBackendEvidence) {
			r.ResourceMeasurement.Segments[0].Baseline.PeakRSS.Availability = "unavailable"
		}, false},
		{"missing_end_measured_aggregate", func(r *minimaRawBackendEvidence) { r.ResourceMeasurement.Segments[0].End.PeakRSS = nil }, false},
		{"missing_end_unavailable_aggregate", func(r *minimaRawBackendEvidence) {
			r.ResourceMeasurement.Segments[0].End.PeakRSS = nil
			r.ResourceMeasurement.PeakRSSAvailability = "unavailable"
			r.ResourceMeasurement.PeakRSSBytes = nil
		}, true},
		{"wrong_baseline_missing_end", func(r *minimaRawBackendEvidence) {
			r.ResourceMeasurement.Segments[0].End.PeakRSS = nil
			r.ResourceMeasurement.PeakRSSAvailability = "unavailable"
			r.ResourceMeasurement.PeakRSSBytes = nil
			r.ResourceMeasurement.Segments[0].Baseline.PeakRSS.ProcessIdentity = "1:999"
		}, false},
		{"missing_baseline_measured_aggregate", func(r *minimaRawBackendEvidence) { r.ResourceMeasurement.Segments[0].Baseline.PeakRSS = nil }, false},
		{"missing_baseline_unavailable_aggregate", func(r *minimaRawBackendEvidence) {
			r.ResourceMeasurement.Segments[0].Baseline.PeakRSS = nil
			r.ResourceMeasurement.PeakRSSAvailability = "unavailable"
			r.ResourceMeasurement.PeakRSSBytes = nil
		}, true},
		{"doctored_envelope", func(r *minimaRawBackendEvidence) {
			copy := r.ResourceMeasurement.Segments[0].Baseline
			copy.LinuxProcessIdentity = "1:999"
			r.ResourceMeasurement.Baseline = &copy
		}, false},
		{"missing_aggregate_with_peak", func(r *minimaRawBackendEvidence) {
			r.ResourceMeasurement.PeakRSSAvailability = ""
			r.ResourceMeasurement.PeakRSSBytes = nil
			r.ResourceMeasurement.PeakRSSScope = ""
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var r minimaRawBackendEvidence
			if err := json.Unmarshal([]byte(`{"resource_measurement":{"peak_rss_bytes":200,"peak_rss_availability":"measured","peak_rss_scope":"max_process_lifetime_highwater_through_segment_endpoints","segments":[{"baseline":{"pid":1,"linux_process_identity":"1:10","peak_rss":{"availability":"measured","bytes":100,"pid":1,"process_identity":"1:10","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}},"end":{"pid":1,"linux_process_identity":"1:10","peak_rss":{"availability":"measured","bytes":200,"pid":1,"process_identity":"1:10","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}}}]}}`), &r); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&r)
			if err := validateMinimaPeakRSSLifetimes(r); (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
		})
	}
}

func TestMinimaPeakRSSRejectsSummedAndUnavailableAsZero(t *testing.T) {
	var resource minimaRawResourceMeasurement
	raw := `{"peak_rss_bytes":200,"peak_rss_availability":"measured","peak_rss_scope":"max_process_lifetime_highwater_through_segment_endpoints","segments":[{"end":{"peak_rss":{"availability":"measured","bytes":100,"pid":1,"process_identity":"1:10","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}}},{"end":{"peak_rss":{"availability":"measured","bytes":200,"pid":2,"process_identity":"2:20","source":"/proc/<pid>/status:VmHWM","scope":"process_lifetime_through_sample"}}}]}`
	if err := json.Unmarshal([]byte(raw), &resource); err != nil {
		t.Fatal(err)
	}
	for i := range resource.Segments {
		resource.Segments[i].Baseline = resource.Segments[i].End
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
			for i := range r.ResourceMeasurement.Segments {
				r.ResourceMeasurement.Segments[i].Baseline.PeakRSS = r.ResourceMeasurement.Segments[i].End.PeakRSS
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
