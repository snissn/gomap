package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestQualificationValidatorAcceptsCompleteFrozenMatrix4329(t *testing.T) {
	if err := validateQualificationReport(validQualificationReport4329()); err != nil {
		t.Fatalf("complete frozen matrix rejected: %v", err)
	}
}

func TestQualificationValidatorRejectsEveryNorthStarGap4329(t *testing.T) {
	tests := []struct {
		name   string
		want   string
		mutate func(*report)
	}{
		{"missing row", "query row cardinality", func(r *report) { r.Queries = r.Queries[:len(r.Queries)-1] }},
		{"wrong scale", "exact 10M cardinality", func(r *report) { r.Config.Rows = 1_000_000 }},
		{"dirty provenance", "clean commit/tree/harness/binary provenance", func(r *report) { r.Context.VCSClean = false }},
		{"config digest", "frozen digest contract", func(r *report) { r.Contract.QuerySetSHA256 = "wrong" }},
		{"failed row", "incomplete or failed", func(r *report) { r.Queries[0].Status = "failed" }},
		{"interrupted run", "report status", func(r *report) { r.Status = "interrupted" }},
		{"missing raw repetitions", "incomplete or failed", func(r *report) { r.Queries[0].RawLatencyNS = nil }},
		{"bad no-doc counter", "fetched documents on no-doc path", func(r *report) { r.Queries[0].TextStats.DocumentsFetched = 1 }},
		{"fallback counter", "fallback/fail-closed", func(r *report) { r.Queries[0].TextStats.FullDocumentScanFallbacks = 1 }},
		{"missing path counter", "block-max path", func(r *report) { r.Queries[0].TextStats.TextPostingBlocksSkipped = 0 }},
		{"scalar isolation", "incomplete or failed", func(r *report) { queryByName4329(r, queryRowHybridTextScalar).IsolationOK = false }},
		{"resource", "load/build/resource", func(r *report) { r.Load.Resource.PeakRSSBytes = 0 }},
		{"WAL accounting", "WAL/total accounting mismatch", func(r *report) { r.StorageSnapshots[0].PhysicalTotalWALExcludedBytes++ }},
		{"missing storage row", "missing storage snapshot", func(r *report) { r.StorageSnapshots = r.StorageSnapshots[:4] }},
		{"false reopen", "reopen/count/query parity", func(r *report) { r.Reopen.QueryParityOK = false }},
		{"maintenance mutation", "mutation/rewrite/checkpoint/reopen parity", func(r *report) { r.Maintenance.Updates = 9_999 }},
		{"maintenance parity", "mutation/rewrite/checkpoint/reopen parity", func(r *report) { r.Maintenance.AfterResultsSHA256 = "different" }},
		{"concurrency", "concurrency sanity", func(r *report) { r.Concurrent.Errors = []string{"race-safe probe failed"} }},
		{"source chunk", "source/chunk row", func(r *report) { r.SourceChunk.ReopenParityOK = false }},
		{"cleanup", "cleanup status", func(r *report) { r.Cleanup.Status = "unknown" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := validQualificationReport4329()
			tc.mutate(&candidate)
			err := validateQualificationReport(candidate)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestRetainedValidatorRejectsRawDigestAndRunStatus4329(t *testing.T) {
	dir := writeRetainedFixture4329(t)
	if err := validateRetainedArtifact(dir); err != nil {
		t.Fatalf("valid retained fixture rejected: %v", err)
	}

	t.Run("raw digest", func(t *testing.T) {
		copyDir := writeRetainedFixture4329(t)
		if err := os.WriteFile(filepath.Join(copyDir, "run.log"), []byte("tampered\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		err := validateRetainedArtifact(copyDir)
		if err == nil || !strings.Contains(err.Error(), "raw evidence digest mismatch for run.log") {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("interrupted status", func(t *testing.T) {
		copyDir := writeRetainedFixture4329(t)
		status := []byte("{\"status\":\"interrupted\",\"exit_code\":130,\"failures\":[{\"phase\":\"queries\",\"status\":\"interrupted\",\"error\":\"signal INT\"}]}\n")
		if err := os.WriteFile(filepath.Join(copyDir, "run_status.json"), status, 0o644); err != nil {
			t.Fatal(err)
		}
		var manifest retainedManifest
		manifestPath := filepath.Join(copyDir, retainedManifestName)
		data, err := os.ReadFile(manifestPath)
		if err != nil || strictJSON(data, &manifest) != nil {
			t.Fatalf("read manifest: %v", err)
		}
		manifest.RawEvidence["run_status.json"] = digestBytes(status)
		writeJSON4329(t, manifestPath, manifest)
		err = validateRetainedArtifact(copyDir)
		if err == nil || !strings.Contains(err.Error(), "failed/interrupted run status") {
			t.Fatalf("error=%v", err)
		}
	})
}

func validQualificationReport4329() report {
	resource := resourceSnapshot{CPUSeconds: 1, PeakRSSBytes: 1024, LiveHeapBytes: 512}
	cfg := reportConfig{
		Rows: requiredScaleRows, BatchSize: 32_768, Dims: 16, M: 8,
		EfConstruction: 128, EfSearch: 128, TopK: 10, CandidateLimit: 655_360,
		Queries: 3, Readers: 4, IncludeVector: true, RunBackfill: true,
		BackfillRows: requiredScaleRows, RunTextOnly: true, TextOnlyRows: requiredScaleRows, RunSourceChunk: true, SourceChunkRows: requiredScaleRows,
		RunReopen: true, RunConcurrent: true, ConcurrentWrites: 1024, RunRewrite: true,
		MaintenanceUpdates: 10_000, MaintenanceDeletes: 5_000, PhaseSelector: "all",
	}
	textStats := collections.TextSearchStats{TextPostingBlocksVisited: 10, TextPostingBlocksSkipped: 2}
	queries := make([]queryReport, 0, len(requiredQueryRows))
	for _, name := range requiredQueryRows {
		row := queryReport{
			Name: name, Status: "passed", Rows: requiredScaleRows, TopK: 10,
			CandidateBudget: cfg.CandidateLimit, Samples: cfg.Queries, Results: 10,
			ResultsSHA256: digestString(name), CorrectnessOK: true, IsolationOK: true,
			RawLatencyNS: []int64{1, 2, 3}, GuardrailOK: true, Resource: resource,
		}
		if strings.HasPrefix(name, "text_") {
			row.Modality = "text"
			stats := textStats
			if name == queryRowTextMultiTermOR {
				stats.TextWANDPivots = 1
			}
			if name == queryRowTextPhrase {
				stats.TextPositionLookups, stats.TextPhraseCandidatesChecked = 1, 1
			}
			if name == queryRowTextCommonFetch {
				stats.DocumentsFetched = 10
			}
			row.TextStats = &stats
		} else {
			row.Modality = "hybrid"
			stats := collections.HybridSearchStats{
				TextCandidatesRequested: 64, TextCandidateBudgetEffective: 10,
				TextCandidatesReturned: 10, CandidatesFused: 10,
				CandidateBudgetPolicy:     collections.HybridCandidateBudgetPolicyFixed,
				CandidateBudgetIterations: 1,
			}
			if name == queryRowHybridText {
				stats.CandidateBudgetPolicy = collections.HybridCandidateBudgetPolicyAdaptiveRRF
			}
			if name == queryRowHybridTextVector || name == queryRowHybridTextVecScalar || name == queryRowHybridTextVecCollapse2 || name == queryRowHybridTextVecScalarFetch {
				stats.VectorCandidatesRequested, stats.VectorCandidateBudgetEffective = 64, 10
			}
			if name == queryRowHybridTextScalar || name == queryRowHybridTextScalarBroad || name == queryRowHybridTextVecScalar || name == queryRowHybridTextVecScalarFetch {
				stats.ScalarFilterLookups, stats.ScalarFilterFinalIDs = 1, 10
			}
			if name == queryRowHybridTextVecScalarFetch {
				stats.DocumentsFetched = 10
			}
			row.HybridStats = &stats
		}
		if name == queryRowHybridTextVecCollapse2 {
			row.CollapseCap = 2
		}
		queries = append(queries, row)
	}
	storage := func(label string) storageSnapshot {
		return storageSnapshot{Label: label, PhysicalIndexPageBytes: 100, PhysicalValueLogBytes: 200, PhysicalWALBytes: 10, PhysicalOtherBytes: 5, PhysicalTotalBytes: 315, PhysicalTotalWALExcludedBytes: 305}
	}
	phases := []string{"load", "queries", "reopen", "concurrent", "maintenance", "backfill", "text_only", "source_chunk"}
	return report{
		SchemaVersion: scaleSchemaVersion, Status: "passed", Complete: true,
		Context:          reportContext{VCSClean: true, Commit: "commit", TreeOID: "tree", TreeDBSubtreeOID: "treedb", HarnessSubtreeOID: "harness", BinarySHA256: "binary"},
		Contract:         reportContract{ConfigSHA256: "config", FixtureSHA256: frozenFixtureSHA256(), QuerySetSHA256: frozenQuerySetSHA256(), RelevanceSHA256: frozenRelevanceSHA256(), Analyzer: "simple", FieldWeights: "title=3,body=1", Seed: 4329},
		Config:           cfg,
		Artifacts:        reportArtifacts{OutDir: "/tmp/scale-4329", DBDir: "/tmp/scale-4329/primary_db"},
		Load:             loadReport{Status: "passed", Mode: "mixed_text_vector", Rows: requiredScaleRows, Batches: 1, CheckpointSeconds: 1, StorageBytesAfterLoad: 1, Resource: resource},
		TextOnly:         &loadReport{Status: "passed", Mode: "text_only_predeclared", Rows: requiredScaleRows, Batches: 1, CheckpointSeconds: 1, StorageBytesAfterLoad: 1, Resource: resource},
		Backfill:         &backfillReport{Status: "passed", Mode: "text_only_post_load_backfill", Rows: requiredScaleRows, BackfillSeconds: 1, CheckpointSeconds: 1, Resource: resource},
		SourceChunk:      &sourceChunkReport{Status: "passed", SourceDocuments: requiredScaleRows, GeneratedChunks: requiredScaleRows, CheckpointSeconds: 1, ReopenParityOK: true, Resource: resource},
		Reopen:           &reopenReport{Status: "passed", CountOK: true, QueryParityOK: true, BeforeResultsSHA256: "same", AfterResultsSHA256: "same", StorageBytes: 1, Resource: resource},
		Concurrent:       &concurrentReport{Status: "passed", Readers: 4, Queries: 4, Writes: 1, GuardrailOK: true, Resource: resource},
		Maintenance:      &maintenanceReport{Status: "passed", Updates: 10_000, Deletes: 5_000, RewriteSeconds: 1, CheckpointSeconds: 1, PostconditionOK: true, ReopenParityOK: true, BeforeResultsSHA256: "same", AfterResultsSHA256: "same", Resource: resource},
		Queries:          queries,
		StorageSnapshots: []storageSnapshot{storage("after_load"), storage("after_reopen"), storage("maintenance_rewrite_fixture"), storage("backfill_fixture"), storage("text_only_fixture"), storage("source_chunk_fixture")},
		SelectedPhases:   phases, CompletedPhases: append([]string(nil), phases...),
		Cleanup:  cleanupReport{Status: "passed", RemovedPaths: []string{"/tmp/scale-4329/primary_db", "/tmp/scale-4329/maintenance_db", "/tmp/scale-4329/backfill_db", "/tmp/scale-4329/text_only_db", "/tmp/scale-4329/source_chunk_db"}, Errors: []string{}},
		Failures: []failureRecord{},
	}
}

func queryByName4329(r *report, name string) *queryReport {
	for i := range r.Queries {
		if r.Queries[i].Name == name {
			return &r.Queries[i]
		}
	}
	panic("missing test query " + name)
}

func writeRetainedFixture4329(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	rep := validQualificationReport4329()
	rep.Config.Rows = requiredScaleRows
	configBytes, err := json.MarshalIndent(rep.Config, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	configBytes = append(configBytes, '\n')
	if err := os.WriteFile(filepath.Join(dir, frozenConfigName), configBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	rep.Contract.ConfigSHA256 = digestBytes(configBytes)
	rep.Context.Commit = strings.TrimSpace(runCmd("git", "rev-parse", "HEAD"))
	rep.Context.TreeOID = strings.TrimSpace(runCmd("git", "rev-parse", "HEAD^{tree}"))
	rep.Context.TreeDBSubtreeOID = strings.TrimSpace(runCmd("git", "rev-parse", rep.Context.TreeOID+":"+treeDBGitPath))
	rep.Context.HarnessSubtreeOID = strings.TrimSpace(runCmd("git", "rev-parse", rep.Context.TreeOID+":"+harnessGitPath))
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	rep.Context.BinarySHA256, err = digestFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(dir, "scale_report.json")
	writeJSON4329(t, reportPath, rep)
	reportBytes, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range requiredRawEvidence {
		if name == frozenConfigName {
			continue
		}
		payload := []byte(name + "\n")
		if name == "run_status.json" {
			payload = []byte("{\"status\":\"complete\",\"exit_code\":0,\"failures\":[]}\n")
		}
		if name == "binary.sha256" {
			payload = []byte(rep.Context.BinarySHA256 + "\n")
		}
		if name == "context.txt" {
			payload = []byte(fmt.Sprintf("commit=%s\ntree_oid=%s\ntreedb_subtree_oid=%s\nharness_subtree_oid=%s\n", rep.Context.Commit, rep.Context.TreeOID, rep.Context.TreeDBSubtreeOID, rep.Context.HarnessSubtreeOID))
		}
		if name == "command.txt" {
			payload = []byte(fmt.Sprintf("treedb_text_hybrid_scale -rows %d -backfill-rows %d -text-only-rows %d -source-chunk-rows %d -queries %d -run-text-only=true -run-source-chunk=true -phases all -keep-db=false\n", rep.Config.Rows, rep.Config.BackfillRows, rep.Config.TextOnlyRows, rep.Config.SourceChunkRows, rep.Config.Queries))
		}
		if name == "resources.txt" {
			payload = []byte("1 maximum resident set size\n")
		}
		if err := os.WriteFile(filepath.Join(dir, name), payload, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	raw := make(map[string]string, len(requiredRawEvidence))
	for _, name := range requiredRawEvidence {
		digest, err := digestFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		raw[name] = digest
	}
	manifest := retainedManifest{
		SchemaVersion: retainedSchemaVersion, Status: "complete", ReportPath: "scale_report.json",
		ReportSHA256: digestBytes(reportBytes), RawEvidence: raw,
		BinarySHA256: rep.Context.BinarySHA256, Commit: rep.Context.Commit, TreeOID: rep.Context.TreeOID,
		TreeDBSubtreeOID: rep.Context.TreeDBSubtreeOID, HarnessSubtreeOID: rep.Context.HarnessSubtreeOID,
		ConfigSHA256: rep.Contract.ConfigSHA256, FixtureSHA256: rep.Contract.FixtureSHA256,
		QuerySetSHA256: rep.Contract.QuerySetSHA256, RelevanceSHA256: rep.Contract.RelevanceSHA256,
		Cleanup: rep.Cleanup, Failures: []failureRecord{},
	}
	writeJSON4329(t, filepath.Join(dir, retainedManifestName), manifest)
	return dir
}

func writeJSON4329(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(payload, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
