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

func TestGeneratorRelevanceOracle4329(t *testing.T) {
	text := []collections.TextSearchResult{{DocumentID: scaleDocID(0)}, {DocumentID: scaleDocID(2)}}
	if got := evaluateTextQueryQuality(queryRowTextCommon, requiredScaleRows, 2, text); !got.OK || got.Relevant != 2 || got.Precision != 1 {
		t.Fatalf("common oracle=%+v want precision 1", got)
	}
	rare := []collections.TextSearchResult{{DocumentID: scaleDocID(997)}, {DocumentID: scaleDocID(1_994)}}
	if got := evaluateTextQueryQuality(queryRowTextRare, requiredScaleRows, 2, rare); !got.OK {
		t.Fatalf("rare oracle=%+v want accepted generator ordinals", got)
	}
	broad := []collections.HybridSearchResult{{ID: scaleDocID(4)}, {ID: scaleDocID(20)}}
	if got := evaluateHybridQueryQuality(queryRowHybridTextScalarBroad, requiredScaleRows, 2, broad); !got.OK {
		t.Fatalf("broad scalar oracle=%+v want accepted tenant-common refund ordinals", got)
	}
	for name, ids := range map[string][][]byte{
		"irrelevant":   {scaleDocID(1)},
		"duplicate":    {scaleDocID(0), scaleDocID(0)},
		"out_of_range": {scaleDocID(requiredScaleRows)},
		"malformed":    {[]byte("doc-not-valid")},
	} {
		t.Run(name, func(t *testing.T) {
			if got := evaluateScaleQueryQuality(queryRowTextCommon, requiredScaleRows, len(ids), ids); got.OK || got.Failure == "" {
				t.Fatalf("oracle=%+v want rejection", got)
			}
		})
	}
	if got := evaluateScaleQueryQuality(queryRowTextCommon, requiredScaleRows, 2, [][]byte{scaleDocID(0)}); got.OK || got.Precision != 0 {
		t.Fatalf("truncated oracle=%+v want top-k denominator rejection", got)
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
		{"wrong candidate limit", "hybrid candidate limit", func(r *report) { r.Config.CandidateLimit-- }},
		{"truncated top-k", "incomplete or failed", func(r *report) { r.Queries[0].Results-- }},
		{"wrong top-k", "top-k", func(r *report) { r.Config.TopK-- }},
		{"wrong postings ceiling", "hybrid postings ceiling", func(r *report) { r.Config.HybridMaxPostingsScanned-- }},
		{"unbound row postings ceiling", "postings budget", func(r *report) { queryByName4329(r, queryRowHybridText).PostingsBudget-- }},
		{"unbound row candidate budget", "candidate budget provenance", func(r *report) { queryByName4329(r, queryRowHybridText).CandidateBudget-- }},
		{"unbound stats candidate budget", "candidate budget provenance", func(r *report) { queryByName4329(r, queryRowHybridText).HybridStats.TextCandidatesRequested-- }},
		{"missing vector candidate provenance", "vector candidate provenance", func(r *report) {
			queryByName4329(r, queryRowHybridTextVecScalar).HybridStats.VectorCandidatesReturned = 0
		}},
		{"unexpected vector work", "unexpectedly reports vector candidate work", func(r *report) { queryByName4329(r, queryRowHybridText).HybridStats.VectorCandidatesRequested = 1 }},
		{"wrong candidate policy", "candidate budget policy", func(r *report) {
			queryByName4329(r, queryRowHybridTextVecScalar).HybridStats.CandidateBudgetPolicy = collections.HybridCandidateBudgetPolicyAdaptiveRRF
		}},
		{"missing relevance oracle", "generator relevance oracle", func(r *report) { queryByName4329(r, queryRowHybridTextVecScalar).QualityOracleOK = false }},
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
		{"logical storage availability", "logical text storage measurement", func(r *report) {
			r.LogicalTextStorage = metricAvailability{State: "unavailable", Reason: "scan omitted"}
		}},
		{"source chunk batch contract", "source/chunk batch contract", func(r *report) { r.SourceChunk.BatchCalls-- }},
		{"maintenance batch contract", "maintenance update batch contract", func(r *report) { r.Maintenance.UpdateBatchCalls = 2 }},
		{"WAL accounting", "WAL/total accounting mismatch", func(r *report) { r.StorageSnapshots[0].PhysicalTotalWALExcludedBytes++ }},
		{"logical bytes", "logical text accounting mismatch", func(r *report) { r.StorageSnapshots[0].TextEncodedBytes = 0 }},
		{"logical lane sum", "logical text accounting mismatch", func(r *report) { r.StorageSnapshots[0].TextDocIDBytes++ }},
		{"logical live count", "logical text accounting mismatch", func(r *report) { r.StorageSnapshots[0].V2LiveDocuments-- }},
		{"logical denominator", "logical text accounting mismatch", func(r *report) { r.StorageSnapshots[0].DocumentDenominator++ }},
		{"phase accounting", "phase accounting mismatch", func(r *report) { r.Load.TextStorage.EncodedBytes++ }},
		{"missing storage row", "missing storage snapshot", func(r *report) { r.StorageSnapshots = r.StorageSnapshots[:4] }},
		{"false reopen", "reopen/count/query parity", func(r *report) { r.Reopen.QueryParityOK = false }},
		{"missing reopen relevance oracle", "generator relevance oracle", func(r *report) { r.Reopen.QualityOracleOK = false }},
		{"maintenance mutation", "mutation/rewrite/checkpoint/reopen parity", func(r *report) { r.Maintenance.Updates = 9_999 }},
		{"maintenance parity", "mutation/rewrite/checkpoint/reopen parity", func(r *report) { r.Maintenance.AfterResultsSHA256 = "different" }},
		{"missing maintenance relevance oracle", "generator relevance oracle", func(r *report) { r.Maintenance.AfterPrecisionAtK = 0 }},
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
		HybridMaxPostingsScanned: requiredHybridMaxPostingsScanned, Queries: 3, Readers: 4, IncludeVector: true, RunBackfill: true,
		BackfillRows: requiredScaleRows, RunTextOnly: true, TextOnlyRows: requiredScaleRows, RunSourceChunk: true, SourceChunkRows: requiredScaleRows, SourceChunkBatchSize: requiredSourceChunkBatchSize,
		RunReopen: true, RunConcurrent: true, ConcurrentWrites: 1024, RunRewrite: true,
		MaintenanceUpdates: 10_000, MaintenanceUpdateBatchSize: requiredMaintenanceUpdateBatchSize, MaintenanceDeletes: 5_000, PhaseSelector: "all",
	}
	textStats := collections.TextSearchStats{TextPostingBlocksVisited: 10, TextPostingBlocksSkipped: 2}
	queries := make([]queryReport, 0, len(requiredQueryRows))
	for _, name := range requiredQueryRows {
		row := queryReport{
			Name: name, Status: "passed", Rows: requiredScaleRows, TopK: 10,
			CandidateBudget: cfg.CandidateLimit, Samples: cfg.Queries, Results: 10,
			ResultsSHA256: digestString(name), CorrectnessOK: true, IsolationOK: true,
			OracleVersion: scaleRelevanceOracleVersion, RelevantResults: 10, PrecisionAtK: 1, QualityOracleOK: true,
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
			row.PostingsBudget = cfg.HybridMaxPostingsScanned
			if name == queryRowHybridTextScalarBroad {
				row.CandidateBudget = cfg.Rows
			}
			stats := collections.HybridSearchStats{
				TextCandidatesRequested: uint64(row.CandidateBudget), TextCandidateBudgetEffective: 10,
				TextCandidatesReturned: 10, CandidatesFused: 10,
				CandidateBudgetPolicy:     collections.HybridCandidateBudgetPolicyFixed,
				CandidateBudgetIterations: 1,
			}
			if name == queryRowHybridText {
				stats.CandidateBudgetPolicy = collections.HybridCandidateBudgetPolicyAdaptiveRRF
			}
			if name == queryRowHybridTextVector || name == queryRowHybridTextVecScalar || name == queryRowHybridTextVecCollapse2 || name == queryRowHybridTextVecScalarFetch {
				stats.VectorCandidatesRequested, stats.VectorCandidateBudgetEffective, stats.VectorCandidatesReturned = uint64(cfg.CandidateLimit), 10, 10
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
		liveDocuments := uint64(requiredScaleRows)
		documentDenominator := uint64(requiredScaleRows)
		switch label {
		case "maintenance_rewrite_fixture":
			liveDocuments -= 5_000
			documentDenominator -= 5_000
		case "source_chunk_fixture":
			liveDocuments = 5 * requiredScaleRows
		}
		return storageSnapshot{
			Label: label, DocumentDenominator: documentDenominator, PhysicalIndexPageBytes: 100, PhysicalValueLogBytes: 200, PhysicalWALBytes: 10, PhysicalOtherBytes: 5, PhysicalTotalBytes: 315, PhysicalTotalWALExcludedBytes: 305,
			TextEncodedBytes: 700, TextDocIDBytes: 100, TextDocMapBytes: 100, TextPostingBlockBytes: 100, TextNormBlockBytes: 100, TextPositionBytes: 100, TextTermStatsBytes: 100, TextStatusFormatBytes: 100,
			V2PostingBlocks: 1, V2LiveDocuments: liveDocuments,
		}
	}
	accounting := func(label string) collections.TextIndexStorageAccounting {
		snapshot := storage(label)
		return collections.TextIndexStorageAccounting{
			Documents: snapshot.V2LiveDocuments, EncodedBytes: snapshot.TextEncodedBytes, Version: collections.TextIndexVersionV2,
			V2PostingBlocks: snapshot.V2PostingBlocks, V2LiveDocuments: snapshot.V2LiveDocuments, V2DeletedDocs: snapshot.V2DeletedDocs,
			V2DocIDBytes: snapshot.TextDocIDBytes, V2DocMapBytes: snapshot.TextDocMapBytes, V2PostingBlockBytes: snapshot.TextPostingBlockBytes,
			V2NormBlockBytes: snapshot.TextNormBlockBytes, V2PositionBytes: snapshot.TextPositionBytes, V2TermStatsBytes: snapshot.TextTermStatsBytes, V2StatusFormatBytes: snapshot.TextStatusFormatBytes,
		}
	}
	phases := []string{"load", "queries", "reopen", "concurrent", "maintenance", "backfill", "text_only", "source_chunk"}
	return report{
		SchemaVersion: scaleSchemaVersion, Status: "passed", Complete: true,
		Context:            reportContext{VCSClean: true, Commit: "commit", TreeOID: "tree", TreeDBSubtreeOID: "treedb", HarnessSubtreeOID: "harness", BinarySHA256: "binary"},
		Contract:           reportContract{ConfigSHA256: "config", FixtureSHA256: frozenFixtureSHA256(), QuerySetSHA256: frozenQuerySetSHA256(), RelevanceSHA256: frozenRelevanceSHA256(), Analyzer: "simple", FieldWeights: "title=3,body=1", Seed: 4329},
		Config:             cfg,
		LogicalTextStorage: metricAvailability{State: "observed"},
		Artifacts:          reportArtifacts{OutDir: "/tmp/scale-4329", DBDir: "/tmp/scale-4329/primary_db"},
		Load:               loadReport{Status: "passed", Mode: "mixed_text_vector", Rows: requiredScaleRows, Batches: 1, CheckpointSeconds: 1, TextStorage: accounting("after_load"), StorageBytesAfterLoad: 1, Resource: resource},
		TextOnly:           &loadReport{Status: "passed", Mode: "text_only_predeclared", Rows: requiredScaleRows, Batches: 1, CheckpointSeconds: 1, TextStorage: accounting("text_only_fixture"), StorageBytesAfterLoad: 1, Resource: resource},
		Backfill:           &backfillReport{Status: "passed", Mode: "text_only_post_load_backfill", Rows: requiredScaleRows, BackfillSeconds: 1, CheckpointSeconds: 1, TextStorage: accounting("backfill_fixture"), Resource: resource},
		SourceChunk:        &sourceChunkReport{Status: "passed", SourceDocuments: requiredScaleRows, GeneratedChunks: 4 * requiredScaleRows, BatchSize: requiredSourceChunkBatchSize, BatchCalls: (requiredScaleRows + requiredSourceChunkBatchSize - 1) / requiredSourceChunkBatchSize, CheckpointSeconds: 1, ReopenParityOK: true, TextStorage: accounting("source_chunk_fixture"), Resource: resource},
		Reopen:             &reopenReport{Status: "passed", CountOK: true, QueryParityOK: true, BeforeResultsSHA256: "same", AfterResultsSHA256: "same", TextStorage: accounting("after_reopen"), StorageBytes: 1, OracleVersion: scaleRelevanceOracleVersion, TextPrecisionAtK: 1, HybridPrecisionAtK: 1, QualityOracleOK: true, Resource: resource},
		Concurrent:         &concurrentReport{Status: "passed", Readers: 4, Queries: 4, Writes: 1, GuardrailOK: true, Resource: resource},
		Maintenance:        &maintenanceReport{Status: "passed", Updates: 10_000, UpdateBatchSize: requiredMaintenanceUpdateBatchSize, UpdateBatchCalls: 1, Deletes: 5_000, RewriteSeconds: 1, CheckpointSeconds: 1, TextStorageAfter: accounting("maintenance_rewrite_fixture"), PostconditionOK: true, ReopenParityOK: true, BeforeResultsSHA256: "same", AfterResultsSHA256: "same", OracleVersion: scaleRelevanceOracleVersion, BeforePrecisionAtK: 1, AfterPrecisionAtK: 1, QualityOracleOK: true, Resource: resource},
		Queries:            queries,
		StorageSnapshots:   []storageSnapshot{storage("after_load"), storage("after_reopen"), storage("maintenance_rewrite_fixture"), storage("backfill_fixture"), storage("text_only_fixture"), storage("source_chunk_fixture")},
		SelectedPhases:     phases, CompletedPhases: append([]string(nil), phases...),
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
			payload = []byte(fmt.Sprintf("treedb_text_hybrid_scale -rows %d -backfill-rows %d -text-only-rows %d -source-chunk-rows %d -queries %d -top-k %d -candidate-limit %d -hybrid-max-postings-scanned %d -run-text-only=true -run-source-chunk=true -phases all -keep-db=false\n", rep.Config.Rows, rep.Config.BackfillRows, rep.Config.TextOnlyRows, rep.Config.SourceChunkRows, rep.Config.Queries, rep.Config.TopK, rep.Config.CandidateLimit, rep.Config.HybridMaxPostingsScanned))
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
