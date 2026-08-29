package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	retainedSchemaVersion              = "treedb_text_hybrid_scale_retained/v1"
	retainedManifestName               = "artifact_manifest.json"
	frozenConfigName                   = "frozen_config.json"
	requiredScaleRows                  = 10_000_000
	requiredSourceChunkBatchSize       = 32_768
	requiredMaintenanceUpdateBatchSize = 10_000
	harnessGitPath                     = "cmd/treedb_text_hybrid_scale"
	treeDBGitPath                      = "TreeDB"
)

var requiredRawEvidence = []string{
	"binary.sha256",
	"command.txt",
	"context.txt",
	frozenConfigName,
	"resources.txt",
	"run.log",
	"run_status.json",
}

var requiredQueryRows = []string{
	queryRowTextCommon,
	queryRowTextRare,
	queryRowTextMultiTermAND,
	queryRowTextMultiTermOR,
	queryRowTextPhrase,
	queryRowTextCommonFetch,
	queryRowHybridText,
	queryRowHybridTextScalar,
	queryRowHybridTextScalarBroad,
	queryRowHybridTextVector,
	queryRowHybridTextVecScalar,
	queryRowHybridTextVecCollapse2,
	queryRowHybridTextVecScalarFetch,
}

type retainedManifest struct {
	SchemaVersion     string            `json:"schema_version"`
	Status            string            `json:"status"`
	ReportPath        string            `json:"report_path"`
	ReportSHA256      string            `json:"report_sha256"`
	RawEvidence       map[string]string `json:"raw_evidence_sha256"`
	BinarySHA256      string            `json:"binary_sha256"`
	Commit            string            `json:"commit"`
	TreeOID           string            `json:"tree_oid"`
	TreeDBSubtreeOID  string            `json:"treedb_subtree_oid"`
	HarnessSubtreeOID string            `json:"harness_subtree_oid"`
	ConfigSHA256      string            `json:"config_sha256"`
	FixtureSHA256     string            `json:"fixture_sha256"`
	QuerySetSHA256    string            `json:"query_set_sha256"`
	RelevanceSHA256   string            `json:"relevance_sha256"`
	Cleanup           cleanupReport     `json:"cleanup"`
	Failures          []failureRecord   `json:"failures"`
}

type runStatus struct {
	Status   string          `json:"status"`
	ExitCode int             `json:"exit_code"`
	Failures []failureRecord `json:"failures"`
}

type failureRecord struct {
	Phase  string `json:"phase"`
	Status string `json:"status"`
	Error  string `json:"error"`
}

func frozenFixtureSHA256() string {
	return digestString("scaleDocument/v2|seed=4329|ids=doc-%09d|rare=997|tenant_rare=16|tenant_narrow=4|regions=8|source_chunk=fixed_window_runes_32_overlap_0")
}

func frozenQuerySetSHA256() string {
	return digestString(strings.Join(requiredQueryRows, "\n") + "\ncommon=refund\nrare=" + rareTextTerm + "\nand=refund AND policy\nor=refund policy\nphrase=refund policy/slop0\nscalar_rare=" + rareTenant + "\nscalar_broad=tenant-common\nvector=exact\ncollapse=0,2\n")
}

func frozenRelevanceSHA256() string {
	return digestString("relevance/v1|repeat_result_order_sha256|scalar_ids_checked_against_generator|reopen_and_maintenance_digest_parity")
}

func digestString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func digestFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return digestBytes(data), nil
}

func strictJSON(data []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func readStrictJSON(path string, value any) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := strictJSON(data, value); err != nil {
		return nil, err
	}
	return data, nil
}

func writeFrozenConfig(outDir string, cfg reportConfig) (string, error) {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return "", err
	}
	data = append(data, '\n')
	if err := atomicWriteFile(filepath.Join(outDir, frozenConfigName), data); err != nil {
		return "", err
	}
	return digestBytes(data), nil
}

func sealRetainedArtifact(dir string) error {
	reportPath := filepath.Join(dir, "scale_report.json")
	var rep report
	reportBytes, err := readStrictJSON(reportPath, &rep)
	if err != nil {
		return fmt.Errorf("read report: %w", err)
	}
	if err := validateQualificationReport(rep); err != nil {
		return fmt.Errorf("report contract: %w", err)
	}
	var frozen reportConfig
	frozenBytes, err := readStrictJSON(filepath.Join(dir, frozenConfigName), &frozen)
	if err != nil {
		return fmt.Errorf("read frozen config: %w", err)
	}
	if !reflect.DeepEqual(frozen, rep.Config) {
		return errors.New("frozen config does not match report config")
	}
	var status runStatus
	if _, err := readStrictJSON(filepath.Join(dir, "run_status.json"), &status); err != nil {
		return fmt.Errorf("read run status: %w", err)
	}
	if status.Status != "complete" || status.ExitCode != 0 || len(status.Failures) != 0 {
		return fmt.Errorf("run status is %q exit=%d failures=%d", status.Status, status.ExitCode, len(status.Failures))
	}
	raw := make(map[string]string, len(requiredRawEvidence))
	for _, name := range requiredRawEvidence {
		digest, err := digestFile(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("digest raw evidence %s: %w", name, err)
		}
		raw[name] = digest
	}
	manifest := retainedManifest{
		SchemaVersion:     retainedSchemaVersion,
		Status:            "complete",
		ReportPath:        "scale_report.json",
		ReportSHA256:      digestBytes(reportBytes),
		RawEvidence:       raw,
		BinarySHA256:      rep.Context.BinarySHA256,
		Commit:            rep.Context.Commit,
		TreeOID:           rep.Context.TreeOID,
		TreeDBSubtreeOID:  rep.Context.TreeDBSubtreeOID,
		HarnessSubtreeOID: rep.Context.HarnessSubtreeOID,
		ConfigSHA256:      digestBytes(frozenBytes),
		FixtureSHA256:     rep.Contract.FixtureSHA256,
		QuerySetSHA256:    rep.Contract.QuerySetSHA256,
		RelevanceSHA256:   rep.Contract.RelevanceSHA256,
		Cleanup:           rep.Cleanup,
		Failures:          rep.Failures,
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := atomicWriteFile(filepath.Join(dir, retainedManifestName), append(payload, '\n')); err != nil {
		return err
	}
	return validateRetainedArtifact(dir)
}

func validateRetainedArtifact(dir string) error {
	var manifest retainedManifest
	if _, err := readStrictJSON(filepath.Join(dir, retainedManifestName), &manifest); err != nil {
		return fmt.Errorf("read retained manifest: %w", err)
	}
	if manifest.SchemaVersion != retainedSchemaVersion {
		return fmt.Errorf("schema_version=%q want %q", manifest.SchemaVersion, retainedSchemaVersion)
	}
	if manifest.Status != "complete" {
		return fmt.Errorf("artifact status=%q", manifest.Status)
	}
	if len(manifest.Failures) != 0 {
		return fmt.Errorf("artifact records %d failed/interrupted rows", len(manifest.Failures))
	}
	if manifest.ReportPath != "scale_report.json" {
		return fmt.Errorf("report_path=%q", manifest.ReportPath)
	}
	reportPath, err := containedArtifactPath(dir, manifest.ReportPath)
	if err != nil {
		return err
	}
	var rep report
	reportBytes, err := readStrictJSON(reportPath, &rep)
	if err != nil {
		return fmt.Errorf("read report: %w", err)
	}
	if digestBytes(reportBytes) != manifest.ReportSHA256 {
		return errors.New("report digest mismatch")
	}
	if err := validateQualificationReport(rep); err != nil {
		return fmt.Errorf("report contract: %w", err)
	}
	if manifest.BinarySHA256 != rep.Context.BinarySHA256 || manifest.Commit != rep.Context.Commit || manifest.TreeOID != rep.Context.TreeOID || manifest.TreeDBSubtreeOID != rep.Context.TreeDBSubtreeOID || manifest.HarnessSubtreeOID != rep.Context.HarnessSubtreeOID {
		return errors.New("manifest provenance does not match report provenance")
	}
	if manifest.ConfigSHA256 != rep.Contract.ConfigSHA256 || manifest.FixtureSHA256 != frozenFixtureSHA256() || manifest.QuerySetSHA256 != frozenQuerySetSHA256() || manifest.RelevanceSHA256 != frozenRelevanceSHA256() {
		return errors.New("frozen config/query/fixture/relevance digest mismatch")
	}
	var frozen reportConfig
	frozenBytes, err := readStrictJSON(filepath.Join(dir, frozenConfigName), &frozen)
	if err != nil {
		return fmt.Errorf("read frozen config: %w", err)
	}
	if digestBytes(frozenBytes) != manifest.ConfigSHA256 || !reflect.DeepEqual(frozen, rep.Config) {
		return errors.New("frozen config payload mismatch")
	}
	if len(manifest.RawEvidence) != len(requiredRawEvidence) {
		return fmt.Errorf("raw evidence cardinality=%d want %d", len(manifest.RawEvidence), len(requiredRawEvidence))
	}
	for _, name := range requiredRawEvidence {
		want, ok := manifest.RawEvidence[name]
		if !ok || want == "" {
			return fmt.Errorf("missing raw evidence digest for %s", name)
		}
		got, err := digestFile(filepath.Join(dir, name))
		if err != nil {
			return fmt.Errorf("read raw evidence %s: %w", name, err)
		}
		if got != want {
			return fmt.Errorf("raw evidence digest mismatch for %s", name)
		}
	}
	if err := validateRawEvidenceBindings(dir, manifest, rep.Config); err != nil {
		return err
	}
	binaryEvidence, err := os.ReadFile(filepath.Join(dir, "binary.sha256"))
	if err != nil {
		return fmt.Errorf("read binary.sha256: %w", err)
	}
	if strings.TrimSpace(string(binaryEvidence)) != manifest.BinarySHA256 {
		return errors.New("binary.sha256 does not match measured binary identity")
	}
	var status runStatus
	if _, err := readStrictJSON(filepath.Join(dir, "run_status.json"), &status); err != nil {
		return fmt.Errorf("decode run status: %w", err)
	}
	if status.Status != "complete" || status.ExitCode != 0 || len(status.Failures) != 0 {
		return fmt.Errorf("failed/interrupted run status=%q exit=%d failures=%d", status.Status, status.ExitCode, len(status.Failures))
	}
	if !reflect.DeepEqual(manifest.Cleanup, rep.Cleanup) {
		return errors.New("manifest cleanup does not match report cleanup")
	}
	if err := verifyGitIdentity(manifest); err != nil {
		return err
	}
	return nil
}

func validateQualificationReport(rep report) error {
	if rep.SchemaVersion != scaleSchemaVersion {
		return fmt.Errorf("report schema_version=%q want %q", rep.SchemaVersion, scaleSchemaVersion)
	}
	if rep.Status != "passed" || !rep.Complete || len(rep.Failures) != 0 {
		return fmt.Errorf("report status=%q complete=%v failures=%d", rep.Status, rep.Complete, len(rep.Failures))
	}
	if !rep.Context.VCSClean || rep.Context.Commit == "" || rep.Context.TreeOID == "" || rep.Context.TreeDBSubtreeOID == "" || rep.Context.HarnessSubtreeOID == "" || rep.Context.BinarySHA256 == "" {
		return errors.New("clean commit/tree/harness/binary provenance is required")
	}
	if rep.Contract.FixtureSHA256 != frozenFixtureSHA256() || rep.Contract.QuerySetSHA256 != frozenQuerySetSHA256() || rep.Contract.RelevanceSHA256 != frozenRelevanceSHA256() || rep.Contract.ConfigSHA256 == "" || rep.Contract.Analyzer != "simple" || rep.Contract.FieldWeights != "title=3,body=1" || rep.Contract.Seed != 4329 {
		return errors.New("report frozen digest contract mismatch")
	}
	if rep.LogicalTextStorage.State != "observed" || rep.LogicalTextStorage.Reason != "" {
		return errors.New("logical text storage measurement must be observed for qualification")
	}
	cfg := rep.Config
	if cfg.Rows != requiredScaleRows || cfg.BackfillRows != requiredScaleRows || cfg.TextOnlyRows != requiredScaleRows || cfg.SourceChunkRows != requiredScaleRows || cfg.SourceChunkBatchSize != requiredSourceChunkBatchSize {
		return fmt.Errorf("exact 10M cardinality and source/chunk batch contract required: rows=%d backfill=%d text_only=%d source_chunk=%d source_chunk_batch=%d", cfg.Rows, cfg.BackfillRows, cfg.TextOnlyRows, cfg.SourceChunkRows, cfg.SourceChunkBatchSize)
	}
	if cfg.PhaseSelector != "all" || !cfg.IncludeVector || !cfg.RunBackfill || !cfg.RunTextOnly || !cfg.RunSourceChunk || !cfg.RunReopen || !cfg.RunConcurrent || !cfg.RunRewrite {
		return errors.New("complete all-phase text/hybrid/lifecycle matrix is required")
	}
	if cfg.Queries < 3 || cfg.MaintenanceUpdates < 10_000 || cfg.MaintenanceUpdateBatchSize != requiredMaintenanceUpdateBatchSize || cfg.MaintenanceDeletes < 5_000 || cfg.Readers < 2 || cfg.ConcurrentWrites < 1 {
		return errors.New("query repetitions, lifecycle mutations, maintenance update batch contract, and concurrency do not meet the frozen minimum")
	}
	wantPhases := []string{"load", "queries", "reopen", "concurrent", "maintenance", "backfill", "text_only", "source_chunk"}
	if !sameStrings(rep.SelectedPhases, wantPhases) || !sameStrings(rep.CompletedPhases, wantPhases) {
		return fmt.Errorf("phase matrix incomplete selected=%v completed=%v", rep.SelectedPhases, rep.CompletedPhases)
	}
	if rep.Load.Status != "passed" || rep.Load.Mode != "mixed_text_vector" || rep.Load.Rows != requiredScaleRows || rep.Load.Batches < 1 || rep.Load.CheckpointSeconds <= 0 || rep.Load.StorageBytesAfterLoad <= 0 || !validResource(rep.Load.Resource) {
		return errors.New("mixed text/vector load/build/resource row incomplete")
	}
	if rep.TextOnly == nil || rep.TextOnly.Status != "passed" || rep.TextOnly.Mode != "text_only_predeclared" || rep.TextOnly.Rows != requiredScaleRows || rep.TextOnly.Batches < 1 || rep.TextOnly.CheckpointSeconds <= 0 || rep.TextOnly.StorageBytesAfterLoad <= 0 || !validResource(rep.TextOnly.Resource) {
		return errors.New("predeclared text-only indexed ingestion row incomplete")
	}
	if rep.Backfill == nil || rep.Backfill.Status != "passed" || rep.Backfill.Mode != "text_only_post_load_backfill" || rep.Backfill.Rows != requiredScaleRows || rep.Backfill.BackfillSeconds <= 0 || rep.Backfill.CheckpointSeconds <= 0 || !validResource(rep.Backfill.Resource) {
		return errors.New("post-load backfill row incomplete")
	}
	expectedSourceChunkCalls := (requiredScaleRows + requiredSourceChunkBatchSize - 1) / requiredSourceChunkBatchSize
	if rep.SourceChunk == nil || rep.SourceChunk.Status != "passed" || rep.SourceChunk.SourceDocuments != requiredScaleRows || rep.SourceChunk.BatchSize != requiredSourceChunkBatchSize || rep.SourceChunk.BatchCalls != expectedSourceChunkCalls || rep.SourceChunk.GeneratedChunks < requiredScaleRows || rep.SourceChunk.CheckpointSeconds <= 0 || !rep.SourceChunk.ReopenParityOK || !validResource(rep.SourceChunk.Resource) {
		return errors.New("application-shaped source/chunk row incomplete: source/chunk batch contract mismatch")
	}
	if rep.Reopen == nil || rep.Reopen.Status != "passed" || !rep.Reopen.CountOK || !rep.Reopen.QueryParityOK || rep.Reopen.BeforeResultsSHA256 == "" || rep.Reopen.BeforeResultsSHA256 != rep.Reopen.AfterResultsSHA256 || rep.Reopen.StorageBytes <= 0 || !validResource(rep.Reopen.Resource) {
		return errors.New("checkpoint/close/reopen/count/query parity row incomplete")
	}
	if rep.Concurrent == nil || rep.Concurrent.Status != "passed" || !rep.Concurrent.GuardrailOK || len(rep.Concurrent.Errors) != 0 || rep.Concurrent.Queries < cfg.Readers || rep.Concurrent.Writes < 1 || !validResource(rep.Concurrent.Resource) {
		return errors.New("concurrency sanity row incomplete")
	}
	if rep.Maintenance == nil || rep.Maintenance.UpdateBatchSize <= 0 {
		return errors.New("mutation/rewrite/checkpoint/reopen parity or maintenance update batch contract incomplete")
	}
	expectedMaintenanceUpdateCalls := (rep.Maintenance.Updates + rep.Maintenance.UpdateBatchSize - 1) / rep.Maintenance.UpdateBatchSize
	if rep.Maintenance.Status != "passed" || rep.Maintenance.Updates < 10_000 || rep.Maintenance.UpdateBatchSize != requiredMaintenanceUpdateBatchSize || rep.Maintenance.UpdateBatchCalls != expectedMaintenanceUpdateCalls || rep.Maintenance.Deletes < 5_000 || rep.Maintenance.RewriteSeconds <= 0 || rep.Maintenance.CheckpointSeconds <= 0 || !rep.Maintenance.PostconditionOK || !rep.Maintenance.ReopenParityOK || rep.Maintenance.BeforeResultsSHA256 == "" || rep.Maintenance.BeforeResultsSHA256 != rep.Maintenance.AfterResultsSHA256 || !validResource(rep.Maintenance.Resource) {
		return errors.New("mutation/rewrite/checkpoint/reopen parity or maintenance update batch contract incomplete")
	}
	if err := validateStorageSnapshots(rep); err != nil {
		return err
	}
	if err := validateQueryMatrix(rep.Queries, cfg); err != nil {
		return err
	}
	if rep.Cleanup.Status != "passed" || rep.Cleanup.DBKept || rep.Artifacts.DBKept || len(rep.Cleanup.Errors) != 0 {
		return errors.New("cleanup status is absent or incomplete")
	}
	wantRemoved := []string{
		rep.Artifacts.DBDir,
		filepath.Join(rep.Artifacts.OutDir, "maintenance_db"),
		filepath.Join(rep.Artifacts.OutDir, "backfill_db"),
		filepath.Join(rep.Artifacts.OutDir, "text_only_db"),
		filepath.Join(rep.Artifacts.OutDir, "source_chunk_db"),
	}
	if rep.Artifacts.DBDir == "" || rep.Artifacts.OutDir == "" || !sameStrings(rep.Cleanup.RemovedPaths, wantRemoved) {
		return errors.New("cleanup paths do not match the complete fixture matrix")
	}
	return nil
}

func validateQueryMatrix(rows []queryReport, cfg reportConfig) error {
	if len(rows) != len(requiredQueryRows) {
		return fmt.Errorf("query row cardinality=%d want %d", len(rows), len(requiredQueryRows))
	}
	byName := make(map[string]queryReport, len(rows))
	for _, row := range rows {
		if _, exists := byName[row.Name]; exists {
			return fmt.Errorf("duplicate query row %q", row.Name)
		}
		byName[row.Name] = row
	}
	for _, name := range requiredQueryRows {
		row, ok := byName[name]
		if !ok {
			return fmt.Errorf("missing query row %q", name)
		}
		for _, sample := range row.RawLatencyNS {
			if sample <= 0 {
				return fmt.Errorf("query row %q has non-positive raw latency", name)
			}
		}
		if strings.HasPrefix(name, "text_") {
			if row.Modality != "text" || row.TextStats == nil || row.HybridStats != nil {
				return fmt.Errorf("query row %q has invalid text modality evidence", name)
			}
		} else if row.Modality != "hybrid" || row.HybridStats == nil || row.TextStats != nil {
			return fmt.Errorf("query row %q has invalid hybrid modality evidence", name)
		}
		if row.Status != "passed" || row.Failure != "" || !row.GuardrailOK || !row.CorrectnessOK || !row.IsolationOK || row.Rows != requiredScaleRows || row.Samples != cfg.Queries || len(row.RawLatencyNS) != cfg.Queries || row.Results < 1 || row.ResultsSHA256 == "" || !validResource(row.Resource) {
			return fmt.Errorf("query row %q is incomplete or failed", name)
		}
		fetch := name == queryRowTextCommonFetch || name == queryRowHybridTextVecScalarFetch
		if row.TextStats != nil {
			if row.TextStats.FullDocumentScanFallbacks != 0 || row.TextStats.FailClosed != 0 {
				return fmt.Errorf("query row %q used fallback/fail-closed path", name)
			}
			if fetch {
				if row.TextStats.DocumentsFetched != uint64(row.Results) {
					return fmt.Errorf("query row %q final fetch count mismatch", name)
				}
			} else if row.TextStats.DocumentsFetched != 0 {
				return fmt.Errorf("query row %q fetched documents on no-doc path", name)
			}
		}
		if row.HybridStats != nil {
			stats := row.HybridStats
			if stats.FullDocumentScanFallbacks != 0 || stats.FailClosed != 0 || stats.TextCandidatesRequested == 0 || stats.TextCandidateBudgetEffective == 0 || stats.CandidatesFused == 0 {
				return fmt.Errorf("query row %q has incomplete candidate/path counters", name)
			}
			if fetch {
				if stats.DocumentsFetched != uint64(row.Results) {
					return fmt.Errorf("query row %q final fetch count mismatch", name)
				}
			} else if stats.DocumentsFetched != 0 {
				return fmt.Errorf("query row %q fetched documents on no-doc path", name)
			}
		}
	}
	common := byName[queryRowTextCommon].TextStats
	if common == nil || common.TextPostingBlocksVisited == 0 || common.TextPostingBlocksSkipped == 0 {
		return errors.New("common text row does not prove block-max path")
	}
	orStats := byName[queryRowTextMultiTermOR].TextStats
	if orStats == nil || orStats.TextWANDPivots == 0 {
		return errors.New("OR row does not prove WAND path")
	}
	phrase := byName[queryRowTextPhrase].TextStats
	if phrase == nil || phrase.TextPositionLookups == 0 || phrase.TextPhraseCandidatesChecked == 0 {
		return errors.New("phrase row does not prove positions path")
	}
	for _, name := range []string{queryRowHybridTextScalar, queryRowHybridTextScalarBroad, queryRowHybridTextVecScalar, queryRowHybridTextVecScalarFetch} {
		stats := byName[name].HybridStats
		if stats == nil || stats.ScalarFilterLookups == 0 || stats.ScalarFilterFinalIDs == 0 {
			return fmt.Errorf("query row %q does not prove scalar path", name)
		}
	}
	adaptive := byName[queryRowHybridText].HybridStats
	if adaptive == nil || adaptive.CandidateBudgetPolicy != collections.HybridCandidateBudgetPolicyAdaptiveRRF || adaptive.CandidateBudgetIterations == 0 {
		return errors.New("hybrid text row does not prove accepted adaptive budget path")
	}
	fixed := byName[queryRowHybridTextVector].HybridStats
	if fixed == nil || fixed.CandidateBudgetPolicy != collections.HybridCandidateBudgetPolicyFixed || fixed.TextCandidateBudgetEffective == 0 || fixed.VectorCandidateBudgetEffective == 0 {
		return errors.New("hybrid text+vector row does not prove fixed budget path")
	}
	if byName[queryRowHybridTextVecCollapse2].CollapseCap != 2 {
		return errors.New("collapse cap=2 row is absent")
	}
	return nil
}

func validResource(resource resourceSnapshot) bool {
	return resource.CPUSeconds > 0 && resource.PeakRSSBytes > 0 && resource.LiveHeapBytes > 0
}

func validateStorageSnapshots(rep report) error {
	required := map[string]uint64{
		"after_load":                  uint64(rep.Config.Rows),
		"after_reopen":                uint64(rep.Config.Rows),
		"maintenance_rewrite_fixture": uint64(rep.Config.Rows - rep.Maintenance.Deletes),
		"backfill_fixture":            uint64(rep.Config.BackfillRows),
		"text_only_fixture":           uint64(rep.Config.TextOnlyRows),
		"source_chunk_fixture":        uint64(rep.SourceChunk.SourceDocuments + rep.SourceChunk.GeneratedChunks),
	}
	expectedDenominator := map[string]uint64{
		"after_load":                  uint64(rep.Config.Rows),
		"after_reopen":                uint64(rep.Config.Rows),
		"maintenance_rewrite_fixture": uint64(rep.Config.Rows - rep.Maintenance.Deletes),
		"backfill_fixture":            uint64(rep.Config.BackfillRows),
		"text_only_fixture":           uint64(rep.Config.TextOnlyRows),
		"source_chunk_fixture":        uint64(rep.SourceChunk.SourceDocuments),
	}
	found := make(map[string]bool, len(required))
	for _, snap := range rep.StorageSnapshots {
		if snap.PhysicalTotalBytes <= 0 || snap.PhysicalIndexPageBytes <= 0 || snap.PhysicalValueLogBytes < 0 || snap.PhysicalWALBytes < 0 || snap.PhysicalOtherBytes < 0 {
			return fmt.Errorf("storage snapshot %q has incomplete physical accounting", snap.Label)
		}
		if snap.PhysicalTotalBytes != snap.PhysicalIndexPageBytes+snap.PhysicalValueLogBytes+snap.PhysicalWALBytes+snap.PhysicalOtherBytes || snap.PhysicalTotalWALExcludedBytes != snap.PhysicalTotalBytes-snap.PhysicalWALBytes {
			return fmt.Errorf("storage snapshot %q WAL/total accounting mismatch", snap.Label)
		}
		expectedLive, requiredRow := required[snap.Label]
		if !requiredRow {
			continue
		}
		laneBytes := snap.TextDocIDBytes + snap.TextDocMapBytes + snap.TextPostingBlockBytes + snap.TextNormBlockBytes + snap.TextPositionBytes + snap.TextTermStatsBytes + snap.TextStatusFormatBytes
		if snap.TextEncodedBytes == 0 || snap.TextEncodedBytes != laneBytes || snap.DocumentDenominator != expectedDenominator[snap.Label] || snap.V2PostingBlocks == 0 || snap.V2LiveDocuments != expectedLive || snap.V2DeletedDocs != 0 {
			return fmt.Errorf("storage snapshot %q logical text accounting mismatch: encoded=%d lane_bytes=%d denominator=%d want_denominator=%d posting_blocks=%d live=%d want_live=%d deleted=%d", snap.Label, snap.TextEncodedBytes, laneBytes, snap.DocumentDenominator, expectedDenominator[snap.Label], snap.V2PostingBlocks, snap.V2LiveDocuments, expectedLive, snap.V2DeletedDocs)
		}
		var accounting collections.TextIndexStorageAccounting
		switch snap.Label {
		case "after_load":
			accounting = rep.Load.TextStorage
		case "after_reopen":
			accounting = rep.Reopen.TextStorage
		case "maintenance_rewrite_fixture":
			accounting = rep.Maintenance.TextStorageAfter
		case "backfill_fixture":
			accounting = rep.Backfill.TextStorage
		case "text_only_fixture":
			accounting = rep.TextOnly.TextStorage
		case "source_chunk_fixture":
			accounting = rep.SourceChunk.TextStorage
		}
		if !storageAccountingMatchesSnapshot(accounting, snap) {
			return fmt.Errorf("storage snapshot %q phase accounting mismatch", snap.Label)
		}
		found[snap.Label] = true
	}
	for label := range required {
		if !found[label] {
			return fmt.Errorf("missing storage snapshot %q", label)
		}
	}
	return nil
}

func storageAccountingMatchesSnapshot(accounting collections.TextIndexStorageAccounting, snap storageSnapshot) bool {
	return accounting.EncodedBytes == snap.TextEncodedBytes &&
		accounting.V2DocIDBytes == snap.TextDocIDBytes &&
		accounting.V2DocMapBytes == snap.TextDocMapBytes &&
		accounting.V2PostingBlockBytes == snap.TextPostingBlockBytes &&
		accounting.V2NormBlockBytes == snap.TextNormBlockBytes &&
		accounting.V2PositionBytes == snap.TextPositionBytes &&
		accounting.V2TermStatsBytes == snap.TextTermStatsBytes &&
		accounting.V2StatusFormatBytes == snap.TextStatusFormatBytes &&
		accounting.V2PostingBlocks == snap.V2PostingBlocks &&
		accounting.V2LiveDocuments == snap.V2LiveDocuments &&
		accounting.V2DeletedDocs == snap.V2DeletedDocs
}

func verifyGitIdentity(manifest retainedManifest) error {
	if manifest.Commit == "" || manifest.TreeOID == "" || manifest.TreeDBSubtreeOID == "" || manifest.HarnessSubtreeOID == "" || manifest.BinarySHA256 == "" {
		return errors.New("incomplete commit/tree/harness/binary identity")
	}
	checks := []struct{ spec, want, label string }{
		{manifest.Commit + "^{tree}", manifest.TreeOID, "tree"},
		{manifest.TreeOID + ":" + treeDBGitPath, manifest.TreeDBSubtreeOID, "TreeDB subtree"},
		{manifest.TreeOID + ":" + harnessGitPath, manifest.HarnessSubtreeOID, "harness subtree"},
	}
	for _, check := range checks {
		cmd := exec.Command("git", "rev-parse", "--verify", check.spec)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("resolve %s: %w: %s", check.label, err, strings.TrimSpace(string(out)))
		}
		if got := strings.TrimSpace(string(out)); got != check.want {
			return fmt.Errorf("%s identity=%s want %s", check.label, got, check.want)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve validator executable: %w", err)
	}
	binarySHA, err := digestFile(executable)
	if err != nil {
		return fmt.Errorf("digest validator executable: %w", err)
	}
	if binarySHA != manifest.BinarySHA256 {
		return errors.New("validator binary digest does not match measured binary")
	}
	return nil
}
func validateRawEvidenceBindings(dir string, manifest retainedManifest, cfg reportConfig) error {
	contextBytes, err := os.ReadFile(filepath.Join(dir, "context.txt"))
	if err != nil {
		return err
	}
	context := make(map[string]string)
	for _, line := range strings.Split(string(contextBytes), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok && key != "" {
			context[key] = value
		}
	}
	for key, want := range map[string]string{
		"commit": manifest.Commit, "tree_oid": manifest.TreeOID,
		"treedb_subtree_oid": manifest.TreeDBSubtreeOID, "harness_subtree_oid": manifest.HarnessSubtreeOID,
	} {
		if context[key] != want {
			return fmt.Errorf("context %s=%q want %q", key, context[key], want)
		}
	}
	commandBytes, err := os.ReadFile(filepath.Join(dir, "command.txt"))
	if err != nil {
		return err
	}
	command := string(commandBytes)
	for _, fragment := range []string{
		fmt.Sprintf("-rows %d", cfg.Rows),
		fmt.Sprintf("-backfill-rows %d", cfg.BackfillRows),
		fmt.Sprintf("-text-only-rows %d", cfg.TextOnlyRows),
		fmt.Sprintf("-source-chunk-rows %d", cfg.SourceChunkRows),
		fmt.Sprintf("-queries %d", cfg.Queries),
		"-run-text-only=true", "-run-source-chunk=true", "-phases all", "-keep-db=false",
	} {
		if !strings.Contains(command, fragment) {
			return fmt.Errorf("command evidence missing %q", fragment)
		}
	}
	resources, err := os.ReadFile(filepath.Join(dir, "resources.txt"))
	if err != nil {
		return err
	}
	resourceText := strings.ToLower(string(resources))
	if !strings.Contains(resourceText, "maximum resident set size") {
		return errors.New("resources.txt lacks peak RSS evidence")
	}
	runLog, err := os.ReadFile(filepath.Join(dir, "run.log"))
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(runLog)) == 0 {
		return errors.New("run.log is empty")
	}
	return nil
}

func containedArtifactPath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != filepath.FromSlash(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || relative == ".." {
		return "", fmt.Errorf("unsafe artifact path %q", relative)
	}
	return filepath.Join(root, filepath.FromSlash(relative)), nil
}

func sameStrings(got, want []string) bool {
	gotCopy, wantCopy := append([]string(nil), got...), append([]string(nil), want...)
	sort.Strings(gotCopy)
	sort.Strings(wantCopy)
	return reflect.DeepEqual(gotCopy, wantCopy)
}

func hashTextResults(results []collections.TextSearchResult) string {
	h := sha256.New()
	for _, result := range results {
		_, _ = fmt.Fprintf(h, "%x\x00%d\x00%.17g\n", result.DocumentID, result.Rank, result.Score)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hashHybridResults(results []collections.HybridSearchResult) string {
	h := sha256.New()
	for _, result := range results {
		_, _ = fmt.Fprintf(h, "%x\x00%d\x00%.17g\n", result.ID, result.Rank, result.FusedScore)
	}
	return hex.EncodeToString(h.Sum(nil))
}
