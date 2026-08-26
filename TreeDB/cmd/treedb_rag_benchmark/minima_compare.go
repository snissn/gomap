package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type minimaRawTimedOverlapRound struct {
	Ordinal                   int   `json:"ordinal"`
	QueriesExecuted           int   `json:"queries_executed"`
	OverlappingReaders        []int `json:"overlapping_readers"`
	AllReadersOverlapObserved bool  `json:"all_readers_overlap_observed"`
}

type minimaRawTimedOverlap struct {
	ConfiguredSearches                   int                          `json:"configured_searches"`
	ExecutedSearches                     int                          `json:"executed_searches"`
	ConfiguredReaderConcurrency          int                          `json:"configured_reader_concurrency"`
	ConfiguredWriterConcurrency          int                          `json:"configured_writer_concurrency"`
	Rounds                               []minimaRawTimedOverlapRound `json:"rounds"`
	AllRoundsWriterSearchOverlapObserved bool                         `json:"all_rounds_writer_search_overlap_observed"`
	TimedExecutionSHA256                 string                       `json:"timed_execution_sha256"`
}

type minimaRawPayloadState struct {
	ExpectedHash string `json:"expected_hash"`
	ActualHash   string `json:"actual_hash"`
	Match        bool   `json:"match"`
}

type minimaRawVectorState struct {
	Algorithm             string  `json:"algorithm,omitempty"`
	CheckedRows           int     `json:"checked_rows"`
	ExpectedRows          int     `json:"expected_rows,omitempty"`
	MismatchRows          int     `json:"mismatch_rows"`
	MaximumComponentDelta float64 `json:"maximum_component_delta"`
	Tolerance             float64 `json:"tolerance"`
	Match                 bool    `json:"match"`
}

type minimaRawFinalState struct {
	Algorithm    string                `json:"algorithm"`
	ExpectedHash string                `json:"expected_hash"`
	ActualHash   string                `json:"actual_hash"`
	ExpectedRows int                   `json:"expected_rows"`
	ActualRows   int                   `json:"actual_rows"`
	Payload      minimaRawPayloadState `json:"payload"`
	Vectors      minimaRawVectorState  `json:"vectors"`
	Match        bool                  `json:"match"`
}

type minimaRawLatencyDistribution struct {
	Count        int   `json:"count"`
	TotalNanos   int64 `json:"total_nanos"`
	MinimumNanos int64 `json:"minimum_nanos"`
	P50Nanos     int64 `json:"p50_nanos"`
	P95Nanos     int64 `json:"p95_nanos"`
	P99Nanos     int64 `json:"p99_nanos"`
	MaximumNanos int64 `json:"maximum_nanos"`
}

type minimaRawResourceSnapshot struct {
	Captured     bool              `json:"captured"`
	RSSBytes     int64             `json:"rss_bytes"`
	CPUSeconds   float64           `json:"cpu_seconds"`
	DiskBytes    int64             `json:"disk_bytes"`
	Availability map[string]string `json:"availability,omitempty"`
}

type minimaRawResourceMeasurement struct {
	Captured   bool                       `json:"captured"`
	RSSBytes   int64                      `json:"rss_bytes"`
	CPUSeconds float64                    `json:"cpu_seconds"`
	DiskBytes  int64                      `json:"disk_bytes"`
	Baseline   *minimaRawResourceSnapshot `json:"baseline,omitempty"`
	End        *minimaRawResourceSnapshot `json:"end,omitempty"`
}

type minimaRawRestartBoundary struct {
	HookIdentity       string `json:"hook_identity"`
	OldPID             int    `json:"old_pid"`
	NewPID             int    `json:"new_pid"`
	OldProcessIdentity string `json:"old_process_identity"`
	NewProcessIdentity string `json:"new_process_identity"`
	PIDChanged         bool   `json:"pid_changed"`
	Verified           bool   `json:"verified"`
}

type minimaRawServiceLog struct {
	Path         string `json:"path"`
	Tail         string `json:"tail"`
	MaxTailBytes int    `json:"max_tail_bytes"`
}

type minimaRawBackendEvidence struct {
	PhaseLatencyDistributions map[string]minimaRawLatencyDistribution `json:"phase_latency_distributions,omitempty"`
	Events                    []json.RawMessage                       `json:"events,omitempty"`
	TimedOverlap              minimaRawTimedOverlap                   `json:"timed_overlap"`
	FinalScrollState          minimaRawFinalState                     `json:"final_scroll_state"`
	ResourceMeasurement       minimaRawResourceMeasurement            `json:"resource_measurement"`
	RestartBoundary           minimaRawRestartBoundary                `json:"restart_boundary"`
	ServiceLog                minimaRawServiceLog                     `json:"service_log,omitempty"`
	ResourceAvailability      map[string]map[string]string            `json:"resource_availability,omitempty"`
	NativeRouteResponses      map[string]json.RawMessage              `json:"native_route_responses,omitempty"`
}

var minimaPayloadEvidenceCache struct {
	once sync.Once
	hash string
	rows int
	err  error
}

func minimaExpectedPayloadEvidence(manifest *minimaManifest) (string, int, error) {
	minimaPayloadEvidenceCache.once.Do(func() {
		deleted := make(map[string]bool)
		overrides := make(map[string]minimaGeneratedDocument)
		additions := make(map[string]minimaGeneratedDocument)
		for _, operation := range manifest.Operations {
			switch operation.Effect {
			case "delete":
				for _, id := range operation.IDs {
					deleted[id] = true
				}
			case "update":
				for _, document := range operation.Documents {
					overrides[document.ID] = document
				}
			case "insert":
				for _, document := range operation.Documents {
					additions[document.ID] = document
				}
			}
		}
		var xor, total [sha256.Size]byte
		add := func(document minimaGeneratedDocument) {
			digest := sha256.New()
			for _, value := range []string{document.ID, document.Content, document.UserID, document.FPath} {
				_, _ = digest.Write([]byte(value))
				_, _ = digest.Write([]byte{0})
			}
			sum := digest.Sum(nil)
			carry := 0
			for i := len(total) - 1; i >= 0; i-- {
				xor[i] ^= sum[i]
				value := int(total[i]) + int(sum[i]) + carry
				total[i], carry = byte(value), value>>8
			}
			minimaPayloadEvidenceCache.rows++
		}
		for _, spec := range manifest.Corpora {
			for ordinal := 0; ordinal < spec.CorpusRows; ordinal++ {
				document, err := minimaDocumentAt(spec, ordinal)
				if err != nil {
					minimaPayloadEvidenceCache.err = err
					return
				}
				if deleted[document.ID] {
					continue
				}
				if replacement, ok := overrides[document.ID]; ok {
					document = replacement
				}
				add(document)
			}
		}
		ids := make([]string, 0, len(additions))
		for id := range additions {
			if !deleted[id] {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		for _, id := range ids {
			add(additions[id])
		}
		value := fmt.Sprintf("minima-committed-payload-v1:%d:%s:%s",
			minimaPayloadEvidenceCache.rows, hex.EncodeToString(xor[:]), hex.EncodeToString(total[:]))
		sum := sha256.Sum256([]byte(value))
		minimaPayloadEvidenceCache.hash = hex.EncodeToString(sum[:])
	})
	return minimaPayloadEvidenceCache.hash, minimaPayloadEvidenceCache.rows, minimaPayloadEvidenceCache.err
}

func readMinimaArtifact(path string) (minimaArtifact, error) {
	var artifact minimaArtifact
	raw, err := os.ReadFile(path)
	if err != nil {
		return artifact, err
	}
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return artifact, err
	}
	return artifact, nil
}

func readMinimaBackendEvidence(path, backend string) (minimaArtifact, error) {
	artifact, err := readMinimaArtifact(path)
	if err != nil {
		return artifact, fmt.Errorf("decode %s evidence: %w", backend, err)
	}
	if artifact.State != "partial" || artifact.Passing || artifact.Recommendation != "not_evaluated" {
		return artifact, fmt.Errorf("%s evidence is not fail-closed partial evidence", backend)
	}
	if err := validateMinimaArtifact(&artifact); err != nil {
		return artifact, fmt.Errorf("validate %s partial evidence: %w", backend, err)
	}
	if len(artifact.Backends) != 1 || artifact.Backends[0].Name != backend {
		return artifact, fmt.Errorf("%s evidence has wrong backend envelope", backend)
	}
	for _, scenario := range artifact.Scenarios {
		if scenario.Backend != backend {
			return artifact, fmt.Errorf("%s evidence contains scenario for %q", backend, scenario.Backend)
		}
	}
	if len(artifact.RawEvidence) != 1 {
		return artifact, fmt.Errorf("%s evidence must contain exactly one namespaced raw evidence object", backend)
	}
	if _, ok := artifact.RawEvidence[backend]; !ok {
		return artifact, fmt.Errorf("%s evidence is missing its namespaced raw evidence object", backend)
	}
	return artifact, nil
}

func combineMinimaEvidence(treedb, qdrant minimaArtifact, recommendation string) minimaArtifact {
	combined := minimaArtifact{
		Schema: minimaArtifactSchema, State: "pass", Passing: true, Manifest: treedb.Manifest,
		Backends:       append(append([]minimaBackendEvidence(nil), treedb.Backends...), qdrant.Backends...),
		Scenarios:      append(append([]minimaScenarioEvidence(nil), treedb.Scenarios...), qdrant.Scenarios...),
		Recommendation: recommendation,
		RawEvidence:    make(map[string]minimaRawBackendEvidence, 2),
	}
	for name, evidence := range treedb.RawEvidence {
		combined.RawEvidence[name] = evidence
	}
	for name, evidence := range qdrant.RawEvidence {
		combined.RawEvidence[name] = evidence
	}
	combined.Failures = append(combined.Failures, treedb.Failures...)
	combined.Failures = append(combined.Failures, qdrant.Failures...)
	if treedb.Manifest.CorpusSHA256 != qdrant.Manifest.CorpusSHA256 ||
		treedb.Manifest.QuerySHA256 != qdrant.Manifest.QuerySHA256 ||
		treedb.Manifest.OperationSHA256 != qdrant.Manifest.OperationSHA256 ||
		treedb.Manifest.ExpectedStateSHA256 != qdrant.Manifest.ExpectedStateSHA256 {
		combined.Failures = append(combined.Failures, "TreeDB and Qdrant evidence embed different manifests")
	}
	if len(combined.Failures) != 0 {
		combined.State, combined.Passing, combined.Recommendation = "partial", false, "not_evaluated"
	}
	return combined
}
func validateMinimaRawEvidence(artifact *minimaArtifact, backends map[string]minimaBackendEvidence) error {
	if len(artifact.RawEvidence) != len(backends) {
		return fmt.Errorf("minima artifact: requires one namespaced raw evidence object per backend")
	}
	expectedHash, expectedRows, err := minimaExpectedPayloadEvidence(&artifact.Manifest)
	if err != nil {
		return fmt.Errorf("minima artifact: compute expected payload state: %w", err)
	}
	timedPlan := artifact.Manifest.Operations[3].TimedPlan
	actualHashes := make(map[string]string, len(backends))
	for name, backend := range backends {
		raw, ok := artifact.RawEvidence[name]
		if !ok {
			return fmt.Errorf("minima artifact: %s raw evidence missing", name)
		}
		overlap := raw.TimedOverlap
		if overlap.ConfiguredSearches != timedPlan.QueryCount ||
			overlap.ExecutedSearches != timedPlan.QueryCount ||
			overlap.ConfiguredReaderConcurrency != timedPlan.ReaderConcurrency ||
			overlap.ConfiguredWriterConcurrency != timedPlan.WriterConcurrency ||
			len(overlap.Rounds) != len(timedPlan.Rounds) ||
			!overlap.AllRoundsWriterSearchOverlapObserved ||
			overlap.TimedExecutionSHA256 != backend.Operations.TimedExecutionSHA256 {
			return fmt.Errorf("minima artifact: %s raw overlap summary mismatch", name)
		}
		for ordinal, expectedRound := range timedPlan.Rounds {
			round := overlap.Rounds[ordinal]
			if round.Ordinal != ordinal || round.QueriesExecuted != expectedRound.QueryCount ||
				len(round.OverlappingReaders) != timedPlan.ReaderConcurrency ||
				!round.AllReadersOverlapObserved {
				return fmt.Errorf("minima artifact: %s raw overlap round %d incomplete", name, ordinal)
			}
			for reader, observed := range round.OverlappingReaders {
				if observed != reader {
					return fmt.Errorf("minima artifact: %s raw overlap round %d readers mismatch", name, ordinal)
				}
			}
		}
		state := raw.FinalScrollState
		if state.Algorithm == "" || !state.Match || !state.Payload.Match || !state.Vectors.Match ||
			state.ExpectedRows != expectedRows || state.ActualRows != expectedRows ||
			state.Vectors.CheckedRows != expectedRows || state.Vectors.ExpectedRows != expectedRows ||
			state.Vectors.MismatchRows != 0 ||
			!finiteNonnegative(state.Vectors.MaximumComponentDelta) ||
			state.Vectors.Tolerance != artifact.Manifest.Config.ScoreTolerance ||
			state.ExpectedHash != expectedHash || state.ActualHash != expectedHash ||
			state.Payload.ExpectedHash != expectedHash || state.Payload.ActualHash != expectedHash {
			return fmt.Errorf("minima artifact: %s raw full-state evidence mismatch", name)
		}
		actualHashes[name] = state.ActualHash
		restart := raw.RestartBoundary
		if !restart.Verified || !restart.PIDChanged ||
			restart.OldPID <= 0 || restart.NewPID <= 0 || restart.OldPID == restart.NewPID ||
			restart.HookIdentity == "" || restart.OldProcessIdentity == "" || restart.NewProcessIdentity == "" {
			return fmt.Errorf("minima artifact: %s backend restart boundary is unproven", name)
		}
		if name == "treedb" {
			if raw.ServiceLog.Path == "" || raw.ServiceLog.Tail == "" || raw.ServiceLog.MaxTailBytes != 64<<10 {
				return fmt.Errorf("minima artifact: TreeDB bounded service log evidence missing")
			}
		}
		resource := raw.ResourceMeasurement
		if !resource.Captured || resource.RSSBytes < 0 || !finiteNonnegative(resource.CPUSeconds) || resource.DiskBytes < 0 {
			return fmt.Errorf("minima artifact: %s raw resource evidence missing", name)
		}
		for _, row := range artifact.Scenarios {
			if row.Backend != name {
				continue
			}
			if !row.Resource.Captured ||
				row.Resource.RSSBytes != resource.RSSBytes ||
				row.Resource.CPUSeconds != resource.CPUSeconds ||
				row.Resource.DiskBytes != resource.DiskBytes {
				return fmt.Errorf("minima artifact: %s scenario resource summary does not match raw measurement", name)
			}
		}
	}
	if actualHashes["treedb"] == "" || actualHashes["treedb"] != actualHashes["qdrant"] {
		return fmt.Errorf("minima artifact: backend actual full-state hashes differ")
	}
	return nil
}

func writeMinimaComparisonArtifacts(artifact minimaArtifact, jsonPath, reportPath string) error {
	if jsonPath == "" || reportPath == "" {
		return errors.New("Minima comparison output and report paths are required")
	}
	if err := os.MkdirAll(filepath.Dir(jsonPath), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(reportPath), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(jsonPath, append(raw, '\n'), 0o644); err != nil {
		return err
	}
	var report strings.Builder
	fmt.Fprintf(&report, "# Minima filtered-ANN qualification\n\nState: **%s**  \nRecommendation: **%s**\n\n", artifact.State, artifact.Recommendation)
	if len(artifact.Failures) > 0 {
		report.WriteString("## Failures\n\n")
		for _, failure := range artifact.Failures {
			fmt.Fprintf(&report, "- %s\n", failure)
		}
		report.WriteByte('\n')
	}
	report.WriteString("## Scenario evidence\n\n| Backend | Scenario | Plan | Membership | Recall | Overlap | Search ms |\n|---|---|---|---|---:|---:|---:|\n")
	for _, row := range artifact.Scenarios {
		fmt.Fprintf(&report, "| %s | %s | %s | %s | %.3f | %.3f | %.3f |\n",
			row.Backend, row.Scenario, row.Route.Plan, row.Route.MembershipSource, row.Recall, row.Overlap, row.Timing.SearchMillis)
	}
	report.WriteString("\nThe JSON artifact is authoritative; this report contains no independently inferred pass state.\n")
	return os.WriteFile(reportPath, []byte(report.String()), 0o644)
}

func compareMinimaEvidence(treedbPath, qdrantPath, jsonPath, reportPath, recommendation string) error {
	treedb, err := readMinimaBackendEvidence(treedbPath, "treedb")
	if err != nil {
		return err
	}
	qdrant, err := readMinimaBackendEvidence(qdrantPath, "qdrant")
	if err != nil {
		return err
	}
	combined := combineMinimaEvidence(treedb, qdrant, recommendation)
	validationErr := validateMinimaArtifact(&combined)
	if validationErr != nil {
		combined.State, combined.Passing, combined.Recommendation = "partial", false, "not_evaluated"
		combined.Failures = append(combined.Failures, "fail-closed validator: "+validationErr.Error())
	}
	if err := writeMinimaComparisonArtifacts(combined, jsonPath, reportPath); err != nil {
		return err
	}
	return validationErr
}
