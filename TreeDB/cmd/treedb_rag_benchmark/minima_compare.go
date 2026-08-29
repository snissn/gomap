package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	minimaResourceRSSSemantics            = "sum of positive per-process end-minus-baseline RSS growth; endpoint delta, not peak RSS"
	minimaResourceCPUSemantics            = "sum of positive per-process end-minus-baseline CPU seconds"
	minimaResourceDiskSemantics           = "sum of positive per-process-segment end-minus-baseline durable storage bytes"
	minimaQdrantInitialHNSWConfig         = `{"m":0,"ef_construct":100,"full_scan_threshold":10000,"max_indexing_threads":0,"on_disk":false}`
	minimaQdrantInitialOptimizerConfig    = `{"deleted_threshold":0.2,"vacuum_min_vector_number":1000,"default_segment_number":0,"indexing_threshold":0,"flush_interval_sec":5}`
	minimaQdrantProductionHNSWConfig      = `{"m":16,"ef_construct":100,"full_scan_threshold":10000,"max_indexing_threads":0,"on_disk":false}`
	minimaQdrantProductionOptimizerConfig = `{"deleted_threshold":0.2,"vacuum_min_vector_number":1000,"default_segment_number":0,"indexing_threshold":10000,"flush_interval_sec":5}`
)

const (
	minimaBatchCorrelationSchema          = "treedb-minima-upsert-batch-correlations/v1"
	minimaCompactBatchCorrelationMaxBytes = 2048
)

const (
	minimaPhaseUnattributedRule          = "total_duration_nanos = sum(phase.duration_nanos) + unattributed_nanos; unattributed_nanos <= max(60000000000, total_duration_nanos / 100); unattributed covers only runner bookkeeping between declared boundaries"
	minimaPhaseUnattributedAbsoluteNanos = int64(60_000_000_000)
)

var minimaTreeDBPhaseNames = []string{
	"initial_durable_load",
	"warmup_search",
	"timed_search_write_overlap",
	"lifecycle_mutations",
	"pre_close_queries",
	"restart_open_readiness",
	"post_reopen",
	"final_state_scroll_artifact_work",
}

var minimaTreeDBRestartOrdinal = slices.Index(minimaTreeDBPhaseNames, "restart_open_readiness")

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

type minimaRawPhaseResourceEndpoint struct {
	Captured        bool              `json:"captured"`
	RSSBytes        int64             `json:"rss_bytes"`
	CPUSeconds      float64           `json:"cpu_seconds"`
	DiskBytes       int64             `json:"disk_bytes"`
	Availability    map[string]string `json:"availability,omitempty"`
	PID             int               `json:"pid"`
	ProcessIdentity string            `json:"process_identity"`
}

type minimaRawPhaseResourceSegment struct {
	Start minimaRawPhaseResourceEndpoint `json:"start"`
	End   minimaRawPhaseResourceEndpoint `json:"end"`
}

type minimaRawPhaseBoundary struct {
	Name                     string                          `json:"name"`
	Classification           string                          `json:"classification"`
	StartNanos               int64                           `json:"start_nanos"`
	EndNanos                 int64                           `json:"end_nanos"`
	DurationNanos            int64                           `json:"duration_nanos"`
	SampleCount              int                             `json:"sample_count"`
	SampleDurationNanos      int64                           `json:"sample_duration_nanos"`
	ResourceSegments         []minimaRawPhaseResourceSegment `json:"resource_segments"`
	ResourceEvidenceComplete *bool                           `json:"resource_evidence_complete,omitempty"`
	IncompleteReason         string                          `json:"incomplete_reason,omitempty"`
}

type minimaRawPhaseAttribution struct {
	Clock              string                   `json:"clock"`
	TotalStartNanos    int64                    `json:"total_start_nanos"`
	TotalEndNanos      int64                    `json:"total_end_nanos"`
	TotalDurationNanos int64                    `json:"total_duration_nanos"`
	UnattributedNanos  int64                    `json:"unattributed_nanos"`
	UnattributedRule   string                   `json:"unattributed_rule"`
	Phases             []minimaRawPhaseBoundary `json:"phases"`
}

type minimaRawResourceSnapshot struct {
	Captured     bool              `json:"captured"`
	RSSBytes     int64             `json:"rss_bytes"`
	CPUSeconds   float64           `json:"cpu_seconds"`
	DiskBytes    int64             `json:"disk_bytes"`
	Availability map[string]string `json:"availability,omitempty"`
}

type minimaRawResourceSemantics struct {
	RSSBytes   string `json:"rss_bytes"`
	CPUSeconds string `json:"cpu_seconds"`
	DiskBytes  string `json:"disk_bytes"`
}

type minimaRawResourceSegment struct {
	Captured   bool                      `json:"captured"`
	RSSBytes   int64                     `json:"rss_bytes"`
	CPUSeconds float64                   `json:"cpu_seconds"`
	DiskBytes  int64                     `json:"disk_bytes"`
	Baseline   minimaRawResourceSnapshot `json:"baseline"`
	End        minimaRawResourceSnapshot `json:"end"`
}

type minimaRawResourceMeasurement struct {
	Captured   bool                       `json:"captured"`
	RSSBytes   int64                      `json:"rss_bytes"`
	CPUSeconds float64                    `json:"cpu_seconds"`
	DiskBytes  int64                      `json:"disk_bytes"`
	Semantics  minimaRawResourceSemantics `json:"semantics"`
	Segments   []minimaRawResourceSegment `json:"segments"`
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

type minimaRawNativeRouteResponse struct {
	MembershipSource     string `json:"membership_source"`
	Plan                 string `json:"plan"`
	ProbeIDs             int    `json:"probe_ids"`
	Candidates           int    `json:"candidates"`
	CandidateIDs         int    `json:"candidate_ids"`
	Retained             int    `json:"retained"`
	Refined              int    `json:"refined"`
	Visited              int    `json:"visited"`
	Scored               int    `json:"scored"`
	Admitted             int    `json:"admitted"`
	VisibilityMismatches int    `json:"visibility_mismatches"`
	VisibilityRetries    int    `json:"visibility_retries"`
}

type minimaRawQdrantConfigurationTransition struct {
	Boundary                string          `json:"boundary"`
	Attempted               bool            `json:"attempted"`
	Completed               bool            `json:"completed"`
	Error                   string          `json:"error,omitempty"`
	InitialUploadHNSW       json.RawMessage `json:"initial_upload_hnsw"`
	InitialUploadOptimizers json.RawMessage `json:"initial_upload_optimizers"`
	ProductionHNSW          json.RawMessage `json:"production_hnsw"`
	ProductionOptimizers    json.RawMessage `json:"production_optimizers"`
}

type minimaRawQdrantOptimizationSnapshot struct {
	Available bool            `json:"available"`
	Detail    json.RawMessage `json:"detail"`
}

type minimaRawQdrantReadinessSnapshot struct {
	Status              string                              `json:"status"`
	OptimizerStatus     json.RawMessage                     `json:"optimizer_status"`
	PointsCount         *int                                `json:"points_count"`
	IndexedVectorsCount *int                                `json:"indexed_vectors_count"`
	SegmentsCount       *int                                `json:"segments_count"`
	PayloadSchema       map[string]json.RawMessage          `json:"payload_schema"`
	Config              json.RawMessage                     `json:"config"`
	Optimizations       minimaRawQdrantOptimizationSnapshot `json:"optimizations"`
	ExactPointsCount    *int                                `json:"exact_points_count"`
}

type minimaRawQdrantReadinessSession struct {
	Phase               string                             `json:"phase"`
	DeadlineSeconds     float64                            `json:"deadline_seconds"`
	ExpectedPointsCount *int                               `json:"expected_points_count"`
	Snapshots           []minimaRawQdrantReadinessSnapshot `json:"snapshots"`
	ResourceSamples     []json.RawMessage                  `json:"resource_samples"`
	Outcome             string                             `json:"outcome"`
	Disposition         string                             `json:"disposition"`
}

type minimaRawQdrantReadiness struct {
	Sessions                  []minimaRawQdrantReadinessSession `json:"sessions"`
	LatestNonReadyDisposition string                            `json:"latest_non_ready_disposition"`
}

type minimaRawBatchCorrelationContract struct {
	Schema                  string   `json:"schema"`
	RecordCount             int      `json:"record_count"`
	MaximumRecordCount      int      `json:"maximum_record_count"`
	CompactCompletedRecords int      `json:"compact_completed_records"`
	FullDiagnosticRecords   int      `json:"full_diagnostic_records"`
	CompactRecordMaxBytes   int      `json:"compact_record_max_bytes"`
	FullStatsRetention      []string `json:"full_stats_retention"`
}
type minimaRawBackendEvidence struct {
	PhaseLatencyDistributions         map[string]minimaRawLatencyDistribution `json:"phase_latency_distributions,omitempty"`
	Events                            []json.RawMessage                       `json:"events,omitempty"`
	TimedOverlap                      minimaRawTimedOverlap                   `json:"timed_overlap"`
	FinalScrollState                  minimaRawFinalState                     `json:"final_scroll_state"`
	ResourceMeasurement               minimaRawResourceMeasurement            `json:"resource_measurement"`
	RestartBoundary                   minimaRawRestartBoundary                `json:"restart_boundary"`
	ServiceLog                        minimaRawServiceLog                     `json:"service_log,omitempty"`
	ResourceAvailability              map[string]map[string]string            `json:"resource_availability,omitempty"`
	NativeRouteResponses              map[string]json.RawMessage              `json:"native_route_responses,omitempty"`
	CollectionConfigurationTransition *minimaRawQdrantConfigurationTransition `json:"collection_configuration_transition,omitempty"`
	Readiness                         *minimaRawQdrantReadiness               `json:"readiness,omitempty"`
	PhaseAttribution                  *minimaRawPhaseAttribution              `json:"phase_attribution,omitempty"`
	UpsertBatchCorrelations           []json.RawMessage                       `json:"upsert_batch_correlations,omitempty"`
	UpsertBatchCorrelationContract    *minimaRawBatchCorrelationContract      `json:"upsert_batch_correlation_contract,omitempty"`
	DiagnosticResume                  json.RawMessage                         `json:"diagnostic_resume,omitempty"`
	Diagnostics                       json.RawMessage                         `json:"diagnostics,omitempty"`
}

type minimaPayloadEvidence struct {
	hash string
	rows int
	err  error
}

var minimaPayloadEvidenceCache struct {
	sync.Mutex
	values map[string]minimaPayloadEvidence
}

func minimaExpectedPayloadEvidence(manifest *minimaManifest) (string, int, error) {
	key, err := minimaDigest(manifest)
	if err != nil {
		return "", 0, fmt.Errorf("digest Minima payload manifest: %w", err)
	}
	minimaPayloadEvidenceCache.Lock()
	cached, ok := minimaPayloadEvidenceCache.values[key]
	minimaPayloadEvidenceCache.Unlock()
	if ok {
		return cached.hash, cached.rows, cached.err
	}

	result := minimaPayloadEvidence{}
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
		result.rows++
	}
	for _, spec := range manifest.Corpora {
		for ordinal := 0; ordinal < spec.CorpusRows; ordinal++ {
			document, documentErr := minimaDocumentAt(spec, ordinal)
			if documentErr != nil {
				result.err = documentErr
				break
			}
			if deleted[document.ID] {
				continue
			}
			if replacement, exists := overrides[document.ID]; exists {
				document = replacement
			}
			add(document)
		}
		if result.err != nil {
			break
		}
	}
	if result.err == nil {
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
			result.rows, hex.EncodeToString(xor[:]), hex.EncodeToString(total[:]))
		sum := sha256.Sum256([]byte(value))
		result.hash = hex.EncodeToString(sum[:])
	}

	minimaPayloadEvidenceCache.Lock()
	if minimaPayloadEvidenceCache.values == nil {
		minimaPayloadEvidenceCache.values = make(map[string]minimaPayloadEvidence)
	}
	if existing, exists := minimaPayloadEvidenceCache.values[key]; exists {
		result = existing
	} else {
		minimaPayloadEvidenceCache.values[key] = result
	}
	minimaPayloadEvidenceCache.Unlock()
	return result.hash, result.rows, result.err
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
func validateMinimaNativeRouteResponse(raw map[string]json.RawMessage, row minimaScenarioEvidence) error {
	encoded, ok := raw[row.Scenario]
	if !ok {
		return fmt.Errorf("minima artifact: TreeDB raw route response missing for %s", row.Scenario)
	}
	var route minimaRawNativeRouteResponse
	if err := json.Unmarshal(encoded, &route); err != nil {
		return fmt.Errorf("minima artifact: decode TreeDB raw route response for %s: %w", row.Scenario, err)
	}
	if row.Route.ProbeIDs == nil ||
		row.Route.CandidateIDs == nil ||
		row.Route.RetainedCandidateIDs == nil ||
		row.Route.RefinedCandidateIDs == nil ||
		row.Route.VisitedCandidates == nil ||
		row.Route.ScoredCandidates == nil ||
		row.Route.AdmittedCandidates == nil ||
		row.Visibility.MismatchCount == nil ||
		row.Visibility.RetryCount == nil {
		return fmt.Errorf("minima artifact: TreeDB route summary counters missing for %s", row.Scenario)
	}
	if route.MembershipSource != row.Route.MembershipSource ||
		route.Plan != row.Route.Plan ||
		route.ProbeIDs != *row.Route.ProbeIDs ||
		route.Candidates != *row.Route.VisitedCandidates ||
		route.CandidateIDs != *row.Route.CandidateIDs ||
		route.Retained != *row.Route.RetainedCandidateIDs ||
		route.Refined != *row.Route.RefinedCandidateIDs ||
		route.Visited != *row.Route.VisitedCandidates ||
		route.Scored != *row.Route.ScoredCandidates ||
		route.Admitted != *row.Route.AdmittedCandidates ||
		route.VisibilityMismatches != *row.Visibility.MismatchCount ||
		route.VisibilityRetries != *row.Visibility.RetryCount {
		return fmt.Errorf("minima artifact: TreeDB raw route response disagrees with summary for %s", row.Scenario)
	}
	return nil
}

func minimaExactHex(value string, bytes int) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytes
}

func validateMinimaTreeDBProvenance(backend minimaBackendEvidence) error {
	configuration := backend.Configuration
	if !minimaExactHex(configuration["product_commit"], 20) ||
		configuration["harness_commit"] != configuration["product_commit"] ||
		configuration["service_binary_vcs_revision"] != configuration["product_commit"] ||
		configuration["service_binary_vcs_modified"] != "false" ||
		!minimaExactHex(configuration["service_binary_sha256"], sha256.Size) ||
		!minimaExactHex(configuration["runner_sha256"], sha256.Size) ||
		backend.Environment["host"] == "" {
		return errors.New("minima artifact: TreeDB product/harness/binary/host provenance is incomplete")
	}
	operationTimeout, operationErr := strconv.ParseFloat(configuration["operation_timeout_seconds"], 64)
	startupTimeout, startupErr := strconv.ParseFloat(configuration["startup_reopen_timeout_seconds"], 64)
	shutdownTimeout, shutdownErr := strconv.ParseFloat(configuration["shutdown_timeout_seconds"], 64)
	if operationErr != nil || operationTimeout != 120 ||
		startupErr != nil || startupTimeout <= 0 || !finiteNonnegative(startupTimeout) ||
		shutdownErr != nil || shutdownTimeout != operationTimeout {
		return errors.New("minima artifact: TreeDB timeout provenance is invalid")
	}
	return nil
}
func validateMinimaExpectedCommit(artifact *minimaArtifact, expected string, required bool) error {
	if expected == "" && !required {
		return nil
	}
	if !minimaExactHex(expected, 20) {
		return errors.New("minima artifact: expected merged commit must be a full 40-hex SHA")
	}
	for _, backend := range artifact.Backends {
		if backend.Name != "treedb" {
			continue
		}
		if backend.Configuration["product_commit"] != expected ||
			backend.Configuration["harness_commit"] != expected ||
			backend.Configuration["service_binary_vcs_revision"] != expected ||
			backend.Configuration["service_binary_vcs_modified"] != "false" {
			return errors.New("minima artifact: TreeDB product/harness/binary commits do not match the expected merged commit")
		}
		return nil
	}
	return errors.New("minima artifact: expected merged commit requires TreeDB evidence")
}

func minimaPhaseUnattributedLimit(totalDurationNanos int64) int64 {
	return max(minimaPhaseUnattributedAbsoluteNanos, totalDurationNanos/100)
}

func validateMinimaTreeDBPhaseAttribution(value minimaRawPhaseAttribution, restart minimaRawRestartBoundary) error {
	if value.Clock != "time.monotonic_ns" ||
		value.UnattributedRule != minimaPhaseUnattributedRule ||
		value.TotalStartNanos <= 0 ||
		value.TotalEndNanos <= value.TotalStartNanos ||
		value.TotalDurationNanos != value.TotalEndNanos-value.TotalStartNanos ||
		len(value.Phases) != len(minimaTreeDBPhaseNames) {
		return errors.New("minima artifact: TreeDB phase attribution envelope is incomplete")
	}
	if minimaTreeDBRestartOrdinal < 0 {
		return errors.New("minima artifact: TreeDB restart phase name is undeclared")
	}
	cursor, attributed := value.TotalStartNanos, int64(0)
	type processKey struct {
		PID      int
		Identity string
	}
	lastProcessCPU := make(map[processKey]float64)
	for ordinal, phase := range value.Phases {
		classification := "production_path"
		expectedSegments := 1
		if ordinal == len(value.Phases)-1 {
			classification = "qualification_only"
		}
		if ordinal == minimaTreeDBRestartOrdinal {
			expectedSegments = 2
		}
		if phase.Name != minimaTreeDBPhaseNames[ordinal] ||
			phase.Classification != classification ||
			phase.StartNanos < cursor ||
			phase.EndNanos <= phase.StartNanos ||
			phase.EndNanos > value.TotalEndNanos ||
			phase.DurationNanos != phase.EndNanos-phase.StartNanos ||
			phase.SampleCount < 0 ||
			(ordinal != minimaTreeDBRestartOrdinal && phase.SampleCount == 0) ||
			phase.SampleDurationNanos < 0 ||
			(phase.ResourceEvidenceComplete != nil && !*phase.ResourceEvidenceComplete) ||
			phase.IncompleteReason != "" ||
			len(phase.ResourceSegments) != expectedSegments ||
			phase.DurationNanos > value.TotalDurationNanos-attributed {
			return fmt.Errorf("minima artifact: TreeDB phase %d is missing or invalid", ordinal)
		}
		for _, segment := range phase.ResourceSegments {
			start, end := segment.Start, segment.End
			if !start.Captured || !end.Captured ||
				start.PID <= 0 || end.PID != start.PID ||
				start.ProcessIdentity == "" || end.ProcessIdentity != start.ProcessIdentity ||
				start.RSSBytes < 0 || end.RSSBytes < 0 ||
				!finiteNonnegative(start.CPUSeconds) || !finiteNonnegative(end.CPUSeconds) ||
				end.CPUSeconds < start.CPUSeconds ||
				start.DiskBytes < 0 || end.DiskBytes < 0 {
				return fmt.Errorf("minima artifact: TreeDB phase %d resource identity is invalid", ordinal)
			}
			process := processKey{PID: start.PID, Identity: start.ProcessIdentity}
			if previous, ok := lastProcessCPU[process]; ok && start.CPUSeconds < previous {
				return fmt.Errorf("minima artifact: TreeDB phase %d cumulative CPU resets within one process", ordinal)
			}
			lastProcessCPU[process] = end.CPUSeconds
		}
		if ordinal < minimaTreeDBRestartOrdinal {
			resource := phase.ResourceSegments[0].Start
			if resource.PID != restart.OldPID || resource.ProcessIdentity != restart.OldProcessIdentity {
				return fmt.Errorf("minima artifact: TreeDB pre-restart phase %d has the wrong process identity", ordinal)
			}
		} else if ordinal > minimaTreeDBRestartOrdinal {
			resource := phase.ResourceSegments[0].Start
			if resource.PID != restart.NewPID || resource.ProcessIdentity != restart.NewProcessIdentity {
				return fmt.Errorf("minima artifact: TreeDB post-restart phase %d has the wrong process identity", ordinal)
			}
		}
		if ordinal == minimaTreeDBRestartOrdinal {
			old, fresh := phase.ResourceSegments[0], phase.ResourceSegments[1]
			if !restart.Verified || !restart.PIDChanged ||
				old.Start.PID != restart.OldPID || old.Start.ProcessIdentity != restart.OldProcessIdentity ||
				fresh.Start.PID != restart.NewPID || fresh.Start.ProcessIdentity != restart.NewProcessIdentity ||
				old.Start.PID == fresh.Start.PID ||
				fresh.Start.RSSBytes != 0 || fresh.Start.CPUSeconds != 0 ||
				fresh.Start.DiskBytes != old.End.DiskBytes {
				return errors.New("minima artifact: TreeDB restart phase resource split is invalid")
			}
		}
		attributed += phase.DurationNanos
		cursor = phase.EndNanos
	}
	if attributed > value.TotalDurationNanos ||
		value.UnattributedNanos != value.TotalDurationNanos-attributed {
		return errors.New("minima artifact: TreeDB phase totals do not reconcile")
	}
	if value.UnattributedNanos > minimaPhaseUnattributedLimit(value.TotalDurationNanos) {
		return errors.New("minima artifact: TreeDB unattributed phase overhead exceeds its bound")
	}
	return nil
}

func validateMinimaResourceMeasurement(backend string, resource minimaRawResourceMeasurement) error {
	if !resource.Captured || resource.RSSBytes < 0 || !finiteNonnegative(resource.CPUSeconds) || resource.DiskBytes < 0 {
		return fmt.Errorf("minima artifact: %s raw resource evidence missing", backend)
	}
	if resource.Semantics.RSSBytes != minimaResourceRSSSemantics ||
		resource.Semantics.CPUSeconds != minimaResourceCPUSemantics ||
		resource.Semantics.DiskBytes != minimaResourceDiskSemantics {
		return fmt.Errorf("minima artifact: %s raw resource semantics mismatch", backend)
	}
	if resource.Baseline == nil || resource.End == nil || len(resource.Segments) == 0 {
		return fmt.Errorf("minima artifact: %s raw resource baseline/end segments missing", backend)
	}
	var rssBytes, diskBytes int64
	var cpuSeconds float64
	for ordinal, segment := range resource.Segments {
		if !segment.Captured || !segment.Baseline.Captured || !segment.End.Captured ||
			segment.Baseline.RSSBytes < 0 || segment.End.RSSBytes < 0 ||
			!finiteNonnegative(segment.Baseline.CPUSeconds) || !finiteNonnegative(segment.End.CPUSeconds) ||
			segment.Baseline.DiskBytes < 0 || segment.End.DiskBytes < 0 {
			return fmt.Errorf("minima artifact: %s raw resource segment %d is incomplete", backend, ordinal)
		}
		wantRSS := max(int64(0), segment.End.RSSBytes-segment.Baseline.RSSBytes)
		wantCPU := max(0, segment.End.CPUSeconds-segment.Baseline.CPUSeconds)
		wantDisk := max(int64(0), segment.End.DiskBytes-segment.Baseline.DiskBytes)
		if segment.RSSBytes != wantRSS || math.Abs(segment.CPUSeconds-wantCPU) > 1e-9 || segment.DiskBytes != wantDisk {
			return fmt.Errorf("minima artifact: %s raw resource segment %d delta mismatch", backend, ordinal)
		}
		rssBytes += wantRSS
		cpuSeconds += wantCPU
		diskBytes += wantDisk
	}
	first, last := resource.Segments[0].Baseline, resource.Segments[len(resource.Segments)-1].End
	if resource.Baseline.Captured != first.Captured ||
		resource.Baseline.RSSBytes != first.RSSBytes ||
		resource.Baseline.CPUSeconds != first.CPUSeconds ||
		resource.Baseline.DiskBytes != first.DiskBytes ||
		resource.End.Captured != last.Captured ||
		resource.End.RSSBytes != last.RSSBytes ||
		resource.End.CPUSeconds != last.CPUSeconds ||
		resource.End.DiskBytes != last.DiskBytes {
		return fmt.Errorf("minima artifact: %s raw resource envelope does not match its segments", backend)
	}
	if resource.RSSBytes != rssBytes || math.Abs(resource.CPUSeconds-cpuSeconds) > 1e-9 || resource.DiskBytes != diskBytes {
		return fmt.Errorf("minima artifact: %s raw resource aggregate delta mismatch", backend)
	}
	return nil
}
func minimaPhaseResourceMatchesSnapshot(phase minimaRawPhaseResourceEndpoint, aggregate minimaRawResourceSnapshot) bool {
	return phase.Captured == aggregate.Captured &&
		phase.RSSBytes == aggregate.RSSBytes &&
		phase.CPUSeconds == aggregate.CPUSeconds &&
		phase.DiskBytes == aggregate.DiskBytes
}

func validateMinimaTreeDBResourceReconciliation(
	phase minimaRawPhaseAttribution,
	resource minimaRawResourceMeasurement,
) error {
	if minimaTreeDBRestartOrdinal < 0 ||
		len(resource.Segments) != 2 ||
		len(phase.Phases) <= minimaTreeDBRestartOrdinal ||
		len(phase.Phases[minimaTreeDBRestartOrdinal].ResourceSegments) != 2 {

		return errors.New("minima artifact: TreeDB phase and aggregate process segments are incomplete")
	}
	restart := phase.Phases[minimaTreeDBRestartOrdinal].ResourceSegments
	if !minimaPhaseResourceMatchesSnapshot(restart[0].End, resource.Segments[0].End) ||
		!minimaPhaseResourceMatchesSnapshot(restart[1].Start, resource.Segments[1].Baseline) {
		return errors.New("minima artifact: TreeDB phase and aggregate restart resources disagree")
	}
	return nil
}
func minimaMaximumBatchCorrelationRecords(manifest minimaManifest) int {
	batchSize := manifest.Config.BatchSize
	if batchSize <= 0 {
		return 0
	}
	maximum := 0
	for _, operation := range manifest.Operations {
		for _, insertion := range operation.InsertRanges {
			maximum += (insertion.Rows + batchSize - 1) / batchSize
		}
		if (operation.Effect == "insert" || operation.Effect == "update") && len(operation.Documents) != 0 {
			maximum += (len(operation.Documents) + batchSize - 1) / batchSize
		}
	}
	return maximum
}

func validateMinimaTreeDBBatchCorrelations(manifest minimaManifest, raw minimaRawBackendEvidence) error {
	contract := raw.UpsertBatchCorrelationContract
	expectedMaximum := minimaMaximumBatchCorrelationRecords(manifest)
	if contract == nil ||
		contract.Schema != minimaBatchCorrelationSchema ||
		contract.RecordCount != len(raw.UpsertBatchCorrelations) ||
		contract.MaximumRecordCount != expectedMaximum ||
		contract.RecordCount > contract.MaximumRecordCount ||
		contract.CompactCompletedRecords+contract.FullDiagnosticRecords != contract.RecordCount ||
		contract.CompactRecordMaxBytes != minimaCompactBatchCorrelationMaxBytes ||
		!reflect.DeepEqual(contract.FullStatsRetention, []string{"failed", "timeout", "slow", "profile_captured"}) {
		return errors.New("minima artifact: TreeDB batch correlation cardinality contract is invalid")
	}
	compact, full := 0, 0
	for _, encoded := range raw.UpsertBatchCorrelations {
		var correlation struct {
			Outcome        string          `json:"outcome"`
			StatsRetention string          `json:"stats_retention"`
			CaptureReason  string          `json:"capture_reason"`
			BeforeStats    json.RawMessage `json:"before_stats"`
			AfterStats     json.RawMessage `json:"after_stats"`
			ProfileCapture struct {
				Status string `json:"status"`
			} `json:"profile_capture"`
		}
		if err := json.Unmarshal(encoded, &correlation); err != nil {
			return fmt.Errorf("minima artifact: decode TreeDB batch correlation: %w", err)
		}
		switch correlation.StatsRetention {
		case "compact_completed":
			var decoded any
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				return fmt.Errorf("minima artifact: decode compact TreeDB batch correlation: %w", err)
			}
			canonical, err := json.Marshal(decoded)
			if err != nil {
				return fmt.Errorf("minima artifact: encode compact TreeDB batch correlation: %w", err)
			}
			if correlation.Outcome != "completed" ||
				len(correlation.BeforeStats) != 0 || len(correlation.AfterStats) != 0 ||
				correlation.ProfileCapture.Status != "not_triggered" ||
				len(canonical) > minimaCompactBatchCorrelationMaxBytes {
				return errors.New("minima artifact: compact TreeDB batch correlation is invalid")
			}
			compact++
		case "full_diagnostic":
			diagnostic := correlation.Outcome == "failed" || correlation.Outcome == "timeout" ||
				correlation.CaptureReason == "slow" || correlation.ProfileCapture.Status != "not_triggered"
			if !diagnostic || len(correlation.BeforeStats) == 0 || len(correlation.AfterStats) == 0 {
				return errors.New("minima artifact: full TreeDB batch correlation lacks diagnostic evidence")
			}
			full++
		default:
			return errors.New("minima artifact: TreeDB batch correlation retention is undeclared")
		}
	}
	if compact != contract.CompactCompletedRecords || full != contract.FullDiagnosticRecords {
		return errors.New("minima artifact: TreeDB batch correlation retention counts disagree")
	}
	return nil
}

func minimaRawJSONMatchesConfiguration(raw json.RawMessage, encoded string) bool {
	if len(raw) == 0 || encoded == "" {
		return false
	}
	var actual, expected any
	if json.Unmarshal(raw, &actual) != nil || json.Unmarshal([]byte(encoded), &expected) != nil {
		return false
	}
	return reflect.DeepEqual(actual, expected)
}

func minimaJSONContainsExpected(actual, expected any) bool {
	expectedMap, ok := expected.(map[string]any)
	if !ok {
		return reflect.DeepEqual(actual, expected)
	}
	actualMap, ok := actual.(map[string]any)
	if !ok {
		return false
	}
	for key, expectedValue := range expectedMap {
		actualValue, exists := actualMap[key]
		if !exists || !minimaJSONContainsExpected(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

func minimaQdrantCollectionConfigMatches(raw json.RawMessage, hnsw, optimizer string) bool {
	var config struct {
		HNSW      json.RawMessage `json:"hnsw_config"`
		Optimizer json.RawMessage `json:"optimizer_config"`
	}
	var actualHNSW, actualOptimizer, expectedHNSW, expectedOptimizer any
	return json.Unmarshal(raw, &config) == nil &&
		json.Unmarshal(config.HNSW, &actualHNSW) == nil &&
		json.Unmarshal(config.Optimizer, &actualOptimizer) == nil &&
		json.Unmarshal([]byte(hnsw), &expectedHNSW) == nil &&
		json.Unmarshal([]byte(optimizer), &expectedOptimizer) == nil &&
		minimaJSONContainsExpected(actualHNSW, expectedHNSW) &&
		minimaJSONContainsExpected(actualOptimizer, expectedOptimizer)
}

func minimaQdrantOptimizerReady(raw json.RawMessage) bool {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text == "ok"
	}
	var status struct {
		OK *bool `json:"ok"`
	}
	return json.Unmarshal(raw, &status) == nil && status.OK != nil && *status.OK
}

func validateMinimaQdrantReadiness(raw minimaRawBackendEvidence, backend minimaBackendEvidence, expectedSessions int) error {
	if raw.CollectionConfigurationTransition == nil || raw.Readiness == nil {
		return fmt.Errorf("minima artifact: Qdrant transition/readiness evidence is missing")
	}
	transition := *raw.CollectionConfigurationTransition
	if transition.Boundary != "initial_batch_insert_to_warmup_search" ||
		!transition.Attempted || !transition.Completed || transition.Error != "" {
		return fmt.Errorf("minima artifact: Qdrant production configuration transition is incomplete")
	}
	for _, check := range []struct {
		raw      json.RawMessage
		key      string
		expected string
	}{
		{transition.InitialUploadHNSW, "initial_upload_hnsw", minimaQdrantInitialHNSWConfig},
		{transition.InitialUploadOptimizers, "initial_upload_optimizers", minimaQdrantInitialOptimizerConfig},
		{transition.ProductionHNSW, "production_hnsw", minimaQdrantProductionHNSWConfig},
		{transition.ProductionOptimizers, "production_optimizers", minimaQdrantProductionOptimizerConfig},
	} {
		if !minimaRawJSONMatchesConfiguration(check.raw, check.expected) ||
			!minimaRawJSONMatchesConfiguration(check.raw, backend.Configuration[check.key]) {
			return fmt.Errorf("minima artifact: Qdrant configuration transition disagrees with %s", check.key)
		}
	}
	if !minimaQdrantCollectionConfigMatches(
		json.RawMessage(backend.Configuration["effective_collection"]),
		minimaQdrantProductionHNSWConfig, minimaQdrantProductionOptimizerConfig,
	) {
		return fmt.Errorf("minima artifact: Qdrant effective production configuration is unproven")
	}
	readiness := raw.Readiness
	if len(readiness.Sessions) != expectedSessions || readiness.LatestNonReadyDisposition != "none" ||
		readiness.Sessions[0].Phase != "initial_upload_collection_created" ||
		readiness.Sessions[1].Phase != "initial_load_to_query" ||
		readiness.Sessions[0].ExpectedPointsCount == nil ||
		*readiness.Sessions[0].ExpectedPointsCount != 0 ||
		readiness.Sessions[1].ExpectedPointsCount == nil ||
		*readiness.Sessions[1].ExpectedPointsCount <= 0 {
		return fmt.Errorf("minima artifact: Qdrant readiness sessions are incomplete")
	}
	for ordinal, session := range readiness.Sessions {
		if session.Phase == "" || !finiteNonnegative(session.DeadlineSeconds) ||
			session.DeadlineSeconds == 0 || session.Outcome != "ready" ||
			session.Disposition != "ready" || len(session.Snapshots) == 0 ||
			len(session.ResourceSamples) == 0 {
			return fmt.Errorf("minima artifact: Qdrant readiness session %d is incomplete", ordinal)
		}
		snapshot := session.Snapshots[len(session.Snapshots)-1]
		if snapshot.Status != "green" || !minimaQdrantOptimizerReady(snapshot.OptimizerStatus) ||
			snapshot.PointsCount == nil || *snapshot.PointsCount < 0 ||
			snapshot.IndexedVectorsCount == nil || *snapshot.IndexedVectorsCount < 0 ||
			snapshot.SegmentsCount == nil || *snapshot.SegmentsCount <= 0 ||
			snapshot.PayloadSchema["user_id"] == nil || snapshot.PayloadSchema["fpath"] == nil ||
			!snapshot.Optimizations.Available || len(snapshot.Optimizations.Detail) == 0 {
			return fmt.Errorf("minima artifact: Qdrant readiness session %d final snapshot is incomplete", ordinal)
		}
		if session.ExpectedPointsCount != nil &&
			(snapshot.ExactPointsCount == nil || *snapshot.ExactPointsCount != *session.ExpectedPointsCount) {
			return fmt.Errorf("minima artifact: Qdrant readiness session %d exact point count mismatch", ordinal)
		}
		wantHNSW, wantOptimizer := minimaQdrantProductionHNSWConfig, minimaQdrantProductionOptimizerConfig
		if ordinal == 0 {
			wantHNSW, wantOptimizer = minimaQdrantInitialHNSWConfig, minimaQdrantInitialOptimizerConfig
		}
		if !minimaQdrantCollectionConfigMatches(snapshot.Config, wantHNSW, wantOptimizer) {
			return fmt.Errorf("minima artifact: Qdrant readiness session %d configuration mismatch", ordinal)
		}
		var detail struct {
			Summary map[string]any    `json:"summary"`
			Running []json.RawMessage `json:"running"`
		}
		if json.Unmarshal(snapshot.Optimizations.Detail, &detail) != nil ||
			detail.Summary == nil || detail.Running == nil {
			return fmt.Errorf("minima artifact: Qdrant readiness session %d optimizer detail is incomplete", ordinal)
		}
	}
	return nil
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
			if len(raw.NativeRouteResponses) != len(artifact.Manifest.Corpora) {
				return fmt.Errorf("minima artifact: TreeDB raw route responses are incomplete")
			}
			if err := validateMinimaTreeDBProvenance(backend); err != nil {
				return err
			}
			if raw.PhaseAttribution == nil {
				return errors.New("minima artifact: TreeDB phase attribution is missing")
			}
			if err := validateMinimaTreeDBPhaseAttribution(*raw.PhaseAttribution, raw.RestartBoundary); err != nil {
				return err
			}
			if err := validateMinimaTreeDBBatchCorrelations(artifact.Manifest, raw); err != nil {
				return err
			}
		}
		if name == "qdrant" {
			if err := validateMinimaQdrantReadiness(raw, backend, len(timedPlan.Rounds)+7); err != nil {
				return err
			}
		}
		resource := raw.ResourceMeasurement
		if err := validateMinimaResourceMeasurement(name, resource); err != nil {
			return err
		}
		if name == "treedb" {
			if err := validateMinimaTreeDBResourceReconciliation(*raw.PhaseAttribution, resource); err != nil {
				return err
			}
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
			if name == "treedb" {
				if err := validateMinimaNativeRouteResponse(raw.NativeRouteResponses, row); err != nil {
					return err
				}
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

func compareMinimaEvidence(treedbPath, qdrantPath, jsonPath, reportPath, recommendation, expectedCommit string) error {
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
	if validationErr == nil {
		validationErr = validateMinimaExpectedCommit(&combined, expectedCommit, true)
	}
	if validationErr != nil {
		combined.State, combined.Passing, combined.Recommendation = "partial", false, "not_evaluated"
		combined.Failures = append(combined.Failures, "fail-closed validator: "+validationErr.Error())
	}
	if err := writeMinimaComparisonArtifacts(combined, jsonPath, reportPath); err != nil {
		return err
	}
	if validationErr != nil {
		return validationErr
	}
	if combined.State != "pass" || !combined.Passing {
		return fmt.Errorf("Minima qualification is not a clean pass: state=%q passing=%t failures=%d", combined.State, combined.Passing, len(combined.Failures))
	}
	return nil
}
