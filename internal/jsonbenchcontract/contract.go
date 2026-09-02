// Package jsonbenchcontract validates the repository-owned sidecar that makes
// external JSONBench evidence canonical. It does not collect timings and must
// stay outside measured benchmark intervals.
package jsonbenchcontract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	SchemaVersion             = "gomap-jsonbench-canonical/v2"
	TreeDBResultSchemaVersion = "jsonbench-treedb-report/v1"
)

const (
	CanonicalQueryMaxRatio        = 1.5
	CanonicalLoadMaxRatio         = 1.5
	CanonicalQ4RegressionMaxRatio = 1.05
	CanonicalStorageLayout        = "column-store-full-prepared"
	CanonicalProjection           = "full"
	CanonicalQueryMode            = "one_shot_end_to_end"
	CanonicalMetadataMode         = "no_aggregate_metadata"
	CanonicalCachePolicy          = "drop-os-page-cache-not-required"
	CanonicalWarmthPolicy         = "cold-open-one-shot"
	CanonicalTargetRevisionPolicy = "revise only with linked same-host evidence and tracker approval"
	CanonicalValidationPolicy     = "canonical-result-hash-and-reconstruction"
)

const (
	ResourceGoBenchmem             = "go_benchmem"
	ResourceProcessPeak            = "process_peak"
	ResourceCumulativeAllocProfile = "cumulative_alloc_profile"
)

var requiredCounters = []string{
	"visible_base_generations",
	"visible_delta_generations",
	"tombstones_applied",
	"parts_decoded",
	"query_time_dictionaries_built",
	"query_time_ranks_built",
	"query_time_offsets_built",
	"document_fallbacks",
	"row_fallbacks",
	"result_hash_validated",
}

type Manifest struct {
	SchemaVersion string                     `json:"schema_version"`
	Canonical     bool                       `json:"canonical"`
	Pins          Pins                       `json:"pins"`
	Host          Host                       `json:"host"`
	ArtifactRoot  string                     `json:"artifact_root"`
	TreeDB        TreeDBRun                  `json:"treedb"`
	ClickHouse    ClickHouseRun              `json:"clickhouse"`
	Comparison    Comparison                 `json:"comparison"`
	Validation    ValidationEvidence         `json:"validation"`
	Counters      map[string]CounterEvidence `json:"counters"`
	Resources     []ResourceEvidence         `json:"resources"`
}

type Pins struct {
	GomapCommit       string     `json:"gomap_commit"`
	JSONBenchCommit   string     `json:"jsonbench_commit"`
	ClickHouseVersion string     `json:"clickhouse_version"`
	Dataset           DatasetPin `json:"dataset"`
}

type DatasetPin struct {
	Identity      string `json:"identity"`
	RequestedRows int64  `json:"requested_rows"`
	ValidRows     int64  `json:"valid_rows"`
	SHA256        string `json:"sha256"`
}

type Host struct {
	Identity string `json:"identity"`
}

type TreeDBRun struct {
	RequestedProfile string            `json:"requested_profile"`
	ResultPaths      []string          `json:"result_paths"`
	RowSelector      ResultRowSelector `json:"row_selector"`
}

type ResultRowSelector struct {
	StorageLayout string `json:"storage_layout"`
	Projection    string `json:"projection"`
}

type ClickHouseRun struct {
	ResultPaths []string `json:"result_paths"`
}

type Comparison struct {
	QueryMode             string   `json:"query_mode"`
	AggregateMetadataMode string   `json:"aggregate_metadata_mode"`
	FallbackPolicy        string   `json:"fallback_policy"`
	CachePolicy           string   `json:"cache_policy"`
	WarmthPolicy          string   `json:"warmth_policy"`
	Attempts              int      `json:"attempts"`
	QueryOrder            []string `json:"query_order"`
	Statistic             string   `json:"statistic"`
	QueryMaxRatio         float64  `json:"query_max_ratio"`
	LoadMaxRatio          float64  `json:"load_max_ratio"`
	Q4RegressionMaxRatio  float64  `json:"q4_regression_max_ratio"`
	TargetRevisionPolicy  string   `json:"target_revision_policy"`
	ValidationPolicy      string   `json:"validation_policy"`
}

type ValidationEvidence struct {
	Status                  string `json:"status"`
	Artifact                string `json:"artifact"`
	TimingBoundary          string `json:"timing_boundary"`
	ResultHashesValidated   bool   `json:"result_hashes_validated"`
	ReconstructionValidated bool   `json:"reconstruction_validated"`
}

type CounterEvidence struct {
	Value  int64  `json:"value"`
	Source string `json:"source"`
}

type ResourceEvidence struct {
	Scope          string          `json:"scope"`
	SourceKind     string          `json:"source_kind"`
	Artifact       string          `json:"artifact"`
	SampleCount    int             `json:"sample_count,omitempty"`
	ContextualOnly bool            `json:"contextual_only,omitempty"`
	Metrics        ResourceMetrics `json:"metrics"`
}

type ResourceMetrics struct {
	NanosPerOp    *float64 `json:"ns_per_op,omitempty"`
	BytesPerOp    *uint64  `json:"bytes_per_op,omitempty"`
	AllocsPerOp   *uint64  `json:"allocs_per_op,omitempty"`
	PeakRSSBytes  *uint64  `json:"peak_rss_bytes,omitempty"`
	LiveHeapBytes *uint64  `json:"live_heap_bytes,omitempty"`
}

// LoadManifest strictly decodes a manifest so schema drift cannot silently
// turn a canonical field into an ignored typo.
func LoadManifest(path string) (Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return Manifest{}, err
	}
	defer f.Close()

	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	if err := ensureEOF(decoder); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); errors.Is(err, io.EOF) {
		return nil
	} else if err != nil {
		return fmt.Errorf("decode trailing manifest data: %w", err)
	}
	return errors.New("manifest contains multiple JSON values")
}

// Validate checks manifest completeness, resource-evidence semantics, and the
// fields that can be cross-checked against a gomap/JSONBench TreeDB result.
// baseDir resolves relative result_paths and is normally the manifest's
// directory.
func Validate(manifest Manifest, baseDir string) error {
	var problems []string
	add := func(format string, args ...any) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	if manifest.SchemaVersion != SchemaVersion {
		add("schema_version must be %q", SchemaVersion)
	}
	if !manifest.Canonical {
		add("canonical must be true")
	}
	validatePins(manifest.Pins, add)
	if strings.TrimSpace(manifest.Host.Identity) == "" {
		add("host.identity is required")
	}
	if strings.TrimSpace(manifest.ArtifactRoot) == "" {
		add("artifact_root is required")
	}
	if strings.TrimSpace(manifest.TreeDB.RequestedProfile) == "" {
		add("treedb.requested_profile is required")
	} else if manifest.TreeDB.RequestedProfile != "durable" {
		add("treedb.requested_profile must be %q for canonical evidence", "durable")
	}
	validateIndependentResultPaths("treedb.result_paths", manifest.TreeDB.ResultPaths, baseDir, manifest.Comparison.Attempts, add)
	if strings.TrimSpace(manifest.TreeDB.RowSelector.StorageLayout) == "" {
		add("treedb.row_selector.storage_layout is required")
	} else if manifest.TreeDB.RowSelector.StorageLayout != CanonicalStorageLayout {
		add("treedb.row_selector.storage_layout must be %q", CanonicalStorageLayout)
	}
	if strings.TrimSpace(manifest.TreeDB.RowSelector.Projection) == "" {
		add("treedb.row_selector.projection is required")
	} else if manifest.TreeDB.RowSelector.Projection != CanonicalProjection {
		add("treedb.row_selector.projection must be %q", CanonicalProjection)
	}
	validateIndependentResultPaths("clickhouse.result_paths", manifest.ClickHouse.ResultPaths, baseDir, manifest.Comparison.Attempts, add)
	validateComparison(manifest.Comparison, add)
	validateValidation(manifest.Validation, add)
	validateCounters(manifest.Counters, manifest.Comparison.FallbackPolicy, add)
	validateResources(manifest.Resources, add)

	artifactRoot := manifest.ArtifactRoot
	if artifactRoot != "" && !filepath.IsAbs(artifactRoot) {
		artifactRoot = filepath.Join(baseDir, artifactRoot)
	}
	if artifactRoot != "" {
		info, err := os.Stat(artifactRoot)
		if err != nil {
			add("artifact_root: %v", err)
		} else if !info.IsDir() {
			add("artifact_root must be a directory")
		}
		validateEvidenceArtifacts(artifactRoot, manifest, add)
	}

	for index, configuredPath := range manifest.TreeDB.ResultPaths {
		resultPath := configuredPath
		if !filepath.IsAbs(resultPath) {
			resultPath = filepath.Join(baseDir, resultPath)
		}
		if artifactRoot != "" && !pathResolvesWithin(artifactRoot, resultPath) {
			add("treedb.result_paths[%d] must resolve inside artifact_root", index)
		}
		if err := validateResult(resultPath, index, manifest, add); err != nil {
			add("treedb result[%d]: %v", index, err)
		}
	}
	for index, configuredPath := range manifest.ClickHouse.ResultPaths {
		resultPath := configuredPath
		if !filepath.IsAbs(resultPath) {
			resultPath = filepath.Join(baseDir, resultPath)
		}
		if artifactRoot != "" && !pathResolvesWithin(artifactRoot, resultPath) {
			add("clickhouse.result_paths[%d] must resolve inside artifact_root", index)
		}
		if err := validateClickHouseResult(resultPath, index, manifest, add); err != nil {
			add("clickhouse result[%d]: %v", index, err)
		}
	}

	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("canonical JSONBench contract rejected:\n- %s", strings.Join(problems, "\n- "))
}

func validateIndependentResultPaths(field string, paths []string, baseDir string, attempts int, add func(string, ...any)) {
	if len(paths) != attempts {
		add("%s has %d independent artifacts, want %d", field, len(paths), attempts)
	}
	seen := make(map[string]bool, len(paths))
	seenContents := make(map[[sha256.Size]byte]bool, len(paths))
	var seenFiles []os.FileInfo
	for index, path := range paths {
		if strings.TrimSpace(path) == "" {
			add("%s[%d] is required", field, index)
			continue
		}
		cleaned := path
		if !filepath.IsAbs(cleaned) {
			cleaned = filepath.Join(baseDir, cleaned)
		}
		cleaned, err := filepath.Abs(cleaned)
		if err != nil {
			add("%s[%d] cannot be resolved: %v", field, index, err)
			continue
		}
		cleaned = filepath.Clean(cleaned)
		if resolved, err := filepath.EvalSymlinks(cleaned); err == nil {
			cleaned = resolved
		}
		duplicate := seen[cleaned]
		if info, err := os.Stat(cleaned); err == nil {
			for _, previous := range seenFiles {
				if os.SameFile(previous, info) {
					duplicate = true
					break
				}
			}
			seenFiles = append(seenFiles, info)
		}
		if duplicate {
			add("%s[%d] duplicates %q; attempts must be independent artifacts", field, index, path)
		}
		if data, err := os.ReadFile(cleaned); err == nil {
			canonicalContent := data
			var decoded any
			if err := json.Unmarshal(data, &decoded); err == nil {
				if normalized, err := json.Marshal(decoded); err == nil {
					canonicalContent = normalized
				}
			}
			digest := sha256.Sum256(canonicalContent)
			if seenContents[digest] {
				add("%s[%d] duplicates the content of an earlier attempt; attempts must come from independent runs", field, index)
			}
			seenContents[digest] = true
		}
		seen[cleaned] = true
	}
}

func validateEvidenceArtifacts(artifactRoot string, manifest Manifest, add func(string, ...any)) {
	check := func(field, path string) {
		if path == "" {
			return
		}
		resolved := path
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(artifactRoot, resolved)
		}
		if !pathResolvesWithin(artifactRoot, resolved) {
			add("%s must resolve inside artifact_root", field)
			return
		}
		info, err := os.Stat(resolved)
		if err != nil {
			add("%s: %v", field, err)
		} else if !info.Mode().IsRegular() {
			add("%s must reference a regular file", field)
		}
	}
	check("validation.artifact", manifest.Validation.Artifact)
	for index, resource := range manifest.Resources {
		check(fmt.Sprintf("resources[%d].artifact", index), resource.Artifact)
	}
}

func pathWithin(root, path string) bool {
	root, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func pathResolvesWithin(root, path string) bool {
	if !pathWithin(root, path) {
		return false
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false
	}
	return pathWithin(resolvedRoot, resolvedPath)
}

func validatePins(pins Pins, add func(string, ...any)) {
	if !validCommit(pins.GomapCommit) {
		add("pins.gomap_commit must be a 40-character hexadecimal commit")
	}
	if pins.JSONBenchCommit == "" {
		add("pins.jsonbench_commit is required")
	} else if !validCommit(pins.JSONBenchCommit) {
		add("pins.jsonbench_commit must be a 40-character hexadecimal commit")
	}
	if strings.TrimSpace(pins.ClickHouseVersion) == "" {
		add("pins.clickhouse_version is required")
	}
	if strings.TrimSpace(pins.Dataset.Identity) == "" {
		add("pins.dataset.identity is required")
	}
	if pins.Dataset.RequestedRows <= 0 {
		add("pins.dataset.requested_rows must be positive")
	}
	if pins.Dataset.ValidRows <= 0 {
		add("pins.dataset.valid_rows must be positive")
	}
	if pins.Dataset.ValidRows > pins.Dataset.RequestedRows {
		add("pins.dataset.valid_rows cannot exceed pins.dataset.requested_rows")
	}
	if len(pins.Dataset.SHA256) != 64 || !validHex(pins.Dataset.SHA256) {
		add("pins.dataset.sha256 must be a 64-character hexadecimal digest")
	}
}

func validCommit(value string) bool {
	return len(value) == 40 && validHex(value)
}

func validHex(value string) bool {
	_, err := hex.DecodeString(value)
	return err == nil
}

func validateComparison(comparison Comparison, add func(string, ...any)) {
	for name, value := range map[string]string{
		"query_mode":              comparison.QueryMode,
		"aggregate_metadata_mode": comparison.AggregateMetadataMode,
		"fallback_policy":         comparison.FallbackPolicy,
		"cache_policy":            comparison.CachePolicy,
		"warmth_policy":           comparison.WarmthPolicy,
		"statistic":               comparison.Statistic,
		"target_revision_policy":  comparison.TargetRevisionPolicy,
		"validation_policy":       comparison.ValidationPolicy,
	} {
		if strings.TrimSpace(value) == "" {
			add("comparison.%s is required", name)
		}
	}
	if comparison.Attempts != 5 {
		add("comparison.attempts must be exactly 5 independent runs for canonical evidence")
	}
	wantQueries := []string{"q1", "q2", "q3", "q4", "q5", "qexpr"}
	if len(comparison.QueryOrder) != len(wantQueries) {
		add("comparison.query_order must be q1,q2,q3,q4,q5,qexpr")
	} else {
		for index := range wantQueries {
			if comparison.QueryOrder[index] != wantQueries[index] {
				add("comparison.query_order must be q1,q2,q3,q4,q5,qexpr")
				break
			}
		}
	}
	if comparison.FallbackPolicy != "forbid" {
		add("comparison.fallback_policy must be %q", "forbid")
	}
	for field, value := range map[string]struct {
		got  string
		want string
	}{
		"query_mode":              {comparison.QueryMode, CanonicalQueryMode},
		"aggregate_metadata_mode": {comparison.AggregateMetadataMode, CanonicalMetadataMode},
		"cache_policy":            {comparison.CachePolicy, CanonicalCachePolicy},
		"warmth_policy":           {comparison.WarmthPolicy, CanonicalWarmthPolicy},
		"target_revision_policy":  {comparison.TargetRevisionPolicy, CanonicalTargetRevisionPolicy},
		"validation_policy":       {comparison.ValidationPolicy, CanonicalValidationPolicy},
	} {
		if value.got != "" && value.got != value.want {
			add("comparison.%s must be %q", field, value.want)
		}
	}
	if comparison.Statistic != "median" {
		add("comparison.statistic must be %q", "median")
	}
	if comparison.QueryMaxRatio != CanonicalQueryMaxRatio {
		add("comparison.query_max_ratio must be %.2f for schema %s", CanonicalQueryMaxRatio, SchemaVersion)
	}
	if comparison.LoadMaxRatio != CanonicalLoadMaxRatio {
		add("comparison.load_max_ratio must be %.2f for schema %s", CanonicalLoadMaxRatio, SchemaVersion)
	}
	if comparison.Q4RegressionMaxRatio != CanonicalQ4RegressionMaxRatio {
		add("comparison.q4_regression_max_ratio must be %.2f for schema %s", CanonicalQ4RegressionMaxRatio, SchemaVersion)
	}
}

func validateValidation(validation ValidationEvidence, add func(string, ...any)) {
	if validation.Status != "passed" {
		add("validation.status must be %q", "passed")
	}
	if strings.TrimSpace(validation.Artifact) == "" {
		add("validation.artifact is required")
	}
	if validation.TimingBoundary != "outside_measured_intervals" {
		add("validation.timing_boundary must be %q", "outside_measured_intervals")
	}
	if !validation.ResultHashesValidated {
		add("validation.result_hashes_validated must be true")
	}
	if !validation.ReconstructionValidated {
		add("validation.reconstruction_validated must be true")
	}
}

func validateCounters(counters map[string]CounterEvidence, fallbackPolicy string, add func(string, ...any)) {
	for _, name := range requiredCounters {
		counter, ok := counters[name]
		if !ok {
			add("counters.%s is required", name)
			continue
		}
		if counter.Value < 0 {
			add("counters.%s.value cannot be negative", name)
		}
		if strings.TrimSpace(counter.Source) == "" {
			add("counters.%s.source is required", name)
		}
	}
	if counter, ok := counters["result_hash_validated"]; ok && counter.Value != 1 {
		add("counters.result_hash_validated.value must be 1")
	}
	if fallbackPolicy == "forbid" {
		for _, name := range []string{"document_fallbacks", "row_fallbacks"} {
			if counter, ok := counters[name]; ok && counter.Value != 0 {
				add("counters.%s.value must be 0 when comparison.fallback_policy is %q", name, "forbid")
			}
		}
	}
}

func validateResources(resources []ResourceEvidence, add func(string, ...any)) {
	benchmemScopes := make(map[string]bool)
	resourceScopes := make(map[string]bool)
	processPeakScopes := make(map[string]bool)
	for index, evidence := range resources {
		prefix := fmt.Sprintf("resources[%d]", index)
		if strings.TrimSpace(evidence.Scope) == "" {
			add("%s.scope is required", prefix)
		} else {
			resourceScopes[evidence.Scope] = true
		}
		if strings.TrimSpace(evidence.Artifact) == "" {
			add("%s.artifact is required", prefix)
		}
		switch evidence.SourceKind {
		case ResourceGoBenchmem:
			if evidence.Scope != "" {
				benchmemScopes[evidence.Scope] = true
			}
			if evidence.SampleCount < 5 {
				add("%s go_benchmem sample_count must be at least 5", prefix)
			}
			if evidence.Metrics.NanosPerOp == nil || evidence.Metrics.BytesPerOp == nil || evidence.Metrics.AllocsPerOp == nil {
				add("%s go_benchmem requires ns_per_op, bytes_per_op, and allocs_per_op", prefix)
			} else if *evidence.Metrics.NanosPerOp <= 0 {
				add("%s go_benchmem ns_per_op must be positive", prefix)
			}
			if evidence.ContextualOnly {
				add("%s go_benchmem cannot be contextual_only", prefix)
			}
		case ResourceProcessPeak:
			if evidence.Scope != "" {
				processPeakScopes[evidence.Scope] = true
			}
			if evidence.Metrics.PeakRSSBytes == nil && evidence.Metrics.LiveHeapBytes == nil {
				add("%s %s process_peak requires peak_rss_bytes or live_heap_bytes", prefix, evidence.Scope)
			} else if (evidence.Metrics.PeakRSSBytes == nil || *evidence.Metrics.PeakRSSBytes == 0) &&
				(evidence.Metrics.LiveHeapBytes == nil || *evidence.Metrics.LiveHeapBytes == 0) {
				add("%s %s process_peak memory value must be positive", prefix, evidence.Scope)
			}
		case ResourceCumulativeAllocProfile:
			if !evidence.ContextualOnly {
				add("%s cumulative_alloc_profile must be contextual_only", prefix)
			}
			if evidence.Metrics.BytesPerOp != nil || evidence.Metrics.AllocsPerOp != nil {
				add("%s cumulative_alloc_profile cannot report bytes_per_op or allocs_per_op", prefix)
			}
		default:
			add("%s.source_kind must be one of %q, %q, or %q", prefix, ResourceGoBenchmem, ResourceProcessPeak, ResourceCumulativeAllocProfile)
		}
	}
	for scope := range resourceScopes {
		if strings.HasPrefix(scope, "query/") && !benchmemScopes[scope] {
			add("%s requires direct go_benchmem evidence", scope)
		}
	}
	for _, scope := range []string{"query/q2", "query/q3", "query/q5"} {
		if !resourceScopes[scope] {
			add("%s requires direct go_benchmem evidence", scope)
		}
	}
	if !processPeakScopes["load"] && !processPeakScopes["open"] {
		add("resources require process_peak evidence for load or open")
	}
}

type recordedResult struct {
	SchemaVersion        string           `json:"schema_version"`
	Query                string           `json:"query"`
	Profile              string           `json:"profile"`
	RequestedRows        int64            `json:"requested_rows"`
	DatasetSize          int64            `json:"dataset_size"`
	QueryMode            string           `json:"query_mode"`
	MetadataMode         string           `json:"metadata_mode"`
	DocumentScanFallback *bool            `json:"document_scan_fallback"`
	ReconstructionStatus string           `json:"reconstruction_status"`
	StorageLayout        string           `json:"storage_layout"`
	Projection           string           `json:"projection"`
	AttemptsSeconds      []float64        `json:"attempts_seconds"`
	Rows                 []recordedResult `json:"rows"`
}

func validateResult(path string, attemptIndex int, manifest Manifest, add func(string, ...any)) error {
	if err := requireRegularFile(path); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var result recordedResult
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if result.SchemaVersion != TreeDBResultSchemaVersion {
		add("treedb result[%d].schema_version must be %q", attemptIndex, TreeDBResultSchemaVersion)
	}
	rows := result.Rows
	if len(rows) == 0 {
		rows = []recordedResult{result}
	}
	selected := make([]recordedResult, 0, len(rows))
	for _, row := range rows {
		if row.StorageLayout == manifest.TreeDB.RowSelector.StorageLayout && row.Projection == manifest.TreeDB.RowSelector.Projection {
			selected = append(selected, row)
		}
	}
	if len(selected) == 0 {
		add("treedb result[%d] has no rows matching row_selector storage_layout=%q projection=%q", attemptIndex, manifest.TreeDB.RowSelector.StorageLayout, manifest.TreeDB.RowSelector.Projection)
		return nil
	}
	seenQueries := make(map[string]bool, len(selected))
	allowedQueries := map[string]bool{"q4a": true, "q4b": true}
	for _, query := range manifest.Comparison.QueryOrder {
		allowedQueries[query] = true
	}
	for index, row := range selected {
		prefix := fmt.Sprintf("treedb result[%d] row[%d]", attemptIndex, index)
		if row.Query == "" {
			add("%s.query is required", prefix)
		} else if !allowedQueries[row.Query] {
			add("%s.query %q is not allowed in the selected canonical lane", prefix, row.Query)
		} else if seenQueries[row.Query] {
			add("treedb result[%d] query %q is duplicated within the selected canonical lane", attemptIndex, row.Query)
		} else {
			seenQueries[row.Query] = true
		}
		if len(row.AttemptsSeconds) != 1 {
			add("%s.attempts_seconds has %d timings, want exactly 1 from an independent one-shot run", prefix, len(row.AttemptsSeconds))
		}
		for _, seconds := range row.AttemptsSeconds {
			if seconds <= 0 {
				add("%s.attempts_seconds timings must be positive", prefix)
				break
			}
		}
		if row.Profile == "" {
			add("%s.profile is required", prefix)
		} else if row.Profile != manifest.TreeDB.RequestedProfile {
			add("requested profile %q does not match recorded profile %q", manifest.TreeDB.RequestedProfile, row.Profile)
		}
		if row.RequestedRows == 0 {
			add("%s.requested_rows is required", prefix)
		} else if row.RequestedRows != manifest.Pins.Dataset.RequestedRows {
			add("%s.requested_rows %d does not match pinned requested_rows %d", prefix, row.RequestedRows, manifest.Pins.Dataset.RequestedRows)
		}
		if row.DatasetSize == 0 {
			add("%s.dataset_size is required", prefix)
		} else if row.DatasetSize != manifest.Pins.Dataset.ValidRows {
			add("%s.dataset_size %d does not match pinned valid_rows %d", prefix, row.DatasetSize, manifest.Pins.Dataset.ValidRows)
		}
		if row.QueryMode == "" {
			add("%s.query_mode is required", prefix)
		} else if row.QueryMode != manifest.Comparison.QueryMode {
			add("%s.query_mode %q does not match comparison.query_mode %q", prefix, row.QueryMode, manifest.Comparison.QueryMode)
		}
		if row.MetadataMode == "" {
			add("%s.metadata_mode is required", prefix)
		} else if row.MetadataMode != manifest.Comparison.AggregateMetadataMode {
			add("%s.metadata_mode %q does not match comparison.aggregate_metadata_mode %q", prefix, row.MetadataMode, manifest.Comparison.AggregateMetadataMode)
		}
		if row.DocumentScanFallback == nil {
			add("%s.document_scan_fallback is required", prefix)
		} else if manifest.Comparison.FallbackPolicy == "forbid" && *row.DocumentScanFallback {
			add("%s.document_scan_fallback must be false when fallback_policy is forbid", prefix)
		}
		if row.ReconstructionStatus == "" {
			add("%s.reconstruction_status is required", prefix)
		} else if !validationStatusKnown(row.ReconstructionStatus) {
			add("%s.reconstruction_status %q is unknown", prefix, row.ReconstructionStatus)
		} else if validationFailed(row.ReconstructionStatus) {
			add("%s.reconstruction_status %q records a validation failure", prefix, row.ReconstructionStatus)
		}
	}
	for _, query := range manifest.Comparison.QueryOrder {
		if !seenQueries[query] {
			add("treedb result[%d] is missing required query %q from the selected canonical lane", attemptIndex, query)
		}
	}
	return nil
}

type clickHouseResult struct {
	System             string      `json:"system"`
	Version            string      `json:"version"`
	RequestedRows      int64       `json:"requested_rows"`
	DatasetSize        int64       `json:"dataset_size"`
	NumLoadedDocuments int64       `json:"num_loaded_documents"`
	Result             [][]float64 `json:"result"`
}

func validateClickHouseResult(path string, attemptIndex int, manifest Manifest, add func(string, ...any)) error {
	if err := requireRegularFile(path); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var result clickHouseResult
	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if result.System != "ClickHouse" {
		add("clickhouse result[%d].system must be %q", attemptIndex, "ClickHouse")
	}
	if result.Version != manifest.Pins.ClickHouseVersion {
		add("clickhouse result[%d].version %q does not match pins.clickhouse_version %q", attemptIndex, result.Version, manifest.Pins.ClickHouseVersion)
	}
	if result.RequestedRows != manifest.Pins.Dataset.RequestedRows {
		add("clickhouse result[%d].requested_rows %d does not match pinned requested_rows %d", attemptIndex, result.RequestedRows, manifest.Pins.Dataset.RequestedRows)
	}
	if result.DatasetSize != manifest.Pins.Dataset.ValidRows {
		add("clickhouse result[%d].dataset_size %d does not match pinned valid_rows %d", attemptIndex, result.DatasetSize, manifest.Pins.Dataset.ValidRows)
	}
	if result.NumLoadedDocuments != manifest.Pins.Dataset.ValidRows {
		add("clickhouse result[%d].num_loaded_documents %d does not match pinned valid_rows %d", attemptIndex, result.NumLoadedDocuments, manifest.Pins.Dataset.ValidRows)
	}
	if len(result.Result) != len(manifest.Comparison.QueryOrder) {
		add("clickhouse result[%d] has %d query lanes, want %d", attemptIndex, len(result.Result), len(manifest.Comparison.QueryOrder))
	}
	for index, attempts := range result.Result {
		if len(attempts) != 1 {
			query := fmt.Sprintf("lane[%d]", index)
			if index < len(manifest.Comparison.QueryOrder) {
				query = manifest.Comparison.QueryOrder[index]
			}
			add("clickhouse result[%d] %s has %d timings, want exactly 1 from an independent one-shot run", attemptIndex, query, len(attempts))
		}
		for _, seconds := range attempts {
			if seconds <= 0 {
				add("clickhouse result[%d] lane[%d] timings must be positive", attemptIndex, index)
				break
			}
		}
	}
	return nil
}

func requireRegularFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	return nil
}

func validationStatusKnown(status string) bool {
	switch status {
	case "validated", "passed", "complete", "not_validated", "failed", "mismatch", "invalid":
		return true
	default:
		return false
	}
}

func validationFailed(status string) bool {
	switch status {
	case "failed", "mismatch", "invalid":
		return true
	default:
		return false
	}
}
