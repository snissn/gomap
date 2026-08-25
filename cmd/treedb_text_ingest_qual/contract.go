package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

const contractVersion = "treedb_text_ingest_qualification/v2"

var requiredModes = []string{"indexed_insert", "post_load_backfill", "source_chunk", "maintenance"}
var requiredScales = []int{10_000, 100_000, 1_000_000}

type manifest struct {
	SchemaVersion string           `json:"schema_version"`
	FixtureSHA256 string           `json:"fixture_sha256"`
	Analyzer      string           `json:"analyzer"`
	FieldWeights  string           `json:"field_weights"`
	IDsSHA256     string           `json:"ids_sha256"`
	Command       string           `json:"command"`
	Commit        string           `json:"commit"`
	Host          string           `json:"host"`
	CacheState    string           `json:"cache_state"`
	Durability    string           `json:"durability"`
	TimedBoundary string           `json:"timed_boundary"`
	Observed      observedIdentity `json:"observed"`
}

type observedIdentity struct {
	VCSClean       bool   `json:"vcs_clean"`
	Commit         string `json:"commit"`
	Durability     string `json:"durability"`
	VectorIndexes  int    `json:"vector_indexes"`
	VectorsEnabled bool   `json:"vectors_enabled"`
}

type report struct {
	SchemaVersion  string             `json:"schema_version"`
	ManifestSHA256 string             `json:"manifest_sha256"`
	Rows           []row              `json:"rows"`
	Summaries      []modeScaleSummary `json:"summaries"`
}

type row struct {
	Mode               string `json:"mode"`
	Scale              int    `json:"scale"`
	Repetition         int    `json:"repetition"`
	PeakRSSScope       string `json:"peak_rss_scope"`
	PeakRSSPID         int    `json:"peak_rss_pid"`
	SourceDocuments    int    `json:"source_documents"`
	GeneratedChunks    int    `json:"generated_chunks"`
	IndexedLiveRows    int    `json:"indexed_live_rows"`
	ParentsTextIndexed bool   `json:"parents_text_indexed"`
	IndexedParentRows  int    `json:"indexed_parent_rows"`
	// ChunkBatchSize and ChunkBatchCount record the largest actual public
	// IngestChunkedDocuments call and the number of durable calls for source_chunk.
	// They are zero for modes that do not use chunk ingestion.
	ChunkBatchSize    int               `json:"chunk_batch_size"`
	ChunkBatchCount   int               `json:"chunk_batch_count"`
	Postings          uint64            `json:"postings"`
	Terms             uint64            `json:"terms"`
	Blocks            uint64            `json:"blocks"`
	Generations       uint64            `json:"generations"`
	StaleDebt         uint64            `json:"stale_debt"`
	TombstoneDebt     uint64            `json:"tombstone_debt"`
	SourceDocsPerSec  float64           `json:"source_docs_per_second"`
	ChunksPerSec      float64           `json:"chunks_per_second"`
	IndexedRowsPerSec float64           `json:"indexed_rows_per_second"`
	WallSeconds       float64           `json:"wall_seconds"`
	CPUSeconds        metric            `json:"cpu_seconds"`
	BytesPerOp        metric            `json:"bytes_per_op"`
	AllocsPerOp       metric            `json:"allocs_per_op"`
	CumulativeAllocs  metric            `json:"cumulative_allocations"`
	PeakRSSBytes      metric            `json:"peak_rss_bytes"`
	Stages            map[string]metric `json:"stages"`
	Storage           storage           `json:"storage"`
	TextV2            textV2            `json:"text_v2"`
	CheckpointOK      bool              `json:"checkpoint_ok"`
	CloseOK           bool              `json:"close_ok"`
	ReopenOK          bool              `json:"reopen_ok"`
	Probe             scoreOnlyProbe    `json:"score_only_probe"`
}

type metric struct {
	State  string  `json:"state"`
	Value  float64 `json:"value,omitempty"`
	Reason string  `json:"reason,omitempty"`
}
type storage struct {
	// Physical categories are disjoint filesystem buckets observed only after
	// checkpoint and close. Logical payload and text-v2 components are reported
	// separately and are explicitly non-additive with physical bytes.
	PhysicalIndexPageBytes        int64    `json:"physical_index_page_bytes"`
	PhysicalValueLogBytes         int64    `json:"physical_value_log_bytes"`
	PhysicalWALBytes              int64    `json:"physical_wal_bytes"`
	PhysicalOtherBytes            int64    `json:"physical_other_bytes"`
	PhysicalTotalBytes            int64    `json:"physical_total_bytes"`
	PhysicalTotalWALExcludedBytes int64    `json:"physical_total_wal_excluded_bytes"`
	OtherPaths                    []string `json:"other_paths,omitempty"`
	LogicalPrimaryPayloadBytes    int64    `json:"logical_primary_payload_bytes"`
	LogicalTextV2Overlap          string   `json:"logical_text_v2_overlap"`
}
type textV2 struct {
	DocIDBytes    int64 `json:"docid_bytes"`
	DocMapBytes   int64 `json:"docmap_bytes"`
	PostingBytes  int64 `json:"posting_bytes"`
	NormBytes     int64 `json:"norm_bytes"`
	PositionBytes int64 `json:"position_bytes"`
	TermBytes     int64 `json:"term_bytes"`
	StatusBytes   int64 `json:"status_bytes"`
}
type scoreOnlyProbe struct {
	Results          int    `json:"results"`
	DocumentsFetched uint64 `json:"documents_fetched"`
	FailClosed       uint64 `json:"fail_closed"`
}
type modeScaleSummary struct {
	Mode                    string  `json:"mode"`
	Scale                   int     `json:"scale"`
	MedianWallSeconds       float64 `json:"median_wall_seconds"`
	P95WallSeconds          float64 `json:"p95_wall_seconds"`
	MedianIndexedRowsPerSec float64 `json:"median_indexed_rows_per_second"`
	P95IndexedRowsPerSec    float64 `json:"p95_indexed_rows_per_second"`
}

func validate(m manifest, r report, manifestSHA string) error {
	if m.SchemaVersion != contractVersion || r.SchemaVersion != contractVersion {
		return fmt.Errorf("schema_version must be %q", contractVersion)
	}
	for name, value := range map[string]string{"fixture_sha256": m.FixtureSHA256, "analyzer": m.Analyzer, "field_weights": m.FieldWeights, "ids_sha256": m.IDsSHA256, "command": m.Command, "commit": m.Commit, "host": m.Host, "cache_state": m.CacheState, "durability": m.Durability, "timed_boundary": m.TimedBoundary, "observed.commit": m.Observed.Commit, "observed.durability": m.Observed.Durability} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("manifest %s is required", name)
		}
	}
	if !m.Observed.VCSClean || m.Observed.VectorsEnabled || m.Observed.VectorIndexes != 0 {
		return fmt.Errorf("observed product identity requires clean VCS and vectors disabled with zero vector indexes")
	}
	if m.Observed.Commit != m.Commit || m.Observed.Durability != m.Durability {
		return fmt.Errorf("observed product identity disagrees with manifest")
	}
	if r.ManifestSHA256 != manifestSHA {
		return fmt.Errorf("manifest_sha256 does not match manifest bytes")
	}
	groups := map[string]map[int]row{}
	peakRSSPIDs := map[int]bool{}
	for i, x := range r.Rows {
		if err := validateRow(x); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
		key := fmt.Sprintf("%s/%d", x.Mode, x.Scale)
		if groups[key] == nil {
			groups[key] = map[int]row{}
		}
		if _, ok := groups[key][x.Repetition]; ok {
			return fmt.Errorf("duplicate repetition %s/%d", key, x.Repetition)
		}
		if peakRSSPIDs[x.PeakRSSPID] {
			return fmt.Errorf("row %d: peak RSS process reused", i)
		}
		peakRSSPIDs[x.PeakRSSPID] = true
		groups[key][x.Repetition] = x
	}
	for _, mode := range requiredModes {
		for _, scale := range requiredScales {
			key := fmt.Sprintf("%s/%d", mode, scale)
			reps := groups[key]
			if reps == nil {
				return fmt.Errorf("missing required mode/scale %s", key)
			}
			if scale == 10_000 {
				if len(reps) != 1 || reps[1].Repetition != 1 {
					return fmt.Errorf("10k smoke %s requires exactly repetition 1", key)
				}
			} else if len(reps) != 3 || reps[1].Repetition != 1 || reps[2].Repetition != 2 || reps[3].Repetition != 3 {
				return fmt.Errorf("retained %s requires exactly repetitions 1,2,3", key)
			}
		}
	}
	if len(r.Summaries) != len(groups) {
		return fmt.Errorf("summaries must appear exactly once per mode/scale")
	}
	seen := map[string]bool{}
	for _, s := range r.Summaries {
		key := fmt.Sprintf("%s/%d", s.Mode, s.Scale)
		reps, ok := groups[key]
		if !ok || seen[key] {
			return fmt.Errorf("summary missing group or duplicate %s", key)
		}
		seen[key] = true
		if err := validateSummary(s, reps); err != nil {
			return err
		}
	}
	return nil
}
func validateRow(r row) error {
	validMode := false
	for _, m := range requiredModes {
		validMode = validMode || r.Mode == m
	}
	if !validMode {
		return fmt.Errorf("unknown mode %q", r.Mode)
	}
	validScale := false
	for _, s := range requiredScales {
		validScale = validScale || r.Scale == s
	}
	if !validScale || r.Repetition < 1 {
		return fmt.Errorf("invalid scale or repetition")
	}
	if r.PeakRSSScope != "fresh_process_per_mode" || r.PeakRSSPID < 1 {
		return fmt.Errorf("peak RSS requires a fresh process measurement")
	}
	if r.SourceDocuments != r.Scale || r.GeneratedChunks < 0 || r.IndexedLiveRows < 1 || r.IndexedParentRows < 0 {
		return fmt.Errorf("document accounting is incomplete")
	}
	if r.Mode == "source_chunk" && (r.GeneratedChunks < 1 || r.SourceDocsPerSec <= 0 || r.ChunksPerSec <= 0 || !r.ParentsTextIndexed || r.IndexedParentRows != r.SourceDocuments || r.IndexedLiveRows != r.IndexedParentRows+r.GeneratedChunks || r.ChunkBatchSize < 1 || r.ChunkBatchCount != (r.SourceDocuments+r.ChunkBatchSize-1)/r.ChunkBatchSize) {
		return fmt.Errorf("source_chunk requires returned parent, generated child, live-row, and batch accounting")
	}
	if r.Mode != "source_chunk" && (r.IndexedParentRows != 0 || r.ChunkBatchSize != 0 || r.ChunkBatchCount != 0) {
		return fmt.Errorf("non-source modes must not claim chunked parent or batch rows")
	}
	if r.Mode == "maintenance" && (r.IndexedLiveRows != r.SourceDocuments/2 || r.TombstoneDebt != uint64(r.SourceDocuments-r.IndexedLiveRows)) {
		return fmt.Errorf("maintenance must record deleted-document tombstone debt")
	}
	if r.Mode != "maintenance" && r.TombstoneDebt != 0 {
		return fmt.Errorf("non-maintenance modes must not claim tombstone debt")
	}
	if r.Postings == 0 || r.Terms == 0 || r.Blocks == 0 || r.Generations == 0 || r.IndexedRowsPerSec <= 0 || r.WallSeconds <= 0 {
		return fmt.Errorf("text-v2 counts or timing incomplete")
	}
	for _, name := range []string{"analyzer", "posting_builder", "root_mutation", "value_log", "checkpoint", "reopen"} {
		v, ok := r.Stages[name]
		if !ok {
			return fmt.Errorf("missing %s stage", name)
		}
		if err := validateMetric(v); err != nil {
			return fmt.Errorf("%s stage: %w", name, err)
		}
		if v.State == "observed" && v.Value <= 0 {
			return fmt.Errorf("%s stage must not use a zero placeholder", name)
		}
	}
	for _, v := range []metric{r.CPUSeconds, r.BytesPerOp, r.AllocsPerOp, r.CumulativeAllocs, r.PeakRSSBytes} {
		if err := validateMetric(v); err != nil {
			return fmt.Errorf("resource metric: %w", err)
		}
	}
	if err := validateStorage(r.Storage); err != nil {
		return err
	}
	if r.TextV2.DocIDBytes <= 0 || r.TextV2.DocMapBytes <= 0 || r.TextV2.PostingBytes <= 0 || r.TextV2.NormBytes <= 0 || r.TextV2.TermBytes <= 0 || r.TextV2.StatusBytes <= 0 || r.TextV2.PositionBytes < 0 {
		return fmt.Errorf("text-v2 component evidence incomplete")
	}
	if !r.CheckpointOK || !r.CloseOK || !r.ReopenOK || r.Probe.Results == 0 || r.Probe.DocumentsFetched != 0 || r.Probe.FailClosed != 0 {
		return fmt.Errorf("checkpoint/close/reopen or score-only probe failed")
	}
	return nil
}
func validateMetric(v metric) error {
	switch v.State {
	case "observed":
		if v.Value < 0 || math.IsNaN(v.Value) || math.IsInf(v.Value, 0) {
			return fmt.Errorf("invalid observed value")
		}
	case "unavailable":
		if strings.TrimSpace(v.Reason) == "" {
			return fmt.Errorf("unavailable metric needs reason")
		}
	default:
		return fmt.Errorf("state must be observed or unavailable")
	}
	return nil
}
func validateStorage(s storage) error {
	if s.PhysicalIndexPageBytes < 0 || s.PhysicalValueLogBytes < 0 || s.PhysicalWALBytes < 0 || s.PhysicalOtherBytes < 0 || s.PhysicalTotalBytes <= 0 || s.PhysicalTotalWALExcludedBytes < 0 || s.LogicalPrimaryPayloadBytes <= 0 {
		return fmt.Errorf("storage accounting incomplete")
	}
	if s.LogicalTextV2Overlap != "logical_text_v2_components_overlap_physical_storage_non_additive" {
		return fmt.Errorf("logical text-v2 overlap label is required")
	}
	if s.PhysicalTotalBytes != s.PhysicalIndexPageBytes+s.PhysicalValueLogBytes+s.PhysicalWALBytes+s.PhysicalOtherBytes {
		return fmt.Errorf("physical total must equal disjoint physical buckets")
	}
	if s.PhysicalTotalWALExcludedBytes != s.PhysicalTotalBytes-s.PhysicalWALBytes {
		return fmt.Errorf("WAL-excluded physical total is inconsistent")
	}
	return nil
}
func validateSummary(s modeScaleSummary, reps map[int]row) error {
	if s.MedianWallSeconds <= 0 || s.P95WallSeconds <= 0 || s.MedianIndexedRowsPerSec <= 0 || s.P95IndexedRowsPerSec <= 0 {
		return fmt.Errorf("summary values required")
	}
	wall := make([]float64, 0, len(reps))
	rate := make([]float64, 0, len(reps))
	for _, r := range reps {
		wall = append(wall, r.WallSeconds)
		rate = append(rate, r.IndexedRowsPerSec)
	}
	sort.Float64s(wall)
	sort.Float64s(rate)
	if !same(s.MedianWallSeconds, percentile(wall, .5)) || !same(s.P95WallSeconds, percentile(wall, .95)) || !same(s.MedianIndexedRowsPerSec, percentile(rate, .5)) || !same(s.P95IndexedRowsPerSec, percentile(rate, .95)) {
		return fmt.Errorf("summary does not recompute from raw repetitions")
	}
	return nil
}
func percentile(a []float64, p float64) float64 { return a[int(math.Ceil(p*float64(len(a))))-1] }
func same(a, b float64) bool                    { return math.Abs(a-b) <= 1e-9*math.Max(1, math.Abs(b)) }
