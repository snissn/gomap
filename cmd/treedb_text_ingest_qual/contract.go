package main

import (
	"fmt"
	"strings"
)

const contractVersion = "treedb_text_ingest_qualification/v1"

type manifest struct {
	SchemaVersion string `json:"schema_version"`
	FixtureSHA256 string `json:"fixture_sha256"`
	Analyzer      string `json:"analyzer"`
	FieldWeights  string `json:"field_weights"`
	IDsSHA256     string `json:"ids_sha256"`
	Command       string `json:"command"`
	Commit        string `json:"commit"`
	Host          string `json:"host"`
	CacheState    string `json:"cache_state"`
	Durability    string `json:"durability"`
	TimedBoundary string `json:"timed_boundary"`
	Vectors       bool   `json:"vectors"`
	Dirty         bool   `json:"dirty"`
}

type report struct {
	SchemaVersion  string `json:"schema_version"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Rows           []row  `json:"rows"`
}

type row struct {
	Mode              string             `json:"mode"`
	Scale             int                `json:"scale"`
	Repetition        int                `json:"repetition"`
	ExpectedDocuments int                `json:"expected_documents"`
	LiveDocuments     int                `json:"live_documents"`
	Postings          uint64             `json:"postings"`
	Terms             uint64             `json:"terms"`
	Blocks            uint64             `json:"blocks"`
	Generations       uint64             `json:"generations"`
	StaleDebt         uint64             `json:"stale_debt"`
	TombstoneDebt     uint64             `json:"tombstone_debt"`
	SourceDocsPerSec  float64            `json:"source_docs_per_second"`
	ChunksPerSec      float64            `json:"chunks_per_second"`
	IndexedRowsPerSec float64            `json:"indexed_rows_per_second"`
	WallSeconds       float64            `json:"wall_seconds"`
	CPUSeconds        float64            `json:"cpu_seconds"`
	BytesPerOp        float64            `json:"bytes_per_op"`
	AllocsPerOp       float64            `json:"allocs_per_op"`
	PeakRSSBytes      int64              `json:"peak_rss_bytes"`
	Stages            map[string]float64 `json:"stages_seconds"`
	Storage           storage            `json:"storage"`
	TextV2            textV2             `json:"text_v2"`
	CheckpointOK      bool               `json:"checkpoint_ok"`
	ReopenOK          bool               `json:"reopen_ok"`
	Probe             scoreOnlyProbe     `json:"score_only_probe"`
	Median            summary            `json:"median"`
	P95               summary            `json:"p95"`
}

type storage struct {
	PrimaryBytes  int64 `json:"primary_bytes"`
	TextRootBytes int64 `json:"text_root_bytes"`
	ValueLogBytes int64 `json:"value_log_bytes"`
	WALBytes      int64 `json:"wal_bytes"`
	TotalBytes    int64 `json:"total_bytes"`
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

type summary struct {
	WallSeconds       float64 `json:"wall_seconds"`
	IndexedRowsPerSec float64 `json:"indexed_rows_per_second"`
}

func validate(m manifest, r report, manifestSHA string) error {
	if m.SchemaVersion != contractVersion || r.SchemaVersion != contractVersion {
		return fmt.Errorf("schema_version must be %q", contractVersion)
	}
	if m.Vectors || m.Dirty {
		return fmt.Errorf("pure-text manifest requires vectors=false and dirty=false")
	}
	for name, value := range map[string]string{"fixture_sha256": m.FixtureSHA256, "analyzer": m.Analyzer, "field_weights": m.FieldWeights, "ids_sha256": m.IDsSHA256, "command": m.Command, "commit": m.Commit, "host": m.Host, "cache_state": m.CacheState, "durability": m.Durability, "timed_boundary": m.TimedBoundary} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("manifest %s is required", name)
		}
	}
	if r.ManifestSHA256 != manifestSHA {
		return fmt.Errorf("manifest_sha256 does not match manifest bytes")
	}
	if len(r.Rows) == 0 {
		return fmt.Errorf("no rows")
	}
	seenModes := map[string]bool{}
	repetitions := map[string]map[int]bool{}
	for i, row := range r.Rows {
		if err := validateRow(row); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
		seenModes[row.Mode] = true
		key := fmt.Sprintf("%s/%d", row.Mode, row.Scale)
		if repetitions[key] == nil {
			repetitions[key] = map[int]bool{}
		}
		if repetitions[key][row.Repetition] {
			return fmt.Errorf("duplicate repetition %s/%d", key, row.Repetition)
		}
		repetitions[key][row.Repetition] = true
	}
	for _, mode := range []string{"indexed_insert", "post_load_backfill", "source_chunk", "maintenance"} {
		if !seenModes[mode] {
			return fmt.Errorf("missing required mode %q", mode)
		}
	}
	for key, reps := range repetitions {
		if strings.HasSuffix(key, "/100000") || strings.HasSuffix(key, "/1000000") {
			if len(reps) != 3 || !reps[1] || !reps[2] || !reps[3] {
				return fmt.Errorf("retained %s requires serialized repetitions 1,2,3", key)
			}
		}
	}
	return nil
}

func validateRow(r row) error {
	if r.Scale != 10_000 && r.Scale != 100_000 && r.Scale != 1_000_000 {
		return fmt.Errorf("scale %d is not predeclared", r.Scale)
	}
	if r.Repetition < 1 || r.ExpectedDocuments < 1 || r.LiveDocuments != r.ExpectedDocuments {
		return fmt.Errorf("invalid repetition or exact document count")
	}
	if r.Postings == 0 || r.Terms == 0 || r.Blocks == 0 || r.Generations == 0 {
		return fmt.Errorf("text-v2 counts must be nonzero")
	}
	if r.IndexedRowsPerSec <= 0 || r.WallSeconds <= 0 || r.CPUSeconds < 0 || r.BytesPerOp < 0 || r.AllocsPerOp < 0 || r.PeakRSSBytes <= 0 {
		return fmt.Errorf("resource accounting is incomplete")
	}
	if r.Mode == "source_chunk" && (r.SourceDocsPerSec <= 0 || r.ChunksPerSec <= 0) {
		return fmt.Errorf("source_chunk requires source and chunk throughput")
	}
	for _, stage := range []string{"analyzer", "posting_builder", "root_mutation", "value_log", "checkpoint", "reopen"} {
		if _, ok := r.Stages[stage]; !ok || r.Stages[stage] < 0 {
			return fmt.Errorf("missing or invalid %s stage", stage)
		}
	}
	if r.Storage.PrimaryBytes < 0 || r.Storage.TextRootBytes < 0 || r.Storage.ValueLogBytes < 0 || r.Storage.WALBytes < 0 || r.Storage.TotalBytes <= 0 {
		return fmt.Errorf("storage accounting is incomplete")
	}
	if r.TextV2.DocIDBytes < 0 || r.TextV2.DocMapBytes < 0 || r.TextV2.PostingBytes < 0 || r.TextV2.NormBytes < 0 || r.TextV2.PositionBytes < 0 || r.TextV2.TermBytes < 0 || r.TextV2.StatusBytes < 0 {
		return fmt.Errorf("text-v2 byte accounting is incomplete")
	}
	if !r.CheckpointOK || !r.ReopenOK || r.Probe.Results == 0 || r.Probe.DocumentsFetched != 0 || r.Probe.FailClosed != 0 {
		return fmt.Errorf("checkpoint/reopen or score-only probe failed")
	}
	if r.Scale >= 100_000 && (r.Median.WallSeconds <= 0 || r.Median.IndexedRowsPerSec <= 0 || r.P95.WallSeconds <= 0 || r.P95.IndexedRowsPerSec <= 0) {
		return fmt.Errorf("retained row requires median and p95 summaries")
	}
	return nil
}
