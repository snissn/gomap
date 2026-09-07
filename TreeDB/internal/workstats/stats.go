// Package workstats observes process-wide work from package initialization,
// including backend recovery before a service or collection manager exists.
// Counters are always on, never reset, and do not represent per-database work.
package workstats

import (
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

var origin = time.Now()

// ScanCounter counts actual dispatches and visited rows, including failed work.
type ScanCounter struct{ Starts, Rows atomic.Uint64 }
type ScanStats struct {
	Starts uint64 `json:"starts"`
	Rows   uint64 `json:"rows"`
}

func (c *ScanCounter) snapshot() ScanStats { return ScanStats{c.Starts.Load(), c.Rows.Load()} }

var IndexedJSON struct {
	ScalarRows          atomic.Uint64
	TextRows            atomic.Uint64
	ColumnRows          atomic.Uint64
	VectorRows          atomic.Uint64
	MaterializationRows atomic.Uint64
}

type IndexedJSONStats struct {
	ScalarRows          uint64 `json:"scalar_rows"`
	TextRows            uint64 `json:"text_rows"`
	ColumnRows          uint64 `json:"column_rows"`
	VectorRows          uint64 `json:"vector_rows"`
	MaterializationRows uint64 `json:"materialization_rows"`
}

var Typed struct {
	ScalarRows   atomic.Uint64
	ScalarValues atomic.Uint64
	TextRows     atomic.Uint64
	TextFields   atomic.Uint64
	TextOldRows  atomic.Uint64
}

type TypedStats struct {
	ScalarRows   uint64 `json:"scalar_rows"`
	ScalarValues uint64 `json:"scalar_values"`
	TextRows     uint64 `json:"text_rows"`
	TextFields   uint64 `json:"text_fields"`
	TextOldRows  uint64 `json:"text_old_rows"`
}

var Runtime struct {
	QueryAttempts    atomic.Uint64
	QueriesCompleted atomic.Uint64
}

// RowIndexCache observes the generic immutable row-offset memo. Residency is
// cache-owned metadata only; live readers can retain evicted slices separately.
var RowIndexCache struct {
	Entries, RetainedBytes, ByteLimit atomic.Uint64
	Hits, Misses, Builds, RowsVisited atomic.Uint64
	Evictions, OversizedBypasses      atomic.Uint64
}

type RowIndexCacheStats struct {
	Entries           uint64 `json:"entries"`
	RetainedBytes     uint64 `json:"retained_bytes"`
	ByteLimit         uint64 `json:"byte_limit"`
	Hits              uint64 `json:"hits"`
	Misses            uint64 `json:"misses"`
	Builds            uint64 `json:"builds"`
	RowsVisited       uint64 `json:"rows_visited"`
	Evictions         uint64 `json:"evictions"`
	OversizedBypasses uint64 `json:"oversized_bypasses"`
}

type RuntimeStats struct {
	QueryAttempts    uint64 `json:"query_attempts"`
	QueriesCompleted uint64 `json:"queries_completed"`
}

var Replay struct {
	FramesAttempted             atomic.Uint64
	FramesApplied               atomic.Uint64
	FrameErrors                 atomic.Uint64
	TypedPayloadFrames          atomic.Uint64
	LegacyCollectionFrames      atomic.Uint64
	DeleteFrames                atomic.Uint64
	TypedRowsDecoded            atomic.Uint64
	LegacyProjectionRowsDecoded atomic.Uint64
}

type ReplayStats struct {
	FramesAttempted             uint64 `json:"frames_attempted"`
	FramesApplied               uint64 `json:"frames_applied"`
	FrameErrors                 uint64 `json:"frame_errors"`
	TypedPayloadFrames          uint64 `json:"typed_payload_frames"`
	LegacyCollectionFrames      uint64 `json:"legacy_collection_frames"`
	DeleteFrames                uint64 `json:"delete_frames"`
	TypedRowsDecoded            uint64 `json:"typed_rows_decoded"`
	LegacyProjectionRowsDecoded uint64 `json:"legacy_projection_rows_decoded"`
}

var Scans struct {
	DenseExact             ScanCounter
	FilteredCount          ScanCounter
	FilteredRetrieval      ScanCounter
	MutationMatch          ScanCounter
	Cursor                 ScanCounter
	CountIDs               ScanCounter
	CollectionExact        ScanCounter
	CollectionIndexedExact ScanCounter
}

type ScanWorkStats struct {
	DenseExact             ScanStats `json:"dense_exact"`
	FilteredCount          ScanStats `json:"filtered_count"`
	FilteredRetrieval      ScanStats `json:"filtered_retrieval"`
	MutationMatch          ScanStats `json:"mutation_match"`
	Cursor                 ScanStats `json:"cursor"`
	CountIDs               ScanStats `json:"count_ids"`
	CollectionExact        ScanStats `json:"collection_exact"`
	CollectionIndexedExact ScanStats `json:"collection_indexed_exact"`
}

// Availability describes implemented producer coverage, not a workload verdict.
// Graph/output/fold work is intentionally unavailable in this schema revision.
type Availability struct {
	IndexedJSON     bool `json:"indexed_json"`
	Typed           bool `json:"typed"`
	RuntimeQuery    bool `json:"runtime_query"`
	Replay          bool `json:"replay"`
	AttributedScans bool `json:"attributed_scans"`
	RowIndexCache   bool `json:"row_index_cache"`
	Graph           bool `json:"graph"`
	Output          bool `json:"output"`
	Fold            bool `json:"fold"`
}

// MemoryStats samples runtime process totals and current heap residency. These
// fields are not per-operation allocation claims; total_alloc and mallocs include
// startup/replay and can be differenced only within this process lifetime.
type MemoryStats struct {
	TotalAlloc uint64 `json:"total_alloc"`
	Mallocs    uint64 `json:"mallocs"`
	HeapAlloc  uint64 `json:"heap_alloc"`
	HeapSys    uint64 `json:"heap_sys"`
	Sys        uint64 `json:"sys"`
	NumGC      uint32 `json:"num_gc"`
}

// Snapshot is nontransactional: cumulative counters are monotonic, while memory
// and cache residency gauges can decrease. Drain operations before comparing
// related totals. A new process has a new origin; never subtract across
// origins. The caller must independently bind PID to executable/start identity.
type Snapshot struct {
	Memory           MemoryStats        `json:"memory"`
	SchemaVersion    string             `json:"schema_version"`
	Scope            string             `json:"scope"`
	PID              int                `json:"pid"`
	OriginKind       string             `json:"origin_kind"`
	OriginUnixNano   int64              `json:"origin_unix_nano"`
	SnapshotUnixNano int64              `json:"snapshot_unix_nano"`
	Available        Availability       `json:"available"`
	IndexedJSON      IndexedJSONStats   `json:"indexed_json"`
	Typed            TypedStats         `json:"typed"`
	Runtime          RuntimeStats       `json:"runtime"`
	Replay           ReplayStats        `json:"replay"`
	Scans            ScanWorkStats      `json:"scans"`
	RowIndexCache    RowIndexCacheStats `json:"row_index_cache"`
}

func Read() Snapshot {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return Snapshot{
		Memory:        MemoryStats{TotalAlloc: mem.TotalAlloc, Mallocs: mem.Mallocs, HeapAlloc: mem.HeapAlloc, HeapSys: mem.HeapSys, Sys: mem.Sys, NumGC: mem.NumGC},
		SchemaVersion: "treedb-work-v1", Scope: "process", PID: os.Getpid(),
		OriginKind: "go_package_init", OriginUnixNano: origin.UnixNano(), SnapshotUnixNano: time.Now().UnixNano(),
		Available: Availability{IndexedJSON: true, Typed: true, RuntimeQuery: true, Replay: true, AttributedScans: true, RowIndexCache: true},
		RowIndexCache: RowIndexCacheStats{
			Entries: RowIndexCache.Entries.Load(), RetainedBytes: RowIndexCache.RetainedBytes.Load(), ByteLimit: RowIndexCache.ByteLimit.Load(),
			Hits: RowIndexCache.Hits.Load(), Misses: RowIndexCache.Misses.Load(), Builds: RowIndexCache.Builds.Load(), RowsVisited: RowIndexCache.RowsVisited.Load(),
			Evictions: RowIndexCache.Evictions.Load(), OversizedBypasses: RowIndexCache.OversizedBypasses.Load(),
		},
		IndexedJSON: IndexedJSONStats{
			ScalarRows:          IndexedJSON.ScalarRows.Load(),
			TextRows:            IndexedJSON.TextRows.Load(),
			ColumnRows:          IndexedJSON.ColumnRows.Load(),
			VectorRows:          IndexedJSON.VectorRows.Load(),
			MaterializationRows: IndexedJSON.MaterializationRows.Load(),
		},
		Typed: TypedStats{
			ScalarRows:   Typed.ScalarRows.Load(),
			ScalarValues: Typed.ScalarValues.Load(),
			TextRows:     Typed.TextRows.Load(),
			TextFields:   Typed.TextFields.Load(),
			TextOldRows:  Typed.TextOldRows.Load(),
		},
		Runtime: RuntimeStats{
			QueryAttempts:    Runtime.QueryAttempts.Load(),
			QueriesCompleted: Runtime.QueriesCompleted.Load(),
		},
		Replay: ReplayStats{
			FramesAttempted:             Replay.FramesAttempted.Load(),
			FramesApplied:               Replay.FramesApplied.Load(),
			FrameErrors:                 Replay.FrameErrors.Load(),
			TypedPayloadFrames:          Replay.TypedPayloadFrames.Load(),
			LegacyCollectionFrames:      Replay.LegacyCollectionFrames.Load(),
			DeleteFrames:                Replay.DeleteFrames.Load(),
			TypedRowsDecoded:            Replay.TypedRowsDecoded.Load(),
			LegacyProjectionRowsDecoded: Replay.LegacyProjectionRowsDecoded.Load(),
		},
		Scans: ScanWorkStats{
			DenseExact:             Scans.DenseExact.snapshot(),
			FilteredCount:          Scans.FilteredCount.snapshot(),
			FilteredRetrieval:      Scans.FilteredRetrieval.snapshot(),
			MutationMatch:          Scans.MutationMatch.snapshot(),
			Cursor:                 Scans.Cursor.snapshot(),
			CountIDs:               Scans.CountIDs.snapshot(),
			CollectionExact:        Scans.CollectionExact.snapshot(),
			CollectionIndexedExact: Scans.CollectionIndexedExact.snapshot(),
		},
	}
}
