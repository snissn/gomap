package db

import (
	"fmt"
	"strings"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/page"
)

// FlushSpanRunFallbackReason is the stable M8 fallback matrix for future
// span-native flush/apply. The string values are exported through stats, bench
// artifacts, and docs; add new reasons append-only so existing dashboards and
// downstream milestones keep their meaning.
type FlushSpanRunFallbackReason uint8

const (
	FlushSpanRunFallbackUnknown FlushSpanRunFallbackReason = iota
	FlushSpanRunFallbackDisabled
	FlushSpanRunFallbackBelowThreshold
	FlushSpanRunFallbackSpanNativeNotImplemented
	FlushSpanRunFallbackPrepareError
	FlushSpanRunFallbackValidationFailed
	FlushSpanRunFallbackRootMismatch
	FlushSpanRunFallbackRangeDeleteBarrier
	FlushSpanRunFallbackLaneBarrier
	FlushSpanRunFallbackCommandWALBarrier
	FlushSpanRunFallbackInexactLeafSpans
	FlushSpanRunFallbackColdBuild
	FlushSpanRunFallbackMaintenance
	FlushSpanRunFallbackBackendChunkSplit
	FlushSpanRunFallbackCloseOrCheckpoint
	FlushSpanRunFallbackMemoryEmergencyCap
	FlushSpanRunFallbackOutputOwnershipFailure
	FlushSpanRunFallbackReducerValidationFailed
	FlushSpanRunFallbackRouteIneligible
	FlushSpanRunFallbackAdmissionPolicyDecline
	FlushSpanRunFallbackReducerValidationGuard

	flushSpanRunFallbackReasonCount
)

// FlushSpanRunFallbackReasonCount is the size of the append-only fallback
// reason enum. It is exported for stats arrays; callers should iterate with
// FlushSpanRunFallbackReasons when they need the concrete stable reasons.
const FlushSpanRunFallbackReasonCount = int(flushSpanRunFallbackReasonCount)

var flushSpanRunFallbackReasonNames = [...]string{
	FlushSpanRunFallbackUnknown:                  "unknown",
	FlushSpanRunFallbackDisabled:                 "disabled",
	FlushSpanRunFallbackBelowThreshold:           "below_threshold",
	FlushSpanRunFallbackSpanNativeNotImplemented: "span_native_not_implemented",
	FlushSpanRunFallbackPrepareError:             "prepare_error",
	FlushSpanRunFallbackValidationFailed:         "validation_failed",
	FlushSpanRunFallbackRootMismatch:             "root_mismatch",
	FlushSpanRunFallbackRangeDeleteBarrier:       "range_delete_barrier",
	FlushSpanRunFallbackLaneBarrier:              "lane_barrier",
	FlushSpanRunFallbackCommandWALBarrier:        "command_wal_barrier",
	FlushSpanRunFallbackInexactLeafSpans:         "inexact_leaf_spans",
	FlushSpanRunFallbackColdBuild:                "cold_build",
	FlushSpanRunFallbackMaintenance:              "maintenance",
	FlushSpanRunFallbackBackendChunkSplit:        "backend_chunk_split",
	FlushSpanRunFallbackCloseOrCheckpoint:        "close_or_checkpoint",
	FlushSpanRunFallbackMemoryEmergencyCap:       "memory_or_emergency_cap",
	FlushSpanRunFallbackOutputOwnershipFailure:   "output_ownership_failure",
	FlushSpanRunFallbackReducerValidationFailed:  "reducer_validation_failed",
	FlushSpanRunFallbackRouteIneligible:          "route_ineligible",
	FlushSpanRunFallbackAdmissionPolicyDecline:   "admission_policy_decline",
	FlushSpanRunFallbackReducerValidationGuard:   "reducer_validation_guard",
}

func (r FlushSpanRunFallbackReason) String() string {
	if int(r) >= 0 && int(r) < len(flushSpanRunFallbackReasonNames) {
		if name := flushSpanRunFallbackReasonNames[r]; name != "" {
			return name
		}
	}
	return flushSpanRunFallbackReasonNames[FlushSpanRunFallbackUnknown]
}

// Valid reports whether r is a known append-only fallback reason.
func (r FlushSpanRunFallbackReason) Valid() bool {
	return int(r) >= 0 && int(r) < int(flushSpanRunFallbackReasonCount) && flushSpanRunFallbackReasonNames[r] != ""
}

// ParseFlushSpanRunFallbackReason parses a stable fallback reason stat value.
func ParseFlushSpanRunFallbackReason(s string) (FlushSpanRunFallbackReason, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return FlushSpanRunFallbackUnknown, false
	}
	for _, reason := range FlushSpanRunFallbackReasons() {
		if reason.String() == s {
			return reason, true
		}
	}
	return FlushSpanRunFallbackUnknown, false
}

// FlushSpanRunFallbackReasons returns the stable fallback reason list in enum
// order. Unknown is included so stats can fail closed instead of silently
// dropping an unclassified fallback.
func FlushSpanRunFallbackReasons() []FlushSpanRunFallbackReason {
	out := make([]FlushSpanRunFallbackReason, 0, int(flushSpanRunFallbackReasonCount))
	for i := FlushSpanRunFallbackReason(0); i < flushSpanRunFallbackReasonCount; i++ {
		if i.Valid() {
			out = append(out, i)
		}
	}
	return out
}

// FlushSpanRunBaseRootValidation records the root identity captured before
// span planning and the root identity observed at guarded publish. Future
// span-native reducers must publish only when Matched is true; mismatches retry
// or fall back and any already prepared output is durable-but-unreachable.
type FlushSpanRunBaseRootValidation struct {
	CapturedRootID uint64
	CurrentRootID  uint64
	Matched        bool
}

// FlushSpanRunTargetLeafSpan is the M8 target-leaf span contract consumed by
// future span-native jobs. PointOpStart/PointOpEnd index the canonical run's
// globally shadowed point-op slice; DeleteRangeStart/DeleteRangeEnd index the
// canonical range-barrier slice. It owns no durable output.
type FlushSpanRunTargetLeafSpan struct {
	SpanIndex int
	Ref       page.ChildRef

	LowKey     []byte
	HighKey    []byte
	FirstOpKey []byte
	LastOpKey  []byte

	PointOpStart     int
	PointOpEnd       int
	DeleteRangeStart int
	DeleteRangeEnd   int
	OpCount          int
	ByteCount        int
}

// FlushSpanRunBackendChunk describes an existing entry-count backend batch
// chunk. M8 keeps this metadata explicit so later milestones can prove whether
// entry-count chunking splits target leaves and whether leaf-aware chunking fixes
// that split.
type FlushSpanRunBackendChunk struct {
	ChunkIndex   int
	PointOpStart int
	PointOpEnd   int
	ByteCount    int
}

// FlushSpanRunMetadata is the canonical run-level metadata contract. M8 does
// not build the production multi-memtable run yet; the structure documents the
// fields M9+ must provide before span planning and reducer execution.
type FlushSpanRunMetadata struct {
	RunID uint64

	BaseRoot FlushSpanRunBaseRootValidation

	SourceMemtables  int
	SourcePointOps   int
	PlannedPointOps  int
	ShadowedPointOps int
	RangeBarriers    int
	LaneBarriers     int

	TargetLeafSpans []FlushSpanRunTargetLeafSpan
	BackendChunks   []FlushSpanRunBackendChunk
}

// FlushSpanRunPlanRequest asks a backend to plan exact target leaf spans for a
// canonical point-op run. PointOps must already be globally sorted and
// shadowed; DeleteRanges are explicit range barriers/ranges that the caller did
// not cross while selecting source memtables.
//
// The request owns no durable output. Slices only need to remain stable for the
// duration of the call.
type FlushSpanRunPlanRequest struct {
	RunID uint64

	SourceMemtables  int
	SourcePointOps   int
	PlannedPointOps  int
	ShadowedPointOps int
	RangeBarriers    int
	LaneBarriers     int

	PointOps     []batch.Entry
	DeleteRanges []batch.DeleteRange
}

// FlushSpanRunChunkPlan is the cache-layer M9 planning result used to pack a
// canonical point run into backend chunks without materializing a second copy of
// every target leaf span. Full TargetLeafSpans remain available through
// PlanFlushSpanRun for tests and future span-native consumers; this compact
// result is the hot-path chunking/observability form.
type FlushSpanRunChunkPlan struct {
	Metadata FlushSpanRunMetadata

	TargetLeafSpans int
	SingleOpSpans   int
	SpanOps         int
	SpanBytes       int

	BackendChunks []FlushSpanRunBackendChunk
	SplitSummary  FlushSpanRunChunkSplitSummary
}

// FlushSpanRunPreparedOutputOwnership describes prepared durable output owned by
// a span job. Prepared leaf/value-log output is persistent storage: on retry or
// root mismatch it becomes durable-but-unreachable and is accounted as abandoned;
// it is never rolled back by truncating durable pointer targets.
type FlushSpanRunPreparedOutputOwnership struct {
	LeafLogPages int
	LeafLogBytes int64
	RetiredRefs  []page.ChildRef

	Installed               bool
	DurableButUnreachable   bool
	AbandonedFallbackReason FlushSpanRunFallbackReason
}

// FlushSpanRunSpanJobInput is the future span-native worker input contract. The
// op slices are canonical slices from FlushSpanRunMetadata and must already have
// same-key shadowing applied globally across source memtables.
type FlushSpanRunSpanJobInput struct {
	RunID      uint64
	BaseRootID uint64
	Span       FlushSpanRunTargetLeafSpan
	PointOps   []batch.Entry
	Ranges     []batch.DeleteRange
}

// FlushSpanRunSpanJobOutput is the deterministic worker output contract fed to
// the reducer. SplitBoundaries are ordered high-key boundaries for ReplacementRefs.
type FlushSpanRunSpanJobOutput struct {
	SpanIndex       int
	ReplacementRefs []page.ChildRef
	SplitBoundaries [][]byte
	PreparedOutput  FlushSpanRunPreparedOutputOwnership
}

// FlushSpanRunReducerInput is the deterministic reducer contract. Outputs must
// be sorted by SpanIndex, and the reducer must validate BaseRoot before publish.
type FlushSpanRunReducerInput struct {
	RunID      uint64
	BaseRoot   FlushSpanRunBaseRootValidation
	SpanOutput []FlushSpanRunSpanJobOutput
}

// FlushSpanRunChunkSplitSummary summarizes whether existing entry-count backend
// chunks split target leaf spans.
type FlushSpanRunChunkSplitSummary struct {
	BackendChunks                 int
	TargetLeafSpans               int
	TargetLeavesSplitAcrossChunks int
	MaxChunksPerTargetLeaf        int
}

// SummarizeFlushSpanRunChunkSplits returns how many target leaves are crossed by
// more than one backend chunk. A later leaf-aware chunker should drive
// TargetLeavesSplitAcrossChunks to zero for point-write spans.
func SummarizeFlushSpanRunChunkSplits(spans []FlushSpanRunTargetLeafSpan, chunks []FlushSpanRunBackendChunk) FlushSpanRunChunkSplitSummary {
	summary := FlushSpanRunChunkSplitSummary{
		BackendChunks:   len(chunks),
		TargetLeafSpans: len(spans),
	}
	for i := range spans {
		span := spans[i]
		if span.PointOpEnd <= span.PointOpStart {
			continue
		}
		overlaps := 0
		for j := range chunks {
			chunk := chunks[j]
			if chunk.PointOpEnd <= chunk.PointOpStart {
				continue
			}
			if chunk.PointOpStart < span.PointOpEnd && chunk.PointOpEnd > span.PointOpStart {
				overlaps++
			}
		}
		if overlaps > summary.MaxChunksPerTargetLeaf {
			summary.MaxChunksPerTargetLeaf = overlaps
		}
		if overlaps > 1 {
			summary.TargetLeavesSplitAcrossChunks++
		}
	}
	return summary
}

// ValidateFlushSpanRunMetadata checks only the ordering/ownership invariants
// needed by the M8 contract. It intentionally does not execute or publish the
// run; M9+ remains responsible for constructing the metadata from sealed
// memtables and base-root snapshots.
func ValidateFlushSpanRunMetadata(meta FlushSpanRunMetadata) error {
	if meta.SourceMemtables < 0 || meta.SourcePointOps < 0 || meta.PlannedPointOps < 0 || meta.ShadowedPointOps < 0 || meta.RangeBarriers < 0 || meta.LaneBarriers < 0 {
		return fmt.Errorf("negative span-run metadata count")
	}
	if meta.SourcePointOps != meta.PlannedPointOps+meta.ShadowedPointOps {
		return fmt.Errorf("source point ops=%d must equal planned point ops=%d plus shadowed point ops=%d", meta.SourcePointOps, meta.PlannedPointOps, meta.ShadowedPointOps)
	}
	if meta.PlannedPointOps > 0 && len(meta.TargetLeafSpans) == 0 {
		return fmt.Errorf("planned point ops=%d require target leaf spans", meta.PlannedPointOps)
	}
	prevEnd := 0
	for i := range meta.TargetLeafSpans {
		span := meta.TargetLeafSpans[i]
		if span.SpanIndex != i {
			return fmt.Errorf("target span %d has span index %d", i, span.SpanIndex)
		}
		if span.PointOpStart < 0 || span.PointOpEnd < span.PointOpStart || span.PointOpEnd > meta.PlannedPointOps {
			return fmt.Errorf("target span %d point op range [%d,%d) out of planned bounds %d", i, span.PointOpStart, span.PointOpEnd, meta.PlannedPointOps)
		}
		if span.DeleteRangeStart < 0 || span.DeleteRangeEnd < span.DeleteRangeStart || span.DeleteRangeEnd > meta.RangeBarriers {
			return fmt.Errorf("target span %d delete range indexes [%d,%d) invalid for range barriers %d", i, span.DeleteRangeStart, span.DeleteRangeEnd, meta.RangeBarriers)
		}
		if span.PointOpStart != prevEnd {
			return fmt.Errorf("target span %d point op range starts at %d, want prior end %d", i, span.PointOpStart, prevEnd)
		}
		if span.OpCount < 0 || span.ByteCount < 0 {
			return fmt.Errorf("target span %d has negative op/byte count", i)
		}
		prevEnd = span.PointOpEnd
	}
	if len(meta.TargetLeafSpans) > 0 && prevEnd != meta.PlannedPointOps {
		return fmt.Errorf("target spans cover point ops through %d, want planned point ops %d", prevEnd, meta.PlannedPointOps)
	}
	prevEnd = 0
	for i := range meta.BackendChunks {
		chunk := meta.BackendChunks[i]
		if chunk.ChunkIndex != i {
			return fmt.Errorf("backend chunk %d has chunk index %d", i, chunk.ChunkIndex)
		}
		if chunk.PointOpStart < 0 || chunk.PointOpEnd < chunk.PointOpStart || chunk.PointOpEnd > meta.PlannedPointOps {
			return fmt.Errorf("backend chunk %d point op range [%d,%d) out of planned bounds %d", i, chunk.PointOpStart, chunk.PointOpEnd, meta.PlannedPointOps)
		}
		if i > 0 && chunk.PointOpStart < prevEnd {
			return fmt.Errorf("backend chunk %d overlaps prior point op range: start=%d prior_end=%d", i, chunk.PointOpStart, prevEnd)
		}
		if chunk.ByteCount < 0 {
			return fmt.Errorf("backend chunk %d has negative byte count", i)
		}
		prevEnd = chunk.PointOpEnd
	}
	return nil
}
