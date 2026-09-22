package zipper

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// ApplyOptions configures a root apply attempt.
type ApplyOptions struct {
	// CollectOldPointerRefs asks the apply engine to count pointer entries removed
	// by overwrites, point deletes, and range deletes while it already owns the old
	// leaf entry. It also reports the total existing entries removed so callers can
	// distinguish insert-only from destructive COW transitions. Counts are
	// attempt-local and returned only for successful apply.
	CollectOldPointerRefs bool

	// PrepareReadOnly asks ApplyWithOptions to run the read-only preparation
	// pass before applying the delta. This is opt-in because it traverses the
	// captured root in addition to the apply pass. It does not publish roots,
	// allocate output pages, append leaf/value-log records, retire old pages, or
	// transfer durable output ownership.
	PrepareReadOnly bool

	// ReadOnlyPrepare reuses buffers for the optional read-only preparation
	// pass. It is ignored unless PrepareReadOnly is true or ParallelApplyConcurrency
	// requests opt-in parallel apply gating.
	ReadOnlyPrepare ReadOnlyPrepareOptions

	// ReadOnlyPrepareWorkers requests deterministic leaf-span worker ranges for
	// reporting. Values <=0 use ParallelApplyConcurrency when opt-in parallel
	// apply is requested.
	ReadOnlyPrepareWorkers int

	// ParallelApplyConcurrency enables the M2 opt-in COW apply worker-pool path.
	// Values <=1 leave the existing apply path unchanged. The effective worker
	// count is also bounded by GOMAXPROCS, planned leaf spans, span bytes, and the
	// minimum work thresholds below.
	ParallelApplyConcurrency int
	// ParallelApplyWorkerPool supplies reusable workers for opt-in parallel apply.
	// If nil, ApplyWithOptions falls back to the existing per-call worker path.
	ParallelApplyWorkerPool *ApplyWorkerPool
	// ParallelApplyMinSpans gates opt-in worker-pool apply by planned leaf spans.
	// Values <=0 disable the span-count gate.
	ParallelApplyMinSpans int
	// ParallelApplyMinSpanOps gates opt-in worker-pool apply by span-local work.
	// Values <=0 disable the op-count gate.
	ParallelApplyMinSpanOps int
	// ParallelApplyMinSpanBytes gates opt-in worker-pool apply by estimated bytes.
	// Values <=0 disable the byte-count gate.
	ParallelApplyMinSpanBytes int

	// SpanNativeApply enables the M10 opt-in span-native apply candidate path.
	// The initial gate is deliberately narrower than M2 parallel COW apply: exact
	// point-only leaf spans, no cold build, no maintenance rewrite, and bounded
	// workers. Unsupported runs fail closed to the recursive apply fallback.
	SpanNativeApply bool
	// SpanNativeAllowMaintenancePointOps lets trusted ordered-root callers use
	// span-native apply for point-delete maintenance plans whose tombstone/edge
	// semantics are intentionally preserved. Raw DB apply must leave this false so
	// delete maintenance keeps using the recursive path that prunes and rebalances
	// empty or underfull pages.
	SpanNativeAllowMaintenancePointOps bool
	// SpanNativeForceFallbackReason forces a prepared span-native candidate to use
	// the safe recursive/parallel fallback and report this stable reason string.
	// It is used by checkpoint/close drains and emergency gates that must not
	// produce span-native durable output.
	SpanNativeForceFallbackReason string
}

// ApplyResult is the complete in-memory result of a root apply attempt. The
// retired page list is pending until the caller's install guard succeeds and
// the new root is committed.
type ApplyResult struct {
	RootID              uint64
	PendingRetiredPages []uint64
	Metrics             adaptive.Metrics
	OldPointerRefs      PointerRefCounts
	// OldEntriesRemoved counts existing leaf entries consumed by overwrites,
	// point deletes, and range deletes. It is valid when
	// OldPointerRefsCollected is true and lets callers distinguish insert-only
	// COW transitions from transitions that can make external leaf resources
	// unreachable.
	OldEntriesRemoved uint64
	// OldPointerRefsCollected distinguishes a successful empty count from a path
	// that did not collect apply-fed reference evidence.
	OldPointerRefsCollected bool

	// ReadOnlyPrepare is populated only when ApplyOptions.PrepareReadOnly is
	// true or opt-in parallel apply needs the M1 span plan for gating. It is
	// planning metadata only; it owns no pager or leaf-log output.
	ReadOnlyPrepare ReadOnlyPrepareResult
	// ReadOnlyPrepareNs is the time spent in the optional read-only preparation
	// pass. It is zero when no read-only preparation ran.
	ReadOnlyPrepareNs uint64
	// ReadOnlyPrepareRequested reports that read-only preparation was requested
	// explicitly or by the opt-in parallel apply gate.
	ReadOnlyPrepareRequested bool
	// ReadOnlyPrepareValidationFailed distinguishes validation failures from
	// read/decode failures for stats.
	ReadOnlyPrepareValidationFailed bool
	// ReadOnlyPrepareFailed distinguishes read/decode failures from later apply
	// failures for callers that need stable fallback labels.
	ReadOnlyPrepareFailed bool
	// ReadOnlyPrepareWorkerSummary is the deterministic worker-range aggregate for
	// the requested read-only worker target.
	ReadOnlyPrepareWorkerSummary ReadOnlyLeafSpanWorkerRangeSummary
	// ParallelApplyWorkers is the effective opt-in worker count selected after
	// applying all gates. ParallelApplyUsed is true when that count was >1.
	ParallelApplyWorkers int
	ParallelApplyUsed    bool

	// SpanNativeEligible reports that the M10 opt-in span-native gate accepted the
	// read-only plan. SpanNativeWorkers is the bounded worker count selected for
	// future span jobs. SpanNativeUsed remains false until the real span-native
	// execution engine replaces recursive fallback for eligible runs.
	SpanNativeEligible bool
	SpanNativeWorkers  int
	SpanNativeUsed     bool
	// SpanNativeFallbackReason is a stable fallback reason string when the
	// span-native candidate failed closed before, during, or after execution. DB
	// callers map it onto their append-only fallback reason enum for stats.
	SpanNativeFallbackReason string
}

// ReadOnlyPrepareOptions configures a read-only root preparation pass. The zero
// value is the normal caller-constructed form. Non-zero buffer reuse options are
// produced by ReadOnlyPrepareResult.ReuseOptions.
type ReadOnlyPrepareOptions struct {
	// OmitKeys skips copying span boundary and first/last op key bytes for
	// point-only plans. It is for hot-path aggregate/chunk planning that only
	// needs op indexes/counts; callers that consume exact span bounds for future
	// execution should leave it false. Plans containing delete ranges retain keys
	// because range-overlap planning needs exact span bounds.
	OmitKeys bool

	// OmitOpKeys skips copying FirstOpKey/LastOpKey while retaining LowKey/HighKey.
	// It is for execution paths that need exact span boundaries plus point index
	// ranges but do not consume the duplicated point key metadata.
	OmitOpKeys bool

	// DiscardLeafSpans streams spans to LeafSpanCallback without retaining them in
	// the result. It is only for aggregate/chunk planning callers that do not need
	// ValidateLeafSpans or exact span ownership after PrepareReadOnlyPlan returns.
	DiscardLeafSpans bool
	LeafSpanCallback func(ReadOnlyLeafSpan)

	// AllowMaintenancePointLeafSpans marks point-delete maintenance plans as exact
	// prepared-output ownership for ordered-root span-native apply. Normal raw DB
	// maintenance leaves this false so the prepared spans remain planning hints.
	AllowMaintenancePointLeafSpans bool

	leafSpans []ReadOnlyLeafSpan
	keyArena  []byte
}

// ReadOnlyLeafSpan describes one existing leaf range touched by a canonical
// sorted apply plan. It contains only in-memory planning metadata; it does not
// own prepared pager pages, leaf-log records, value-log records, or pending
// retired pages.
type ReadOnlyLeafSpan struct {
	// Ref identifies the existing leaf that owns this span. When ColdBuild is
	// true there is no existing leaf; Ref is the zero ChildRef and is not
	// actionable.
	Ref page.ChildRef

	// LowKey is the inclusive lower bound for the leaf span. HighKey is the
	// exclusive upper bound. A nil bound is open-ended. Non-nil bound slices and
	// op-key slices are owned by ReadOnlyPrepareResult until ReuseOptions is used,
	// except that OmitOpKeys may intentionally leave FirstOpKey/LastOpKey nil.
	LowKey  []byte
	HighKey []byte

	// FirstOpKey and LastOpKey describe the point operations assigned to this
	// span. They are nil for range-only spans.
	FirstOpKey []byte
	LastOpKey  []byte

	// OpCount is the span-local work count: PointOpCount plus DeleteRangeCount.
	// Delete ranges that overlap multiple leaves are counted once per touched
	// span, because future workers need each overlapping range in every touched
	// leaf span.
	OpCount          int
	PointOpCount     int
	DeleteRangeCount int
	ByteCount        int

	// PointOpStart/PointOpEnd index the canonical point-op slice returned by the
	// batch apply plan. DeleteRangeStart/DeleteRangeEnd index the canonical merged
	// delete-range slice. The ranges are half-open and intended for future worker
	// construction; they do not keep the originating batch alive.
	PointOpStart     int
	PointOpEnd       int
	DeleteRangeStart int
	DeleteRangeEnd   int
}

// ReadOnlyLeafSpanSummary is a compact, allocation-free summary of a read-only
// leaf-span plan. It is intended for callers and benchmarks that need to report
// span distribution without requiring each caller to walk or retain the span
// slice.
type ReadOnlyLeafSpanSummary struct {
	// Ops is the logical root apply operation count: canonical point ops plus
	// canonical delete ranges.
	Ops          int
	PointOps     int
	DeleteRanges int

	// SpanOps and SpanBytes are summed over spans. SpanOps may exceed Ops when a
	// delete range overlaps multiple leaf spans.
	SpanOps   int
	SpanBytes int
	Spans     int

	ExactLeafSpans bool
	ColdBuild      bool
	Maintenance    bool

	// MinSpanOps/MaxSpanOps and MinSpanBytes/MaxSpanBytes are zero when Spans is
	// zero.
	MinSpanOps    int
	MaxSpanOps    int
	MinSpanBytes  int
	MaxSpanBytes  int
	SingleOpSpans int
	OpenLowSpans  int
	OpenHighSpans int
}

// ReadOnlyLeafSpanWorkerRange assigns a contiguous range of read-only leaf
// spans to one future worker. The range is a planning primitive only; it does
// not imply parallel execution or prepared output ownership.
type ReadOnlyLeafSpanWorkerRange struct {
	FirstSpan int
	SpanCount int
	Ops       int
	Bytes     int
}

// ReadOnlyLeafSpanWorkerRangeSummary is an allocation-free aggregate of the
// deterministic worker ranges derived from a read-only leaf-span plan.
type ReadOnlyLeafSpanWorkerRangeSummary struct {
	TargetWorkers    int
	Ranges           int
	Ops              int
	Bytes            int
	MinRangeOps      int
	MaxRangeOps      int
	MinRangeBytes    int
	MaxRangeBytes    int
	SingleSpanRanges int
}

// ReadOnlyPrepareResult is the read-only portion of a root apply attempt. It is
// safe to discard on root mismatch because it has not allocated or persisted
// output pages.
type ReadOnlyPrepareResult struct {
	RootID       uint64
	Ops          int
	PointOps     int
	DeleteRanges int
	ColdBuild    bool
	Maintenance  bool
	OmitKeys     bool
	OmitOpKeys   bool

	// ExactLeafSpans is true when LeafSpans fully describe the existing leaves
	// touched by the delta. Delete-containing maintenance can merge/rebalance
	// adjacent leaves, so its direct key spans are useful planning hints but are
	// not complete prepared-output ownership. Range deletes do not enable
	// maintenance in the current apply path, so their overlapping leaf spans are
	// exact for planning purposes.
	ExactLeafSpans bool

	LeafSpans []ReadOnlyLeafSpan
	Metrics   adaptive.Metrics

	keyArena         []byte
	discardLeafSpans bool
	leafSpanCallback func(ReadOnlyLeafSpan)
}

const (
	readOnlyPrepareResultReuseLeafSpanKeepCap         = 65536
	readOnlyPrepareResultReuseOmitKeysLeafSpanKeepCap = 1 << 20
	readOnlyPrepareResultReuseKeyArenaKeepCap         = 16 << 20
)

// LeafSpanSummary returns an allocation-free aggregate view of r's leaf spans.
func (r ReadOnlyPrepareResult) LeafSpanSummary() ReadOnlyLeafSpanSummary {
	summary := ReadOnlyLeafSpanSummary{
		Ops:            r.Ops,
		PointOps:       r.PointOps,
		DeleteRanges:   r.DeleteRanges,
		Spans:          len(r.LeafSpans),
		ExactLeafSpans: r.ExactLeafSpans,
		ColdBuild:      r.ColdBuild,
		Maintenance:    r.Maintenance,
	}
	for i := range r.LeafSpans {
		span := &r.LeafSpans[i]
		summary.SpanOps += span.OpCount
		summary.SpanBytes += span.ByteCount
		if i == 0 || span.OpCount < summary.MinSpanOps {
			summary.MinSpanOps = span.OpCount
		}
		if i == 0 || span.OpCount > summary.MaxSpanOps {
			summary.MaxSpanOps = span.OpCount
		}
		if i == 0 || span.ByteCount < summary.MinSpanBytes {
			summary.MinSpanBytes = span.ByteCount
		}
		if i == 0 || span.ByteCount > summary.MaxSpanBytes {
			summary.MaxSpanBytes = span.ByteCount
		}
		if span.OpCount == 1 {
			summary.SingleOpSpans++
		}
		if span.LowKey == nil {
			summary.OpenLowSpans++
		}
		if span.HighKey == nil {
			summary.OpenHighSpans++
		}
	}
	return summary
}

// AppendLeafSpanWorkerRanges appends deterministic contiguous span partitions
// to dst. It creates at most workers ranges and never creates empty ranges. The
// returned ranges preserve span order and are suitable for future parallel
// preparation steps that still need serial output append and assembly order.
// No-op inputs return dst unchanged.
func (r ReadOnlyPrepareResult) AppendLeafSpanWorkerRanges(dst []ReadOnlyLeafSpanWorkerRange, workers int) []ReadOnlyLeafSpanWorkerRange {
	leafSummary := r.LeafSpanSummary()
	r.forEachLeafSpanWorkerRange(workers, leafSummary, func(workerRange ReadOnlyLeafSpanWorkerRange) {
		dst = append(dst, workerRange)
	})
	return dst
}

// AppendLeafSpanWorkUnitRanges appends one contiguous span-native work unit per
// active worker to dst. The active worker count is clamped by the non-empty
// prepared leaf spans, ranges preserve span order, and no empty range is
// emitted. Outputs still publish by original span index.
func (r ReadOnlyPrepareResult) AppendLeafSpanWorkUnitRanges(dst []ReadOnlyLeafSpanWorkerRange, workers int) []ReadOnlyLeafSpanWorkerRange {
	leafSummary := r.LeafSpanSummary()
	r.forEachLeafSpanWorkerRange(workers, leafSummary, func(workerRange ReadOnlyLeafSpanWorkerRange) {
		dst = append(dst, workerRange)
	})
	return dst
}

// LeafSpanWorkerRangeSummary returns an aggregate of the deterministic worker
// ranges for workers without retaining the ranges themselves.
func (r ReadOnlyPrepareResult) LeafSpanWorkerRangeSummary(workers int) ReadOnlyLeafSpanWorkerRangeSummary {
	summary := ReadOnlyLeafSpanWorkerRangeSummary{TargetWorkers: workers}
	leafSummary := r.LeafSpanSummary()
	summary.Ops = leafSummary.SpanOps
	summary.Bytes = leafSummary.SpanBytes
	r.forEachLeafSpanWorkerRange(workers, leafSummary, func(workerRange ReadOnlyLeafSpanWorkerRange) {
		summary.Ranges++
		if summary.Ranges == 1 || workerRange.Ops < summary.MinRangeOps {
			summary.MinRangeOps = workerRange.Ops
		}
		if summary.Ranges == 1 || workerRange.Ops > summary.MaxRangeOps {
			summary.MaxRangeOps = workerRange.Ops
		}
		if summary.Ranges == 1 || workerRange.Bytes < summary.MinRangeBytes {
			summary.MinRangeBytes = workerRange.Bytes
		}
		if summary.Ranges == 1 || workerRange.Bytes > summary.MaxRangeBytes {
			summary.MaxRangeBytes = workerRange.Bytes
		}
		if workerRange.SpanCount == 1 {
			summary.SingleSpanRanges++
		}
	})
	return summary
}

func (r ReadOnlyPrepareResult) forEachLeafSpanWorkerRange(workers int, leafSummary ReadOnlyLeafSpanSummary, fn func(ReadOnlyLeafSpanWorkerRange)) {
	if fn == nil || workers <= 0 || len(r.LeafSpans) == 0 {
		return
	}
	if workers > len(r.LeafSpans) {
		workers = len(r.LeafSpans)
	}
	totalOps := int64(leafSummary.SpanOps)
	if totalOps <= 0 {
		totalOps = int64(r.Ops)
	}

	spanIdx := 0
	cumulativeOps := int64(0)
	for rangeIdx := 0; rangeIdx < workers && spanIdx < len(r.LeafSpans); rangeIdx++ {
		firstSpan := spanIdx
		rangeOps := 0
		rangeBytes := 0
		remainingRanges := workers - rangeIdx - 1
		lastAllowedSpan := len(r.LeafSpans) - remainingRanges
		targetCumulativeOps := readOnlyPrepareCeilDiv64(totalOps*int64(rangeIdx+1), int64(workers))

		for spanIdx < lastAllowedSpan {
			spanOps := r.LeafSpans[spanIdx].OpCount
			rangeOps += spanOps
			rangeBytes += r.LeafSpans[spanIdx].ByteCount
			cumulativeOps += int64(spanOps)
			spanIdx++
			if remainingRanges > 0 && cumulativeOps >= targetCumulativeOps {
				break
			}
		}
		fn(ReadOnlyLeafSpanWorkerRange{
			FirstSpan: firstSpan,
			SpanCount: spanIdx - firstSpan,
			Ops:       rangeOps,
			Bytes:     rangeBytes,
		})
	}
}

func readOnlyPrepareCeilDiv64(n, d int64) int64 {
	if d <= 0 {
		return 0
	}
	return (n + d - 1) / d
}

// ReuseOptions returns buffers from r for a later read-only preparation pass.
// The returned options must not be used while r's LeafSpans are still needed.
func (r ReadOnlyPrepareResult) ReuseOptions() ReadOnlyPrepareOptions {
	return ReadOnlyPrepareOptions{
		OmitKeys:   r.OmitKeys,
		OmitOpKeys: r.OmitOpKeys,
		leafSpans:  clearReadOnlyLeafSpanBuffer(r.LeafSpans),
		keyArena:   r.keyArena[:0],
	}
}

func clearReadOnlyLeafSpanBuffer(spans []ReadOnlyLeafSpan) []ReadOnlyLeafSpan {
	if cap(spans) == 0 {
		return nil
	}
	// ReadOnlyLeafSpan contains key slices into keyArena. Clear the full backing
	// array before pooling so stale slice headers beyond len(spans) cannot keep an
	// oversized/dropped key arena live across later flushes.
	clear(spans[:cap(spans)])
	return spans[:0]
}

// ResetForReuse clears result metadata while retaining bounded reusable
// leaf-span and key-arena buffers for a later ReuseOptions call. Oversized
// buffers are dropped so temporary prepare pools do not retain one-off large
// batch state indefinitely.
func (r *ReadOnlyPrepareResult) ResetForReuse() {
	if r == nil {
		return
	}
	omitKeys := r.OmitKeys
	omitOpKeys := r.OmitOpKeys
	leafSpans := r.LeafSpans
	keyArena := r.keyArena
	*r = ReadOnlyPrepareResult{OmitKeys: omitKeys, OmitOpKeys: omitOpKeys}
	leafSpanKeepCap := readOnlyPrepareResultReuseLeafSpanKeepCap
	if omitKeys {
		leafSpanKeepCap = readOnlyPrepareResultReuseOmitKeysLeafSpanKeepCap
	}
	if cap(leafSpans) <= leafSpanKeepCap {
		r.LeafSpans = clearReadOnlyLeafSpanBuffer(leafSpans)
	}
	if !omitKeys && cap(keyArena) <= readOnlyPrepareResultReuseKeyArenaKeepCap {
		r.keyArena = keyArena[:0]
	}
}

// ValidateLeafSpans checks the deterministic planning invariants for r's
// read-only leaf-span view. It is intended for tests and future prepared-output
// callers that want to assert a plan before using it. ApplyWithOptions and DB
// wrappers validate opt-in plans before apply/publish so invalid plans fail
// closed before durable output is produced.
func (r ReadOnlyPrepareResult) ValidateLeafSpans() error {
	if r.Ops < 0 {
		return fmt.Errorf("zipper: read-only prepare has negative op count %d", r.Ops)
	}
	if r.PointOps < 0 || r.DeleteRanges < 0 {
		return fmt.Errorf("zipper: read-only prepare has negative point/range counts %d/%d", r.PointOps, r.DeleteRanges)
	}
	if r.PointOps+r.DeleteRanges > 0 && r.Ops != r.PointOps+r.DeleteRanges {
		return fmt.Errorf("zipper: read-only prepare has ops=%d but point/range counts=%d/%d", r.Ops, r.PointOps, r.DeleteRanges)
	}
	if r.Ops == 0 {
		if len(r.LeafSpans) != 0 {
			return fmt.Errorf("zipper: read-only prepare has %d spans for zero ops", len(r.LeafSpans))
		}
		return nil
	}
	if len(r.LeafSpans) == 0 {
		return fmt.Errorf("zipper: read-only prepare has %d ops but no leaf spans", r.Ops)
	}
	if r.ColdBuild && len(r.LeafSpans) != 1 {
		return fmt.Errorf("zipper: cold read-only prepare has %d leaf spans, want 1", len(r.LeafSpans))
	}
	totalPointOps := 0
	totalSpanOps := 0
	var prevLastOp []byte
	var prevHigh []byte
	prevPointEnd := -1
	prevRangeStart := -1
	omitSpanKeys := r.OmitKeys
	omitOpKeys := r.OmitKeys || r.OmitOpKeys
	for i, span := range r.LeafSpans {
		if span.OpCount <= 0 {
			return readOnlyPrepareSpanError(i, "has non-positive op count %d", span.OpCount)
		}
		if span.PointOpCount < 0 || span.DeleteRangeCount < 0 || span.ByteCount < 0 {
			return readOnlyPrepareSpanError(i, "has negative point/range/byte counts %d/%d/%d", span.PointOpCount, span.DeleteRangeCount, span.ByteCount)
		}
		pointCount := span.PointOpCount
		rangeCount := span.DeleteRangeCount
		// Preserve compatibility with hand-built point-only test plans that set
		// OpCount and op key bounds but not the derived point counters. Empty keys
		// are valid point keys, so presence is represented by a non-nil slice.
		if pointCount == 0 && rangeCount == 0 && span.FirstOpKey != nil && span.LastOpKey != nil {
			pointCount = span.OpCount
		}
		if pointCount+rangeCount != span.OpCount {
			return readOnlyPrepareSpanError(i, "op count %d does not match point/range counts %d/%d", span.OpCount, span.PointOpCount, span.DeleteRangeCount)
		}
		if pointCount > 0 && !omitOpKeys {
			if span.FirstOpKey == nil {
				return readOnlyPrepareSpanError(i, "has nil first op key")
			}
			if span.LastOpKey == nil {
				return readOnlyPrepareSpanError(i, "has nil last op key")
			}
			if bytes.Compare(span.FirstOpKey, span.LastOpKey) > 0 {
				return readOnlyPrepareSpanError(i, "first op key %s is after last op key %s", readOnlyPrepareKeyForError(span.FirstOpKey), readOnlyPrepareKeyForError(span.LastOpKey))
			}
			if prevLastOp != nil && bytes.Compare(prevLastOp, span.FirstOpKey) >= 0 {
				return readOnlyPrepareSpanError(i, "first op key %s is not after previous last op key %s", readOnlyPrepareKeyForError(span.FirstOpKey), readOnlyPrepareKeyForError(prevLastOp))
			}
		}
		if !omitSpanKeys {
			if span.LowKey != nil && span.HighKey != nil && bytes.Compare(span.LowKey, span.HighKey) >= 0 {
				return readOnlyPrepareSpanError(i, "low key %s is not before high key %s", readOnlyPrepareKeyForError(span.LowKey), readOnlyPrepareKeyForError(span.HighKey))
			}
			if i > 0 && span.LowKey == nil {
				return readOnlyPrepareSpanError(i, "has open low key after earlier span")
			}
			if i > 0 && prevHigh == nil {
				return readOnlyPrepareSpanError(i, "follows previous span with open high key")
			}
			if i > 0 && bytes.Compare(span.LowKey, prevHigh) < 0 {
				return readOnlyPrepareSpanError(i, "low key %s is before previous high key %s", readOnlyPrepareKeyForError(span.LowKey), readOnlyPrepareKeyForError(prevHigh))
			}
			if pointCount > 0 && !omitOpKeys && span.LowKey != nil && bytes.Compare(span.FirstOpKey, span.LowKey) < 0 {
				return readOnlyPrepareSpanError(i, "first op key %s is before low key %s", readOnlyPrepareKeyForError(span.FirstOpKey), readOnlyPrepareKeyForError(span.LowKey))
			}
			if pointCount > 0 && !omitOpKeys && span.HighKey != nil && bytes.Compare(span.LastOpKey, span.HighKey) >= 0 {
				return readOnlyPrepareSpanError(i, "last op key %s is not before high key %s", readOnlyPrepareKeyForError(span.LastOpKey), readOnlyPrepareKeyForError(span.HighKey))
			}
		}
		if pointCount > 0 {
			pointWidth := span.PointOpEnd - span.PointOpStart
			if omitOpKeys {
				if span.PointOpStart < 0 || span.PointOpEnd < 0 {
					return readOnlyPrepareSpanError(i, "point index range [%d,%d) has negative bound", span.PointOpStart, span.PointOpEnd)
				}
				if pointWidth <= 0 {
					return readOnlyPrepareSpanError(i, "point index range [%d,%d) is empty under omitted op-key planning", span.PointOpStart, span.PointOpEnd)
				}
				if pointWidth != pointCount {
					return readOnlyPrepareSpanError(i, "point index range [%d,%d) does not match point count %d", span.PointOpStart, span.PointOpEnd, pointCount)
				}
				expectedStart := 0
				if prevPointEnd >= 0 {
					expectedStart = prevPointEnd
				}
				if span.PointOpStart != expectedStart {
					return readOnlyPrepareSpanError(i, "point index range [%d,%d) starts at %d, want %d under omitted op-key planning", span.PointOpStart, span.PointOpEnd, span.PointOpStart, expectedStart)
				}
				prevPointEnd = span.PointOpEnd
			} else if pointWidth > 0 {
				if pointWidth != pointCount {
					return readOnlyPrepareSpanError(i, "point index range [%d,%d) does not match point count %d", span.PointOpStart, span.PointOpEnd, pointCount)
				}
				if prevPointEnd >= 0 && span.PointOpStart < prevPointEnd {
					return readOnlyPrepareSpanError(i, "point index range [%d,%d) overlaps previous point end %d", span.PointOpStart, span.PointOpEnd, prevPointEnd)
				}
				prevPointEnd = span.PointOpEnd
			}
		}
		if rangeCount > 0 {
			if span.DeleteRangeEnd <= span.DeleteRangeStart {
				return readOnlyPrepareSpanError(i, "delete range index range [%d,%d) is empty", span.DeleteRangeStart, span.DeleteRangeEnd)
			}
			if span.DeleteRangeEnd-span.DeleteRangeStart != rangeCount {
				return readOnlyPrepareSpanError(i, "delete range index range [%d,%d) does not match range count %d", span.DeleteRangeStart, span.DeleteRangeEnd, rangeCount)
			}
			if prevRangeStart > span.DeleteRangeStart {
				return readOnlyPrepareSpanError(i, "delete range start index %d is before previous start %d", span.DeleteRangeStart, prevRangeStart)
			}
			prevRangeStart = span.DeleteRangeStart
		}
		totalPointOps += pointCount
		totalSpanOps += span.OpCount
		if !omitOpKeys {
			if pointCount > 0 {
				prevLastOp = span.LastOpKey
			}
		}
		if !omitSpanKeys {
			prevHigh = span.HighKey
		}
	}
	if omitOpKeys && totalPointOps > 0 && prevPointEnd != r.PointOps {
		return fmt.Errorf("zipper: read-only omitted op-key leaf spans end at point op %d, want %d", prevPointEnd, r.PointOps)
	}
	if r.PointOps > 0 {
		if totalPointOps != r.PointOps {
			return fmt.Errorf("zipper: read-only leaf spans cover %d point ops, want %d", totalPointOps, r.PointOps)
		}
	} else if r.DeleteRanges == 0 && totalSpanOps != r.Ops {
		return fmt.Errorf("zipper: read-only leaf spans cover %d ops, want %d", totalSpanOps, r.Ops)
	}
	if r.DeleteRanges > 0 && totalSpanOps < r.DeleteRanges {
		return fmt.Errorf("zipper: read-only leaf spans cover %d span ops, fewer than %d delete ranges", totalSpanOps, r.DeleteRanges)
	}
	return nil
}

func readOnlyPrepareSpanError(spanIdx int, format string, args ...any) error {
	args = append([]any{spanIdx}, args...)
	return fmt.Errorf("zipper: read-only leaf span %d "+format, args...)
}

func readOnlyPrepareKeyForError(key []byte) string {
	const maxPrefix = 8
	if key == nil {
		return "nil"
	}
	if len(key) <= maxPrefix {
		return fmt.Sprintf("len=%d hex=%x", len(key), key)
	}
	return fmt.Sprintf("len=%d hex_prefix=%x", len(key), key[:maxPrefix])
}

func (r *ReadOnlyPrepareResult) cloneKey(src []byte) []byte {
	if r != nil && r.OmitKeys {
		return nil
	}
	if src == nil {
		return nil
	}
	if len(src) == 0 {
		return []byte{}
	}
	start := len(r.keyArena)
	r.keyArena = append(r.keyArena, src...)
	return r.keyArena[start : start+len(src)]
}

func (r *ReadOnlyPrepareResult) cloneOpKey(src []byte) []byte {
	if r != nil && (r.OmitKeys || r.OmitOpKeys) {
		return nil
	}
	return r.cloneKey(src)
}

func readOnlyPrepareEntryBytes(op batch.Entry) int {
	n := len(op.Key)
	if op.IsPtr {
		n += page.ValuePtrSize
	} else {
		n += len(op.Value)
	}
	return n
}

func readOnlyPrepareRangeBytes(r batch.DeleteRange) int {
	return len(r.Start) + len(r.End)
}

func (r *ReadOnlyPrepareResult) ensureInitialCapacity(ops []batch.Entry, ranges []batch.DeleteRange) {
	if r == nil {
		return
	}
	if !r.discardLeafSpans {
		spanHint := len(ops) + len(ranges)
		leafSpanKeepCap := readOnlyPrepareResultReuseLeafSpanKeepCap
		if r.OmitKeys {
			leafSpanKeepCap = readOnlyPrepareResultReuseOmitKeysLeafSpanKeepCap
		}
		if spanHint > leafSpanKeepCap {
			spanHint = leafSpanKeepCap
		}
		if spanHint > 0 && cap(r.LeafSpans) < spanHint {
			r.LeafSpans = make([]ReadOnlyLeafSpan, 0, spanHint)
		}
	}
	if !r.OmitKeys {
		keyHintUnits := len(ops) + len(ranges)
		if r.OmitOpKeys {
			keyHintUnits = len(ranges)
			if len(ops) > 0 {
				// Boundary copies scale with touched leaf/internal span count rather
				// than point-op count. Keep the hint below the full op-key path while
				// avoiding repeated arena growth for sparse many-leaf plans.
				keyHintUnits += len(ops)/16 + 2
			}
		}
		keyHint := 0
		if keyHintUnits > 0 {
			if keyHintUnits > readOnlyPrepareResultReuseKeyArenaKeepCap/32 {
				keyHint = readOnlyPrepareResultReuseKeyArenaKeepCap
			} else {
				keyHint = keyHintUnits * 32
			}
		}
		if keyHint > 0 && cap(r.keyArena) < keyHint {
			r.keyArena = make([]byte, 0, keyHint)
		}
	}
}

func (r *ReadOnlyPrepareResult) addLeafSpan(ref page.ChildRef, low, high []byte, ops []batch.Entry, opBase int, ranges []batch.DeleteRange, rangeBase int) {
	if len(ops) == 0 && len(ranges) == 0 {
		return
	}
	span := ReadOnlyLeafSpan{
		Ref:              ref,
		LowKey:           r.cloneKey(low),
		HighKey:          r.cloneKey(high),
		PointOpCount:     len(ops),
		DeleteRangeCount: len(ranges),
		OpCount:          len(ops) + len(ranges),
		PointOpStart:     opBase,
		PointOpEnd:       opBase + len(ops),
		DeleteRangeStart: rangeBase,
		DeleteRangeEnd:   rangeBase + len(ranges),
	}
	if len(ops) > 0 {
		span.FirstOpKey = r.cloneOpKey(ops[0].Key)
		span.LastOpKey = r.cloneOpKey(ops[len(ops)-1].Key)
		for i := range ops {
			span.ByteCount += readOnlyPrepareEntryBytes(ops[i])
		}
	}
	for i := range ranges {
		span.ByteCount += readOnlyPrepareRangeBytes(ranges[i])
	}
	if r.leafSpanCallback != nil {
		r.leafSpanCallback(span)
	}
	if !r.discardLeafSpans {
		r.LeafSpans = append(r.LeafSpans, span)
	}
}

func elapsedNsSince(start time.Time) uint64 {
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return 1
	}
	if ns := elapsed.Nanoseconds(); ns > 0 {
		return uint64(ns)
	}
	return 1
}

func resolveParallelApplyWorkers(opts ApplyOptions, summary ReadOnlyLeafSpanSummary) int {
	workers := opts.ParallelApplyConcurrency
	if workers <= 1 {
		return 0
	}
	if gomax := runtime.GOMAXPROCS(0); gomax > 0 && workers > gomax {
		workers = gomax
	}
	if summary.Spans <= 1 {
		return 0
	}
	if workers > summary.Spans {
		workers = summary.Spans
	}
	if opts.ParallelApplyMinSpans > 0 && summary.Spans < opts.ParallelApplyMinSpans {
		return 0
	}
	if opts.ParallelApplyMinSpanOps > 0 && summary.SpanOps < opts.ParallelApplyMinSpanOps {
		return 0
	}
	if opts.ParallelApplyMinSpanBytes > 0 && summary.SpanBytes < opts.ParallelApplyMinSpanBytes {
		return 0
	}
	if workers <= 1 {
		return 0
	}
	return workers
}

func resolveSpanNativeApplyWorkers(opts ApplyOptions, summary ReadOnlyLeafSpanSummary) int {
	if !opts.SpanNativeApply {
		return 0
	}
	workers := opts.ParallelApplyConcurrency
	if workers <= 0 {
		workers = 1
	}
	if gomax := runtime.GOMAXPROCS(0); gomax > 0 && workers > gomax {
		workers = gomax
	}
	if summary.Spans > 0 && workers > summary.Spans {
		workers = summary.Spans
	}
	if workers <= 0 {
		workers = 1
	}
	return workers
}

func spanNativeApplyFallbackReason(opts ApplyOptions, summary ReadOnlyLeafSpanSummary, err error, validationFailure bool) (int, string) {
	if !opts.SpanNativeApply {
		return 0, "disabled"
	}
	if opts.SpanNativeForceFallbackReason != "" {
		return 0, opts.SpanNativeForceFallbackReason
	}
	if validationFailure {
		return 0, "validation_failed"
	}
	if err != nil {
		return 0, "prepare_error"
	}
	if summary.ColdBuild {
		return 0, "cold_build"
	}
	allowMaintenancePointOps := opts.SpanNativeAllowMaintenancePointOps && summary.PointOps > 0 && summary.DeleteRanges == 0
	if summary.Maintenance && !allowMaintenancePointOps {
		return 0, "maintenance"
	}
	if summary.DeleteRanges > 0 {
		return 0, "range_delete_barrier"
	}
	if !summary.ExactLeafSpans {
		return 0, "inexact_leaf_spans"
	}
	if summary.PointOps <= 0 || summary.Spans <= 0 {
		return 0, "below_threshold"
	}
	workers := resolveSpanNativeApplyWorkers(opts, summary)
	if workers <= 0 {
		return 0, "below_threshold"
	}
	return workers, ""
}

func spanNativeApplyExecutionFallbackReason(err error) string {
	if errors.Is(err, ErrSpanNativeOutputOwnership) {
		return "output_ownership_failure"
	}
	if errors.Is(err, ErrSpanNativeReducerValidation) {
		return "reducer_validation_failed"
	}
	return "unknown"
}

func readOnlyWorkerTarget(opts ApplyOptions) int {
	if opts.ReadOnlyPrepareWorkers > 0 {
		return opts.ReadOnlyPrepareWorkers
	}
	if opts.ParallelApplyConcurrency > 0 {
		return opts.ParallelApplyConcurrency
	}
	if opts.SpanNativeApply {
		return 1
	}
	return 0
}

// ApplyWithOptions applies the batch to the tree rooted at rootID and returns a
// result object suitable for guarded install paths. When opts.PrepareReadOnly is
// true, or when opt-in parallel apply needs span gating, it first runs
// PrepareReadOnly and validates the plan before applying the delta. If read-only
// preparation or validation fails, no root apply runs and no durable output
// ownership is produced.
func (z *Zipper) ApplyWithOptions(rootID uint64, b *batch.Batch, opts ApplyOptions) (ApplyResult, error) {
	var prepared ReadOnlyPrepareResult
	var preparedNs uint64
	result := ApplyResult{}
	prepareRequested := opts.PrepareReadOnly || opts.ParallelApplyConcurrency > 1 || opts.SpanNativeApply
	if prepareRequested {
		result.ReadOnlyPrepareRequested = true
		var err error
		if opts.SpanNativeApply {
			// Span-native apply consumes exact leaf span boundaries to distinguish
			// whole-root from sparse/partial replacement sets. OmitKeys plans use nil
			// bounds for point-only spans and are planning-only, not execution-safe,
			// but the execution engine uses canonical point index ranges rather than
			// duplicated FirstOpKey/LastOpKey metadata.
			opts.ReadOnlyPrepare.OmitKeys = false
			opts.ReadOnlyPrepare.OmitOpKeys = true
			if opts.SpanNativeAllowMaintenancePointOps {
				opts.ReadOnlyPrepare.AllowMaintenancePointLeafSpans = true
			}
		}
		prepareStart := time.Now()
		prepared, err = z.PrepareReadOnly(rootID, b, opts.ReadOnlyPrepare)
		preparedNs = elapsedNsSince(prepareStart)
		result.ReadOnlyPrepare = prepared
		result.ReadOnlyPrepareNs = preparedNs
		workerTarget := readOnlyWorkerTarget(opts)
		result.ReadOnlyPrepareWorkerSummary = prepared.LeafSpanWorkerRangeSummary(workerTarget)
		if err != nil {
			result.ReadOnlyPrepareFailed = true
			return result, err
		}
		if err := prepared.ValidateLeafSpans(); err != nil {
			result.ReadOnlyPrepareValidationFailed = true
			return result, fmt.Errorf("zipper: invalid read-only prepare plan: %w", err)
		}
	}

	if workers, reason := spanNativeApplyFallbackReason(opts, prepared.LeafSpanSummary(), nil, false); reason == "" {
		result.SpanNativeEligible = true
		result.SpanNativeWorkers = workers
		var spanOps []batch.Entry
		if b != nil && b.HasDeleteRanges() {
			spanOps, _ = b.ApplyPlan()
		} else if b != nil {
			spanOps = b.SortedEntries()
		}
		spanResult, used, spanErr := z.applySpanNativeWithPrepared(rootID, spanOps, prepared, opts, workers, opts.ParallelApplyWorkerPool)
		if used {
			spanResult.ReadOnlyPrepare = prepared
			spanResult.ReadOnlyPrepareNs = preparedNs
			spanResult.ReadOnlyPrepareRequested = result.ReadOnlyPrepareRequested
			spanResult.ReadOnlyPrepareWorkerSummary = result.ReadOnlyPrepareWorkerSummary
			spanResult.SpanNativeEligible = true
			spanResult.SpanNativeWorkers = workers
			if spanErr != nil && spanResult.SpanNativeFallbackReason == "" {
				spanResult.SpanNativeFallbackReason = spanNativeApplyExecutionFallbackReason(spanErr)
			}
			return spanResult, spanErr
		}
	} else if opts.SpanNativeApply {
		result.SpanNativeFallbackReason = reason
	}

	applyCfg := applyRunConfig{}
	var oldPointerRefs PointerRefCounts
	var oldEntriesRemoved uint64
	if opts.CollectOldPointerRefs {
		applyCfg.oldPointerRefs = &oldPointerRefs
		applyCfg.oldEntriesRemoved = &oldEntriesRemoved
	}
	if opts.ParallelApplyConcurrency > 1 && prepared.DeleteRanges == 0 {
		workers := resolveParallelApplyWorkers(opts, prepared.LeafSpanSummary())
		if workers > 1 {
			applyCfg.maxParallelWorkers = workers
			applyCfg.workerPool = opts.ParallelApplyWorkerPool
			result.ParallelApplyWorkers = workers
			result.ParallelApplyUsed = true
		}
	}
	newRoot, retired, metrics, err := z.applyWithConfig(rootID, b, applyCfg)
	result.RootID = newRoot
	result.PendingRetiredPages = retired
	result.Metrics = metrics
	if err == nil && opts.CollectOldPointerRefs {
		result.OldPointerRefs = oldPointerRefs
		result.OldEntriesRemoved = oldEntriesRemoved
		result.OldPointerRefsCollected = true
	}
	result.ReadOnlyPrepare = prepared
	result.ReadOnlyPrepareNs = preparedNs
	if result.ReadOnlyPrepareWorkerSummary.TargetWorkers == 0 && prepareRequested {
		result.ReadOnlyPrepareWorkerSummary = prepared.LeafSpanWorkerRangeSummary(readOnlyWorkerTarget(opts))
	}
	return result, err
}

// PrepareReadOnly discovers the existing leaf spans touched by b without
// allocating or writing pager/leaf-log output. It is the safe preparation phase
// that future prepared-output paths can run before the final install section.
func (z *Zipper) PrepareReadOnly(rootID uint64, b *batch.Batch, opts ReadOnlyPrepareOptions) (ReadOnlyPrepareResult, error) {
	if b == nil {
		return ReadOnlyPrepareResult{OmitKeys: opts.OmitKeys, OmitOpKeys: opts.OmitOpKeys || opts.OmitKeys, LeafSpans: opts.leafSpans[:0], keyArena: opts.keyArena[:0]}, errors.New("zipper: nil batch")
	}
	var ops []batch.Entry
	var ranges []batch.DeleteRange
	if b.HasDeleteRanges() {
		ops, ranges = b.ApplyPlan()
	} else {
		ops = b.SortedEntries()
	}
	return z.PrepareReadOnlyPlan(rootID, ops, ranges, opts)
}

// PrepareReadOnlyPlan discovers the existing leaf spans touched by an already
// canonical sorted apply plan without first materializing a batch. Point ops must
// be sorted in apply order, and ranges must be the canonical merged range slice
// that batch.ApplyPlan would have produced. It is side-effect-free like
// PrepareReadOnly and exists for hot planning paths that already own canonical
// op slices.
func (z *Zipper) PrepareReadOnlyPlan(rootID uint64, ops []batch.Entry, ranges []batch.DeleteRange, opts ReadOnlyPrepareOptions) (ReadOnlyPrepareResult, error) {
	result := ReadOnlyPrepareResult{
		OmitKeys:         opts.OmitKeys,
		OmitOpKeys:       opts.OmitOpKeys || opts.OmitKeys,
		LeafSpans:        opts.leafSpans[:0],
		keyArena:         opts.keyArena[:0],
		discardLeafSpans: opts.DiscardLeafSpans,
		leafSpanCallback: opts.LeafSpanCallback,
	}
	if len(ranges) > 0 {
		// OmitKeys is only safe for point-only planning. Delete-range overlap
		// partitioning relies on exact leaf span bounds, so retain keys and report
		// the effective mode in the result.
		result.OmitKeys = false
		result.OmitOpKeys = false
	}
	result.RootID = rootID
	result.PointOps = len(ops)
	result.DeleteRanges = len(ranges)
	result.Ops = len(ops) + len(ranges)
	if result.Ops == 0 {
		result.ExactLeafSpans = true
		return result, nil
	}
	result.ensureInitialCapacity(ops, ranges)
	maintenance, _ := z.shouldRunMaintenance(ops)
	if len(ranges) > 0 {
		maintenance = false
	}
	result.Maintenance = maintenance
	result.ExactLeafSpans = !maintenance || opts.AllowMaintenancePointLeafSpans
	if rootID == 0 {
		result.ColdBuild = true
		result.ExactLeafSpans = true
		result.addLeafSpan(page.ChildRef{}, nil, nil, ops, 0, ranges, 0)
		return result, nil
	}

	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)
	err := z.prepareReadOnlyRecursive(page.PageChildRef(rootID), ops, 0, ranges, 0, nil, nil, &result, scratch)
	return result, err
}

func (z *Zipper) prepareReadOnlyRecursive(ref page.ChildRef, ops []batch.Entry, opBase int, ranges []batch.DeleteRange, rangeBase int, low, high []byte, result *ReadOnlyPrepareResult, scratch *mergeScratch) error {
	if ref.Kind == page.ChildRefLeafLog {
		// Leaf-log child refs are only emitted for outer leaf pages. The read-only
		// prepare pass needs the leaf boundary carried by the parent internal page,
		// not the leaf body itself, so avoid decompressing the persistent leaf-log
		// record here. The subsequent apply pass still validates and reads the leaf
		// before producing durable output.
		result.addLeafSpan(ref, low, high, ops, opBase, ranges, rangeBase)
		return nil
	}
	oldNode, _, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(ref, scratch)
	if err != nil {
		return err
	}
	recordZipperNodeLoad(&result.Metrics, ref, oldNode, loadSource)
	if leafScratchRef {
		defer releaseLeafPageScratch(scratch, leafScratch)
	}

	switch oldNode.Type() {
	case page.PageTypeLeaf, 0:
		result.addLeafSpan(ref, low, high, ops, opBase, ranges, rangeBase)
		return nil
	case page.PageTypeInternal:
		count := oldNode.Count()
		baseDeltaInternalKeys := oldNode.InternalBaseDeltaEnabled()
		opIdx := 0
		for i := uint16(0); i < count; i++ {
			key, childRef, err := oldNode.GetInternalEntryRefView(i)
			if err != nil {
				return err
			}
			if key == nil {
				key = []byte{}
			}
			keyNonEmpty := len(key) != 0
			firstOpBeforeKey := false
			if keyNonEmpty && opIdx < len(ops) && bytes.Compare(ops[opIdx].Key, key) < 0 {
				firstOpBeforeKey = true
			}

			var endKey []byte
			if i+1 < count {
				nextKey, _, err := oldNode.GetInternalEntryRefView(i + 1)
				if err != nil {
					return err
				}
				if nextKey == nil {
					nextKey = []byte{}
				}
				endKey = nextKey
			}

			startOpIdx := opIdx
			for opIdx < len(ops) {
				if endKey == nil || bytes.Compare(ops[opIdx].Key, endKey) < 0 {
					opIdx++
					continue
				}
				break
			}
			childOps := ops[startOpIdx:opIdx]

			childLow := low
			childLowFromSeparator := false
			if keyNonEmpty {
				if len(childOps) > 0 && firstOpBeforeKey {
					childLow = childOps[0].Key
				} else {
					childLow = key
					childLowFromSeparator = true
				}
			}
			childHigh := high
			childHighFromSeparator := false
			if endKey != nil {
				childHigh = endKey
				childHighFromSeparator = true
			}

			rangeStart, rangeEnd := 0, 0
			if len(ranges) > 0 {
				childLow, childHigh, childLowFromSeparator, childHighFromSeparator, err = stabilizeReadOnlyChildSeparatorBounds(oldNode, i, childLow, childHigh, childLowFromSeparator, childHighFromSeparator, baseDeltaInternalKeys, result)
				if err != nil {
					return err
				}
				rangeStart, rangeEnd = deleteRangeIndexSpan(ranges, childLow, childHigh)
			}
			if len(childOps) == 0 && rangeStart == rangeEnd {
				continue
			}

			childLow, childHigh, _, _, err = stabilizeReadOnlyChildSeparatorBounds(oldNode, i, childLow, childHigh, childLowFromSeparator, childHighFromSeparator, baseDeltaInternalKeys, result)
			if err != nil {
				return err
			}
			if err := z.prepareReadOnlyRecursive(childRef, childOps, opBase+startOpIdx, ranges[rangeStart:rangeEnd], rangeBase+rangeStart, childLow, childHigh, result, scratch); err != nil {
				return err
			}
		}
		return nil
	default:
		return page.ErrInvalidPageType
	}
}

func stabilizeReadOnlyChildSeparatorBounds(oldNode node.Node, childIndex uint16, childLow, childHigh []byte, childLowFromSeparator, childHighFromSeparator, baseDeltaInternalKeys bool, result *ReadOnlyPrepareResult) ([]byte, []byte, bool, bool, error) {
	if childLowFromSeparator {
		if baseDeltaInternalKeys {
			if childHighFromSeparator {
				// Base-delta internal keys can share node scratch; keep the
				// already-decoded high separator stable before refetching low.
				childHigh = result.cloneKey(childHigh)
				childHighFromSeparator = false
			}
			key, _, err := oldNode.GetInternalEntryRefView(childIndex)
			if err != nil {
				return nil, nil, false, false, err
			}
			if key == nil {
				key = []byte{}
			}
			childLow = key
		}
		childLow = result.cloneKey(childLow)
		childLowFromSeparator = false
	}
	if childHighFromSeparator {
		childHigh = result.cloneKey(childHigh)
		childHighFromSeparator = false
	}
	return childLow, childHigh, childLowFromSeparator, childHighFromSeparator, nil
}

func deleteRangeIndexSpan(ranges []batch.DeleteRange, low, high []byte) (int, int) {
	if len(ranges) == 0 {
		return 0, 0
	}
	start := -1
	end := -1
	for i, r := range ranges {
		if batch.DeleteRangeOverlapsSpan(r, low, high) {
			if start < 0 {
				start = i
			}
			end = i + 1
			continue
		}
		if start >= 0 {
			break
		}
		if high != nil && r.Start != nil && bytes.Compare(r.Start, high) >= 0 {
			break
		}
	}
	if start < 0 {
		return 0, 0
	}
	return start, end
}
