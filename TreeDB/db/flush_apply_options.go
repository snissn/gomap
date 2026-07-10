package db

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/zipper"
)

func (db *DB) flushApplyOptions() zipper.ApplyOptions {
	if db == nil {
		return zipper.ApplyOptions{}
	}
	if db.flushApplyConcurrency <= 1 && !db.flushApplySpanNative {
		return zipper.ApplyOptions{}
	}
	opts := zipper.ApplyOptions{
		PrepareReadOnly:           db.flushApplyConcurrency > 1 || db.flushApplySpanNative,
		ReadOnlyPrepareWorkers:    db.flushApplyConcurrency,
		ParallelApplyConcurrency:  db.flushApplyConcurrency,
		ParallelApplyWorkerPool:   db.flushApplyWorkerPool,
		ParallelApplyMinSpans:     db.flushApplyMinSpans,
		ParallelApplyMinSpanOps:   db.flushApplyMinEntries,
		ParallelApplyMinSpanBytes: db.flushApplyMinBytes,
		SpanNativeApply:           db.flushApplySpanNative,
	}
	if opts.SpanNativeApply && db.flushApplySpanNativeReducerValidationGuard.Load() {
		opts.SpanNativeForceFallbackReason = FlushSpanRunFallbackReducerValidationGuard.String()
	}
	return opts
}

func flushApplyUseOptions(opts zipper.ApplyOptions) bool {
	return opts.ParallelApplyConcurrency > 1 || opts.SpanNativeApply || opts.PrepareReadOnly || opts.CollectOldPointerRefs
}

func (db *DB) observeFlushApplyPrepareResult(result zipper.ApplyResult, err error) {
	if db == nil || !result.ReadOnlyPrepareRequested {
		return
	}
	summary := result.ReadOnlyPrepare.LeafSpanSummary()
	workerSummary := result.ReadOnlyPrepareWorkerSummary
	if workerSummary.TargetWorkers == 0 && db.flushApplyConcurrency > 1 {
		workerSummary = result.ReadOnlyPrepare.LeafSpanWorkerRangeSummary(db.flushApplyConcurrency)
	}
	db.observeFlushApplyReadOnlyPrepare(summary, workerSummary, result.ReadOnlyPrepareNs, err, result.ReadOnlyPrepareValidationFailed)
	explicitReason, hasExplicitReason := parseFlushApplySpanNativeFallbackReason(result.SpanNativeFallbackReason)
	db.maybeTripFlushApplySpanNativeReducerValidationGuard(explicitReason, hasExplicitReason, err)
	if result.SpanNativeEligible {
		db.observeFlushApplySpanNativeEligible(summary)
		if result.SpanNativeUsed {
			db.observeFlushApplySpanNativeUsed(summary)
		}
		if hasExplicitReason {
			db.observeFlushApplySpanNativeFallbackAfterCandidate(summary, explicitReason, false)
		} else if !result.SpanNativeUsed {
			db.observeFlushApplySpanNativeFallbackAfterCandidate(summary, FlushSpanRunFallbackSpanNativeNotImplemented, false)
		}
		return
	}
	if hasExplicitReason {
		db.observeFlushApplySpanNativeFallback(summary, explicitReason)
		return
	}
	db.observeFlushApplySpanNativeFallback(summary, classifyFlushApplySpanNativeFallback(summary, err, result.ReadOnlyPrepareValidationFailed))
}

func (db *DB) maybeTripFlushApplySpanNativeReducerValidationGuard(reason FlushSpanRunFallbackReason, hasReason bool, err error) {
	if db == nil || err == nil {
		return
	}
	if !errors.Is(err, zipper.ErrSpanNativeReducerValidation) && (!hasReason || reason != FlushSpanRunFallbackReducerValidationFailed) {
		return
	}
	if db.flushApplySpanNativeReducerValidationGuard.CompareAndSwap(false, true) {
		db.flushApplySpanNativeReducerValidationGuardTrips.Add(1)
	}
}

type flushApplySpanNativePublishSnapshot struct {
	summary                  zipper.ReadOnlyLeafSpanSummary
	readOnlyPrepareRequested bool
	spanNativeEligible       bool
	spanNativeUsed           bool
}

func newFlushApplySpanNativePublishSnapshot(result zipper.ApplyResult) flushApplySpanNativePublishSnapshot {
	return flushApplySpanNativePublishSnapshot{
		summary:                  result.ReadOnlyPrepare.LeafSpanSummary(),
		readOnlyPrepareRequested: result.ReadOnlyPrepareRequested,
		spanNativeEligible:       result.SpanNativeEligible,
		spanNativeUsed:           result.SpanNativeUsed,
	}
}

func (s flushApplySpanNativePublishSnapshot) preparedSpanNativeCandidate() bool {
	return s.readOnlyPrepareRequested && (s.spanNativeEligible || s.spanNativeUsed)
}

func parseFlushApplySpanNativeFallbackReason(raw string) (FlushSpanRunFallbackReason, bool) {
	if raw == "" {
		return FlushSpanRunFallbackUnknown, false
	}
	if reason, ok := ParseFlushSpanRunFallbackReason(raw); ok {
		return reason, true
	}
	return FlushSpanRunFallbackUnknown, true
}

func (db *DB) observeFlushApplySpanNativePublishFallback(snapshot flushApplySpanNativePublishSnapshot, reason FlushSpanRunFallbackReason) {
	if db == nil || !reason.Valid() || reason == FlushSpanRunFallbackUnknown {
		return
	}
	if !snapshot.preparedSpanNativeCandidate() {
		return
	}
	db.observeFlushApplySpanNativeFallbackAfterCandidate(snapshot.summary, reason, false)
}
