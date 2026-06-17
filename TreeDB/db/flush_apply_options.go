package db

import (
	"github.com/snissn/gomap/TreeDB/zipper"
)

func (db *DB) flushApplyOptions() zipper.ApplyOptions {
	if db == nil {
		return zipper.ApplyOptions{}
	}
	if db.flushApplyConcurrency <= 1 && !db.flushApplySpanNative {
		return zipper.ApplyOptions{}
	}
	return zipper.ApplyOptions{
		PrepareReadOnly:           db.flushApplyConcurrency > 1 || db.flushApplySpanNative,
		ReadOnlyPrepareWorkers:    db.flushApplyConcurrency,
		ParallelApplyConcurrency:  db.flushApplyConcurrency,
		ParallelApplyWorkerPool:   db.flushApplyWorkerPool,
		ParallelApplyMinSpans:     db.flushApplyMinSpans,
		ParallelApplyMinSpanOps:   db.flushApplyMinEntries,
		ParallelApplyMinSpanBytes: db.flushApplyMinBytes,
		SpanNativeApply:           db.flushApplySpanNative,
	}
}

func flushApplyUseOptions(opts zipper.ApplyOptions) bool {
	return opts.ParallelApplyConcurrency > 1 || opts.SpanNativeApply || opts.PrepareReadOnly
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

func parseFlushApplySpanNativeFallbackReason(raw string) (FlushSpanRunFallbackReason, bool) {
	if raw == "" {
		return FlushSpanRunFallbackUnknown, false
	}
	if reason, ok := ParseFlushSpanRunFallbackReason(raw); ok {
		return reason, true
	}
	return FlushSpanRunFallbackUnknown, true
}

func (db *DB) observeFlushApplySpanNativePublishFallback(result zipper.ApplyResult, reason FlushSpanRunFallbackReason) {
	if db == nil || !reason.Valid() || reason == FlushSpanRunFallbackUnknown {
		return
	}
	if !result.ReadOnlyPrepareRequested || (!result.SpanNativeEligible && !result.SpanNativeUsed) {
		return
	}
	db.observeFlushApplySpanNativeFallbackAfterCandidate(result.ReadOnlyPrepare.LeafSpanSummary(), reason, false)
}
