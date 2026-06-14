package db

import (
	"github.com/snissn/gomap/TreeDB/zipper"
)

func (db *DB) flushApplyOptions() zipper.ApplyOptions {
	if db == nil || db.flushApplyConcurrency <= 1 {
		return zipper.ApplyOptions{}
	}
	return zipper.ApplyOptions{
		PrepareReadOnly:           true,
		ReadOnlyPrepareWorkers:    db.flushApplyConcurrency,
		ParallelApplyConcurrency:  db.flushApplyConcurrency,
		ParallelApplyWorkerPool:   db.flushApplyWorkerPool,
		ParallelApplyMinSpans:     db.flushApplyMinSpans,
		ParallelApplyMinSpanOps:   db.flushApplyMinEntries,
		ParallelApplyMinSpanBytes: db.flushApplyMinBytes,
	}
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
}
