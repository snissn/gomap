package db

import (
	"fmt"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/zipper"
)

// ReadOnlyApplyPlanOptions configures an opt-in read-only flush/apply planning
// pass. The pass captures the current root through a DB snapshot, traverses the
// tree without allocating durable output, validates the resulting leaf-span
// plan, and optionally constructs deterministic worker span ranges. It does not
// publish roots, retire pages, append value-log/leaf-log output, or transfer
// ownership of prepared output.
type ReadOnlyApplyPlanOptions struct {
	// Workers is the requested future worker count for deterministic span range
	// construction. Values <=0 skip worker-range construction while still
	// validating the leaf-span plan.
	Workers int

	// Zipper reuses buffers for the zipper read-only prepare pass.
	Zipper zipper.ReadOnlyPrepareOptions
}

// ReadOnlyApplyPlan is the validated, side-effect-free planning output for the
// captured root. WorkerRanges is empty when ReadOnlyApplyPlanOptions.Workers <=0
// or when the prepare result has no spans.
type ReadOnlyApplyPlan struct {
	Prepare      zipper.ReadOnlyPrepareResult
	WorkerRanges []zipper.ReadOnlyLeafSpanWorkerRange
	PrepareNs    uint64
}

func elapsedReadOnlyPrepareNs(start time.Time) uint64 {
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return 0
	}
	return uint64(elapsed.Nanoseconds())
}

// PrepareReadOnlyApplyPlan runs the reusable read-only prepare/leaf-span
// planning contract against the current user root. It is default-off and is
// intended for tests, benchmark evidence, and future cached flush/ordered-root
// apply integrations.
func (db *DB) PrepareReadOnlyApplyPlan(b *Batch, opts ReadOnlyApplyPlanOptions) (ReadOnlyApplyPlan, error) {
	var out ReadOnlyApplyPlan
	if db == nil {
		return out, ErrClosed
	}
	if b == nil || b.batch == nil {
		return out, fmt.Errorf("treedb: nil read-only apply batch")
	}
	var ops []batch.Entry
	var ranges []batch.DeleteRange
	if b.batch.HasDeleteRanges() {
		ops, ranges = b.batch.ApplyPlan()
	} else {
		ops = b.batch.SortedEntries()
	}
	return db.prepareReadOnlyApplyPlanFromOps(ops, ranges, opts)
}

func (db *DB) prepareReadOnlyApplyPlanFromOps(ops []batch.Entry, ranges []batch.DeleteRange, opts ReadOnlyApplyPlanOptions) (ReadOnlyApplyPlan, error) {
	var out ReadOnlyApplyPlan
	snap := db.AcquireSnapshot()
	if snap == nil || snap.idx == nil || snap.state == nil {
		if snap != nil {
			_ = snap.Close()
		}
		return out, ErrClosed
	}
	defer func() { _ = snap.Close() }()

	prepareStart := time.Now()
	prepared, err := snap.idx.zipper.PrepareReadOnlyPlan(snap.state.RootPageID, ops, ranges, opts.Zipper)
	out.PrepareNs = elapsedReadOnlyPrepareNs(prepareStart)
	out.Prepare = prepared
	summary := prepared.LeafSpanSummary()
	workerSummary := prepared.LeafSpanWorkerRangeSummary(opts.Workers)
	if err != nil {
		db.observeFlushApplyReadOnlyPrepare(summary, workerSummary, out.PrepareNs, err, false)
		return out, err
	}
	if validationErr := prepared.ValidateLeafSpans(); validationErr != nil {
		err = fmt.Errorf("treedb: invalid read-only apply plan: %w", validationErr)
		db.observeFlushApplyReadOnlyPrepare(summary, workerSummary, out.PrepareNs, err, true)
		return out, err
	}
	if opts.Workers > 0 {
		out.WorkerRanges = prepared.AppendLeafSpanWorkerRanges(nil, opts.Workers)
		workerSummary = prepared.LeafSpanWorkerRangeSummary(opts.Workers)
	}
	db.observeFlushApplyReadOnlyPrepare(summary, workerSummary, out.PrepareNs, nil, false)
	return out, nil
}
