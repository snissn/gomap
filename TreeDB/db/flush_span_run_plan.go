package db

import (
	"fmt"

	"github.com/snissn/gomap/TreeDB/zipper"
)

// PlanFlushSpanRun plans exact target-leaf spans for an already-canonical flush
// run. It is side-effect-free: it captures the current root, runs the read-only
// prepare pass against the supplied point/range operations, and returns M8/M9
// span-run metadata for cache-layer chunk planning and future span-native jobs.
func (db *DB) PlanFlushSpanRun(req FlushSpanRunPlanRequest) (FlushSpanRunMetadata, error) {
	meta := FlushSpanRunMetadata{
		RunID:            req.RunID,
		SourceMemtables:  req.SourceMemtables,
		SourcePointOps:   req.SourcePointOps,
		PlannedPointOps:  req.PlannedPointOps,
		ShadowedPointOps: req.ShadowedPointOps,
		RangeBarriers:    req.RangeBarriers,
		LaneBarriers:     req.LaneBarriers,
	}
	if db == nil {
		return meta, ErrClosed
	}
	if req.PlannedPointOps != len(req.PointOps) {
		return meta, fmt.Errorf("treedb: flush span run planned point ops=%d but point slice has %d", req.PlannedPointOps, len(req.PointOps))
	}
	if req.SourcePointOps != req.PlannedPointOps+req.ShadowedPointOps {
		return meta, fmt.Errorf("treedb: flush span run source point ops=%d must equal planned=%d plus shadowed=%d", req.SourcePointOps, req.PlannedPointOps, req.ShadowedPointOps)
	}

	reserve := len(req.PointOps) + len(req.DeleteRanges)
	bif := db.newBatchWithReserveHint(reserve)
	b, ok := bif.(*Batch)
	if !ok || b == nil || b.batch == nil {
		if bif != nil {
			_ = bif.Close()
		}
		return meta, fmt.Errorf("treedb: flush span run planner could not create backend batch")
	}
	defer func() { _ = b.Close() }()

	if len(req.PointOps) > 0 {
		if err := b.batch.SetOps(req.PointOps); err != nil {
			return meta, err
		}
	}
	for i := range req.DeleteRanges {
		r := req.DeleteRanges[i]
		if err := b.batch.DeleteRange(r.Start, r.End); err != nil {
			return meta, err
		}
	}

	plan, err := db.PrepareReadOnlyApplyPlan(b, ReadOnlyApplyPlanOptions{Workers: db.flushApplyConcurrency})
	prepared := plan.Prepare
	if prepared.RootID != 0 {
		meta.BaseRoot.CapturedRootID = prepared.RootID
		meta.BaseRoot.CurrentRootID = prepared.RootID
		meta.BaseRoot.Matched = true
	}
	if err != nil {
		return meta, err
	}
	meta.TargetLeafSpans = appendFlushSpanRunTargetLeafSpans(nil, prepared.LeafSpans)
	if err := ValidateFlushSpanRunMetadata(meta); err != nil {
		return meta, err
	}
	return meta, nil
}

func appendFlushSpanRunTargetLeafSpans(dst []FlushSpanRunTargetLeafSpan, spans []zipper.ReadOnlyLeafSpan) []FlushSpanRunTargetLeafSpan {
	if len(spans) == 0 {
		return dst
	}
	for i := range spans {
		span := spans[i]
		dst = append(dst, FlushSpanRunTargetLeafSpan{
			SpanIndex:        i,
			Ref:              span.Ref,
			LowKey:           cloneFlushSpanRunKey(span.LowKey),
			HighKey:          cloneFlushSpanRunKey(span.HighKey),
			FirstOpKey:       cloneFlushSpanRunKey(span.FirstOpKey),
			LastOpKey:        cloneFlushSpanRunKey(span.LastOpKey),
			PointOpStart:     span.PointOpStart,
			PointOpEnd:       span.PointOpEnd,
			DeleteRangeStart: span.DeleteRangeStart,
			DeleteRangeEnd:   span.DeleteRangeEnd,
			OpCount:          span.OpCount,
			ByteCount:        span.ByteCount,
		})
	}
	return dst
}

func cloneFlushSpanRunKey(key []byte) []byte {
	if key == nil {
		return nil
	}
	return append([]byte(nil), key...)
}
