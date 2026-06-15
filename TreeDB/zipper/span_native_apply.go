package zipper

import (
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/page"
)

func (z *Zipper) applySpanNativeWithPrepared(rootID uint64, ops []batch.Entry, prepared ReadOnlyPrepareResult) (ApplyResult, bool, error) {
	if prepared.DeleteRanges != 0 || prepared.ColdBuild || prepared.Maintenance || !prepared.ExactLeafSpans || len(prepared.LeafSpans) != 1 {
		return ApplyResult{}, false, nil
	}
	span := prepared.LeafSpans[0]
	if span.DeleteRangeStart != span.DeleteRangeEnd || span.PointOpStart < 0 || span.PointOpEnd > len(ops) || span.PointOpEnd <= span.PointOpStart {
		return ApplyResult{}, false, nil
	}

	applyStart := time.Now()
	var metrics adaptive.Metrics
	metrics.ZipperApplyOps = len(ops)
	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)

	var retired []uint64
	newRef, splits, err := z.writeRecursive(span.Ref, ops[span.PointOpStart:span.PointOpEnd], nil, false, nil, &metrics, span.LowKey, span.HighKey, &retired, scratch, false, applyRunConfig{})
	if err != nil {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
		return ApplyResult{Metrics: metrics}, true, err
	}
	if rootID != 0 && !(span.Ref.Kind == page.ChildRefPage && span.Ref.Page == rootID) {
		retired = append(retired, rootID)
	}

	rootReduceStart := time.Now()
	newRootID, err := z.reduceSpanNativeSingleLeafRoot(newRef, splits, &metrics)
	metrics.ZipperRootReduceNs += time.Since(rootReduceStart).Nanoseconds()
	metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
	if err != nil {
		return ApplyResult{Metrics: metrics, PendingRetiredPages: retired}, true, err
	}
	return ApplyResult{
		RootID:              newRootID,
		PendingRetiredPages: retired,
		Metrics:             metrics,
		SpanNativeEligible:  true,
		SpanNativeWorkers:   1,
		SpanNativeUsed:      true,
	}, true, nil
}

func (z *Zipper) reduceSpanNativeSingleLeafRoot(newRef page.ChildRef, splits []Split, metrics *adaptive.Metrics) (uint64, error) {
	if len(splits) != 0 {
		// Multi-split deterministic reducer support is added after the single-leaf
		// job path is proven. Fail closed rather than silently re-entering the
		// recursive reducer for already-prepared span output.
		return 0, page.ErrInvalidPageType
	}
	return z.ensureRootPage([]byte{}, newRef, metrics)
}
