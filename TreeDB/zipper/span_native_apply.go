package zipper

import (
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
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
	if len(splits) == 0 {
		return z.ensureRootPage([]byte{}, newRef, metrics)
	}
	currentLevelNodes := make([]Split, 0, len(splits)+1)
	currentLevelNodes = append(currentLevelNodes, Split{Key: []byte{}, Ref: newRef})
	currentLevelNodes = append(currentLevelNodes, splits...)
	for {
		if len(currentLevelNodes) == 1 {
			return z.ensureRootPage(currentLevelNodes[0].Key, currentLevelNodes[0].Ref, metrics)
		}
		metrics.ZipperRootSplitLevels++
		nextLevelNodes, err := z.reduceSpanNativeSplitLevel(currentLevelNodes, metrics)
		if err != nil {
			return 0, err
		}
		currentLevelNodes = nextLevelNodes
	}
}

func (z *Zipper) reduceSpanNativeSplitLevel(currentLevelNodes []Split, metrics *adaptive.Metrics) ([]Split, error) {
	var nextLevelNodes []Split
	var currentBuilder *node.Builder
	var currentStartKey []byte

	for i, child := range currentLevelNodes {
		if currentBuilder == nil {
			allocHint := uint64(0)
			if child.Ref.Kind == page.ChildRefPage {
				allocHint = child.Ref.Page
			}
			pid, err := z.allocator.Alloc(allocHint)
			if err != nil {
				return nil, err
			}
			data, err := z.pager.GetForWrite(pid)
			if err != nil {
				return nil, err
			}
			currentBuilder = z.newBuilderForType(data, page.PageTypeInternal, nil)
			currentBuilder.SetPageID(pid)
			currentStartKey = child.Key
			currentBuilder.SetInternalFenceBounds(currentStartKey, nil)
		}

		childKey := child.Key
		if childKey == nil {
			childKey = []byte{}
		}
		childSize := 2 + 8 + len(childKey)
		if child.Ref.Kind == page.ChildRefLeafLog {
			childSize = 2 + page.LogRecordRefSize + len(childKey)
		} else if z.indexInternalBaseDelta {
			childSize = 2 + 4 + len(childKey)
		}
		var err error
		if z.internalSoftFull(currentBuilder, childSize) {
			err = node.ErrNodeFull
		} else {
			err = currentBuilder.AddInternalChildRef(childKey, child.Ref)
			if err == nil {
				recordZipperInternalChildRef(metrics, child.Ref)
			}
		}
		if err == node.ErrNodeFull {
			currentBuilder.FinishNoNode()
			recordZipperInternalPageWrite(metrics)
			nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})

			pid, allocErr := z.allocator.Alloc(currentBuilder.PageID())
			if allocErr != nil {
				return nil, allocErr
			}
			data, getErr := z.pager.GetForWrite(pid)
			if getErr != nil {
				return nil, getErr
			}
			currentBuilder = z.newBuilderForType(data, page.PageTypeInternal, nil)
			currentBuilder.SetPageID(pid)
			currentStartKey = child.Key
			currentBuilder.SetInternalFenceBounds(currentStartKey, nil)

			if addErr := currentBuilder.AddInternalChildRef(childKey, child.Ref); addErr != nil {
				return nil, addErr
			}
			recordZipperInternalChildRef(metrics, child.Ref)
		} else if err != nil {
			return nil, err
		}

		if i == len(currentLevelNodes)-1 {
			currentBuilder.FinishNoNode()
			recordZipperInternalPageWrite(metrics)
			nextLevelNodes = append(nextLevelNodes, Split{Key: currentStartKey, Ref: page.PageChildRef(currentBuilder.PageID())})
			currentBuilder = nil
		}
	}
	return nextLevelNodes, nil
}
