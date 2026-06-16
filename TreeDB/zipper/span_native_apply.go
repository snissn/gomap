package zipper

import (
	"bytes"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func (z *Zipper) applySpanNativeWithPrepared(rootID uint64, ops []batch.Entry, prepared ReadOnlyPrepareResult) (ApplyResult, bool, error) {
	if prepared.DeleteRanges != 0 || prepared.ColdBuild || prepared.Maintenance || !prepared.ExactLeafSpans || len(prepared.LeafSpans) == 0 {
		return ApplyResult{}, false, nil
	}
	if len(prepared.LeafSpans) > 1 && !spanNativeCoversWholeRoot(prepared.LeafSpans) {
		// Partial multi-leaf replacement needs parent-context stitching. Keep that
		// out of the first opt-in reducer and fall back before preparing output.
		return ApplyResult{}, false, nil
	}

	applyStart := time.Now()
	var metrics adaptive.Metrics
	metrics.ZipperApplyOps = len(ops)
	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)

	var retired []uint64
	spanOutputs := make([]Split, 0, len(prepared.LeafSpans))
	for i := range prepared.LeafSpans {
		span := prepared.LeafSpans[i]
		if span.DeleteRangeStart != span.DeleteRangeEnd || span.PointOpStart < 0 || span.PointOpEnd > len(ops) || span.PointOpEnd <= span.PointOpStart {
			return ApplyResult{}, false, nil
		}
		if i > 0 && span.PointOpStart != prepared.LeafSpans[i-1].PointOpEnd {
			return ApplyResult{}, false, nil
		}
		newRef, splits, err := z.writeRecursive(span.Ref, ops[span.PointOpStart:span.PointOpEnd], nil, false, nil, &metrics, span.LowKey, span.HighKey, &retired, scratch, false, applyRunConfig{})
		if err != nil {
			metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
			return ApplyResult{Metrics: metrics}, true, err
		}
		spanKey := span.LowKey
		if spanKey == nil {
			spanKey = []byte{}
		}
		spanOutputs = append(spanOutputs, Split{Key: spanKey, Ref: newRef})
		spanOutputs = append(spanOutputs, splits...)
	}
	if len(prepared.LeafSpans) > 0 && prepared.LeafSpans[0].PointOpStart != 0 {
		return ApplyResult{}, false, nil
	}
	if len(prepared.LeafSpans) > 0 && prepared.LeafSpans[len(prepared.LeafSpans)-1].PointOpEnd != len(ops) {
		return ApplyResult{}, false, nil
	}
	if rootID != 0 && (len(prepared.LeafSpans) > 1 || !(prepared.LeafSpans[0].Ref.Kind == page.ChildRefPage && prepared.LeafSpans[0].Ref.Page == rootID)) {
		retired = append(retired, rootID)
	}

	rootReduceStart := time.Now()
	newRootID, err := z.reduceSpanNativeRoot(spanOutputs, &metrics)
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

func spanNativeCoversWholeRoot(spans []ReadOnlyLeafSpan) bool {
	return len(spans) > 0 && spans[0].LowKey == nil && spans[len(spans)-1].HighKey == nil
}

func (z *Zipper) reduceSpanNativeRoot(currentLevelNodes []Split, metrics *adaptive.Metrics) (uint64, error) {
	if err := validateSpanNativeReducerRefs(currentLevelNodes); err != nil {
		return 0, err
	}
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

func validateSpanNativeReducerRefs(refs []Split) error {
	if len(refs) == 0 {
		return page.ErrInvalidPageType
	}
	if len(refs[0].Key) != 0 {
		return page.ErrInvalidPageType
	}
	prev := refs[0].Key
	for i := 1; i < len(refs); i++ {
		if len(refs[i].Key) == 0 || bytes.Compare(refs[i].Key, prev) <= 0 {
			return page.ErrInvalidPageType
		}
		prev = refs[i].Key
	}
	return nil
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
