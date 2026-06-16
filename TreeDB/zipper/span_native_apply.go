package zipper

import (
	"bytes"
	"sync"
	"time"

	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/adaptive"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func (z *Zipper) applySpanNativeWithPrepared(rootID uint64, ops []batch.Entry, prepared ReadOnlyPrepareResult, workers int, workerPool *ApplyWorkerPool) (ApplyResult, bool, error) {
	if !validateSpanNativePreparedPlan(ops, prepared) {
		return ApplyResult{}, false, nil
	}

	applyStart := time.Now()
	var metrics adaptive.Metrics
	metrics.ZipperApplyOps = len(ops)
	scratch := z.acquireApplyScratch()
	defer z.releaseApplyScratch(scratch)

	spanCount := len(prepared.LeafSpans)
	if workers <= 0 {
		workers = 1
	}
	if workers > spanCount {
		workers = spanCount
	}

	replacements := make([]spanNativeLeafReplacement, spanCount)
	spanMetrics := make([]adaptive.Metrics, spanCount)
	spanRetired := make([][]uint64, spanCount)
	workerRanges := prepared.AppendLeafSpanWorkerRanges(nil, workers)
	if len(workerRanges) == 0 {
		workerRanges = append(workerRanges, ReadOnlyLeafSpanWorkerRange{FirstSpan: 0, SpanCount: spanCount, Ops: len(ops)})
	}
	workers = len(workerRanges)
	var firstErr error
	var errOnce sync.Once
	runSpan := func(i int) bool {
		span := prepared.LeafSpans[i]
		var childMetrics adaptive.Metrics
		var childRetired []uint64
		newRef, splits, err := z.writeRecursive(span.Ref, ops[span.PointOpStart:span.PointOpEnd], nil, false, nil, &childMetrics, span.LowKey, span.HighKey, &childRetired, scratch, false, applyRunConfig{maxParallelWorkers: 1})
		if err != nil {
			errOnce.Do(func() { firstErr = err })
			return false
		}
		spanKey := span.LowKey
		if spanKey == nil {
			spanKey = []byte{}
		}
		refs := make([]Split, 0, len(splits)+1)
		refs = append(refs, Split{Key: spanKey, Ref: newRef})
		refs = append(refs, splits...)
		replacements[i] = spanNativeLeafReplacement{span: span, refs: refs}
		spanMetrics[i] = childMetrics
		spanRetired[i] = childRetired
		return true
	}
	runRange := func(_ int, job int) {
		workerRange := workerRanges[job]
		end := workerRange.FirstSpan + workerRange.SpanCount
		for i := workerRange.FirstSpan; i < end; i++ {
			if !runSpan(i) {
				return
			}
		}
	}
	if err := workerPool.Run(workers, len(workerRanges), runRange); err != nil {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
		return ApplyResult{Metrics: metrics}, true, err
	}

	var retired []uint64
	for i := range spanMetrics {
		mergeMetrics(&metrics, &spanMetrics[i])
		if len(spanRetired[i]) > 0 {
			retired = append(retired, spanRetired[i]...)
		}
	}
	if firstErr != nil {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
		return ApplyResult{Metrics: metrics, PendingRetiredPages: retired}, true, firstErr
	}

	rootReduceStart := time.Now()
	newRootID, err := z.reduceSpanNativeRootWithContext(rootID, replacements, &metrics, &retired, scratch)
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
		SpanNativeWorkers:   workers,
		SpanNativeUsed:      true,
	}, true, nil
}

func validateSpanNativePreparedPlan(ops []batch.Entry, prepared ReadOnlyPrepareResult) bool {
	if prepared.DeleteRanges != 0 || prepared.ColdBuild || prepared.Maintenance || !prepared.ExactLeafSpans || len(prepared.LeafSpans) == 0 {
		return false
	}
	spans := prepared.LeafSpans
	expectedPointStart := 0
	var prevHigh []byte
	for i := range spans {
		span := spans[i]
		if span.DeleteRangeStart != span.DeleteRangeEnd || span.PointOpStart != expectedPointStart || span.PointOpEnd > len(ops) || span.PointOpEnd <= span.PointOpStart {
			return false
		}
		if span.LowKey != nil && span.HighKey != nil && bytes.Compare(span.LowKey, span.HighKey) >= 0 {
			return false
		}
		if i > 0 {
			if prevHigh == nil || span.LowKey == nil || bytes.Compare(span.LowKey, prevHigh) < 0 {
				return false
			}
		}
		if span.LowKey != nil && bytes.Compare(ops[span.PointOpStart].Key, span.LowKey) < 0 {
			return false
		}
		if span.HighKey != nil && bytes.Compare(ops[span.PointOpEnd-1].Key, span.HighKey) >= 0 {
			return false
		}
		prevHigh = span.HighKey
		expectedPointStart = span.PointOpEnd
	}
	return expectedPointStart == len(ops)
}

type spanNativeLeafReplacement struct {
	span ReadOnlyLeafSpan
	refs []Split
}

func (z *Zipper) reduceSpanNativeRootWithContext(rootID uint64, replacements []spanNativeLeafReplacement, metrics *adaptive.Metrics, retired *[]uint64, scratch *mergeScratch) (uint64, error) {
	if len(replacements) == 0 {
		return 0, page.ErrInvalidPageType
	}
	if spanNativeReplacementsCoverWholeRoot(replacements) {
		var refs []Split
		for i := range replacements {
			refs = append(refs, replacements[i].refs...)
		}
		return z.reduceSpanNativeRoot(refs, metrics)
	}
	ref, splits, changed, err := z.stitchSpanNativeRecursive(page.PageChildRef(rootID), nil, nil, replacements, metrics, retired, scratch)
	if err != nil {
		return 0, err
	}
	if !changed {
		return 0, page.ErrInvalidPageType
	}
	refs := make([]Split, 0, len(splits)+1)
	refs = append(refs, Split{Key: []byte{}, Ref: ref})
	refs = append(refs, splits...)
	return z.reduceSpanNativeRoot(refs, metrics)
}

func spanNativeReplacementsCoverWholeRoot(replacements []spanNativeLeafReplacement) bool {
	if len(replacements) == 0 || replacements[0].span.LowKey != nil || replacements[len(replacements)-1].span.HighKey != nil {
		return false
	}
	prevHigh := replacements[0].span.HighKey
	for i := 1; i < len(replacements); i++ {
		if !bytes.Equal(replacements[i].span.LowKey, prevHigh) {
			return false
		}
		prevHigh = replacements[i].span.HighKey
	}
	return true
}

func (z *Zipper) stitchSpanNativeRecursive(ref page.ChildRef, low, high []byte, replacements []spanNativeLeafReplacement, metrics *adaptive.Metrics, retired *[]uint64, scratch *mergeScratch) (page.ChildRef, []Split, bool, error) {
	oldNode, oldFromPager, leafScratch, leafScratchRef, loadSource, err := z.loadNodeRef(ref, scratch)
	if err != nil {
		return page.ChildRef{}, nil, false, err
	}
	recordZipperNodeLoad(metrics, ref, oldNode, loadSource)
	if leafScratchRef {
		defer releaseLeafPageScratch(scratch, leafScratch)
	}

	switch oldNode.Type() {
	case page.PageTypeLeaf, 0:
		for i := range replacements {
			if replacements[i].span.Ref == ref {
				refs := append([]Split(nil), replacements[i].refs...)
				if len(refs) == 0 {
					return page.ChildRef{}, nil, false, page.ErrInvalidPageType
				}
				if low == nil {
					refs[0].Key = []byte{}
				} else {
					refs[0].Key = low
				}
				return refs[0].Ref, refs[1:], true, nil
			}
		}
		return ref, nil, false, nil
	case page.PageTypeInternal:
		count := oldNode.Count()
		children := make([]Split, 0, count)
		changed := false
		replacementIdx := 0
		for i := uint16(0); i < count; i++ {
			key, childRef, err := oldNode.GetInternalEntryRefView(i)
			if err != nil {
				return page.ChildRef{}, nil, false, err
			}
			if key == nil {
				key = []byte{}
			}
			childLow := low
			if len(key) != 0 {
				childLow = append([]byte(nil), key...)
			}
			childHigh := high
			if i+1 < count {
				nextKey, _, err := oldNode.GetInternalEntryRefView(i + 1)
				if err != nil {
					return page.ChildRef{}, nil, false, err
				}
				if nextKey == nil {
					nextKey = []byte{}
				}
				childHigh = append([]byte(nil), nextKey...)
			}

			for replacementIdx < len(replacements) && spanNativeReplacementBeforeRange(replacements[replacementIdx], childLow) {
				replacementIdx++
			}
			childReplacementStart := replacementIdx
			childReplacementEnd := childReplacementStart
			for childReplacementEnd < len(replacements) && spanNativeReplacementOverlapsRange(replacements[childReplacementEnd], childLow, childHigh) {
				childReplacementEnd++
			}
			childReplacements := replacements[childReplacementStart:childReplacementEnd]
			replacementIdx = childReplacementEnd
			if len(childReplacements) == 0 {
				children = append(children, Split{Key: key, Ref: childRef})
				continue
			}
			if len(childReplacements) == 1 && childReplacements[0].span.Ref == childRef {
				replacement := childReplacements[0]
				if len(replacement.refs) == 0 {
					return page.ChildRef{}, nil, false, page.ErrInvalidPageType
				}
				changed = true
				children = append(children, Split{Key: key, Ref: replacement.refs[0].Ref})
				children = append(children, replacement.refs[1:]...)
				continue
			}

			newRef, childSplits, childChanged, err := z.stitchSpanNativeRecursive(childRef, childLow, childHigh, childReplacements, metrics, retired, scratch)
			if err != nil {
				return page.ChildRef{}, nil, false, err
			}
			if childChanged {
				changed = true
				children = append(children, Split{Key: key, Ref: newRef})
				children = append(children, childSplits...)
			} else {
				children = append(children, Split{Key: key, Ref: childRef})
			}
		}
		if !changed {
			return ref, nil, false, nil
		}
		if oldFromPager && retired != nil && ref.Kind == page.ChildRefPage && ref.Page != 0 {
			*retired = append(*retired, ref.Page)
		}
		refs, err := z.reduceSpanNativeSplitLevel(children, metrics)
		if err != nil {
			return page.ChildRef{}, nil, false, err
		}
		if len(refs) == 0 {
			return page.ChildRef{}, nil, false, page.ErrInvalidPageType
		}
		return refs[0].Ref, refs[1:], true, nil
	default:
		return page.ChildRef{}, nil, false, page.ErrInvalidPageType
	}
}

func spanNativeReplacementBeforeRange(replacement spanNativeLeafReplacement, low []byte) bool {
	span := replacement.span
	return span.HighKey != nil && low != nil && bytes.Compare(span.HighKey, low) <= 0
}

func spanNativeReplacementOverlapsRange(replacement spanNativeLeafReplacement, low, high []byte) bool {
	span := replacement.span
	if span.HighKey != nil && low != nil && bytes.Compare(span.HighKey, low) <= 0 {
		return false
	}
	if high != nil && span.LowKey != nil && bytes.Compare(span.LowKey, high) >= 0 {
		return false
	}
	return true
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
