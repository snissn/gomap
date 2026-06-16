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

	workerRanges := prepared.AppendLeafSpanWorkerRanges(nil, workers)
	if len(workerRanges) == 0 {
		workerRanges = append(workerRanges, ReadOnlyLeafSpanWorkerRange{FirstSpan: 0, SpanCount: spanCount, Ops: len(ops)})
	}
	workers = len(workerRanges)
	outputs := scratch.acquireSpanNativeLeafOutputs(spanCount)
	rangeMetrics := make([]adaptive.Metrics, len(workerRanges))
	rangeRetired := make([][]uint64, len(workerRanges))
	var firstErr error
	var errOnce sync.Once
	runRange := func(_ int, job int) {
		workerRange := workerRanges[job]
		end := workerRange.FirstSpan + workerRange.SpanCount
		var localMetrics adaptive.Metrics
		var localRetired []uint64
		for i := workerRange.FirstSpan; i < end; i++ {
			span := prepared.LeafSpans[i]
			newRef, splits, err := z.writeRecursive(span.Ref, ops[span.PointOpStart:span.PointOpEnd], nil, false, nil, &localMetrics, span.LowKey, span.HighKey, &localRetired, scratch, false, applyRunConfig{maxParallelWorkers: 1})
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
			outputs[i] = spanNativeLeafOutput{ref: newRef, splits: splits}
		}
		rangeMetrics[job] = localMetrics
		rangeRetired[job] = localRetired
	}
	if err := workerPool.Run(workers, len(workerRanges), runRange); err != nil {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
		return ApplyResult{Metrics: metrics}, true, err
	}

	var retired []uint64
	for i := range rangeMetrics {
		mergeMetrics(&metrics, &rangeMetrics[i])
		if len(rangeRetired[i]) > 0 {
			retired = append(retired, rangeRetired[i]...)
		}
	}
	if firstErr != nil {
		metrics.ZipperApplyWallNs = time.Since(applyStart).Nanoseconds()
		return ApplyResult{Metrics: metrics, PendingRetiredPages: retired}, true, firstErr
	}

	rootReduceStart := time.Now()
	newRootID, err := z.reduceSpanNativeRootWithContext(rootID, spanNativeLeafReplacements{spans: prepared.LeafSpans, outputs: outputs}, &metrics, &retired, scratch)
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

type spanNativeLeafOutput struct {
	ref    page.ChildRef
	splits []Split
}

func (s *mergeScratch) acquireSpanNativeLeafOutputs(count int) []spanNativeLeafOutput {
	if count <= 0 {
		return nil
	}
	if s == nil {
		return make([]spanNativeLeafOutput, count)
	}
	if cap(s.spanNativeOutputScratch) < count {
		s.spanNativeOutputScratch = make([]spanNativeLeafOutput, count)
		return s.spanNativeOutputScratch
	}
	s.spanNativeOutputScratch = s.spanNativeOutputScratch[:count]
	clear(s.spanNativeOutputScratch)
	return s.spanNativeOutputScratch
}

type spanNativeLeafReplacements struct {
	spans   []ReadOnlyLeafSpan
	outputs []spanNativeLeafOutput
}

func (r spanNativeLeafReplacements) len() int {
	if len(r.spans) < len(r.outputs) {
		return len(r.spans)
	}
	return len(r.outputs)
}

func (r spanNativeLeafReplacements) slice(start, end int) spanNativeLeafReplacements {
	return spanNativeLeafReplacements{spans: r.spans[start:end], outputs: r.outputs[start:end]}
}

func (z *Zipper) reduceSpanNativeRootWithContext(rootID uint64, replacements spanNativeLeafReplacements, metrics *adaptive.Metrics, retired *[]uint64, scratch *mergeScratch) (uint64, error) {
	if replacements.len() == 0 {
		return 0, page.ErrInvalidPageType
	}
	if spanNativeReplacementsCoverWholeRoot(replacements) {
		var refs []Split
		for i := 0; i < replacements.len(); i++ {
			refs = append(refs, spanNativeReplacementHeadSplit(replacements.spans[i], replacements.outputs[i]))
			refs = append(refs, replacements.outputs[i].splits...)
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

func spanNativeReplacementHeadSplit(span ReadOnlyLeafSpan, output spanNativeLeafOutput) Split {
	spanKey := span.LowKey
	if spanKey == nil {
		spanKey = []byte{}
	}
	return Split{Key: spanKey, Ref: output.ref}
}

func spanNativeReplacementsCoverWholeRoot(replacements spanNativeLeafReplacements) bool {
	if replacements.len() == 0 || replacements.spans[0].LowKey != nil || replacements.spans[replacements.len()-1].HighKey != nil {
		return false
	}
	prevHigh := replacements.spans[0].HighKey
	for i := 1; i < replacements.len(); i++ {
		if !bytes.Equal(replacements.spans[i].LowKey, prevHigh) {
			return false
		}
		prevHigh = replacements.spans[i].HighKey
	}
	return true
}

func (z *Zipper) stitchSpanNativeRecursive(ref page.ChildRef, low, high []byte, replacements spanNativeLeafReplacements, metrics *adaptive.Metrics, retired *[]uint64, scratch *mergeScratch) (page.ChildRef, []Split, bool, error) {
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
		for i := 0; i < replacements.len(); i++ {
			if replacements.spans[i].Ref == ref {
				return replacements.outputs[i].ref, replacements.outputs[i].splits, true, nil
			}
		}
		return ref, nil, false, nil
	case page.PageTypeInternal:
		count := oldNode.Count()
		copyKeys := oldNode.InternalBaseDeltaEnabled()
		writer := spanNativeSplitLevelWriter{z: z, metrics: metrics}
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
				childLow = key
				if copyKeys {
					childLow = append([]byte(nil), key...)
				}
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
				childHigh = nextKey
				if copyKeys {
					childHigh = append([]byte(nil), nextKey...)
				}
			}

			for replacementIdx < replacements.len() && spanNativeReplacementBeforeRange(replacements.spans[replacementIdx], childLow) {
				replacementIdx++
			}
			childReplacementStart := replacementIdx
			childReplacementEnd := childReplacementStart
			for childReplacementEnd < replacements.len() && spanNativeReplacementOverlapsRange(replacements.spans[childReplacementEnd], childLow, childHigh) {
				childReplacementEnd++
			}
			childReplacements := replacements.slice(childReplacementStart, childReplacementEnd)
			replacementIdx = childReplacementEnd
			if childReplacements.len() == 0 {
				if err := writer.append(Split{Key: key, Ref: childRef}); err != nil {
					return page.ChildRef{}, nil, false, err
				}
				continue
			}
			if childReplacements.len() == 1 && childReplacements.spans[0].Ref == childRef {
				changed = true
				if err := writer.append(Split{Key: key, Ref: childReplacements.outputs[0].ref}); err != nil {
					return page.ChildRef{}, nil, false, err
				}
				for _, s := range childReplacements.outputs[0].splits {
					if err := writer.append(s); err != nil {
						return page.ChildRef{}, nil, false, err
					}
				}
				continue
			}

			newRef, childSplits, childChanged, err := z.stitchSpanNativeRecursive(childRef, childLow, childHigh, childReplacements, metrics, retired, scratch)
			if err != nil {
				return page.ChildRef{}, nil, false, err
			}
			if !childChanged {
				return page.ChildRef{}, nil, false, page.ErrInvalidPageType
			}
			changed = true
			if err := writer.append(Split{Key: key, Ref: newRef}); err != nil {
				return page.ChildRef{}, nil, false, err
			}
			for _, s := range childSplits {
				if err := writer.append(s); err != nil {
					return page.ChildRef{}, nil, false, err
				}
			}
		}
		if !changed {
			return ref, nil, false, nil
		}
		if oldFromPager && retired != nil && ref.Kind == page.ChildRefPage && ref.Page != 0 {
			*retired = append(*retired, ref.Page)
		}
		refs, err := writer.finish()
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

type spanNativeSplitLevelWriter struct {
	z               *Zipper
	metrics         *adaptive.Metrics
	nextLevelNodes  []Split
	currentBuilder  *node.Builder
	currentStartKey []byte
}

func (w *spanNativeSplitLevelWriter) append(child Split) error {
	if w.currentBuilder == nil {
		allocHint := uint64(0)
		if child.Ref.Kind == page.ChildRefPage {
			allocHint = child.Ref.Page
		}
		pid, err := w.z.allocator.Alloc(allocHint)
		if err != nil {
			return err
		}
		data, err := w.z.pager.GetForWrite(pid)
		if err != nil {
			return err
		}
		w.currentBuilder = w.z.newBuilderForType(data, page.PageTypeInternal, nil)
		w.currentBuilder.SetPageID(pid)
		w.currentStartKey = child.Key
		w.currentBuilder.SetInternalFenceBounds(w.currentStartKey, nil)
	}

	childKey := child.Key
	if childKey == nil {
		childKey = []byte{}
	}
	childSize := 2 + 8 + len(childKey)
	if child.Ref.Kind == page.ChildRefLeafLog {
		childSize = 2 + page.LogRecordRefSize + len(childKey)
	} else if w.z.indexInternalBaseDelta {
		childSize = 2 + 4 + len(childKey)
	}
	var err error
	if w.z.internalSoftFull(w.currentBuilder, childSize) {
		err = node.ErrNodeFull
	} else {
		err = w.currentBuilder.AddInternalChildRef(childKey, child.Ref)
		if err == nil {
			recordZipperInternalChildRef(w.metrics, child.Ref)
		}
	}
	if err == node.ErrNodeFull {
		w.finishCurrent()

		pid, allocErr := w.z.allocator.Alloc(w.currentBuilder.PageID())
		if allocErr != nil {
			return allocErr
		}
		data, getErr := w.z.pager.GetForWrite(pid)
		if getErr != nil {
			return getErr
		}
		w.currentBuilder = w.z.newBuilderForType(data, page.PageTypeInternal, nil)
		w.currentBuilder.SetPageID(pid)
		w.currentStartKey = child.Key
		w.currentBuilder.SetInternalFenceBounds(w.currentStartKey, nil)

		if addErr := w.currentBuilder.AddInternalChildRef(childKey, child.Ref); addErr != nil {
			return addErr
		}
		recordZipperInternalChildRef(w.metrics, child.Ref)
	} else if err != nil {
		return err
	}
	return nil
}

func (w *spanNativeSplitLevelWriter) finishCurrent() {
	w.currentBuilder.FinishNoNode()
	recordZipperInternalPageWrite(w.metrics)
	w.nextLevelNodes = append(w.nextLevelNodes, Split{Key: w.currentStartKey, Ref: page.PageChildRef(w.currentBuilder.PageID())})
}

func (w *spanNativeSplitLevelWriter) finish() ([]Split, error) {
	if w.currentBuilder != nil {
		w.finishCurrent()
		w.currentBuilder = nil
	}
	return w.nextLevelNodes, nil
}

func spanNativeReplacementBeforeRange(span ReadOnlyLeafSpan, low []byte) bool {
	return span.HighKey != nil && low != nil && bytes.Compare(span.HighKey, low) <= 0
}

func spanNativeReplacementOverlapsRange(span ReadOnlyLeafSpan, low, high []byte) bool {
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
